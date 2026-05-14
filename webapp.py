"""
webapp.py — Quiniela WFC 2026 — F2 Fase Eliminatoria
FastAPI backend para picks de la fase eliminatoria (brackets).

Requisitos:
    pip install fastapi uvicorn gspread google-auth

Uso:
    python webapp.py --sheet SPREADSHEET_ID [--creds credentials.json] [--port 8000]

Luego abrir: http://localhost:8000
"""

import argparse
import asyncio
import os
import re
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

# Forzar UTF-8 en consola Windows (evita charmap errors con tildes/emojis)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import gspread
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Cookie, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

# âââ Constantes âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

ESTADOS_BLOQUEADOS = {"EN VIVO", "MEDIO TIEMPO", "FINAL", "PRORROGA", "PENALES",
                      "POSPUESTO", "CANCELADO", "SUSPENDIDO"}

ESPN_BASE     = "https://site.api.espn.com/apis/site/v2/sports/soccer"
ESPN_FALLBACK = f"{ESPN_BASE}/all/summary"
# ESPN_SUMMARY se construye dinámicamente desde state["cfg"]["ESPN_LEAGUE"]

STATUS_MAP = {
    "STATUS_FINAL": "FINAL", "STATUS_FULL_TIME": "FINAL",
    "STATUS_IN_PROGRESS": "EN VIVO", "STATUS_HALFTIME": "MEDIO TIEMPO",
    "STATUS_FIRST_HALF": "EN VIVO", "STATUS_SECOND_HALF": "EN VIVO",
    "STATUS_END_PERIOD": "EN VIVO", "STATUS_OVERTIME": "PRORROGA",
    "STATUS_EXTRA_TIME": "PRORROGA", "STATUS_PENALTY": "PENALES",
    "STATUS_SHOOTOUT": "PENALES", "STATUS_SCHEDULED": "PROG",
    "STATUS_POSTPONED": "POSPUESTO", "STATUS_CANCELED": "CANCELADO",
    "STATUS_SUSPENDED": "SUSPENDIDO", "STATUS_DELAYED": "RETRASADO",
}

def parse_ronda(comp: dict, event: dict = None, eq1: str = "", eq2: str = "") -> str:
    """Retorna etiqueta de ronda eliminatoria: R32, R16, QF, SF, 3ER, FINAL."""
    fuentes = []
    if comp.get("notes"):
        fuentes.append(comp["notes"][0].get("headline", ""))
    fuentes.append(comp.get("series", {}).get("summary", ""))
    groups = comp.get("groups", {})
    if isinstance(groups, dict):
        fuentes.append(groups.get("name", ""))
    elif isinstance(groups, list) and groups:
        fuentes.append(groups[0].get("name", ""))
    if event:
        for note in event.get("competitions", [{}])[0].get("notes", []):
            fuentes.append(note.get("headline", ""))
    # También buscar en nombres de equipos (ej: "Round of 32 1 Winner")
    fuentes += [eq1, eq2]

    for raw in fuentes:
        if not raw: continue
        rl = raw.lower()
        # Ganador de Semis → es la FINAL
        if ("ganador semifinal" in rl or "semifinal winner" in rl or
                "winner semifinal" in rl):
            return "FINAL"
        # Perdedor de Semis → 3er lugar
        if ("perdedor semifinal" in rl or "semifinal loser" in rl or
                "loser semifinal" in rl or "third place" in rl or
                "third" in rl or "tercer" in rl or "3rd" in rl or "3er" in rl):
            return "3ER"
        # Ganador de Cuartos → Semifinal
        if ("ganador cuartos" in rl or "quarterfinal winner" in rl or
                "cuartos de final" in rl or "quarter" in rl or "cuartos" in rl):
            return "SF"
        # Ganador de Octavos → Cuartos
        if ("ganador octavos" in rl or "round of 16 winner" in rl or
                "octavos de final" in rl):
            return "QF"
        # Ganador de R32 → R16
        if ("round of 32 winner" in rl or "ganador ronda de 32" in rl or
                "round of 32" in rl):
            return "R16"
        # Keywords directos de ESPN
        if "round of 16" in rl or "octavos" in rl or "dieciseisavos" in rl:
            return "R16"
        if "semi" in rl:
            return "SF"
        if "final" in rl:
            return "FINAL"
        # Grupo Winner / 2nd Place → R32 (primera ronda eliminatoria)
        if ("group" in rl and ("winner" in rl or "2nd place" in rl or "place" in rl)):
            return "R32"
    return ""  # sin dato suficiente; el caller asignará por posición


def _espn_summary_url():
    league = state.get("cfg", {}).get("ESPN_LEAGUE", "fifa.world")
    return f"{ESPN_BASE}/{league}/summary"

def espn_get(url, params):
    try:
        r = requests.get(url, params={**params, "lang": "es", "region": "mx"}, timeout=10)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

def parse_score(data):
    try:
        comp = data["header"]["competitions"][0]
    except (KeyError, IndexError):
        return None
    status     = comp.get("status", {})
    status_type = status.get("type", {})
    estado = STATUS_MAP.get(status_type.get("name", ""), "")

    # Minuto del partido (displayClock = "45:00+", "90+2", etc.)
    clock_raw = status.get("displayClock", "") or status_type.get("shortDetail", "")
    minuto = ""
    if estado in ("EN VIVO", "PRORROGA", "PENALES"):
        minuto = clock_raw.strip().rstrip("'").strip() if clock_raw else ""
    elif estado == "MEDIO TIEMPO":
        minuto = "MT"

    competitors = comp.get("competitors", [])
    if len(competitors) < 2:
        return {"estado": estado, "gol1": "", "gol2": "", "ganador": "", "minuto": minuto}

    # Identificar equipos por posición home/away
    eq1_name = eq2_name = ""
    for ci in competitors:
        n = ci.get("team", {}).get("displayName", "")
        if ci.get("homeAway") == "home": eq1_name = n
        else: eq2_name = n
    if not eq1_name and len(competitors) >= 1:
        eq1_name = competitors[0].get("team", {}).get("displayName", "")
    if not eq2_name and len(competitors) >= 2:
        eq2_name = competitors[1].get("team", {}).get("displayName", "")

    try:
        s0 = int((competitors[0].get("score", "") or "0").strip())
        s1 = int((competitors[1].get("score", "") or "0").strip())
    except (ValueError, AttributeError):
        s0, s1 = 0, 0

    en_juego = estado in {"FINAL", "EN VIVO", "MEDIO TIEMPO", "PRORROGA", "PENALES"}

    # F2: ganador es nombre del equipo (no "1"/"2"/"E") — nunca hay empate final
    ganador = ""
    if en_juego:
        if s0 > s1:
            ganador = eq1_name
        elif s1 > s0:
            ganador = eq2_name
        else:
            # Empate a 90 min → buscar ganador vía campo "winner" (penales/prorroga)
            for ci in competitors:
                if ci.get("winner"):
                    ganador = eq1_name if ci.get("homeAway") == "home" else eq2_name
                    break

    return {"estado": estado, "gol1": str(s0) if en_juego else "",
            "gol2": str(s1) if en_juego else "", "ganador": ganador, "minuto": minuto,
            "eq1": eq1_name, "eq2": eq2_name}

def col_idx(letter):
    r = 0
    for ch in letter.upper().strip():
        r = r * 26 + (ord(ch) - ord("A") + 1)
    return r

def idx_col(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# Versión del build — cambia en cada reinicio del servidor.
# El SW usa este valor en el nombre del caché, forzando invalidación en iOS/Android.
APP_VERSION = str(int(time.time()))

# ─── Directorio persistente (Railway Volume montado en /data) ─────────────────
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ─── Push Notifications (VAPID) ───────────────────────────────────────────────
_VAPID_FILE = DATA_DIR / "vapid_keys.json"
_SUBS_FILE  = DATA_DIR / "push_subs.json"
_push_subs: list = []   # [{endpoint, keys:{p256dh, auth}, _phone, _email}]

def _subs_load():
    """Carga suscripciones push guardadas en disco."""
    global _push_subs
    if _SUBS_FILE.exists():
        try:
            import json as _j
            _push_subs[:] = _j.loads(_SUBS_FILE.read_text(encoding="utf-8"))
            print(f"[push] {len(_push_subs)} suscripción(es) cargada(s) desde disco")
        except Exception as e:
            print(f"[push] Error cargando suscripciones: {e}")

def _subs_save():
    """Persiste suscripciones push en disco."""
    try:
        import json as _j
        _SUBS_FILE.write_text(_j.dumps(_push_subs, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"[push] Error guardando suscripciones: {e}")

def _vapid_generate_keys() -> dict:
    """Genera claves VAPID y las devuelve en el formato correcto para pywebpush."""
    from py_vapid import Vapid
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PublicFormat, PrivateFormat, NoEncryption
    )
    import base64
    v = Vapid()
    v.generate_keys()
    # pywebpush espera la clave privada en base64url-DER (lo que from_string/from_der parsea)
    der_bytes = v._private_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    private_b64 = base64.urlsafe_b64encode(der_bytes).decode().rstrip("=")
    pub_hex = v.public_key.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint).hex()
    return {"private": private_b64, "public": pub_hex}


def _load_vapid() -> dict:
    """Carga o genera claves VAPID para Web Push."""
    import json
    if _VAPID_FILE.exists():
        keys = json.loads(_VAPID_FILE.read_text())
        # Migrar formato antiguo (PEM) a base64url-DER que espera pywebpush
        if keys.get("private", "").startswith("-----"):
            print("[push] Migrando claves VAPID de PEM a DER...")
            try:
                from py_vapid import Vapid
                from cryptography.hazmat.primitives.serialization import (
                    Encoding, PrivateFormat, NoEncryption
                )
                import base64
                v = Vapid.from_pem(keys["private"].encode())
                der_bytes = v._private_key.private_bytes(
                    Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
                )
                keys["private"] = base64.urlsafe_b64encode(der_bytes).decode().rstrip("=")
                _VAPID_FILE.write_text(json.dumps(keys))
                print("[push] Migración VAPID completada")
            except Exception as e:
                print(f"[push] Error migrando VAPID: {e}")
        return keys
    try:
        keys = _vapid_generate_keys()
        _VAPID_FILE.write_text(json.dumps(keys))
        print("[push] Claves VAPID generadas")
        return keys
    except Exception as e:
        print(f"[push] VAPID no disponible: {e}")
        return {}

_vapid_keys: dict = {}

def _send_push_one(sub: dict, payload_str: str) -> bool:
    """Envía push a un suscriptor. Retorna False si la suscripción está muerta."""
    from pywebpush import webpush, WebPushException
    from py_vapid import Vapid
    # Limpiar campos internos antes de pasar a webpush
    clean_sub = {k: v for k, v in sub.items() if not k.startswith("_")}
    endpoint_short = clean_sub.get("endpoint","")[:60]
    print(f"[push] Enviando a {endpoint_short}...")
    # Obtener clave privada en formato base64url-DER (lo que pywebpush/from_string espera)
    priv = _vapid_keys.get("private", "")
    if priv.startswith("-----"):
        # Convertir PEM legado a base64url-DER en caso de claves antiguas
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
        import base64
        _vtmp = Vapid.from_pem(priv.encode())
        _der  = _vtmp._private_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
        priv  = base64.urlsafe_b64encode(_der).decode().rstrip("=")
    try:
        webpush(
            subscription_info=clean_sub,
            data=payload_str,
            vapid_private_key=priv,   # base64url-DER string: lo que from_string espera
            vapid_claims={"sub": "mailto:admin@quiniela.app"},
            ttl=86400,
        )
        print(f"[push] OK -> {endpoint_short}")
        return True
    except WebPushException as ex:
        code = ex.response.status_code if ex.response else 0
        body = ""
        try: body = ex.response.text[:200] if ex.response else ""
        except: pass
        print(f"[push] WebPushException HTTP {code}: {ex} | body: {body}")
        return code not in (404, 410)
    except Exception as ex:
        import traceback
        print(f"[push] Error: {ex}")
        traceback.print_exc()
        return True


def _send_push_all(title: str, body: str, data: dict = None):
    """Envía push notification a todos los suscriptores."""
    if not _vapid_keys or not _push_subs:
        return
    try:
        import json
        payload = json.dumps({"title": title, "body": body, "data": data or {}})
        dead = []
        for sub in list(_push_subs):
            alive = _send_push_one(sub, payload)
            if not alive:
                dead.append(sub)
        for d in dead:
            if d in _push_subs:
                _push_subs.remove(d)
        if dead: _subs_save()
    except Exception as e:
        print(f"[push] Error general: {e}")

state: dict = {}

# Lock global para serializar acceso a Google Sheets — evita conflictos entre
# el hilo del updater y los requests HTTP del webapp
_sheets_lock = threading.Lock()

# Última vez que se actualizó la tabla de posiciones (epoch seconds)
# Solo recalcular si hubo cambios de score O si pasaron más de 5 minutos
_standings_last_update: float = 0.0
_STANDINGS_MIN_INTERVAL = 300  # 5 minutos entre actualizaciones forzadas
# Lock para evitar standings concurrentes cuando corre en hilo propio
_standings_lock = threading.Lock()

# âââ Caché en memoria âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Evita releer Google Sheets en cada request HTTP
_cache: dict = {
    "players":   {},   # "email:x" / "phone:x" -> player dict
    "players_ts": 0.0, # timestamp de la ultima carga completa de JUGADORES
    "games":     None,
    "estados":   None,
    "games_ts":  0.0,
    "prob":      None, # resultado de _compute_probabilities()
    "prob_ts":   0.0,  # timestamp del último cálculo de probabilidades
    "top5_text": "",   # top 5 en texto plano (para notificaciones)
    "top3_text": "",   # top 3 compacto (para push)
}
GAMES_TTL    = 5    # segundos antes de refrescar juegos
PLAYERS_TTL  = 120  # segundos antes de refrescar lista de jugadores
PROB_TTL     = 180  # segundos antes de recalcular probabilidades (3 min)


def _load_players_cache():
    """Carga TODOS los jugadores de una vez, indexados por email y telefono."""
    with _sheets_lock:
        ws   = _sheets_retry(lambda: state["sh"].worksheet("JUGADORES"), base_delay=5)
        rows = ws.get_all_values()
    hi, headers = _jugadores_headers(rows)
    new_cache = {}
    for row in rows[hi + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = _normalize_player({headers[k]: (row[k].strip() if k < len(row) else "")
                               for k in range(len(headers))})
        if d.get("EMAIL"):
            new_cache["email:" + d["EMAIL"].lower()] = d
        phone_val = _normalize_phone(d.get("WHATSAPP","") or d.get("TELEFONO",""))
        if phone_val:
            new_cache["phone:" + phone_val] = d
    _cache["players"]    = new_cache
    _cache["players_ts"] = time.time()
    print(f"[players-cache] {len([k for k in new_cache if k.startswith('email:')])} jugadores cargados")


def _players_cache_ok():
    return bool(_cache["players"]) and (time.time() - _cache["players_ts"]) < PLAYERS_TTL

def _get_games_cache():
    """Retorna (games_list, estados_dict) desde caché o Sheet si expiró."""
    now = time.time()
    if _cache["games"] is None or now - _cache["games_ts"] > GAMES_TTL:
        cfg   = state.get("cfg", {})
        fila  = int(cfg.get("FILA_INICIO_DATOS", 3))
        total = int(cfg.get("TOTAL_JUEGOS_F2", 32))
        with _sheets_lock:
            ws    = state["sh"].worksheet("HORARIOS")
            filas = ws.get(f"A{fila}:L{fila + total - 1}")
        games, estados = [], {}
        for row in filas:
            def c(i, r=row): return r[i].strip() if len(r) > i else ""
            if not c(0): continue
            # datetime_utc: ISO string para que el frontend convierta a hora local
            dt_utc = f"{c(2)}T{c(3)}:00Z" if c(2) and c(3) else ""
            games.append({"jgo": c(0), "ronda": c(1), "fecha": c(2), "hora": c(3),
                          "datetime_utc": dt_utc,
                          "eq1": c(4), "eq2": c(5), "espn_id": c(6), "estado": c(7),
                          "gol1": c(8), "gol2": c(9), "ganador": c(10)})
            estados[c(0)] = c(7)
        _cache["games"]    = games
        _cache["estados"]  = estados
        _cache["games_ts"] = now
    return _cache["games"], _cache["estados"]

def _invalidate_games():
    _cache["games_ts"] = 0

def _invalidate_players():
    _cache["players_ts"] = 0

# ── Propagación de bracket (Modo Prueba) ──────────────────────────────────────

def _parse_bracket_ref(name: str):
    """
    Detecta si un nombre de equipo es un placeholder de bracket y retorna
    {'ronda': str, 'nth': int, 'type': 'winner'|'loser'} o None.
    Soporta inglés ("Round of 32 1 Winner") y español ("Ganador Octavos de Final (1)",
    "Ganador Semifinal 1", "Perdedor Semifinal 2").
    """
    if not name:
        return None
    # Inglés: "Round of 32 1 Winner"
    m = re.match(r'Round of (\d+) (\d+) Winner', name, re.I)
    if m:
        ronda_map = {32: 'R32', 16: 'R16', 8: 'QF', 4: 'SF'}
        ronda = ronda_map.get(int(m.group(1)))
        if ronda:
            return {'ronda': ronda, 'nth': int(m.group(2)), 'type': 'winner'}

    # Español: Ganador/Perdedor + nombre de ronda + número
    is_loser = bool(re.match(r'Perdedor', name, re.I))
    if re.match(r'(Ganador|Perdedor)', name, re.I):
        span_map = [
            (re.compile(r'Dieciseisavos', re.I), 'R32'),
            (re.compile(r'Octavos',       re.I), 'R16'),
            (re.compile(r'Cuartos',       re.I), 'QF'),
            (re.compile(r'Semifinal',     re.I), 'SF'),
        ]
        for pat, ronda in span_map:
            if pat.search(name):
                m_paren = re.search(r'\((\d+)\)', name)
                m_end   = re.search(r'\s(\d+)\s*$', name)
                nth = int(m_paren.group(1)) if m_paren else (int(m_end.group(1)) if m_end else None)
                if nth:
                    return {'ronda': ronda, 'nth': nth, 'type': 'loser' if is_loser else 'winner'}
    return None


def _propagate_bracket(sh=None, ws_h=None) -> list:
    """
    Lee HORARIOS y actualiza EQ1/EQ2 de juegos futuros cuyo nombre sea un
    placeholder de bracket que ya puede resolverse con los GANADOR actuales.
    Retorna lista de strings describiendo los cambios hechos.
    """
    cfg          = state.get("cfg", {})
    fila_inicio  = int(cfg.get("FILA_INICIO_DATOS", 3))
    total_juegos = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    fila_fin     = fila_inicio + total_juegos - 1

    if sh is None:
        sh = state["sh"]
    if ws_h is None:
        with _sheets_lock:
            ws_h = sh.worksheet("HORARIOS")

    with _sheets_lock:
        filas = ws_h.get(f"A{fila_inicio}:K{fila_fin}")  # K=GANADOR idx10, I=GOL1 idx8, J=GOL2 idx9, H=ESTADO idx7

    # Construir mapa de juegos por ronda (ordenados por JGO)
    all_games   = []
    ronda_games = {}   # ronda → [game, ...]

    for i, fila in enumerate(filas):
        def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
        jgo = c(0)
        if not jgo:
            continue
        game = {
            'jgo':     jgo,
            'row':     fila_inicio + i,
            'ronda':   c(1),
            'eq1':     c(4),
            'eq2':     c(5),
            'estado':  c(7),
            'gol1':    c(8),
            'gol2':    c(9),
            'ganador': c(10),
        }
        all_games.append(game)
        ronda_games.setdefault(c(1), []).append(game)

    # Ordenar por JGO dentro de cada ronda
    for k in ronda_games:
        ronda_games[k].sort(key=lambda g: int(g['jgo']) if g['jgo'].isdigit() else 0)

    # ── WC2026: traducir ESPN bracket slot → posición cronológica en R32 ─────
    # ESPN numera los R32 por posición de bracket (no por fecha de juego).
    # Cuando HORARIOS tiene "Round of 32 X Winner", X es el slot ESPN, no el JGO.
    # Esta tabla convierte: ESPN_slot → posición cronológica entre los R32 juegos.
    # Derivada del bracket FIFA WC2026 publicado.
    _WC2026_R32 = {
        1:3, 2:6, 3:1, 4:4, 5:12, 6:11, 7:10, 8:9,
        9:2, 10:5, 11:7, 12:8, 13:15, 14:14, 15:13, 16:16
    }
    _use_wc2026 = (
        "fifa"  in cfg.get("ESPN_LEAGUE", "").lower() or
        "world" in cfg.get("ESPN_LEAGUE", "").lower() or
        cfg.get("BRACKET_SLOT_MAP", "").strip().upper() == "WC2026"
    )

    def resolve(name, depth=0):
        if not name or depth > 8:
            return name
        ref = _parse_bracket_ref(name)
        if not ref:
            return name
        lst = ronda_games.get(ref['ronda'], [])
        nth = ref['nth']
        # Para R32 en WC2026: ESPN slot ≠ nuestro orden cronológico → traducir
        if ref['ronda'] == 'R32' and _use_wc2026 and len(lst) == 16:
            nth = _WC2026_R32.get(nth, nth)
        idx = nth - 1
        if idx < 0 or idx >= len(lst):
            return name
        g = lst[idx]
        if not g['ganador']:
            return name   # sin resultado aún
        if ref['type'] == 'loser':
            eq1 = resolve(g['eq1'], depth + 1)
            eq2 = resolve(g['eq2'], depth + 1)
            if g['ganador'] == eq1:
                return eq2 or name
            if g['ganador'] == eq2:
                return eq1 or name
            return name
        return resolve(g['ganador'], depth + 1)

    batch           = []
    changes         = []
    placeholder_map = {}  # {placeholder_str: real_team_name} para resolver picks de jugadores

    # ── Paso 0: corregir GANADOR cuando no coincide con EQ1/EQ2 ───────────────────
    # Si un partido es FINAL y GANADOR no es EQ1 ni EQ2, recalcular desde GOL1/GOL2
    for game in all_games:
        if game['estado'] != 'FINAL':
            continue
        eq1, eq2, gan = game['eq1'], game['eq2'], game['ganador']
        if not eq1 or not eq2:
            continue
        if gan in (eq1, eq2):
            continue   # ya está correcto
        # GANADOR incorrecto (ej. nombre de club test) → recalcular desde goles
        try:
            g1 = int(game['gol1']) if game['gol1'].isdigit() else -1
            g2 = int(game['gol2']) if game['gol2'].isdigit() else -1
        except Exception:
            continue
        if g1 < 0 or g2 < 0:
            continue
        # En empate: avanza EQ1 (desempate automático para pruebas)
        new_gan = eq1 if g1 >= g2 else eq2
        if new_gan:
            batch.append({"range": f"K{game['row']}", "values": [[new_gan]]})
            changes.append(f"JGO {game['jgo']} GANADOR: {gan!r} → {new_gan!r}")
            game['ganador'] = new_gan

    # ── Paso 1: resolver placeholders existentes (lógica original) ──────────────

    for game in all_games:
        for slot, col_letter in (('eq1', 'E'), ('eq2', 'F')):
            raw      = game[slot]
            resolved = resolve(raw)
            # Solo actualizar si cambió y el resultado ya no es un placeholder
            if resolved and resolved != raw and not _parse_bracket_ref(resolved):
                batch.append({"range": f"{col_letter}{game['row']}", "values": [[resolved]]})
                changes.append(f"JGO {game['jgo']} {slot.upper()}: {raw!r} → {resolved!r}")
                game[slot] = resolved   # actualizar en memoria para el Paso 2
                if raw and _parse_bracket_ref(raw):
                    placeholder_map[raw] = resolved  # para actualizar picks de jugadores

    # ── Paso 2: rellenar EQ1/EQ2 vacíos usando ganadores de ronda anterior ──────
    # Orden secuencial: pares de juegos src alimentan cada juego dst
    # R32[0]+R32[1]→R16[0], R32[2]+R32[3]→R16[1], … mismo para R16→QF, QF→SF
    RONDA_CHAIN = [('R32','R16'), ('R16','QF'), ('QF','SF')]

    def sorted_by_jgo(lst):
        return sorted(lst, key=lambda g: int(g['jgo']) if str(g['jgo']).isdigit() else 0)

    for src_r, dst_r in RONDA_CHAIN:
        src_lst = sorted_by_jgo(ronda_games.get(src_r, []))
        dst_lst = sorted_by_jgo(ronda_games.get(dst_r, []))
        for di, dst in enumerate(dst_lst):
            for offset, (slot, col) in enumerate([('eq1','E'), ('eq2','F')]):
                si = di * 2 + offset
                if si >= len(src_lst):
                    continue
                src = src_lst[si]
                if not src['ganador'] or _parse_bracket_ref(src['ganador']):
                    continue   # ganador aún no es concreto
                if dst[slot] == src['ganador']:
                    continue   # ya está correcto, no hacer nada
                # NO sobreescribir si Paso 1 ya resolvió un equipo concreto distinto
                # (el emparejamiento secuencial de Paso 2 puede no coincidir con el bracket real de ESPN)
                if dst[slot] and not _parse_bracket_ref(dst[slot]):
                    continue   # ya tiene un equipo concreto — resuelto por Paso 1, no pisar
                # NO sobreescribir si el slot tiene una referencia de bracket válida:
                # significa que Paso 1 aún no pudo resolverla (R32 sin ganador aún).
                # Paso 2 NO debe pisar esa referencia con emparejamiento secuencial incorrecto;
                # Paso 1 la resolverá correctamente cuando llegue el ganador real.
                if dst[slot] and _parse_bracket_ref(dst[slot]):
                    continue   # esperar a que Paso 1 resuelva con el bracket correcto
                # Solo aplicar emparejamiento secuencial si el slot está completamente vacío
                # (hoja nueva sin placeholders de bracket definidos)
                if dst[slot]:
                    continue   # cualquier valor existente — no pisar
                old_val = "''"
                batch.append({"range": f"{col}{dst['row']}", "values": [[src['ganador']]]})
                changes.append(f"JGO {dst['jgo']} {slot.upper()}: {old_val} → {src['ganador']!r} [seq-fallback]")
                dst[slot] = src['ganador']

    # SF → FINAL (ganadores) y SF → 3ER (perdedores)
    sf_lst  = sorted_by_jgo(ronda_games.get('SF',    []))
    fin_lst = sorted_by_jgo(ronda_games.get('FINAL', []))
    ter_lst = sorted_by_jgo(ronda_games.get('3ER',   []))

    for si, (slot, col) in enumerate([('eq1','E'), ('eq2','F')]):
        if si >= len(sf_lst):
            continue
        sf_g = sf_lst[si]
        gan  = sf_g['ganador']
        if not gan or _parse_bracket_ref(gan):
            continue
        # FINAL ← ganadores SF
        if fin_lst and fin_lst[0][slot] != gan:
            if fin_lst[0][slot] and _parse_bracket_ref(fin_lst[0][slot]):
                placeholder_map[fin_lst[0][slot]] = gan
            batch.append({"range": f"{col}{fin_lst[0]['row']}", "values": [[gan]]})
            changes.append(f"JGO {fin_lst[0]['jgo']} {slot.upper()} (FINAL): {fin_lst[0][slot]!r} → {gan!r}")
            fin_lst[0][slot] = gan
        # 3ER ← perdedores SF
        if ter_lst and not ter_lst[0][slot]:
            eq1_sf = resolve(sf_g['eq1'])
            eq2_sf = resolve(sf_g['eq2'])
            loser  = eq2_sf if gan == eq1_sf else (eq1_sf if gan == eq2_sf else None)
            if loser and not _parse_bracket_ref(loser) and ter_lst[0][slot] != loser:
                if ter_lst[0][slot] and _parse_bracket_ref(ter_lst[0][slot]):
                    placeholder_map[ter_lst[0][slot]] = loser
                batch.append({"range": f"{col}{ter_lst[0]['row']}", "values": [[loser]]})
                changes.append(f"JGO {ter_lst[0]['jgo']} {slot.upper()} (3ER-loser): {ter_lst[0][slot]!r} → {loser!r}")
                ter_lst[0][slot] = loser

    if batch:
        with _sheets_lock:
            ws_h.batch_update(batch, value_input_option="RAW")
        _invalidate_games()
        print(f"[propagate-bracket] {len(changes)} cambios: {changes}")

    # ── Paso 3: actualizar PICK_EQ1/PICK_EQ2/PICK_GANADOR en tabs de jugadores ──
    # Reemplaza placeholder_map keys ("Round of 32 3 Winner", "Ganador Octavos de Final (1)", …)
    # por el nombre real del equipo en las columnas de picks de cada jugador.
    if placeholder_map:
        try:
            if not _players_cache_ok():
                _load_players_cache()

            seen_tabs: set = set()
            tab_names: list = []
            for k, v in _cache["players"].items():
                if k.startswith("phone:") and v.get("TAB_NOMBRE"):
                    tab = v["TAB_NOMBRE"]
                    if tab not in seen_tabs:
                        seen_tabs.add(tab)
                        tab_names.append(tab)

            fila_fin_p = fila_inicio + total_juegos - 1
            # Columnas de picks: F=PICK_EQ1(5), I=PICK_EQ2(8), J=PICK_GANADOR(9) — 0-indexed
            PICK_COLS = [("F", 5), ("I", 8), ("J", 9)]

            for tab_name in tab_names:
                try:
                    with _sheets_lock:
                        ws_p  = sh.worksheet(tab_name)
                        rows  = ws_p.get(f"A{fila_inicio}:J{fila_fin_p}")
                    pick_batch = []
                    for i, row in enumerate(rows):
                        def _c(idx, r=row): return r[idx].strip() if len(r) > idx else ""
                        row_num = fila_inicio + i
                        jgo_val = _c(0)
                        for col_letter, col_idx in PICK_COLS:
                            val = _c(col_idx)
                            if val in placeholder_map:
                                new_val = placeholder_map[val]
                                pick_batch.append({
                                    "range":  f"{col_letter}{row_num}",
                                    "values": [[new_val]],
                                })
                                changes.append(
                                    f"[{tab_name}] JGO {jgo_val} {col_letter}: {val!r} → {new_val!r}"
                                )
                    if pick_batch:
                        with _sheets_lock:
                            ws_p.batch_update(pick_batch, value_input_option="RAW")
                        print(f"[propagate-bracket] {tab_name}: {len(pick_batch)} picks actualizados")
                except Exception as _e_tab:
                    print(f"[propagate-bracket] error actualizando tab {tab_name}: {_e_tab}")
        except Exception as _e_picks:
            print(f"[propagate-bracket] error en Paso 3 (picks): {_e_picks}")

    return changes


def _sheets_retry(fn, retries=4, base_delay=15):
    """Ejecuta fn() con reintentos exponenciales ante error 429 de Sheets."""
    import gspread as _gs
    for attempt in range(retries):
        try:
            return fn()
        except _gs.exceptions.APIError as e:
            if attempt < retries - 1 and "[429]" in str(e):
                wait = base_delay * (2 ** attempt)
                print(f"[sheets] Quota 429 — esperando {wait}s (intento {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                raise


# âââ Modelos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

class AuthCheck(BaseModel):
    phone: str = ""      # nuevo: teléfono como llave principal
    email: str = ""      # legacy / opcional

class RegisterBody(BaseModel):
    phone: str           # llave principal
    nombre: str
    email: str = ""      # opcional
    telefono: str = ""   # alias (se unifica con phone)

class ArchiveResetBody(BaseModel):
    keyword: str

class PushSubscribeBody(BaseModel):
    subscription: dict
    email: str = ""
    phone: str = ""

class Pick(BaseModel):
    jgo: int
    eq1: str = ""    # equipo 1 elegido por usuario
    gol1: str = ""   # goles equipo 1 (a 90 min)
    gol2: str = ""   # goles equipo 2 (a 90 min)
    eq2: str = ""    # equipo 2 elegido por usuario
    ganador: str = ""  # ganador obligatorio (nombre del equipo)

class SavePicksBody(BaseModel):
    email: str = ""
    phone: str = ""
    picks: list[Pick]

# Rondas de F2
RONDA_BASE       = "R32"   # se bloquea partido a partido

# Pestañas del Sheet que NUNCA se borran — usar esta constante en todo el código
RESERVED_TABS = {"HORARIOS", "JUGADORES", "POSICIONES", "CONFIG", "Ligas", "CHAT"}
RONDAS_SUPERIORES = {"R16", "QF", "SF", "3ER", "FINAL"}  # se bloquean juntas al inicio del último R32

# âââ Helpers de Sheets ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _ensure_base_sheets(sh):
    """Crea las hojas requeridas si el Sheet está vacío / recién creado."""
    existing = {ws.title for ws in sh.worksheets()}

    # CONFIG
    if "CONFIG" not in existing:
        ws = sh.add_worksheet(title="CONFIG", rows=30, cols=3)
        ws.update([
            ["CLAVE", "VALOR", ""],
            ["ESPN_LEAGUE", "fifa.world", ""],
            ["TOTAL_JUEGOS_F2", "32", ""],
            ["FILA_INICIO_DATOS", "3", ""],
            ["INTERVAL_SEGS", "60", ""],
            ["UTC_OFFSET", "-6", ""],
            ["COL_ESPN_ID", "7", ""],
            ["COL_EQ1", "5", ""],
            ["COL_EQ2", "6", ""],
            ["COL_ESTADO", "8", ""],
            ["COL_GOL1", "9", ""],
            ["COL_GOL2", "10", ""],
            ["COL_GANADOR", "11", ""],
            ["COL_ULT_ACT", "12", ""],
        ], "A1")
        print("[init] Hoja CONFIG creada")

    # HORARIOS
    if "HORARIOS" not in existing:
        ws = sh.add_worksheet(title="HORARIOS", rows=50, cols=15)
        ws.update([["NRO", "RONDA", "FECHA", "HORA", "EQ1", "EQ2",
                    "ESPN_ID", "ESTADO", "GOL1", "GOL2", "GANADOR", "ULT_ACT"]], "A1")
        ws.update([["NRO", "RONDA", "FECHA", "HORA", "EQ1", "EQ2",
                    "ESPN_ID", "ESTADO", "GOL1", "GOL2", "GANADOR", "ULT_ACT"]], "A2")
        print("[init] Hoja HORARIOS creada")

    # JUGADORES
    if "JUGADORES" not in existing:
        ws = sh.add_worksheet(title="JUGADORES", rows=100, cols=6)
        ws.update([["EMAIL", "NOMBRE", "TELEFONO", "FECHA_REGISTRO", "TAB_NOMBRE"]], "A1")
        print("[init] Hoja JUGADORES creada")

    # POSICIONES
    if "POSICIONES" not in existing:
        ws = sh.add_worksheet(title="POSICIONES", rows=100, cols=6)
        ws.update([["POS", "NOMBRE", "PTS", "DIF"]], "A1")
        print("[init] Hoja POSICIONES creada")

    # Eliminar Sheet1 / Hoja1 vacía inicial si existe
    for default_name in ("Sheet1", "Hoja 1", "Hoja1"):
        if default_name in existing and len(sh.worksheets()) > 1:
            try:
                sh.del_worksheet(sh.worksheet(default_name))
                print(f"[init] Hoja por defecto '{default_name}' eliminada")
            except Exception:
                pass


def read_config(sh) -> dict:
    ws = sh.worksheet("CONFIG")
    return {r[0].strip(): r[1].strip()
            for r in ws.get_all_values()
            if len(r) >= 2 and r[0].strip()}


def _jugadores_headers(rows: list) -> tuple[int, list]:
    """Encuentra la fila de headers en JUGADORES.
    Entre todas las filas que tienen 'EMAIL', elige la que tenga MAS columnas no-vacías.
    Esto evita detectar filas de título mergeadas (ej. fila 1 con solo 'EMAIL' en col A)."""
    best_idx, best_count = None, 0
    for i, row in enumerate(rows):
        if any(c.strip().upper() == "EMAIL" for c in row):
            non_empty = sum(1 for c in row if c.strip())
            if non_empty > best_count:
                best_count = non_empty
                best_idx = i
    if best_idx is not None:
        return best_idx, [c.strip().upper() for c in rows[best_idx]]
    # Fallback: fila 0
    return 0, [c.strip().upper() for c in (rows[0] if rows else [])]


def _normalize_player(d: dict) -> dict:
    """Normaliza nombres de columna a los que espera el código."""
    # TAB SHEET / TAB_SHEET â TAB_NOMBRE
    for k in ("TAB SHEET", "TAB_SHEET", "TAB NOMBRE"):
        if k in d and "TAB_NOMBRE" not in d:
            d["TAB_NOMBRE"] = d[k]
    # WHATSAPP â TELEFONO
    if "WHATSAPP" in d and "TELEFONO" not in d:
        d["TELEFONO"] = d["WHATSAPP"]
    return d


def _normalize_phone(phone: str) -> str:
    """Elimina espacios, guiones y paréntesis. Deja solo + y dígitos."""
    import re
    return re.sub(r"[\s\-().]+", "", phone.strip())

def find_player(email: str) -> dict | None:
    email = email.strip().lower()
    if not email:
        return None
    if not _players_cache_ok():
        _load_players_cache()
    return _cache["players"].get("email:" + email)

def find_player_by_phone(phone: str) -> dict | None:
    """Busca jugador por número de teléfono (llave principal nueva)."""
    phone = _normalize_phone(phone)
    if not phone:
        return None
    if not _players_cache_ok():
        _load_players_cache()
    return _cache["players"].get("phone:" + phone)

def find_player_any(phone: str = "", email: str = "") -> dict | None:
    """Busca por teléfono primero, luego por email como fallback."""
    if phone:
        p = find_player_by_phone(phone)
        if p:
            return p
    if email:
        return find_player(email)
    return None


def generate_tab_name(nombre: str) -> str:
    parts = nombre.strip().split()
    base = f"{parts[0]} {parts[1][0]}." if len(parts) >= 2 else parts[0]
    existing = {ws.title for ws in state["sh"].worksheets()}
    if base not in existing:
        return base
    i = 2
    while f"{base} {i}" in existing:
        i += 1
    return f"{base} {i}"


def ensure_jugadores_headers():
    """Solo crea headers si la hoja está completamente vacía."""
    ws   = state["sh"].worksheet("JUGADORES")
    rows = ws.get_all_values()
    # Si ya existe alguna fila con "EMAIL", no tocar nada
    if any(any(c.strip().upper() == "EMAIL" for c in row) for row in rows):
        return
    # Hoja vacía: crear headers mínimos
    ws.update([["EMAIL", "NOMBRE", "TELEFONO", "FECHA_REGISTRO", "TAB_NOMBRE"]], "A1:E1")


def create_player_tab(tab_name: str):
    sh = state["sh"]
    reserved = RESERVED_TABS

    # Buscar pestaña de jugador existente para duplicar (la más limpia)
    template = None
    for ws in sh.worksheets():
        if ws.title not in reserved:
            template = ws
            break

    total    = int(state.get("cfg", {}).get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total

    if template:
        new_ws = sh.duplicate_sheet(template.id, new_sheet_name=tab_name)
        new_ws.batch_clear([f"F4:H{last_row}"])  # limpiar picks del template
    else:
        new_ws = sh.add_worksheet(title=tab_name, rows=last_row + 10, cols=16)
        _init_player_tab(new_ws)

    return new_ws


def _init_player_tab(ws):
    """Crea pestaña F2 desde cero con headers y fórmulas (19 columnas A-S).
    Estructura:
      A: JGO  B: RONDA  C: FECHA  D: EQ1_REAL  E: EQ2_REAL
      F: PICK_EQ1  G: PICK_GOL1  H: PICK_GOL2  I: PICK_EQ2  J: PICK_GANADOR
      K: GOL1_REAL  L: GOL2_REAL  M: GAN_REAL  N: ESTADO
      O: PTS_LOGRO  P: PTS_GAN  Q: PTS_GOL1  R: PTS_GOL2  S: PTS_TOTAL
    Puntuación F2: Logro(2) + Ganador(2) + Gol EQ1(1) + Gol EQ2(1) = máx 6 pts
    Usa ';' como separador (locale español de Google Sheets)."""
    headers = [
        "JGO", "RONDA", "FECHA", "EQ1 REAL", "EQ2 REAL",
        "PICK EQ1", "PICK GOL1", "PICK GOL2", "PICK EQ2", "PICK GANADOR",
        "GOL1 REAL", "GOL2 REAL", "GAN REAL", "ESTADO",
        "PTS LOGRO", "PTS GAN", "PTS GOL1", "PTS GOL2", "PTS TOTAL"
    ]
    ws.update([headers], "A1:S1")

    total    = int(state.get("cfg", {}).get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total

    rows = []
    for i in range(1, total + 1):
        r = i + 3
        # Fórmulas de scoring F2:
        # Liberation = al menos 1 pick (eq1, eq2 O ganador) coincide con equipo real
        # El fallback J (PICK_GANADOR) es clave cuando eq1/eq2 se guardaron vacíos (TBD)
        lib = f"OR(F{r}=D{r};F{r}=E{r};I{r}=D{r};I{r}=E{r};J{r}=D{r};J{r}=E{r})"
        rows.append([
            i,
            # B: RONDA (HORARIOS col B = índice 2)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;2;FALSE);"")' ,
            # C: FECHA (HORARIOS col C = índice 3)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;3;FALSE);"")' ,
            # D: EQ1_REAL (HORARIOS col E = índice 5)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;5;FALSE);"")' ,
            # E: EQ2_REAL (HORARIOS col F = índice 6)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;6;FALSE);"")' ,
            # F-J: inputs del usuario (vacíos)
            "", "", "", "", "",
            # K: GOL1_REAL (HORARIOS col I = índice 9)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;9;FALSE);"")' ,
            # L: GOL2_REAL (HORARIOS col J = índice 10)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;10;FALSE);"")' ,
            # M: GAN_REAL  (HORARIOS col K = índice 11 — nombre del equipo)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;11;FALSE);"")' ,
            # N: ESTADO    (HORARIOS col H = índice 8)
            f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;8;FALSE);"")' ,
            # O: PTS_LOGRO — 2pts si el resultado a 90min (1/X/2) predicho coincide con real
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X"));2;0);"")' ,
            # P: PTS_GAN — 2pts si el ganador predicho coincide con el ganador real
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(J{r}=M{r};2;0);"")' ,
            # Q: PTS_GOL1 — 1pt si el gol del equipo 1 predicho coincide con el real
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(G{r}&""=K{r}&"";1;0);"")' ,
            # R: PTS_GOL2 — 1pt si el gol del equipo 2 predicho coincide con el real
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(H{r}&""=L{r}&"";1;0);"")' ,
            # S: PTS_TOTAL — máx 6 pts (Logro 2 + Ganador 2 + Gol1 1 + Gol2 1)
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IFERROR(SUM(O{r}:R{r});0);"")' ,
        ])
    ws.update(rows, f"A4:S{last_row}", value_input_option="USER_ENTERED")


# âââ FastAPI ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

# Rastreo de estados anteriores para detectar cambios y enviar notificaciones
_prev_states: dict = {}   # {espn_id: {estado, gol1, gol2}}
_reminded:    set  = set()  # espn_ids que ya recibieron el recordatorio 10-min
_reminded_5:  set  = set()  # espn_ids que ya recibieron el recordatorio 5-min
_reminded_3:  set  = set()  # espn_ids que ya recibieron el recordatorio 3-min
_reminded_1:  set  = set()  # espn_ids que ya recibieron el recordatorio 1-min
_reminded_15: set  = set()  # espn_ids que ya recibieron el recordatorio 15-min
_live_clocks: dict = {}   # {espn_id: "45'"} — minuto actual de partidos en vivo
_pending_notifs: list = []   # notificaciones de gol/final pendientes hasta tener standings frescos
_day_end_notified:     set  = set()  # fechas "YYYY-MM-DD" que ya recibieron notif de fin de día
_quiniela_end_notified: bool = False  # si ya se envió la notificación de fin de quiniela

def _top_by_day(fecha: str) -> str:
    """Puntos por jugador acumulados en los partidos de una fecha específica."""
    try:
        games, _ = _get_games_cache()
        day_games = [g for g in games if g.get("fecha") == fecha and g.get("estado") == "FINAL"]
        if not day_games:
            return ""
        cfg = state.get("cfg", {})
        total_j = int(cfg.get("TOTAL_JUEGOS_F2", 32))
        ws_j = state["sh"].worksheet("JUGADORES")
        j_rows = ws_j.get_all_values()
        hi, headers = _jugadores_headers(j_rows)
        players = []
        for row in j_rows[hi + 1:]:
            d = _normalize_player({headers[k]: (row[k].strip() if k < len(row) else "")
                                   for k in range(len(headers))})
            if d.get("NOMBRE") and d.get("TAB_NOMBRE"):
                players.append(d)
        scores = []
        for p in players:
            try:
                ws_p = state["sh"].worksheet(p["TAB_NOMBRE"])
                tab  = ws_p.get(f"A4:S{3 + total_j}")
                pts_day = 0
                for g in day_games:
                    row_idx = int(g["jgo"]) - 1  # jgo 1 → índice 0 en tab
                    if row_idx < len(tab) and len(tab[row_idx]) >= 19:
                        try: pts_day += float(tab[row_idx][18])  # col S = PTS_TOTAL F2
                        except: pass
                scores.append((p["NOMBRE"], pts_day))
            except Exception:
                pass
        scores.sort(key=lambda x: -x[1])
        lines = [f"  {i+1}. {n} ({int(pts)}pts)" for i, (n, pts) in enumerate(scores) if pts > 0]
        return "\n".join(lines) if lines else ""
    except Exception as e:
        print(f"[top_by_day] {e}")
        return ""


def _top5_text() -> str:
    """Retorna texto con top 5 de la tabla de posiciones (usa caché en memoria)."""
    cached = _cache.get("top5_text", "")
    if cached:
        return cached
    try:
        ws   = state["sh"].worksheet("POSICIONES")
        rows = ws.get_all_values()
        if len(rows) < 2: return ""
        lines = []
        for row in rows[1:6]:  # top 5
            if row and len(row) >= 2:
                lines.append(f"  {row[0]}. {row[1]}")
        return "\n".join(lines)
    except Exception:
        return ""

def _send_push_players(phones: list, emails: list, title: str, body: str, data: dict = None):
    """Envía push solo a los jugadores con esos teléfonos/emails (para recordatorios personalizados)."""
    if not _vapid_keys or not _push_subs:
        return
    phones_norm = {_normalize_phone(p) for p in phones if p}
    emails_norm = {e.strip().lower() for e in emails if e}
    targets = [s for s in _push_subs
               if (s.get("_phone") and s["_phone"] in phones_norm)
               or (s.get("_email") and s["_email"] in emails_norm)]
    if not targets:
        return
    payload = json.dumps({"title": title, "body": body, "data": data or {}})
    dead = []
    try:
        for sub in targets:
            alive = _send_push_one(sub, payload)
            if not alive:
                dead.append(sub)
    except Exception as e:
        print(f"[push-players] Error: {e}")
    for d in dead:
        if d in _push_subs:
            _push_subs.remove(d)

def _top3_push() -> str:
    """Top 3 para notificaciones push (texto compacto, usa caché en memoria)."""
    cached = _cache.get("top3_text", "")
    if cached:
        return cached
    try:
        sh = state.get("sh")
        if not sh: return ""
        ws  = sh.worksheet("POSICIONES")
        rows = ws.get_all_values()
        data = [r for r in rows[2:] if any(c.strip() for c in r)][:3]
        if not data: return ""
        return " · ".join(
            f"{r[0]}. {r[1]} ({r[2]}pts)" for r in data if len(r) >= 3
        )
    except Exception:
        return ""

def _check_reminders(filas, fila_inicio, cfg):
    """Envía recordatorio 15 min y 10 min antes a jugadores que no apostaron."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _tdt
    now_utc = _dt.now(_tz.utc)
    games, _ = _get_games_cache()

    for i, fila in enumerate(filas):
        def cel(c, f=fila): return f[c-1].strip() if len(f) > c-1 else ""
        espn_id = cel(col_idx("G"))
        estado  = cel(col_idx("H"))
        if not espn_id or (estado != "PROG" and estado != ""):
            continue

        # Buscar la fecha/hora del partido
        game = next((g for g in games if g.get("espn_id","") == espn_id or
                     g.get("jgo","") == cel(col_idx("A"))), None)
        if not game or not game.get("fecha") or not game.get("hora"):
            continue
        try:
            dt_game = _dt.fromisoformat(f"{game['fecha']}T{game['hora']}:00+00:00")
            mins    = (dt_game - now_utc).total_seconds() / 60
        except Exception:
            continue

        # Determinar qué recordatorio aplica según ventana de tiempo
        if 13 <= mins <= 17 and espn_id not in _reminded_15:
            recordatorio_mins = 15
        elif 8 <= mins <= 12 and espn_id not in _reminded:
            recordatorio_mins = 10
        elif 4 <= mins <= 6 and espn_id not in _reminded_5:
            recordatorio_mins = 5
        elif 2 <= mins <= 4 and espn_id not in _reminded_3:
            recordatorio_mins = 3
        elif 0.5 <= mins <= 2 and espn_id not in _reminded_1:
            recordatorio_mins = 1
        else:
            continue

        # Detectar jugadores sin pick para este juego
        jgo = cel(col_idx("A"))
        row_num = int(jgo) + 3 if jgo.isdigit() else None
        if not row_num:
            continue

        sin_pick = []        # nombres
        sin_pick_phones = [] # teléfonos para push personalizado
        sin_pick_emails = [] # emails para push personalizado
        try:
            ws_j = state["sh"].worksheet("JUGADORES")
            j_rows = ws_j.get_all_values()
            hi, headers = _jugadores_headers(j_rows)
            for jrow in j_rows[hi+1:]:
                d = _normalize_player({headers[k]: (jrow[k].strip() if k < len(jrow) else "")
                                       for k in range(len(headers))})
                if not d.get("TAB_NOMBRE"): continue
                try:
                    ws_p = state["sh"].worksheet(d["TAB_NOMBRE"])
                    pick_row = ws_p.row_values(row_num)
                    g1 = pick_row[5].strip() if len(pick_row) > 5 else ""
                    if not g1:
                        sin_pick.append(d.get("NOMBRE","?"))
                        sin_pick_phones.append(d.get("WHATSAPP","") or d.get("TELEFONO",""))
                        sin_pick_emails.append(d.get("EMAIL",""))
                except Exception:
                    pass
        except Exception:
            pass

        eq1    = game.get("eq1","")
        eq2    = game.get("eq2","")
        nivel  = cfg.get("BLOQUEO_NIVEL", "partido")
        min_lbl = f"~{recordatorio_mins} min"

        if sin_pick:
            names = ", ".join(sin_pick)
            _tg_send(
                f"⏰ <b>Faltan {min_lbl}:</b> {eq1} vs {eq2}\n"
                f"Aún sin apostar: {names}\n"
                f"¡Entra y regístrala antes que empiece!"
            )
            # Push personalizado por jugador
            if nivel == "partido":
                for nombre, phone, email in zip(sin_pick, sin_pick_phones, sin_pick_emails):
                    _send_push_players(
                        [phone], [email],
                        f"⏰ ¡Faltan {recordatorio_mins} minutos!",
                        f"Hola {nombre}, {eq1} vs {eq2} — ¡Aún no registraste tu pick!",
                        {"tipo": "recordatorio", "eq1": eq1, "eq2": eq2}
                    )
            else:
                _send_push_players(
                    sin_pick_phones, sin_pick_emails,
                    f"⏰ ¡Faltan {recordatorio_mins} minutos!",
                    f"{eq1} vs {eq2} — Revisa tu pick antes que empiece",
                    {"tipo": "recordatorio", "eq1": eq1, "eq2": eq2}
                )
        else:
            _tg_send(f"⏰ <b>Faltan {min_lbl}:</b> {eq1} vs {eq2}\n✅ Todos apostaron este partido.")

        # Marcar como enviado en el set correspondiente
        if recordatorio_mins == 15:
            _reminded_15.add(espn_id)
        elif recordatorio_mins == 10:
            _reminded.add(espn_id)
        elif recordatorio_mins == 5:
            _reminded_5.add(espn_id)
        elif recordatorio_mins == 3:
            _reminded_3.add(espn_id)
        elif recordatorio_mins == 1:
            _reminded_1.add(espn_id)


def _batch_read_player_tabs(sh, players: list, last_row: int) -> dict:
    """
    Lee los tabs de TODOS los jugadores en llamadas batch (50 rangos por request).
    Con 100 jugadores: 2 requests × ~2s = ~4s en vez de 100 × ~1.5s = ~150s.
    Retorna dict: TAB_NOMBRE → list[list[str]]
    Fallback individual si la API falla.
    """
    if not players:
        return {}

    CHUNK = 50   # límite seguro por request de batchGet
    result_map: dict = {}

    for start in range(0, len(players), CHUNK):
        chunk = players[start:start + CHUNK]
        # Comillas simples alrededor del nombre para tabs con espacios/caracteres especiales
        ranges = [f"'{p['TAB_NOMBRE']}'!A4:S{last_row}" for p in chunk]
        try:
            with _sheets_lock:
                resp = sh.values_batch_get(ranges)
            for i, vr in enumerate(resp.get("valueRanges", [])):
                if i < len(chunk):
                    result_map[chunk[i]["TAB_NOMBRE"]] = vr.get("values", [])
        except Exception as e:
            print(f"[batch-read] chunk {start}: {e} — leyendo individualmente")
            for p in chunk:
                try:
                    with _sheets_lock:
                        ws_p = sh.worksheet(p["TAB_NOMBRE"])
                        result_map[p["TAB_NOMBRE"]] = ws_p.get(f"A4:S{last_row}")
                    time.sleep(0.2)
                except Exception as e2:
                    print(f"[batch-read] {p['TAB_NOMBRE']}: {e2}")
                    result_map[p["TAB_NOMBRE"]] = []

    return result_map


def _update_standings():
    """Calcula la tabla de posiciones leyendo la pestaña de cada jugador
    y escribe los resultados ordenados en la hoja POSICIONES."""
    sh  = state.get("sh")
    cfg = state.get("cfg", {})
    if not sh:
        return

    total_juegos = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row     = 3 + total_juegos   # fila final de datos en la pestaña

    # Leer lista de jugadores
    with _sheets_lock:
        ws_j = sh.worksheet("JUGADORES")
        j_rows = ws_j.get_all_values()

    hi, headers = _jugadores_headers(j_rows)
    players = []
    for row in j_rows[hi + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = _normalize_player({headers[k]: (row[k].strip() if k < len(row) else "")
                                for k in range(len(headers))})
        if d.get("NOMBRE") and d.get("TAB_NOMBRE"):
            players.append(d)

    if not players:
        return

    standings = []
    # ── Leer TODOS los tabs en batch (1-2 requests en vez de N) ─────────────
    t_read = time.time()
    tab_data_map = _batch_read_player_tabs(sh, players, last_row)
    print(f"[standings] {len(players)} tabs leídos en {time.time()-t_read:.1f}s (batch)")

    for p in players:
        try:
            tab_data = tab_data_map.get(p["TAB_NOMBRE"], [])

            pts_total   = 0
            jugados     = 0
            gan_acert   = 0
            g1_acert    = 0
            g2_acert    = 0

            for fila in tab_data:
                def c(i, f=fila): return f[i].strip() if len(f) > i else ""
                estado = c(13)   # col N — ESTADO (F2: cols O-S = indices 14-18)
                if not estado or estado == "PROG" or not c(0):
                    continue
                jugados += 1
                # PTS por columna F2: O=PTS_LOGRO(14), P=PTS_GAN(15), Q=PTS_GOL1(16), R=PTS_GOL2(17), S=PTS_TOTAL(18)
                try: g1_acert  += int(float(c(14))) > 0   # logro acertado
                except: pass
                try: g2_acert  += int(float(c(15))) > 0   # ganador acertado
                except: pass
                try: gan_acert += int(float(c(16))) > 0   # gol1 acertado
                except: pass
                try: pts_total += int(float(c(18))) if c(18) else 0
                except: pass

            standings.append({
                "nombre":   p.get("NOMBRE", ""),
                "email":    p.get("EMAIL", ""),
                "pts":      pts_total,
                "jugados":  jugados,
                "gan":      gan_acert,
                "g1":       g1_acert,
                "g2":       g2_acert,
            })
        except Exception as e:
            print(f"[standings] Error procesando {p.get('TAB_NOMBRE','?')}: {e}")

    # Ordenar: pts desc â ganador acertados desc â g1+g2 desc â nombre asc
    standings.sort(key=lambda x: (-x["pts"], -x["gan"], -(x["g1"]+x["g2"]), x["nombre"]))

    # Escribir headers en fila 2 y datos desde fila 3
    headers_row = [["POS", "NOMBRE", "Ptos", "Diferencia"]]
    lider_pts = standings[0]["pts"] if standings else 0
    rows_out = []
    # Ranking de competencia (1224): empates comparten posición,
    # la siguiente posición salta según cuántos empataron antes.
    pos = 1
    for i, s in enumerate(standings):
        if i > 0:
            prev = standings[i - 1]
            same_rank = (
                s["pts"]            == prev["pts"] and
                s["gan"]            == prev["gan"] and
                (s["g1"] + s["g2"]) == (prev["g1"] + prev["g2"])
            )
            if not same_rank:
                pos = i + 1  # salta tantos lugares como jugadores hubo antes
        diferencia = s["pts"] - lider_pts  # 0 para el líder, negativo para el resto
        rows_out.append([pos, s["nombre"], s["pts"], diferencia])

    # ── Actualizar caché de top5/top3 en memoria (para notificaciones rápidas) ─
    _cache["top5_text"] = "\n".join(
        f"  {r[0]}. {r[1]}" for r in rows_out[:5]
    )
    _cache["top3_text"] = " · ".join(
        f"{r[0]}. {r[1]} ({r[2]}pts)" for r in rows_out[:3] if len(r) >= 3
    )

    ws_pos = sh.worksheet("POSICIONES")
    fila_fin_clear = max(len(standings) + 10, 50)
    with _sheets_lock:
        # 1. Limpiar TODO primero (incluyendo columnas viejas)
        ws_pos.batch_clear([f"A2:Z{fila_fin_clear}"])
        # 2. Escribir headers limpios
        ws_pos.update(headers_row, "A2:D2", value_input_option="RAW")
        # 3. Escribir datos
        if rows_out:
            ws_pos.update(rows_out, f"A3:D{2 + len(rows_out)}", value_input_option="RAW")

    print(f"[standings] {len(standings)} jugador(es) -> POSICIONES actualizada")


def _updater_loop():
    """Corre el loop de actualización de scores en un hilo separado."""
    cESPN    = col_idx("G")
    cESTADO  = col_idx("H")
    cGOL1    = col_idx("I")
    cGOL2    = col_idx("J")
    cGANADOR = col_idx("K")
    cULT     = col_idx("L")

    print("[updater] Iniciando en segundo plano")
    while True:
        try:
            t0 = time.time()
            cfg          = state.get("cfg", {})
            interval     = int(cfg.get("INTERVAL_SEGS", 60))
            fila_inicio  = int(cfg.get("FILA_INICIO_DATOS", 3))
            total_juegos = int(cfg.get("TOTAL_JUEGOS_F2", 32))
            fila_fin     = fila_inicio + total_juegos - 1

            modo_prueba = cfg.get("MODO_PRUEBA", "0").strip() not in ("", "0", "false", "no")
            ws_h  = state["sh"].worksheet("HORARIOS")
            # Leer hasta col M para incluir ESPN_ID_TEST (col 13) en modo prueba
            filas = ws_h.get(f"A{fila_inicio}:M{fila_fin}")

            # Verificar recordatorios 10 min antes
            try:
                _check_reminders(filas, fila_inicio, cfg)
            except Exception as e:
                print(f"[updater-reminder] {e}")

            batch, n = [], 0

            for i, fila in enumerate(filas):
                row = fila_inicio + i
                def cel(c, f=fila): return f[c-1].strip() if len(f) > c-1 else ""
                espn_id_real = cel(cESPN)
                espn_id_test = cel(13) if modo_prueba else ""  # col M = ESPN_ID_TEST
                espn_id      = (espn_id_test or espn_id_real)
                estado_prev  = cel(cESTADO)
                if not espn_id or estado_prev == "FINAL":
                    continue
                data = espn_get(_espn_summary_url(), {"event": espn_id}) or \
                       espn_get(ESPN_FALLBACK,       {"event": espn_id})
                if not data:
                    continue
                sc = parse_score(data)
                if not sc:
                    continue

                # En MODO_PRUEBA: el ganador debe ser EQUIPO 1/2 de HORARIOS, no el
                # equipo del partido de prueba. Usamos los goles para determinarlo;
                # en caso de empate (penales/prórroga) usamos qué equipo del test ganó
                # y lo mapeamos a EQ1 o EQ2 real.
                if modo_prueba and espn_id_test:
                    eq1_real = cel(col_idx("E"))
                    eq2_real = cel(col_idx("F"))
                    if eq1_real or eq2_real:  # ya conocemos los equipos reales
                        g1 = int(sc["gol1"]) if sc["gol1"].isdigit() else -1
                        g2 = int(sc["gol2"]) if sc["gol2"].isdigit() else -1
                        if g1 > g2:
                            sc["ganador"] = eq1_real
                        elif g2 > g1:
                            sc["ganador"] = eq2_real
                        elif sc["ganador"]:
                            # Empate a 90' → ver qué posición (eq1/eq2) del test ganó
                            sc["ganador"] = eq1_real if sc["ganador"] == sc.get("eq1","") else eq2_real
                        # En MODO_PRUEBA no sobreescribir nombres de equipos desde ESPN test
                        sc["eq1"] = eq1_real or sc["eq1"]
                        sc["eq2"] = eq2_real or sc["eq2"]

                # Actualizar minuto SIEMPRE (aunque el score no cambie)
                nuevo_minuto = sc.get("minuto", "")
                if nuevo_minuto:
                    if _live_clocks.get(espn_id) != nuevo_minuto:
                        _live_clocks[espn_id] = nuevo_minuto
                        _invalidate_games()   # frontend ve el minuto actualizado
                elif sc["estado"] == "FINAL":
                    _live_clocks.pop(espn_id, None)

                # Si nada cambió en score/estado/equipos, no hace falta escribir al sheet
                eq1_sheet = cel(col_idx("E"))
                eq2_sheet = cel(col_idx("F"))
                eq1_espn  = sc.get("eq1", "")
                eq2_espn  = sc.get("eq2", "")
                freeze = cfg.get("FREEZE_EQUIPOS", "0").strip() not in ("", "0", "false", "no")
                teams_changed = not freeze and ((eq1_espn and eq1_espn != eq1_sheet) or (eq2_espn and eq2_espn != eq2_sheet))
                if (sc["estado"] == estado_prev and sc["gol1"] == cel(cGOL1) and
                        sc["gol2"] == cel(cGOL2) and sc["ganador"] == cel(cGANADOR) and not teams_changed):
                    time.sleep(0.3); continue

                # Detectar cambios para notificaciones Telegram
                jgo  = cel(col_idx("A"))
                eq1  = cel(col_idx("E"))
                eq2  = cel(col_idx("F"))
                prev = _prev_states.get(espn_id, {})

                if sc["estado"] != "PROG" and estado_prev == "PROG":
                    # Partido inicia
                    #msg_inicio = f"INICIO: {eq1} vs {eq2} — ¡Que empiece el partido!"
                    msg_inicio = f"INICIO: {eq1} vs {eq2}"
                    _tg_send(f"🟡 <b>INICIO:</b> {eq1} vs {eq2}\nJornada")
                    _send_push_all("⚽ Partido iniciado", f"{eq1} vs {eq2}", {"tipo":"inicio","eq1":eq1,"eq2":eq2})
                    try:
                        _wa("POST", "/send", json={"message": f"🟡 INICIO: {eq1} vs {eq2}\n"})
                    except Exception as e:
                        print(f"[WA] Error inicio: {e}")
                elif sc["estado"] in ("EN VIVO","MEDIO TIEMPO","PRORROGA","PENALES"):
                    # prev vacío = primer ciclo tras reinicio; no confundir "" → "0" con un gol real
                    if prev and (sc["gol1"] != prev.get("gol1","") or sc["gol2"] != prev.get("gol2","")):
                        minuto  = _live_clocks.get(espn_id, "")
                        min_txt = f" ({minuto}')" if minuto and minuto != "MT" else (" (MT)" if sc["estado"]=="MEDIO TIEMPO" else "")
                        # Encolar — se enviará con standings frescos después de _update_standings()
                        _pending_notifs.append({
                            "tipo": "gol", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                            "min_txt": min_txt, "minuto": minuto,
                        })
                elif sc["estado"] == "FINAL" and estado_prev != "FINAL":
                    # Si el score cambió justo al llegar al FINAL (gol en tiempo de descuento),
                    # encolar también el gol para que no quede sin anunciar
                    if prev and (sc["gol1"] != prev.get("gol1","") or sc["gol2"] != prev.get("gol2","")):
                        _pending_notifs.append({
                            "tipo": "gol", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                            "min_txt": "", "minuto": "",
                        })
                    gan_eq  = sc["ganador"] if sc["ganador"] else "Sin definir"
                    _pending_notifs.append({
                        "tipo": "final", "eq1": eq1, "eq2": eq2,
                        "gol1": sc["gol1"], "gol2": sc["gol2"],
                        "ganador": sc["ganador"], "gan_eq": gan_eq,
                    })


                _prev_states[espn_id] = {"estado": sc["estado"],
                                          "gol1": sc["gol1"], "gol2": sc["gol2"]}
                if teams_changed:
                    if eq1_espn and eq1_espn != eq1_sheet:
                        batch.append({"range": f"E{row}", "values": [[eq1_espn]]})
                        print(f"[updater] JGO {cel(col_idx('A'))} EQ1: {eq1_sheet!r} → {eq1_espn!r}")
                    if eq2_espn and eq2_espn != eq2_sheet:
                        batch.append({"range": f"F{row}", "values": [[eq2_espn]]})
                        print(f"[updater] JGO {cel(col_idx('A'))} EQ2: {eq2_sheet!r} → {eq2_espn!r}")
                    _invalidate_games()
                batch.append({
                    "range":  f"{idx_col(cESTADO)}{row}:{idx_col(cULT)}{row}",
                    "values": [[sc["estado"], sc["gol1"], sc["gol2"], sc["ganador"],
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                })
                n += 1
                time.sleep(0.3)

            if batch:
                with _sheets_lock:
                    ws_h.batch_update(batch, value_input_option="RAW")
                _invalidate_games()
                print(f"[updater] {n} fila(s) actualizadas")
                # Propagar ganadores al siguiente cruce del bracket
                try:
                    _propagate_bracket(ws_h=ws_h)
                except Exception as _pe:
                    print(f"[updater] propagate-bracket error: {_pe}")

            # ── Flush de notificaciones INMEDIATO (no espera standings) ─────
            # Usa top5/top3 cacheados del ciclo anterior — llegan en segundos
            if _pending_notifs:
                top  = _top5_text()
                top3 = _top3_push()
                for notif in _pending_notifs:
                    try:
                        if notif["tipo"] == "gol":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            g1, g2     = notif["gol1"], notif["gol2"]
                            mt         = notif["min_txt"]
                            minuto_n   = notif["minuto"]
                            _tg_send(
                                f"⚽ <b>MARCADOR:</b> {eq1n} {g1} – {g2} {eq2n}{mt}\n"
                                + (f"\n🏆 <b>Top 5:</b>\n{top}" if top else "")
                            )
                            push_body = f"{eq1n} {g1} – {g2} {eq2n}{mt}"
                            if top3: push_body += f"\n🏆 {top3}"
                            _send_push_all("⚽ Gol!", push_body,
                                {"tipo":"gol","eq1":eq1n,"eq2":eq2n,
                                 "gol1":g1,"gol2":g2,"minuto":minuto_n})
                            try:
                                wa_msg = f"⚽ GOL: {eq1n} {g1} – {g2} {eq2n}{mt}"
                                if top3: wa_msg += f"\n🏆 {top3}"
                                _wa("POST", "/send", json={"message": wa_msg})
                            except Exception as e:
                                print(f"[WA] Error gol: {e}")
                        elif notif["tipo"] == "final":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            g1, g2     = notif["gol1"], notif["gol2"]
                            gan        = notif["ganador"]
                            gan_eq_n   = notif["gan_eq"]
                            gan_txt    = (f"🏅 Gana <b>{eq1n}</b>" if gan=="1"
                                          else f"🏅 Gana <b>{eq2n}</b>" if gan=="2"
                                          else "🤝 <b>Empate</b>")
                            _tg_send(
                                f"🏁 <b>FINAL:</b> {eq1n} {g1} – {g2} {eq2n}\n"
                                f"{gan_txt}\n"
                                + (f"\n🏆 <b>Top 5:</b>\n{top}" if top else "")
                            )
                            push_body = f"{eq1n} {g1} – {g2} {eq2n} · {gan_eq_n}"
                            if top3: push_body += f"\n🏆 {top3}"
                            _send_push_all("🏁 Partido finalizado", push_body,
                                {"tipo":"final","eq1":eq1n,"eq2":eq2n,
                                 "gol1":g1,"gol2":g2,"ganador":gan})
                            try:
                                gan_wa = ("🏅 Gana " + eq1n if gan=="1"
                                          else "🏅 Gana " + eq2n if gan=="2"
                                          else "🤝 Empate")
                                wa_msg = f"🏁 FINAL: {eq1n} {g1} – {g2} {eq2n}\n{gan_wa}"
                                if top3: wa_msg += f"\n\n🏆 Top 3:\n{top3}"
                                _wa("POST", "/send", json={"message": wa_msg})
                            except Exception as e:
                                print(f"[WA] Error final: {e}")
                    except Exception as e:
                        print(f"[updater] notif-flush ERROR: {e}")
                _pending_notifs.clear()

            # ── Recalcular tabla de posiciones en hilo separado ───────────────
            # No bloquea el loop — el próximo ciclo empieza sin esperar
            global _standings_last_update
            scores_changed  = bool(batch)
            time_since_last = time.time() - _standings_last_update
            should_update   = scores_changed or (time_since_last >= _STANDINGS_MIN_INTERVAL)
            if should_update:
                def _standings_async():
                    if not _standings_lock.acquire(blocking=False):
                        return  # Ya hay un standings corriendo, saltarlo
                    try:
                        _update_standings()
                        global _standings_last_update
                        _standings_last_update = time.time()
                    except Exception as e:
                        print(f"[standings-async] ERROR: {e}")
                    finally:
                        _standings_lock.release()
                threading.Thread(target=_standings_async, daemon=True, name="standings").start()

            # ── Fin de día y fin de quiniela ─────────────────────────────────
            try:
                _check_day_end_notif(filas, fila_inicio)
            except Exception as e:
                print(f"[updater] day-end ERROR: {e}")

        except Exception as e:
            print(f"[updater] ERROR: {e}")

        # ── Notificación 2 min antes del sorteo (fuera del try principal) ─
        try:
            _check_sorteo_notif()
        except Exception as e:
            print(f"[updater] sorteo-notif ERROR: {e}")

        time.sleep(max(0, interval - (time.time() - t0)))


def _check_day_end_notif(filas, fila_inicio):
    """Detecta fin de dia y fin de quiniela, envia notificacion si corresponde."""
    global _quiniela_end_notified
    today = datetime.now().strftime("%Y-%m-%d")
    games, _ = _get_games_cache()
    if not games:
        return
    from collections import defaultdict
    by_date = defaultdict(list)
    for g in games:
        if g.get("fecha") and g.get("eq1"):
            by_date[g["fecha"]].append(g)

    # Fin de quiniela
    all_games = [g for gs in by_date.values() for g in gs]
    all_final = all_games and all(g.get("estado") == "FINAL" for g in all_games)
    if all_final and not _quiniela_end_notified:
        _quiniela_end_notified = True
        top5  = _top5_text()
        top3  = _top3_push()
        torneo = state.get("cfg", {}).get("TORNEO", "Quiniela")
        nl = "\n"
        _tg_send(
            "🎉 <b>\u00a1" + torneo + " terminada!</b>\n\n"
            "🏆 <b>Clasificaci\u00f3n final:</b>\n" + (top5 or "(sin datos)")
        )
        _send_push_all(
            "🎉 \u00a1" + torneo + " terminada!",
            ("Resultado final \u00b7 " + top3) if top3 else "Revisa la clasificaci\u00f3n final",
            {"tipo": "quiniela_fin"}
        )
        try:
            wa_msg = ("🎉 \u00a1" + torneo + " terminada!\n\n"
                      "🏆 Clasificaci\u00f3n final:\n"
                      + (top3.replace(" \u00b7 ", "\n") if top3 else top5 or ""))
            _wa("POST", "/send", json={"message": wa_msg})
        except Exception:
            pass
        return

    # Fin de dia
    today_games = by_date.get(today, [])
    if not today_games:
        return
    all_today_final = all(g.get("estado") == "FINAL" for g in today_games)
    if all_today_final and today not in _day_end_notified:
        _day_end_notified.add(today)
        top_day = _top_by_day(today)
        torneo  = state.get("cfg", {}).get("TORNEO", "Quiniela")
        from datetime import datetime as _dt
        fecha_fmt = _dt.strptime(today, "%Y-%m-%d").strftime("%d/%m")
        _tg_send(
            "\u2705 <b>Jornada del " + fecha_fmt + " terminada</b> \u2014 No hay m\u00e1s partidos hoy.\n\n"
            "📊 <b>Puntos del d\u00eda:</b>\n" + (top_day or "(sin datos)")
        )
        top3 = _top3_push()
        _send_push_all(
            "\u2705 Jornada del " + fecha_fmt + " terminada",
            ("Puntos del d\u00eda \u00b7 " + top3) if top3 else "No hay m\u00e1s partidos hoy",
            {"tipo": "dia_fin", "fecha": today}
        )
        try:
            wa_msg = ("\u2705 Jornada del " + fecha_fmt + " terminada \u2014 No hay m\u00e1s partidos hoy.\n\n"
                      "📊 Puntos del d\u00eda:\n" + (top_day or "(sin datos)"))
            _wa("POST", "/send", json={"message": wa_msg})
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    creds_path = os.environ.get("QL_CREDS", "credentials.json")
    sheet_id   = os.environ.get("QL_SHEET", "")

    # Si el archivo no existe, recrearlo desde GOOGLE_CREDENTIALS (Railway/Fly.io)
    if not os.path.exists(creds_path):
        gc_env = os.environ.get("GOOGLE_CREDENTIALS", "")
        if gc_env:
            with open(creds_path, "w", encoding="utf-8") as _f:
                _f.write(gc_env)
            print(f"[webapp] credentials.json recreado desde GOOGLE_CREDENTIALS")
        else:
            raise FileNotFoundError(f"No se encontró {creds_path} ni la variable GOOGLE_CREDENTIALS")

    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    state["sh"]  = gc.open_by_key(sheet_id)
    _ensure_base_sheets(state["sh"])   # crea HORARIOS/JUGADORES/POSICIONES/CONFIG si no existen
    state["cfg"] = read_config(state["sh"])
    ensure_jugadores_headers()
    global _vapid_keys
    _vapid_keys = _load_vapid()
    _subs_load()
    print(f"[webapp] Conectado a: {state['sh'].title}")
    print(f"[webapp] Corriendo en http://localhost:{os.environ.get('QL_PORT', 8000)}")

    # Arrancar updater en hilo de fondo (daemon = se cierra solo al cerrar el webapp)
    t = threading.Thread(target=_updater_loop, daemon=True)
    t.start()

    yield


app = FastAPI(lifespan=lifespan)

# Middleware: todos los endpoints /api/* llevan Cache-Control: no-store
# Esto evita que iOS Safari cachee respuestas de datos en vivo
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as _Req

class NoCacheAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: _Req, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheAPIMiddleware)


# ââ Frontend ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _make_png(size: int) -> bytes:
    """Genera un ícono de balón de fútbol sobre fondo verde."""
    try:
        from PIL import Image, ImageDraw
        import io, math

        img  = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # Fondo verde redondeado
        corner = size // 5
        draw.rounded_rectangle([0, 0, size - 1, size - 1],
                                radius=corner, fill=(6, 78, 59, 255))

        # Balón blanco
        pad  = size // 7
        lw   = max(1, size // 45)
        ball = [pad, pad, size - pad, size - pad]
        draw.ellipse(ball, fill=(240, 240, 240), outline=(25, 25, 25), width=lw)

        cx = cy = size / 2
        br = (size - 2 * pad) / 2      # radio del balón
        pr = br * 0.30                  # radio del pentágono central

        # Pentágono negro central
        def pent(r, offset=0):
            return [(cx + r * math.cos(math.radians(90 + offset + i * 72)),
                     cy - r * math.sin(math.radians(90 + offset + i * 72)))
                    for i in range(5)]

        draw.polygon(pent(pr), fill=(25, 25, 25))

        # 5 líneas desde cada vértice del pentágono hacia el borde del balón
        pts = pent(pr)
        for i in range(5):
            px, py = pts[i]
            angle  = math.atan2(py - cy, px - cx)
            ex     = cx + br * math.cos(angle)
            ey     = cy + br * math.sin(angle)
            draw.line([(px, py), (ex, ey)], fill=(25, 25, 25), width=lw)

        # 5 pentágonos pequeños en la periferia
        outer_r = br * 0.72
        for i in range(5):
            angle = math.radians(90 + i * 72 + 36)
            ox = cx + outer_r * math.cos(angle)
            oy = cy - outer_r * math.sin(angle)
            pts_o = [(ox + pr * 0.55 * math.cos(math.radians(90 + j * 72)),
                      oy - pr * 0.55 * math.sin(math.radians(90 + j * 72)))
                     for j in range(5)]
            draw.polygon(pts_o, fill=(25, 25, 25))

        buf = io.BytesIO()
        img.save(buf, "PNG")
        return buf.getvalue()

    except ImportError:
        # Fallback: cuadrado verde sólido si Pillow no está instalado
        import struct, zlib
        def chunk(tag, data):
            raw = tag + data
            return struct.pack('>I', len(data)) + raw + struct.pack('>I', zlib.crc32(raw) & 0xffffffff)
        ihdr = struct.pack('>IIBBBBB', size, size, 8, 2, 0, 0, 0)
        raw  = b''.join(b'\x00' + bytes([6, 78, 59] * size) for _ in range(size))
        return (b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', ihdr) +
                chunk(b'IDAT', zlib.compress(raw, 9)) + chunk(b'IEND', b''))


@app.get("/", response_class=HTMLResponse)
async def index(ql_session: str = Cookie(default="")):
    html    = Path(__file__).parent / "index.html"
    torneo  = state.get("cfg", {}).get("TORNEO", "Mundial de Fútbol · WFC 2026")
    bloqueo = state.get("cfg", {}).get("BLOQUEO_NIVEL", "partido")
    content = (html.read_text(encoding="utf-8")
               .replace("{{TORNEO}}", torneo)
               .replace("{{SERVER_SESSION}}", ql_session or "")
               .replace("{{BLOQUEO_NIVEL}}", bloqueo))
    return HTMLResponse(content, headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/manifest.json")
async def manifest():
    torneo = state.get("cfg", {}).get("TORNEO", "WFC 2026")
    return JSONResponse({
        "name": f"Quiniela {torneo}",
        "short_name": "Quiniela",
        "description": f"Quiniela de fútbol — {torneo}",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#002868",
        "theme_color": "#002868",
        "orientation": "portrait",
        "categories": ["sports", "games"],
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ]
    })


@app.get("/app.apk")
async def download_apk():
    """Sirve el APK de Android para instalación directa."""
    apk_path = Path(__file__).parent / "quiniela.apk"
    if not apk_path.exists():
        raise HTTPException(404, "APK no disponible aún. Contacta al administrador.")
    return Response(
        content=apk_path.read_bytes(),
        media_type="application/vnd.android.package-archive",
        headers={"Content-Disposition": "attachment; filename=quiniela.apk"}
    )


@app.get("/favicon.ico")
@app.get("/apple-touch-icon.png")
@app.get("/apple-touch-icon-precomposed.png")
async def favicon():
    p = Path(__file__).parent / "icon-192.png"
    if p.exists():
        return Response(content=p.read_bytes(), media_type="image/png")
    return Response(content=_make_png(192), media_type="image/png")


@app.get("/icon-192.png")
async def icon192():
    for p in [DATA_DIR / "icon-192.png", Path(__file__).parent / "icon-192.png"]:
        if p.exists():
            return Response(content=p.read_bytes(), media_type="image/png")
    return Response(content=_make_png(192), media_type="image/png")


@app.get("/icon-512.png")
async def icon512():
    for p in [DATA_DIR / "icon-512.png", Path(__file__).parent / "icon-512.png"]:
        if p.exists():
            return Response(content=p.read_bytes(), media_type="image/png")
    return Response(content=_make_png(512), media_type="image/png")


@app.get("/sw.js")
async def service_worker():
    sw = f"""
const CACHE = 'quiniela-v{APP_VERSION}';

// Instalar: precachear solo la raíz
self.addEventListener('install', e => {{
  e.waitUntil(caches.open(CACHE).then(c => c.add('/')));
  self.skipWaiting();
}});

// Activar: eliminar cachés de versiones anteriores y tomar control
self.addEventListener('activate', e => {{
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
      .then(() => {{
        // Notificar a todos los clientes que hay una versión nueva
        self.clients.matchAll({{ type: 'window' }}).then(clients =>
          clients.forEach(c => c.postMessage({{ type: 'SW_UPDATED', version: '{APP_VERSION}' }}))
        );
      }})
  );
}});

// Push: mostrar notificación
self.addEventListener('push', e => {{
  let data = {{ title: 'Quiniela', body: '', data: {{}} }};
  try {{ data = e.data.json(); }} catch(_) {{ data.body = e.data ? e.data.text() : ''; }}
  e.waitUntil(
    self.registration.showNotification(data.title || 'Quiniela', {{
      body: data.body || '',
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      data: data.data || {{}},
      vibrate: [200, 100, 200]
    }})
  );
}});

// Notificationclick: abrir/enfocar la app
self.addEventListener('notificationclick', e => {{
  e.notification.close();
  e.waitUntil(
    clients.matchAll({{ type: 'window', includeUncontrolled: true }})
      .then(list => {{
        for (const c of list) {{ if ('focus' in c) return c.focus(); }}
        if (clients.openWindow) return clients.openWindow('/');
      }})
  );
}});

// Fetch: network-first, caché solo como fallback offline
self.addEventListener('fetch', e => {{
  if (e.request.method !== 'GET') return;
  // No cachear llamadas a la API — siempre frescos
  if (e.request.url.includes('/api/')) return;
  e.respondWith(
    fetch(e.request)
      .then(res => {{
        // Guardar en caché solo respuestas válidas
        if (res && res.status === 200 && res.type === 'basic') {{
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }}
        return res;
      }})
      .catch(() => caches.match(e.request))
  );
}});
"""
    return Response(content=sw, media_type="application/javascript",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


# ââ Auth ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _set_session_cookie(response: Response, email: str):
    """Cookie HTTP server-side — sobrevive entre Safari y iOS standalone mode."""
    response.set_cookie("ql_session", email.strip().lower(),
                        max_age=365*24*3600, path="/", samesite="lax", httponly=False)


@app.post("/api/auth/check")
async def auth_check(body: AuthCheck, response: Response):
    p = find_player_any(phone=body.phone, email=body.email)
    if not p:
        return {"registered": False}
    # Usar teléfono como sesión si existe, si no email
    session_key = _normalize_phone(body.phone) if body.phone else body.email.strip().lower()
    _set_session_cookie(response, session_key)
    pagado_raw = p.get("PAGADO", "").upper()
    is_paid    = pagado_raw in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X")
    stripe_activo = state.get("cfg", {}).get("STRIPE_ACTIVO", "0") == "1"
    return {"registered": True, "nombre": p.get("NOMBRE", ""),
            "tab": p.get("TAB_NOMBRE", ""),
            "phone": p.get("WHATSAPP","") or p.get("TELEFONO",""),
            "email": p.get("EMAIL",""),
            "pagado": is_paid,
            "stripe_activo": stripe_activo}


@app.post("/api/auth/register")
async def auth_register(body: RegisterBody, response: Response):
    phone_norm = _normalize_phone(body.phone)
    if not phone_norm:
        raise HTTPException(400, "Número de teléfono requerido")
    if find_player_by_phone(phone_norm):
        raise HTTPException(409, "Este número ya está registrado")
    if body.email and find_player(body.email.strip().lower()):
        raise HTTPException(409, "Este correo ya está registrado")
    tab = generate_tab_name(body.nombre)
    create_player_tab(tab)

    ws   = state["sh"].worksheet("JUGADORES")
    rows = ws.get_all_values()
    header_idx, headers = _jugadores_headers(rows)
    next_row = len(rows) + 1

    # Construir fila respetando el orden de columnas del sheet existente
    def col(name, *aliases):
        for n in (name, *aliases):
            if n in headers:
                return headers.index(n)
        return -1

    num_cols = len(headers) if headers else 6
    nueva_fila = [""] * num_cols

    def set_col(val, *names):
        idx = col(*names)
        if 0 <= idx < num_cols:
            nueva_fila[idx] = val

    set_col(str(len(rows) - header_idx), "#")
    set_col(body.email.strip().lower() if body.email else "", "EMAIL")
    set_col(body.nombre.strip(),        "NOMBRE")
    set_col(phone_norm,                 "WHATSAPP", "TELEFONO")
    set_col(datetime.now().strftime("%Y-%m-%d %H:%M"), "FECHA REG.", "FECHA_REGISTRO")
    set_col(tab,                        "TAB SHEET", "TAB_NOMBRE", "TAB_SHEET")

    last_col_letter = chr(ord("A") + num_cols - 1)
    ws.update([nueva_fila], f"A{next_row}:{last_col_letter}{next_row}")
    _cache["players"].clear()

    # ── Auto-agregar al grupo de WhatsApp si ya existe ────────────────────────
    def _wa_add_new():
        try:
            status = requests.get(f"{_WA_BASE}/status", timeout=5).json()
            if status.get("connected") and status.get("groupId") and phone_norm:
                requests.post(f"{_WA_BASE}/add-member",
                              json={"phone": phone_norm}, timeout=15)
        except Exception:
            pass  # WhatsApp opcional — no bloquea el registro
    threading.Thread(target=_wa_add_new, daemon=True).start()

    _set_session_cookie(response, phone_norm)
    return {"success": True, "tab": tab, "nombre": body.nombre.strip(), "phone": phone_norm}


@app.post("/api/auth/logout")
async def auth_logout(response: Response):
    response.delete_cookie("ql_session", path="/")
    return {"ok": True}


# ââ Partidos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/games")
async def get_games():
    games, _ = _get_games_cache()
    has_finals = any(g["estado"] == "FINAL" for g in games)
    # Adjuntar minuto desde cache en memoria (sin llamada extra a Sheets)
    for g in games:
        g["minuto"] = _live_clocks.get(g.get("espn_id", ""), "")
    return {"games": games, "has_finals": has_finals}


# ââ Picks âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/picks")
async def get_picks(email: str = Query(""), phone: str = Query("")):
    p = find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    cfg     = state.get("cfg", {})
    total_j = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    try:
        with _sheets_lock:
            ws    = _sheets_retry(lambda: state["sh"].worksheet(p["TAB_NOMBRE"]), base_delay=5)
            filas = ws.get(f"A4:J{3 + total_j}")
    except Exception as e:
        if "429" in str(e):
            raise HTTPException(503, "Servidor ocupado, intenta en unos segundos")
        if "WorksheetNotFound" in type(e).__name__ or "WorksheetNotFound" in str(e):
            return {"picks": {}}   # jugador sin tab todavía → picks vacíos
        raise
    picks = {}
    for row in filas:
        def c(i): return row[i].strip() if len(row) > i else ""
        if c(0):
            picks[c(0)] = {
                "eq1": c(5), "gol1": c(6), "gol2": c(7),
                "eq2": c(8), "ganador": c(9)
            }
    return {"picks": picks}


@app.post("/api/picks")
async def save_picks(body: SavePicksBody):
    p = find_player_any(phone=body.phone, email=body.email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")

    games, estado_jgo = _get_games_cache()

    # Logica de bloqueo F2:
    # En MODO_PRUEBA: nunca bloquear (permite probar scoring con cualquier estado)
    modo_prueba = state.get("cfg", {}).get("MODO_PRUEBA", "") in ("1", "true", "True")

    # R32: se bloquea partido a partido (igual que F1)
    # Rondas superiores (R16/QF/SF/3ER/FINAL): se bloquean todas juntas
    # cuando el ULTIMO partido de R32 arranca (ya no esta en PROG)
    r32_games = [g for g in games if g.get("ronda") == RONDA_BASE]
    upper_locked = False
    if r32_games and not modo_prueba:
        last_r32 = max(r32_games, key=lambda g: int(g.get("jgo", 0) or 0))
        last_r32_estado = last_r32.get("estado", "")
        upper_locked = bool(last_r32_estado and last_r32_estado != "PROG")

    batch = []
    guardados = bloqueados = 0

    for pick in body.picks:
        game = next((g for g in games if g["jgo"] == str(pick.jgo)), None)
        if not game:
            bloqueados += 1
            continue

        ronda  = game.get("ronda", "")
        estado = game.get("estado", "")

        if not modo_prueba:
            if ronda in RONDAS_SUPERIORES:
                bloq = upper_locked
            else:  # R32 o sin ronda
                bloq = bool(estado and estado != "PROG")
            if bloq:
                bloqueados += 1
                continue

        row = pick.jgo + 3  # JGO 1 -> fila 4
        # F2: 5 campos en cols F-J (pick_eq1, pick_gol1, pick_gol2, pick_eq2, pick_ganador)
        batch.append({
            "range":  f"F{row}:J{row}",
            "values": [[pick.eq1, pick.gol1, pick.gol2, pick.eq2, pick.ganador]]
        })
        guardados += 1

    if batch:
        # Lock para evitar conflicto con el hilo del updater
        acquired = _sheets_lock.acquire(timeout=10)
        if not acquired:
            raise HTTPException(503, "Servidor ocupado, intenta de nuevo")
        try:
            ws_p = state["sh"].worksheet(p["TAB_NOMBRE"])
            ws_p.batch_update(batch, value_input_option="RAW")
        finally:
            _sheets_lock.release()

    return {"guardados": guardados, "bloqueados": bloqueados}


# ââ Posiciones ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/public-config")
async def get_public_config():
    """Configuración pública (sin datos sensibles) para el frontend."""
    cfg = state.get("cfg", {})
    return {
        "telegram_invite": cfg.get("TELEGRAM_INVITE_LINK", ""),
        "torneo": cfg.get("TORNEO", ""),
        "color_scheme": cfg.get("COLOR_SCHEME", "wfc2026"),
        "premios_reglas": cfg.get("PREMIOS_REGLAS", ""),
        "costo_quiniela": cfg.get("COSTO_QUINIELA", "10"),
    }


@app.get("/api/standings")
async def get_standings():
    try:
        ws   = state["sh"].worksheet("POSICIONES")
        rows = ws.get_all_values()
        # Fila 1 = título "TABLA DE POSICIONES" (mergeada), fila 2 = headers de columnas
        # El frontend espera: rows[0]=headers, rows[1:]=datos
        # Saltamos la fila de título y devolvemos desde la fila 2 en adelante
        data = [r for r in rows[1:] if any(c.strip() for c in r)]
        return {"rows": data}
    except Exception:
        return {"rows": []}


# ââ Mis Puntos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/my-points")
async def get_my_points(email: str = Query(""), phone: str = Query("")):
    try:
        p = find_player_any(phone=phone, email=email)
        if not p:
            raise HTTPException(404, "Jugador no encontrado")
        games, _ = _get_games_cache()
        cfg      = state.get("cfg", {})
        total_j  = int(cfg.get("TOTAL_JUEGOS_F2", 32))

        acquired = _sheets_lock.acquire(timeout=15)
        if not acquired:
            raise HTTPException(503, "Servidor ocupado, intenta de nuevo en unos segundos")
        try:
            ws       = state["sh"].worksheet(p["TAB_NOMBRE"])
            tab_data = ws.get(f"A4:R{3+total_j}")
        finally:
            _sheets_lock.release()

        by_day = {}
        total_pts = 0

        for row in tab_data:
            def c(i, r=row): return r[i].strip() if len(r) > i else ""
            jgo    = c(0)
            estado = c(13)   # col N — ESTADO (F2)
            if not jgo or not estado or estado == "PROG":
                continue
            try: pts = int(float(c(18))) if c(18) else 0  # col S = PTS_TOTAL F2
            except: pts = 0

            game  = next((g for g in games if g["jgo"] == jgo), None)
            fecha = game["fecha"] if game else c(2)

            if fecha not in by_day:
                by_day[fecha] = {"fecha": fecha, "pts": 0, "games": []}
            by_day[fecha]["pts"] += pts
            by_day[fecha]["games"].append({
                "jgo": jgo, "ronda": c(1), "eq1_real": c(3), "eq2_real": c(4),
                "pick_eq1": c(5), "pick_gol1": c(6), "pick_gol2": c(7),
                "pick_eq2": c(8), "pick_ganador": c(9),
                "gol1_real": c(10), "gol2_real": c(11), "gan_real": c(12),
                "pts_eq1": c(14), "pts_eq2": c(15), "pts_gan": c(16),
                "pts": pts, "estado": estado,
            })
            total_pts += pts

        days = sorted(by_day.values(), key=lambda x: x["fecha"], reverse=True)
        return {"total": total_pts, "days": days}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[my-points] ERROR: {e}")
        raise HTTPException(500, "Error leyendo puntos, intenta de nuevo")


# ââ Chat ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

class ChatMsg(BaseModel):
    email: str = ""
    phone: str = ""
    msg:   str

_chat_ws  = None   # worksheet cacheada para no buscarla cada vez
_chat_cache: dict = {"msgs": [], "ts": 0.0}
CHAT_TTL = 15      # segundos entre lecturas del sheet

def _ensure_chat_sheet():
    global _chat_ws
    if _chat_ws is not None:
        return _chat_ws
    try:
        _chat_ws = state["sh"].worksheet("CHAT")
    except gspread.exceptions.WorksheetNotFound:
        _chat_ws = state["sh"].add_worksheet("CHAT", rows=1000, cols=4)
        _chat_ws.update([["TIMESTAMP","EMAIL","NOMBRE","MENSAJE"]], "A1:D1")
    return _chat_ws

@app.get("/api/chat")
async def get_chat():
    now = time.time()
    # Usar caché si está fresco — evita rate limit
    if now - _chat_cache["ts"] < CHAT_TTL:
        return {"messages": _chat_cache["msgs"]}
    try:
        with _sheets_lock:
            ws   = _ensure_chat_sheet()
            rows = ws.get_all_values()
        msgs = [{"ts": r[0], "nombre": r[2], "msg": r[3]}
                for r in rows[1:] if len(r) >= 4 and r[3]][-60:]
        _chat_cache["msgs"] = msgs
        _chat_cache["ts"]   = now
        return {"messages": msgs}
    except Exception:
        return {"messages": _chat_cache["msgs"]}

@app.post("/api/chat")
async def send_chat(body: ChatMsg):
    p = find_player_any(phone=body.phone, email=body.email)
    if not p:
        raise HTTPException(403, "No autorizado")
    msg = body.msg.strip()[:300]
    if not msg:
        raise HTTPException(400, "Vacío")
    ts = datetime.now().strftime("%d/%m %H:%M")
    with _sheets_lock:
        ws = _ensure_chat_sheet()
        ident = _normalize_phone(body.phone) if body.phone else body.email
    ws.append_row([ts, ident, p.get("NOMBRE","?"), msg],
                      value_input_option="RAW")
    _chat_cache["ts"] = 0.0   # forzar refresh en próxima lectura
    return {"ok": True}


# ââ Telegram helpers ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _tg_send(text: str):
    """Envía mensaje al grupo de Telegram configurado."""
    cfg     = state.get("cfg", {})
    token   = cfg.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = cfg.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id or cfg.get("TELEGRAM_ENABLED","0") != "1":
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=8
        )
    except Exception as e:
        print(f"[telegram] {e}")

def _tg_send_personal(chat_id_user: str, text: str):
    """Envía DM personal a un usuario."""
    cfg   = state.get("cfg", {})
    token = cfg.get("TELEGRAM_BOT_TOKEN", "")
    if not token or not chat_id_user:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id_user, "text": text, "parse_mode": "HTML"},
            timeout=8
        )
    except Exception as e:
        print(f"[telegram-dm] {e}")


# ââ Apuestas de todos los jugadores por partido ââââââââââââââââââââââââââââââ

_game_picks_cache: dict = {}   # {jgo_str: {data, ts}}
GAME_PICKS_TTL       = 30    # segundos (partidos en vivo)
GAME_PICKS_TTL_FINAL = 300   # segundos (partidos finalizados — no cambian)

@app.get("/api/game-picks")
async def get_game_picks(jgo: int = Query(...)):
    now = time.time()
    key = str(jgo)

    # Usar caché si está fresco
    if key in _game_picks_cache:
        cached = _game_picks_cache[key]
        ttl = GAME_PICKS_TTL_FINAL if cached.get("final") else GAME_PICKS_TTL
        if now - cached["ts"] < ttl:
            return cached["data"]

    games, _ = _get_games_cache()
    game = next((g for g in games if g["jgo"] == str(jgo)), None)
    if not game:
        raise HTTPException(404, "Juego no encontrado")
    if not game["estado"] or game["estado"] == "PROG":
        raise HTTPException(403, "El juego aún no ha iniciado")

    # Leer lista de jugadores con su propia adquisición del lock
    with _sheets_lock:
        ws_j = state["sh"].worksheet("JUGADORES")
        rows = ws_j.get_all_values()
    hi, headers = _jugadores_headers(rows)
    players = []
    for row in rows[hi + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = _normalize_player({headers[i]: (row[i].strip() if i < len(row) else "")
                                for i in range(len(headers))})
        if d.get("NOMBRE") and d.get("TAB_NOMBRE"):
            players.append(d)

    row_num = jgo + 3   # JGO 1 → fila 4

    # Leer tabs de todos los jugadores en paralelo
    import concurrent.futures

    def _read_player_pick(player):
        try:
            with _sheets_lock:
                ws_p     = state["sh"].worksheet(player["TAB_NOMBRE"])
                row_data = ws_p.row_values(row_num)

            pick_eq1  = row_data[5].strip() if len(row_data) > 5 else ""   # F
            pick_gol1 = row_data[6].strip() if len(row_data) > 6 else ""   # G
            pick_gol2 = row_data[7].strip() if len(row_data) > 7 else ""   # H
            pick_eq2  = row_data[8].strip() if len(row_data) > 8 else ""   # I
            pick_gan  = row_data[9].strip() if len(row_data) > 9 else ""   # J

            pts = None
            if game["estado"] and game["estado"] != "PROG":
                real_eq1 = game.get("eq1", "")
                real_eq2 = game.get("eq2", "")
                real_gan = game.get("ganador", "")
                # Pick incompleto: requiere marcador (gol1+gol2) Y ganador para recibir puntos
                if not pick_gol1 or not pick_gol2 or not pick_gan:
                    pts = 0
                else:
                    # Liberation check — fallback to ganador for old picks without eq1/eq2
                    liberation = (pick_eq1 in (real_eq1, real_eq2) or
                                  pick_eq2 in (real_eq1, real_eq2) or
                                  (bool(pick_gan) and pick_gan in (real_eq1, real_eq2)))
                    pts_gol1 = 1 if str(pick_gol1) == str(game.get("gol1","")) else 0
                    pts_gol2 = 1 if str(pick_gol2) == str(game.get("gol2","")) else 0
                    gan_in_match = pick_gan in (real_eq1, real_eq2)
                    pts_gan = 3 if (liberation and gan_in_match and pick_gan == real_gan) else 0
                    pts = (pts_gol1 + pts_gol2 + pts_gan) if liberation else 0

            return {"nombre": player.get("NOMBRE","?"),
                    "pick_eq1": pick_eq1, "pick_gol1": pick_gol1,
                    "pick_gol2": pick_gol2, "pick_eq2": pick_eq2,
                    "pick_gan": pick_gan, "pts": pts}
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_read_player_pick, p) for p in players]
        result  = [fut.result() for fut in concurrent.futures.as_completed(futures)
                   if fut.result() is not None]

    result.sort(key=lambda x: (x["pts"] if x["pts"] is not None else -1), reverse=True)
    is_final = game.get("estado") == "FINAL"
    data = {"game": game, "picks": result}
    _game_picks_cache[key] = {"data": data, "ts": now, "final": is_final}
    return data


# ── Comparar picks de todos los jugadores (partidos iniciados) ─────────────

_compare_cache: dict = {"data": None, "ts": 0.0, "all_final": False}
COMPARE_TTL       = 30   # segundos — partidos en vivo
COMPARE_TTL_FINAL = 300  # segundos — cuando todos son FINAL


def _compute_compare_picks() -> dict:
    """
    Devuelve todos los partidos INICIADOS (estado != '' y != 'PROG') con
    los picks de cada jugador, usando _batch_read_player_tabs() para eficiencia.
    """
    sh = state.get("sh")
    if not sh:
        return {"games": [], "error": "sin conexión"}

    games, _ = _get_games_cache()
    started   = [g for g in games if g.get("estado") and g["estado"] != "PROG"]
    if not started:
        return {"games": []}

    cfg      = state.get("cfg", {})
    total_j  = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total_j

    if not _players_cache_ok():
        _load_players_cache()

    seen_tabs: set = set()
    players: list  = []
    for k, v in _cache["players"].items():
        if k.startswith("phone:") and v.get("TAB_NOMBRE") and v.get("NOMBRE"):
            tab = v["TAB_NOMBRE"]
            if tab not in seen_tabs:
                seen_tabs.add(tab)
                players.append(v)

    if not players:
        return {"games": []}

    t0 = time.time()
    tab_data_map = _batch_read_player_tabs(sh, players, last_row)
    print(f"[compare] {len(players)} tabs leídos en {time.time()-t0:.1f}s")

    # Indexar filas por jgo_str para cada jugador
    player_rows: dict = {}  # TAB_NOMBRE -> {jgo_str: row}
    for p in players:
        rows_by_jgo: dict = {}
        for row in tab_data_map.get(p["TAB_NOMBRE"], []):
            if row and row[0].strip():
                rows_by_jgo[row[0].strip()] = row
        player_rows[p["TAB_NOMBRE"]] = rows_by_jgo

    all_final = True
    result_games = []

    for game in started:
        jgo_str  = game.get("jgo", "")
        real_gan = game.get("ganador", "")
        real_g1  = game.get("gol1",    "")
        real_g2  = game.get("gol2",    "")
        estado   = game.get("estado",  "")

        if estado != "FINAL":
            all_final = False

        g1_known  = real_g1  != ""
        g2_known  = real_g2  != ""
        gan_known = real_gan != ""

        game_picks = []
        for p in players:
            row = player_rows.get(p["TAB_NOMBRE"], {}).get(jgo_str)
            def c(i, r=row): return r[i].strip() if r and len(r) > i else ""
            # F2 player tab: A=JGO B=RONDA C=FECHA D=EQ1_REAL E=EQ2_REAL
            #                F(5)=PICK_EQ1  G(6)=PICK_GOL1  H(7)=PICK_GOL2
            #                I(8)=PICK_EQ2  J(9)=PICK_GANADOR
            pick_eq1  = c(5)
            pick_gol1 = c(6)
            pick_gol2 = c(7)
            pick_eq2  = c(8)
            pick_gan  = c(9)

            real_eq1 = game.get("eq1", "")
            real_eq2 = game.get("eq2", "")
            # Pick incompleto: requiere marcador (gol1+gol2) Y ganador
            if not pick_gol1 or not pick_gol2 or not pick_gan:
                pts = 0
                pts_gol1 = pts_gol2 = pts_gan = pts_logro = 0
            else:
                # PTS_LOGRO: 2pts si resultado 90min (1/X/2) coincide
                def _res(g1, g2):
                    try: return "1" if int(g1) > int(g2) else ("2" if int(g1) < int(g2) else "X")
                    except: return ""
                pts_logro = 2 if (g1_known and g2_known and
                                   _res(pick_gol1, pick_gol2) == _res(real_g1, real_g2)) else 0
                # PTS_GAN: 2pts si ganador predicho coincide con ganador real
                pts_gan = 2 if (gan_known and pick_gan == real_gan) else 0
                # PTS_GOL1: 1pt si gol EQ1 coincide
                pts_gol1 = 1 if (g1_known and pick_gol1 == real_g1) else 0
                # PTS_GOL2: 1pt si gol EQ2 coincide
                pts_gol2 = 1 if (g2_known and pick_gol2 == real_g2) else 0
                pts = pts_logro + pts_gan + pts_gol1 + pts_gol2

            game_picks.append({
                "nombre":    p.get("NOMBRE", "?"),
                "g1":        pick_gol1,
                "g2":        pick_gol2,
                "gan":       pick_gan,
                "pts":       pts,
                "logro_ok":  bool(pts_logro > 0),
                "g1_ok":     bool(pts_gol1 > 0),
                "g2_ok":     bool(pts_gol2 > 0),
                "gan_ok":    bool(pts_gan > 0),
                "g1_set":    bool(pick_gol1),
                "g2_set":    bool(pick_gol2),
                "gan_set":   bool(pick_gan),
            })

        game_picks.sort(key=lambda x: x["pts"], reverse=True)

        result_games.append({
            "jgo":     jgo_str,
            "eq1":     game.get("eq1",     ""),
            "eq2":     game.get("eq2",     ""),
            "estado":  estado,
            "gol1":    real_g1,
            "gol2":    real_g2,
            "ganador": real_gan,
            "picks":   game_picks,
        })

    data = {
        "games":       result_games,
        "computed_at": datetime.now().isoformat(),
    }
    _compare_cache["data"]      = data
    _compare_cache["ts"]        = time.time()
    _compare_cache["all_final"] = all_final
    return data


@app.get("/api/compare-picks")
async def get_compare_picks_all():
    now    = time.time()
    cached = _compare_cache
    ttl    = COMPARE_TTL_FINAL if cached.get("all_final") else COMPARE_TTL
    if cached["data"] is not None and now - cached["ts"] < ttl:
        return cached["data"]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _compute_compare_picks)



# -- Picks proximos (PROG): para la pestana Comparar --------------------------

_upcoming_cache: dict = {"data": None, "ts": 0.0}
UPCOMING_TTL = 60  # segundos


def _compute_upcoming_picks() -> dict:
    """Retorna los partidos PROXIMOS (PROG) con los picks de cada jugador."""
    sh = state.get("sh")
    if not sh:
        return {"games": [], "error": "sin conexion"}

    games, _ = _get_games_cache()
    upcoming  = [g for g in games if not g.get("estado") or g["estado"] == "PROG"]
    if not upcoming:
        return {"games": []}

    cfg      = state.get("cfg", {})
    total_j  = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total_j

    if not _players_cache_ok():
        _load_players_cache()

    seen_tabs: set = set()
    players: list  = []
    for k, v in _cache["players"].items():
        if k.startswith("phone:") and v.get("TAB_NOMBRE") and v.get("NOMBRE"):
            tab = v["TAB_NOMBRE"]
            if tab not in seen_tabs:
                seen_tabs.add(tab)
                players.append(v)

    if not players:
        return {"games": []}

    tab_data_map = _batch_read_player_tabs(sh, players, last_row)

    # Indexar filas por jgo_str para cada jugador
    player_rows: dict = {}
    for p in players:
        rows_by_jgo: dict = {}
        for row in tab_data_map.get(p["TAB_NOMBRE"], []):
            if row and row[0].strip():
                rows_by_jgo[row[0].strip()] = row
        player_rows[p["TAB_NOMBRE"]] = rows_by_jgo

    result_games = []
    for game in upcoming:
        jgo_str = game.get("jgo", "")
        game_picks = []
        for p in players:
            row = player_rows.get(p["TAB_NOMBRE"], {}).get(jgo_str)
            def c(i, r=row): return r[i].strip() if r and len(r) > i else ""
            pick_gol1 = c(6)   # G = PICK_GOL1
            pick_gol2 = c(7)   # H = PICK_GOL2
            pick_gan  = c(9)   # J = PICK_GANADOR (nombre equipo en F2)
            game_picks.append({
                "nombre": p.get("NOMBRE", "?"),
                "gol1":   pick_gol1,
                "gol2":   pick_gol2,
                "gan":    pick_gan,
            })

        # Ordenar: primero quienes tienen pick, luego por nombre
        game_picks.sort(key=lambda x: (not bool(x["gan"]), x["nombre"]))

        result_games.append({
            "jgo":          jgo_str,
            "ronda":        game.get("ronda",  ""),
            "eq1":          game.get("eq1",    ""),
            "eq2":          game.get("eq2",    ""),
            "fecha":        game.get("fecha",  ""),
            "hora":         game.get("hora",   ""),
            "datetime_utc": game.get("datetime_utc", ""),
            "picks":        game_picks,
        })

    # Ordenar por fecha/hora
    result_games.sort(key=lambda g: g.get("datetime_utc") or g.get("fecha") or "")

    data = {"games": result_games, "computed_at": datetime.now().isoformat()}
    _upcoming_cache["data"] = data
    _upcoming_cache["ts"]   = time.time()
    return data


@app.get("/api/upcoming-picks")
async def get_upcoming_picks():
    now = time.time()
    if _upcoming_cache["data"] is not None and now - _upcoming_cache["ts"] < UPCOMING_TTL:
        return _upcoming_cache["data"]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _compute_upcoming_picks)


# âââ Admin ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

ADMIN_CONFIG_FIELDS = [
    ("TORNEO",               "Nombre del torneo"),
    ("ESPN_LEAGUE",          "Liga ESPN (ej: fifa.world, spa.1, eng.1)"),
    ("FECHA_INICIO_F2",      "Fecha inicio F2 eliminatoria (YYYY-MM-DD)"),
    ("FECHA_FIN_F2",         "Fecha fin F2 eliminatoria (YYYY-MM-DD)"),
    ("JORNADA",              "Número de jornada inicial (para ligas sin grupos, ej: 32)"),
    ("BLOQUEO_NIVEL",        "Nivel de bloqueo de picks"),
    ("INTERVAL_SEGS",        "Intervalo updater (segundos)"),
    ("TELEGRAM_BOT_TOKEN",   "Token del bot de Telegram"),
    ("TELEGRAM_CHAT_ID",     "ID del grupo de Telegram"),
    ("TELEGRAM_ENABLED",     "Notificaciones Telegram activas (1=sí, 0=no)"),
    ("TELEGRAM_INVITE_LINK", "Enlace de invitación al grupo (https://t.me/+...)"),
    ("RESET_KEY",           "Clave para resetear el torneo"),
    ("DIA_INICIO_JORNADA",  "Día inicio de jornada (0=Lun, 1=Mar, 2=Mié, 3=Jue, 4=Vie, 5=Sáb, 6=Dom)"),
    ("COLOR_SCHEME",        "Esquema de colores de la app"),
    ("PREMIOS_REGLAS",      "Premios y reglas (texto libre, saltos de línea permitidos)"),
    ("SORTEO_FECHA",        "Fecha del sorteo en vivo — activa la pestaña sorteo"),
    ("SORTEO_HORA",         "Hora del sorteo en vivo (en UTC — España verano = UTC+2, réstale 2h)"),
    ("SORTEO_ANIM",         "Animación del sorteo"),
    ("STRIPE_ACTIVO",       "Pago con tarjeta activo (1=sí, 0=no)"),
    ("FREEZE_EQUIPOS",      "Congelar nombres de equipos (1=no sobreescribir desde ESPN, 0=actualizar)"),
    ("MODO_PRUEBA",         "Modo Prueba (1=usar ESPN_ID_TEST para scores, 0=producción)"),
    ("PTS_LOGRO",           "Puntos por resultado 90min correcto (1/X/2)"),
    ("PTS_GAN",             "Puntos por ganador correcto (extra/penales)"),
    ("PTS_GOL1",            "Puntos por gol equipo 1 correcto"),
    ("PTS_GOL2",            "Puntos por gol equipo 2 correcto"),
    # TOTAL_JUEGOS_F2 se actualiza automáticamente al recargar partidos
]

BLOQUEO_OPCIONES = [
    ("partido",  "Por partido — bloquea solo el partido que ya inició"),
    ("jornada",  "Por jornada — cuando inicia el 1er partido de la jornada, bloquea toda la jornada"),
    ("evento",   "Por evento — cuando inicia cualquier partido, bloquea todo"),
]


def _admin_check(ql_admin: str = "") -> bool:
    return ql_admin == "ql_admin_ok"


def _torneo_activo() -> dict:
    """Retorna {activo: bool, razon: str} indicando si el torneo está en curso.
    Condiciones para bloqueo:
      1. Fecha actual dentro del rango FECHA_INICIO_F2 – FECHA_FIN_F2
      2. Al menos un partido ya comenzó (estado != PROG y != vacío)
      3. Al menos 2 jugadores registrados
    """
    from datetime import date as _date
    try:
        cfg = state.get("cfg", {})
        # Condición 1: rango de fechas
        hoy = _date.today()
        try:
            fecha_ini = _date.fromisoformat(cfg.get("FECHA_INICIO_F2", "2026-07-01"))
            fecha_fin = _date.fromisoformat(cfg.get("FECHA_FIN_F2", "2026-07-19"))
        except ValueError:
            return {"activo": False, "razon": ""}
        if not (fecha_ini <= hoy <= fecha_fin):
            return {"activo": False, "razon": ""}

        # Condición 2: al menos un partido iniciado
        games, _ = _get_games_cache()
        partido_iniciado = any(
            g.get("estado") and g.get("estado") not in ("PROG", "")
            for g in games
        )
        if not partido_iniciado:
            return {"activo": False, "razon": ""}

        # Condición 3: al menos 2 jugadores registrados
        with _sheets_lock:
            ws_j = state["sh"].worksheet("JUGADORES")
            j_rows = ws_j.get_all_values()
        hi, _ = _jugadores_headers(j_rows)
        jugadores = [r for r in j_rows[hi + 1:] if any(c.strip() for c in r)]
        if len(jugadores) < 2:
            return {"activo": False, "razon": ""}

        return {
            "activo": True,
            "razon": f"Torneo en curso ({fecha_ini} → {fecha_fin}) · "
                     f"{len(jugadores)} jugadores · primer partido iniciado"
        }
    except Exception as e:
        return {"activo": False, "razon": str(e)}


class AdminLogin(BaseModel):
    user: str
    password: str

class AdminConfigSave(BaseModel):
    fields: dict

class SimResultBody(BaseModel):
    jgo:     str
    gol1:    str
    gol2:    str
    ganador: str          # "eq1" o "eq2"
    estado:  str = "FINAL"


@app.get("/api/admin/torneo-status")
async def admin_torneo_status(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    return _torneo_activo()


@app.post("/api/admin/login")
async def admin_login(body: AdminLogin, response: Response):
    cfg = state.get("cfg", {})
    ok_user = cfg.get("ADMIN_USER", "admin")
    ok_pass = cfg.get("ADMIN_PASS", "quiniela2026")
    if body.user.strip() == ok_user and body.password.strip() == ok_pass:
        response.set_cookie("ql_admin", "ql_admin_ok",
                            max_age=8*3600, path="/", samesite="lax", httponly=False)
        return {"ok": True}
    raise HTTPException(401, "Credenciales incorrectas")


@app.post("/api/admin/logout")
async def admin_logout(response: Response):
    response.delete_cookie("ql_admin", path="/")
    return {"ok": True}


@app.get("/api/admin/ligas")
async def admin_get_ligas(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    try:
        ws   = state["sh"].worksheet("Ligas")
        rows = ws.get_all_values()
        ligas = []
        for r in rows[1:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                ligas.append({
                    "nombre":   r[0].strip(),
                    "codigo":   r[1].strip(),
                    "espn_id":  r[2].strip() if len(r) > 2 else "",
                })
        return {"ligas": ligas}
    except Exception:
        return {"ligas": []}


@app.get("/api/admin/config")
async def admin_get_config(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    cfg = state.get("cfg", {})
    return {"fields":   {k: cfg.get(k, "") for k, _ in ADMIN_CONFIG_FIELDS},
            "labels":   {k: label for k, label in ADMIN_CONFIG_FIELDS},
            "bloqueo_opciones": BLOQUEO_OPCIONES}


@app.post("/api/admin/config")
async def admin_save_config(body: AdminConfigSave, ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    ws   = state["sh"].worksheet("CONFIG")
    rows = ws.get_all_values()

    # Construir mapa clave→fila existente
    row_map = {}
    for i, row in enumerate(rows):
        if row and row[0].strip():
            row_map[row[0].strip()] = i + 1  # 1-indexed

    # Separar actualizaciones vs. inserciones nuevas
    updates   = []   # gspread batch_update format
    new_rows  = []   # filas a agregar al final

    for key, val in body.fields.items():
        if key in row_map:
            updates.append({
                "range": f"B{row_map[key]}",
                "values": [[val]],
            })
        else:
            new_rows.append([key, val])

    # Un solo batch_update para todas las actualizaciones (evita Quota 429)
    if updates:
        _sheets_retry(lambda: ws.batch_update(updates, value_input_option="RAW"))

    # Inserciones de campos nuevos (normalmente pocas o ninguna)
    for pair in new_rows:
        next_row = len(rows) + 1
        rows.append(pair)  # actualizar lista local para siguiente índice
        _sheets_retry(lambda p=pair, r=next_row: ws.update(
            [p], f"A{r}:B{r}", value_input_option="RAW"))

    # Refrescar config en memoria
    state["cfg"] = read_config(state["sh"])
    _invalidate_games()
    return {"ok": True}


@app.get("/api/admin/players")
async def admin_get_players(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    with _sheets_lock:
        ws   = _sheets_retry(lambda: state["sh"].worksheet("JUGADORES"))
        rows = _sheets_retry(lambda: ws.get_all_values())
    header_idx, headers = _jugadores_headers(rows)
    players = []
    for row in rows[header_idx + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
        d = _normalize_player(d)
        pagado_raw = d.get("PAGADO", "").upper()
        players.append({
            "nombre": d.get("NOMBRE", ""),
            "email":  d.get("EMAIL", ""),
            "fecha":  d.get("FECHA REG.", d.get("FECHA_REGISTRO", "")),
            "tab":    d.get("TAB_NOMBRE", ""),
            "pagado": pagado_raw in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X"),
        })
    return {"players": players}


def _read_jugadores_cached() -> tuple:
    """Lee JUGADORES sheet con lock+retry. Retorna (rows, header_idx, headers)."""
    with _sheets_lock:
        ws   = _sheets_retry(lambda: state["sh"].worksheet("JUGADORES"))
        rows = _sheets_retry(lambda: ws.get_all_values())
    header_idx, headers = _jugadores_headers(rows)
    return ws, rows, header_idx, headers


@app.get("/api/admin/prize-and-players")
async def admin_prize_and_players(ql_admin: str = Cookie(default="")):
    """Endpoint combinado: devuelve info de premios + lista de jugadores en una sola lectura."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    try:
        _, rows, header_idx, headers = _read_jugadores_cached()
        cfg         = state.get("cfg", {})
        cost        = float(cfg.get("COSTO_QUINIELA", "10") or "10")
        cat_a       = int(float(cfg.get("CAT_A_MAX",   "10") or "10"))
        cat_b       = int(float(cfg.get("CAT_B_MAX",   "20") or "20"))
        pct_1       = float(cfg.get("PCT_1_LUGAR",     "70") or "70")
        sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2")  or "2"))
        ganadores   = [cfg.get(f"SORTEO_GANADOR_{i+1}", "") for i in range(sorteo_cant)]
        paid    = 0
        players = []
        for row in rows[header_idx + 1:]:
            if not any(c.strip() for c in row):
                continue
            d = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
            d = _normalize_player(d)
            pagado_raw = d.get("PAGADO", "").upper()
            is_paid    = pagado_raw in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X")
            if is_paid:
                paid += 1
            players.append({
                "nombre": d.get("NOMBRE", ""),
                "email":  d.get("EMAIL", ""),
                "phone":  d.get("WHATSAPP", d.get("TELEFONO", "")),
                "fecha":  d.get("FECHA REG.", d.get("FECHA_REGISTRO", "")),
                "tab":    d.get("TAB_NOMBRE", ""),
                "pagado": is_paid,
            })
        tie_1st, tie_2nd = _get_tie_counts()
        fee_pct = float(cfg.get("FEE_PCT", "0") or "0")
        prize = _calc_prize(paid, cost, cat_a_max=cat_a, cat_b_max=cat_b,
                            pct_1=pct_1, sorteo_cant=sorteo_cant,
                            sorteo_ganadores=ganadores,
                            tie_1st=tie_1st, tie_2nd=tie_2nd,
                            fee_pct=fee_pct)
        prize["costo"] = cost
        prize["stripe_activo"] = cfg.get("STRIPE_ACTIVO", "0") == "1"
        prize["torneo_activo"] = _torneo_activo().get("activo", False)
        return {"prize": prize, "players": players}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[prize-and-players] ERROR: {e}\n{traceback.format_exc()}")
        raise HTTPException(503, f"Error: {e}")


def _get_tie_counts() -> tuple:
    """Lee la hoja POSICIONES y retorna (tie_1st, tie_2nd):
    cuántos jugadores comparten el 1° lugar y el 2° lugar."""
    try:
        ws_pos = state["sh"].worksheet("POSICIONES")
        rows = ws_pos.get_all_values()
        from collections import Counter
        pos_counter: Counter = Counter()
        for r in rows[2:]:  # fila 1 = título, fila 2 = headers
            if r and r[0].strip().isdigit():
                pos_counter[int(r[0].strip())] += 1
        tie_1 = pos_counter.get(1, 1)
        second_pos = min((p for p in pos_counter if p > 1), default=None)
        tie_2 = pos_counter.get(second_pos, 1) if second_pos else 0
        return tie_1, tie_2
    except Exception:
        return 1, 1


def _calc_prize(paid: int, cost: float,
                cat_a_max: int = 10, cat_b_max: int = 20,
                pct_1: float = 70.0, sorteo_cant: int = 2,
                sorteo_ganadores: list = None,
                tie_1st: int = 1, tie_2nd: int = 1,
                fee_pct: float = 0.0) -> dict:
    """Calcula distribución del pozo según reglamento.
    Categoría A: ≤cat_a_max              → 100% al 1°
    Categoría B: cat_a_max+1…cat_b_max   → pct_1% al 1°, resto al 2°
    Categoría C: >cat_b_max              → sorteo_cant quinielas (costo c/u), resto pct_1%/…%
    En caso de empate los premios de las posiciones empatadas se dividen.
    fee_pct: % que se retiene antes de repartir (comisión del organizador).
    """
    if sorteo_ganadores is None:
        sorteo_ganadores = []
    pct_1    = max(1.0, min(99.0, float(pct_1)))
    pct_2    = round(100.0 - pct_1, 1)
    fee_pct  = max(0.0, min(50.0, float(fee_pct)))
    total    = paid * cost
    fee_amt  = round(total * fee_pct / 100, 2)
    net      = round(total - fee_amt, 2)  # pozo real a repartir

    if paid == 0:
        return {
            "paid": 0, "total": 0, "categoria": "-", "dist": [],
            "cat_a_max": cat_a_max, "cat_b_max": cat_b_max,
            "pct_1": pct_1, "pct_2": pct_2, "sorteo_cant": sorteo_cant,
            "sorteo_ganadores": [],
        }

    # ── helpers para dividir premios en empate ──────────────────────────────
    def _split_prize(lugar: str, pct_base: float, monto_base: float,
                     nota_base: str, n: int) -> list:
        if n <= 1:
            return [{"lugar": lugar, "pct": round(pct_base, 1),
                     "monto": round(monto_base, 2), "nota": nota_base}]
        pct_c   = round(pct_base / n, 1)
        monto_c = round(monto_base / n, 2)
        return [{"lugar": f"{lugar} (empate ×{n}, c/u)", "pct": pct_c,
                 "monto": monto_c, "nota": f"{nota_base} ÷ {n}"}]

    def _pool_split(slots: list, n: int) -> list:
        """Junta varios slots de premio y los divide entre n empatados."""
        if not slots: return []
        if n <= 1:   return slots
        pool_pct   = sum(s["pct"]   for s in slots)
        pool_monto = sum(s["monto"] for s in slots)
        pct_c   = round(pool_pct   / n, 1)
        monto_c = round(pool_monto / n, 2)
        lbl = f"1°-2° (empate ×{n}, c/u)" if len(slots) > 1 else f"1° (empate ×{n}, c/u)"
        nota = f"Premios de {len(slots)} puesto(s) divididos entre {n}"
        return [{"lugar": lbl, "pct": pct_c, "monto": monto_c, "nota": nota}]

    if paid <= cat_a_max:
        cat  = "A"
        s1   = {"lugar": "1°", "pct": 100.0, "monto": round(net, 2), "nota": "100% del pozo neto"}
        dist = _split_prize("1°", 100.0, s1["monto"], s1["nota"], tie_1st)
    elif paid <= cat_b_max:
        cat  = "B"
        s1   = {"lugar": "1°", "pct": pct_1, "monto": round(net * pct_1 / 100, 2), "nota": f"{pct_1}% del pozo neto"}
        s2   = {"lugar": "2°", "pct": pct_2, "monto": round(net * pct_2 / 100, 2), "nota": f"{pct_2}% del pozo neto"}
        if tie_1st >= 2:
            slots_pool = [s1, s2] if tie_1st >= 2 else [s1]
            dist = _pool_split(slots_pool, tie_1st)
        else:
            dist = [s1] + _split_prize("2°", pct_2, s2["monto"], s2["nota"], max(tie_2nd, 1))
    else:
        cat          = "C"
        sorteo_total = cost * sorteo_cant
        resto        = net - sorteo_total
        p1_real      = round(resto * pct_1 / 100, 2)
        p2_real      = round(resto * pct_2 / 100, 2)
        pct_1_tot    = round(p1_real / net * 100, 1) if net else 0
        pct_2_tot    = round(p2_real / net * 100, 1) if net else 0
        pct_s        = round(cost / net * 100, 1) if net else 0
        s1   = {"lugar": "1°", "pct": pct_1_tot, "monto": p1_real, "nota": f"{pct_1}% del resto"}
        s2   = {"lugar": "2°", "pct": pct_2_tot, "monto": p2_real, "nota": f"{pct_2}% del resto"}
        if tie_1st >= 2:
            slots_pool = [s1, s2] if tie_1st >= 2 else [s1]
            dist = _pool_split(slots_pool, tie_1st)
        else:
            dist = [s1] + _split_prize("2°", pct_2_tot, p2_real, s2["nota"], max(tie_2nd, 1))
        for i in range(sorteo_cant):
            nombre = sorteo_ganadores[i] if i < len(sorteo_ganadores) else ""
            lbl    = f" — {nombre}" if nombre else ""
            dist.append({
                "lugar": f"Sorteo #{i+1}{lbl}",
                "pct":   pct_s,
                "monto": round(cost, 2),
                "nota":  "Reembolso de entrada",
            })

    return {
        "paid":             paid,
        "total":            round(total, 2),
        "fee_pct":          fee_pct,
        "fee_amt":          fee_amt,
        "net":              net,
        "categoria":        cat,
        "dist":             dist,
        "cat_a_max":        cat_a_max,
        "cat_b_max":        cat_b_max,
        "pct_1":            pct_1,
        "pct_2":            pct_2,
        "sorteo_cant":      sorteo_cant,
        "sorteo_ganadores": sorteo_ganadores,
    }


@app.get("/api/admin/prize")
async def admin_prize(ql_admin: str = Cookie(default="")):
    """Resumen del pozo y distribución actual."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    _, rows, header_idx, headers = _read_jugadores_cached()
    cfg         = state.get("cfg", {})
    cost        = float(cfg.get("COSTO_QUINIELA", "10") or "10")
    cat_a       = int(float(cfg.get("CAT_A_MAX",   "10") or "10"))
    cat_b       = int(float(cfg.get("CAT_B_MAX",   "20") or "20"))
    pct_1       = float(cfg.get("PCT_1_LUGAR",     "70") or "70")
    sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2")  or "2"))
    fee_pct     = float(cfg.get("FEE_PCT",          "0") or "0")
    ganadores   = [cfg.get(f"SORTEO_GANADOR_{i+1}", "") for i in range(sorteo_cant)]
    paid = 0
    for row in rows[header_idx + 1:]:
        if not any(c.strip() for c in row): continue
        d = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
        d = _normalize_player(d)
        if d.get("PAGADO", "").upper() in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X"):
            paid += 1
    tie_1st, tie_2nd = _get_tie_counts()
    result = _calc_prize(paid, cost, cat_a_max=cat_a, cat_b_max=cat_b,
                         pct_1=pct_1, sorteo_cant=sorteo_cant,
                         sorteo_ganadores=ganadores,
                         tie_1st=tie_1st, tie_2nd=tie_2nd,
                         fee_pct=fee_pct)
    result["costo"] = cost
    return result


@app.post("/api/admin/player-paid")
async def admin_player_paid(body: dict, ql_admin: str = Cookie(default="")):
    """Marca o desmarca un jugador como pagado. Acepta email o phone."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    email = (body.get("email") or "").strip().lower()
    phone = _normalize_phone(body.get("phone") or "")
    paid  = body.get("paid", True)
    if not email and not phone:
        raise HTTPException(400, "email o teléfono requerido")
    ws, rows, header_idx, headers = _read_jugadores_cached()
    # Asegurar columna PAGADO
    if "PAGADO" not in headers:
        col = len(headers) + 1
        with _sheets_lock:
            _sheets_retry(lambda: ws.update_cell(header_idx + 1, col, "PAGADO"))
        headers.append("PAGADO")
    pagado_col = headers.index("PAGADO") + 1
    email_col  = headers.index("EMAIL") + 1 if "EMAIL" in headers else None
    phone_col  = None
    for pk in ("WHATSAPP", "TELEFONO"):
        if pk in headers:
            phone_col = headers.index(pk) + 1
            break
    for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        row_email = (row[email_col - 1].strip().lower() if email_col and email_col - 1 < len(row) else "")
        row_phone = _normalize_phone(row[phone_col - 1] if phone_col and phone_col - 1 < len(row) else "")
        if (email and row_email == email) or (phone and row_phone == phone):
            with _sheets_lock:
                _sheets_retry(lambda r=i, c=pagado_col: ws.update_cell(r, c, "1" if paid else ""))
            return {"ok": True, "paid": paid}
    raise HTTPException(404, "Jugador no encontrado")


@app.post("/api/admin/player-delete")
async def admin_player_delete(body: dict, ql_admin: str = Cookie(default="")):
    """Elimina un jugador de la hoja JUGADORES y su pestaña de picks."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    if _torneo_activo().get("activo"):
        raise HTTPException(403, "No se puede eliminar jugadores una vez iniciado el torneo")
    email = (body.get("email") or "").strip().lower()
    phone = _normalize_phone(body.get("phone") or "")
    if not email and not phone:
        raise HTTPException(400, "email o teléfono requerido")
    ws, rows, header_idx, headers = _read_jugadores_cached()
    email_col = headers.index("EMAIL") + 1 if "EMAIL" in headers else None
    phone_col = None
    for pk in ("WHATSAPP", "TELEFONO"):
        if pk in headers:
            phone_col = headers.index(pk) + 1
            break
    tab_col = headers.index("TAB_NOMBRE") + 1 if "TAB_NOMBRE" in headers else (
              headers.index("TAB SHEET") + 1 if "TAB SHEET" in headers else None)
    for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        row_email = (row[email_col - 1].strip().lower() if email_col and email_col - 1 < len(row) else "")
        row_phone = _normalize_phone(row[phone_col - 1] if phone_col and phone_col - 1 < len(row) else "")
        if (email and row_email == email) or (phone and row_phone == phone):
            tab_nombre = row[tab_col - 1].strip() if tab_col and tab_col - 1 < len(row) else ""
            with _sheets_lock:
                _sheets_retry(lambda r=i: ws.delete_rows(r))
            if tab_nombre:
                try:
                    reserved = RESERVED_TABS
                    if tab_nombre not in reserved:
                        tab_ws = _sheets_retry(lambda t=tab_nombre: state["sh"].worksheet(t))
                        _sheets_retry(lambda t=tab_ws: state["sh"].del_worksheet(t))
                        print(f"[admin] Pestaña '{tab_nombre}' eliminada")
                except Exception as e:
                    print(f"[admin] No se pudo borrar pestaña '{tab_nombre}': {e}")
            if email: _cache["players"].pop(f"email:{email}", None)
            if phone: _cache["players"].pop(f"phone:{phone}", None)
            _invalidate_players()
            return {"ok": True}
    raise HTTPException(404, "Jugador no encontrado")


@app.get("/api/prize-info")
async def prize_info():
    """Info pública del pozo (sin datos sensibles)."""
    _, rows, header_idx, headers = _read_jugadores_cached()
    cfg         = state.get("cfg", {})
    cost        = float(cfg.get("COSTO_QUINIELA", "10") or "10")
    cat_a       = int(float(cfg.get("CAT_A_MAX",   "10") or "10"))
    cat_b       = int(float(cfg.get("CAT_B_MAX",   "20") or "20"))
    pct_1       = float(cfg.get("PCT_1_LUGAR",     "70") or "70")
    sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2")  or "2"))
    fee_pct     = float(cfg.get("FEE_PCT",          "0") or "0")
    ganadores   = [cfg.get(f"SORTEO_GANADOR_{i+1}", "") for i in range(sorteo_cant)]
    paid  = 0
    for row in rows[header_idx + 1:]:
        if not any(c.strip() for c in row): continue
        d = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
        d = _normalize_player(d)
        if d.get("PAGADO", "").upper() in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X"):
            paid += 1
    tie_1st, tie_2nd = _get_tie_counts()
    result = _calc_prize(paid, cost, cat_a_max=cat_a, cat_b_max=cat_b,
                         pct_1=pct_1, sorteo_cant=sorteo_cant,
                         sorteo_ganadores=ganadores,
                         tie_1st=tie_1st, tie_2nd=tie_2nd,
                         fee_pct=fee_pct)
    result["costo"] = cost
    return result


# ─── Estado en memoria del sorteo en vivo ────────────────────────────────────
_sorteo = {
    "fase":      "idle",   # idle | lobby | live | done
    "ganadores": [],       # nombres confirmados
    "elegibles": [],       # candidatos (excluye top posiciones)
    "anim":      "bolas",  # bolas | slot | ruleta
}
_sorteo_notif_sent  = False   # True cuando ya se envió la notif de 2 min antes
_sorteo_notif_key   = ""      # "FECHA|HORA" para detectar cambio de config


def _sorteo_elegibles() -> list:
    """Jugadores pagados que NO son top-1 ni top-2 en la tabla de posiciones."""
    import random as _rnd
    cfg = state.get("cfg", {})
    sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2") or "2"))

    # Leer standings para excluir puestos 1° y 2° (respetando empates)
    top_names = set()
    try:
        ws_p = state["sh"].worksheet("POSICIONES")
        rows = ws_p.get_all_values()
        data_rows = [r for r in rows[2:] if any(c.strip() for c in r)]  # skip título+headers
        second_pos = None
        for r in data_rows:
            if len(r) < 2: continue
            pos_str = r[0].strip()
            if not pos_str.isdigit(): continue
            pos_n = int(pos_str)
            if pos_n == 1:
                top_names.add(r[1].strip().lower())
            else:
                if second_pos is None:
                    second_pos = pos_n
                if pos_n == second_pos:
                    top_names.add(r[1].strip().lower())
    except Exception:
        pass

    # Jugadores pagados fuera del top
    _, rows, header_idx, headers = _read_jugadores_cached()
    elegibles = []
    for row in rows[header_idx + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
        d = _normalize_player(d)
        if d.get("PAGADO", "").upper() not in ("1", "SI", "SÍ", "YES", "TRUE", "✓", "X"):
            continue
        nombre = d.get("NOMBRE", "?")
        if nombre.strip().lower() not in top_names:
            elegibles.append(nombre)
    return elegibles


def _sorteo_dt_utc(cfg, fecha, hora):
    """La hora del sorteo se ingresa directamente en UTC — sin conversión."""
    from datetime import datetime as _dt2
    return _dt2.fromisoformat(f"{fecha}T{hora}:00")


def _check_sorteo_notif():
    """Envía push + WA ~2 min antes del sorteo (una sola vez por sorteo configurado)."""
    global _sorteo_notif_sent, _sorteo_notif_key
    from datetime import datetime as _dt2, timezone as _tz2
    cfg   = state.get("cfg", {})
    fecha = cfg.get("SORTEO_FECHA", "").strip()
    hora  = cfg.get("SORTEO_HORA",  "").strip()
    if not fecha or not hora:
        return
    key = f"{fecha}|{hora}"
    if key != _sorteo_notif_key:          # nueva fecha/hora → resetear flag
        _sorteo_notif_key  = key
        _sorteo_notif_sent = False
    if _sorteo_notif_sent:
        return
    if _sorteo["fase"] not in ("idle", "lobby"):
        return
    try:
        dt_utc  = _sorteo_dt_utc(cfg, fecha, hora)
        minutos = (dt_utc - _dt2.now(_tz2.utc).replace(tzinfo=None)).total_seconds() / 60
    except Exception:
        return
    if not (0 < minutos <= 3):            # ventana: entre 0 y 3 minutos antes
        return
    _sorteo_notif_sent = True
    torneo = cfg.get("TORNEO", "Quiniela")
    msg    = (f"🎲 ¡El sorteo de {torneo} está por comenzar!\n"
              f"Abre la app ahora y ve a la pestaña ✨ Sorteo para ver quién gana.")
    # Push a todos
    try:
        _send_push_all("🎲 ¡Sorteo en 2 minutos!", f"Entra a la pestaña Sorteo de {torneo}", {"tipo": "sorteo", "url": "/"})
        print(f"[sorteo] Push de 2-min enviado")
    except Exception as e:
        print(f"[sorteo] Error push: {e}")
    # WhatsApp al grupo
    try:
        _wa("POST", "/send", json={"message": msg})
        print(f"[sorteo] WA de 2-min enviado")
    except Exception as e:
        print(f"[sorteo] Error WA: {e}")


@app.get("/api/sorteo/estado")
async def sorteo_estado():
    """Estado público del sorteo — lo consultan todos los clientes."""
    cfg  = state.get("cfg", {})
    fecha = cfg.get("SORTEO_FECHA", "")
    hora  = cfg.get("SORTEO_HORA",  "00:00")
    anim  = cfg.get("SORTEO_ANIM",  "bolas")
    sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2") or "2"))

    # Calcular si ya llegó la hora del lobby (15 min antes del sorteo)
    # La hora configurada está en la zona UTC_OFFSET del torneo → convertir a UTC
    fase_actual = _sorteo["fase"]
    dt_utc_sorteo = None
    if fecha:
        try:
            from datetime import datetime as _dt2, timezone as _tz2
            dt_utc_sorteo = _sorteo_dt_utc(cfg, fecha, hora)
            ahora_utc     = _dt2.now(_tz2.utc).replace(tzinfo=None)
            minutos       = (dt_utc_sorteo - ahora_utc).total_seconds() / 60
            if fase_actual == "idle":
                if minutos <= 0:
                    _sorteo["fase"] = "lobby"
                    fase_actual = "lobby"
                elif minutos <= 15:
                    fase_actual = "lobby"  # mostrar tab pero no cambiar estado
            # Precompute eligibles when entering lobby so they show on screen
            if fase_actual == "lobby" and not _sorteo["elegibles"]:
                try:
                    _sorteo["elegibles"] = _sorteo_elegibles()
                except Exception:
                    pass
        except Exception:
            pass

    # Pasar timestamp UTC al frontend para que cada browser muestre hora local
    dt_utc_iso = (dt_utc_sorteo.strftime("%Y-%m-%dT%H:%M:%SZ")
                  if dt_utc_sorteo else "")

    return {
        "fase":        fase_actual,
        "ganadores":   _sorteo["ganadores"],
        "elegibles":   _sorteo["elegibles"],   # available from lobby onwards
        "anim":        anim,
        "sorteo_cant": sorteo_cant,
        "fecha":       fecha,
        "hora":        hora,
        "dt_utc":      dt_utc_iso,   # ISO UTC — el browser lo convierte a hora local
    }


@app.post("/api/admin/sorteo-launch")
async def admin_sorteo_launch(ql_admin: str = Cookie(default="")):
    """Activa la pantalla del sorteo en vivo para todos los usuarios."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    elegibles = _sorteo_elegibles()
    if len(elegibles) < 1:
        raise HTTPException(400, "No hay jugadores elegibles para el sorteo")
    cfg = state.get("cfg", {})
    _sorteo["fase"]      = "live"
    _sorteo["ganadores"] = []
    _sorteo["elegibles"] = elegibles
    _sorteo["anim"]      = cfg.get("SORTEO_ANIM", "bolas")
    torneo = cfg.get("TORNEO", "Quiniela")
    # Push a todos
    _send_push_all("🎲 ¡El sorteo está comenzando!", "Abre la app ahora para ver quién gana", {"url": "/", "tipo": "sorteo"})
    # WhatsApp al grupo
    try:
        cant = int(float(cfg.get("SORTEO_CANT", "2") or "2"))
        participantes = "\n".join(f"  {i+1}. {e}" for i, e in enumerate(elegibles))
        msg = (f"🎲 *¡El sorteo de {torneo} está EN VIVO ahora!*\n\n"
               f"Se sortearán *{cant} ganador{'es' if cant > 1 else ''}* entre estos {len(elegibles)} participantes:\n"
               f"{participantes}\n\n"
               f"🔔 Abre la app para ver el sorteo en tiempo real 👉 pestaña *Sorteo*")
        _wa("POST", "/send", json={"message": msg})
    except Exception as e:
        print(f"[sorteo] Error WA launch: {e}")
    return {"ok": True, "elegibles": len(elegibles)}


@app.post("/api/admin/sorteo-draw")
async def admin_sorteo_draw(ql_admin: str = Cookie(default="")):
    """Saca UN ganador del sorteo (llamar una vez por cada ganador)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    import random
    cfg = state.get("cfg", {})
    sorteo_cant = int(float(cfg.get("SORTEO_CANT", "2") or "2"))

    # Elegibles = los del estado live, excluyendo ya ganadores
    elegibles = _sorteo.get("elegibles") or _sorteo_elegibles()
    ya_ganaron = {w.lower() for w in _sorteo["ganadores"]}
    candidatos = [e for e in elegibles if e.lower() not in ya_ganaron]

    if not candidatos:
        raise HTTPException(400, "No quedan candidatos elegibles")

    winner = random.choice(candidatos)
    _sorteo["ganadores"].append(winner)

    # Si ya se sacaron todos los ganadores → fase done
    if len(_sorteo["ganadores"]) >= sorteo_cant:
        _sorteo["fase"] = "done"
        winners = _sorteo["ganadores"]
        # Anunciar ganadores por WhatsApp
        try:
            torneo = cfg.get("TORNEO", "Quiniela")
            medallas = ["🥇", "🥈", "🥉", "🏅", "🏅"]
            lista = "\n".join(f"  {medallas[i] if i < len(medallas) else '🏅'} {w}" for i, w in enumerate(winners))
            msg = (f"🎉 *¡Ganadores del sorteo de {torneo}!*\n\n"
                   f"{lista}\n\n"
                   f"¡Felicidades a los ganadores! 🎲🏆")
            _wa("POST", "/send", json={"message": msg})
        except Exception as e:
            print(f"[sorteo] Error WA ganadores: {e}")
    else:
        winners = _sorteo["ganadores"][:]
    # Guardar en hoja CONFIG
    with _sheets_lock:
        ws_cfg   = _sheets_retry(lambda: state["sh"].worksheet("CONFIG"))
        cfg_rows = _sheets_retry(lambda: ws_cfg.get_all_values())
    def _set_cfg_cell(key: str, val: str):
        for i, r in enumerate(cfg_rows):
            if r and r[0].strip().upper() == key.upper():
                with _sheets_lock:
                    _sheets_retry(lambda ri=i, v=val: ws_cfg.update_cell(ri + 1, 2, v))
                return
        with _sheets_lock:
            _sheets_retry(lambda k=key, v=val: ws_cfg.append_row([k, v]))
    # Limpiar ganadores anteriores (puede haber más o menos que antes)
    for i in range(1, 20):   # limpiar hasta 20 slots anteriores
        _set_cfg_cell(f"SORTEO_GANADOR_{i}", "")
        state["cfg"][f"SORTEO_GANADOR_{i}"] = ""
    for i, w in enumerate(winners, 1):
        _set_cfg_cell(f"SORTEO_GANADOR_{i}", w)
        state["cfg"][f"SORTEO_GANADOR_{i}"] = w
    return {"ok": True, "ganadores": winners}


@app.post("/api/admin/sorteo-reset")
async def admin_sorteo_reset(ql_admin: str = Cookie(default="")):
    """Limpia los ganadores del sorteo y resetea la fase a idle."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    _sorteo["fase"]      = "idle"
    _sorteo["ganadores"] = []
    _sorteo["elegibles"] = []
    with _sheets_lock:
        ws_cfg   = _sheets_retry(lambda: state["sh"].worksheet("CONFIG"))
        cfg_rows = _sheets_retry(lambda: ws_cfg.get_all_values())
    def _set_cfg_cell(key: str, val: str):
        for i, r in enumerate(cfg_rows):
            if r and r[0].strip().upper() == key.upper():
                with _sheets_lock:
                    _sheets_retry(lambda ri=i, v=val: ws_cfg.update_cell(ri + 1, 2, v))
                return
    for i in range(1, 20):
        _set_cfg_cell(f"SORTEO_GANADOR_{i}", "")
        state["cfg"][f"SORTEO_GANADOR_{i}"] = ""
    return {"ok": True}


@app.post("/api/admin/upload-logo")
async def admin_upload_logo(file: UploadFile = File(...),
                            ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    try:
        from PIL import Image, ImageOps
        import io

        data  = await file.read()
        img   = Image.open(io.BytesIO(data)).convert("RGBA")
        base  = DATA_DIR

        # Recorte cuadrado centrado
        w, h  = img.size
        side  = min(w, h)
        img   = img.crop(((w-side)//2, (h-side)//2,
                           (w+side)//2, (h+side)//2))

        # Agregar esquinas redondeadas
        def rounded(im, r):
            from PIL import ImageDraw
            mask = Image.new("L", im.size, 0)
            ImageDraw.Draw(mask).rounded_rectangle(
                [0, 0, im.size[0]-1, im.size[1]-1], radius=r, fill=255)
            im.putalpha(mask)
            return im

        for size in [192, 512]:
            out = img.resize((size, size), Image.LANCZOS)
            out = rounded(out, size // 6)
            out.save(base / f"icon-{size}.png", "PNG")

        # Guardar original para el header de la app
        img.resize((256, 256), Image.LANCZOS).save(base / "logo.png", "PNG")

        return {"ok": True, "msg": "Logo actualizado. Recarga la app para verlo."}
    except Exception as e:
        raise HTTPException(500, f"Error procesando imagen: {e}")


@app.get("/api/version")
async def get_version():
    """Retorna la versión actual del servidor (timestamp de inicio).
    El cliente la sondea periódicamente; si cambia, recarga la página."""
    return JSONResponse({"version": APP_VERSION},
                        headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/logo.png")
async def get_logo():
    for p in [DATA_DIR / "logo.png", DATA_DIR / "icon-192.png",
              Path(__file__).parent / "logo.png", Path(__file__).parent / "icon-192.png"]:
        if p.exists():
            return Response(content=p.read_bytes(), media_type="image/png")
    return Response(content=_make_png(256), media_type="image/png")


@app.post("/api/admin/telegram-verify")
async def tg_verify(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    token = state.get("cfg",{}).get("TELEGRAM_BOT_TOKEN","")
    if not token: raise HTTPException(400, "Sin token. Guarda TELEGRAM_BOT_TOKEN primero.")
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8)
        if r.status_code == 200:
            bot = r.json().get("result",{})
            return {"ok": True, "name": bot.get("first_name"), "username": bot.get("username")}
        raise HTTPException(400, "Token inválido")
    except Exception as e:
        raise HTTPException(400, str(e))

@app.get("/api/admin/telegram-chats")
async def tg_chats(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    token = state.get("cfg",{}).get("TELEGRAM_BOT_TOKEN","")
    if not token: raise HTTPException(400, "Sin token")
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates",
                         params={"limit": 100}, timeout=8)
        updates = r.json().get("result", [])
        chats = {}
        for u in updates:
            for key in ("message","my_chat_member","chat_member"):
                chat = u.get(key,{}).get("chat",{})
                if chat and chat.get("id"):
                    cid = str(chat["id"])
                    chats[cid] = {
                        "id": cid,
                        "title": chat.get("title") or chat.get("first_name",""),
                        "type": chat.get("type",""),
                    }
        return {"chats": list(chats.values())}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/admin/telegram-test")
async def tg_test(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    cfg = state.get("cfg",{})
    if not cfg.get("TELEGRAM_BOT_TOKEN") or not cfg.get("TELEGRAM_CHAT_ID"):
        raise HTTPException(400, "Configura token y chat_id primero")
    _tg_send("â <b>Bot conectado</b>\nQuiniela notificaciones funcionando correctamente ð")
    return {"ok": True}


@app.get("/api/push/vapid-public-key")
async def push_vapid_public_key():
    """Devuelve la clave pública VAPID en base64url para que el SW pueda suscribirse."""
    if not _vapid_keys:
        raise HTTPException(503, "VAPID no configurado")
    pub_hex = _vapid_keys.get("public", "")
    if not pub_hex:
        raise HTTPException(503, "Clave pública VAPID no disponible")
    import base64, binascii
    pub_bytes = binascii.unhexlify(pub_hex)
    pub_b64 = base64.urlsafe_b64encode(pub_bytes).decode().rstrip("=")
    return {"publicKey": pub_b64}


class PushSubscribeBody(BaseModel):
    subscription: dict
    phone: str = ""
    email: str = ""

@app.post("/api/push/subscribe")
async def push_subscribe(body: PushSubscribeBody):
    """Registra o actualiza una suscripción push vinculada al jugador (phone/email)."""
    sub = body.subscription
    if not sub.get("endpoint"):
        raise HTTPException(400, "Suscripción inválida")
    phone_norm = _normalize_phone(body.phone) if body.phone else ""
    email_norm = body.email.strip().lower() if body.email else ""
    for s in _push_subs:
        if s.get("endpoint") == sub["endpoint"]:
            s["_phone"] = phone_norm
            s["_email"] = email_norm
            _subs_save()
            return {"ok": True, "action": "updated"}
    new_sub = {**sub, "_phone": phone_norm, "_email": email_norm}
    _push_subs.append(new_sub)
    _subs_save()
    print(f"[push] Nueva suscripción: phone={phone_norm} email={email_norm} endpoint={sub['endpoint'][:60]}")
    return {"ok": True, "action": "registered"}


class PushUnsubscribeBody(BaseModel):
    endpoint: str

@app.post("/api/push/unsubscribe")
async def push_unsubscribe(body: PushUnsubscribeBody):
    """Elimina una suscripción push por endpoint."""
    before = len(_push_subs)
    _push_subs[:] = [s for s in _push_subs if s.get("endpoint") != body.endpoint]
    if len(_push_subs) < before:
        _subs_save()
        print(f"[push] Suscripción eliminada: {body.endpoint[:60]}")
        return {"ok": True}
    return {"ok": False, "msg": "Suscripción no encontrada"}


@app.post("/api/admin/push-setup")
async def push_setup(ql_admin: str = Cookie(default="")):
    """Instala py_vapid/pywebpush y genera claves VAPID si no existen."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    import subprocess, sys, json
    global _vapid_keys

    # 1. Instalar dependencias si faltan
    pkgs_installed = []
    for pkg in ["py_vapid", "pywebpush"]:
        try:
            __import__(pkg.replace("-","_"))
        except ImportError:
            print(f"[push-setup] Instalando {pkg}...")
            r = subprocess.run(
                [sys.executable, "-m", "pip", "install", pkg, "--break-system-packages", "-q"],
                capture_output=True, text=True
            )
            if r.returncode != 0:
                raise HTTPException(500, f"Error instalando {pkg}: {r.stderr[:200]}")
            pkgs_installed.append(pkg)

    # 2. Cargar o generar claves (con migración automática de formato PEM→DER)
    _vapid_keys = _load_vapid()
    if not _vapid_keys:
        raise HTTPException(500, "No se pudieron generar claves VAPID")
    was_existing = _VAPID_FILE.exists()
    return {"ok": True,
            "msg": "Claves VAPID cargadas" if was_existing else "Claves VAPID generadas",
            "installed": pkgs_installed,
            "public": _vapid_keys.get("public","")[:20]+"..."}


@app.get("/api/admin/push-status")
async def push_status(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    subs_info = [{"endpoint": s.get("endpoint","")[:80], "phone": s.get("_phone",""), "email": s.get("_email","")} for s in _push_subs]
    return {"vapid_ok": bool(_vapid_keys), "subs": len(_push_subs), "detalle": subs_info,
            "subs_file": str(_SUBS_FILE), "file_exists": _SUBS_FILE.exists()}


@app.get("/api/admin/test-espn")
async def admin_test_espn(fecha: str = "", ql_admin: str = Cookie(default="")):
    """Diagnóstico: consulta ESPN y devuelve los partidos encontrados para una fecha."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    from datetime import date as _date
    cfg    = state.get("cfg", {})
    league = cfg.get("ESPN_LEAGUE", "fifa.world")
    leagues = [l.strip() for l in league.split(",") if l.strip()]
    if not fecha:
        fecha = _date.today().strftime("%Y%m%d")
    else:
        fecha = fecha.replace("-", "")
    events = []
    errors = []
    for lg in leagues:
        url = f"{ESPN_BASE}/{lg}/scoreboard"
        try:
            r = requests.get(url, params={"dates": fecha}, timeout=10)
            data = r.json()
            for ev in data.get("events", []):
                comp = (ev.get("competitions") or [{}])[0]
                teams = comp.get("competitors", [])
                home = next((t for t in teams if t.get("homeAway") == "home"), {})
                away = next((t for t in teams if t.get("homeAway") == "away"), {})
                status = comp.get("status", {})
                events.append({
                    "liga":   lg,
                    "nombre": ev.get("name", ""),
                    "home":   home.get("team", {}).get("displayName", "?"),
                    "away":   away.get("team", {}).get("displayName", "?"),
                    "score":  f"{home.get('score','?')} - {away.get('score','?')}",
                    "estado": status.get("type", {}).get("shortDetail", ""),
                    "uid":    ev.get("uid", ""),
                })
        except Exception as e:
            errors.append(f"{lg}: {e}")
    return {"fecha": fecha, "ligas": leagues, "total": len(events), "events": events, "errors": errors}


@app.post("/api/admin/test-notif")
async def admin_test_notif(body: dict = None, ql_admin: str = Cookie(default="")):
    """Envía una notificación push + WhatsApp de prueba."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    msg = "🔔 Prueba de notificación desde Quiniela ✅"
    results = {}
    if _push_subs:
        _send_push_all("🔔 Prueba", msg)
        results["push"] = f"{len(_push_subs)} enviado(s)"
    else:
        results["push"] = "Sin suscriptores"
    try:
        _wa("POST", "/send", json={"message": msg})
        results["whatsapp"] = "Enviado"
    except Exception as e:
        results["whatsapp"] = f"Error: {e}"
    return {"ok": True, "results": results}


@app.post("/api/admin/push-test")
async def push_test(ql_admin: str = Cookie(default="")):
    """Envía una notificación push de prueba a todos los suscriptores."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    if not _push_subs:
        return {"ok": False, "msg": "Sin suscriptores push activos"}
    _send_push_all(
        title="🔔 Prueba de notificación",
        body="Las notificaciones push funcionan correctamente ✅",
        data={"url": "/"}
    )
    return {"ok": True, "msg": f"Enviado a {len(_push_subs)} suscriptor(es)"}


# ─── WhatsApp (Baileys) endpoints ─────────────────────────────────────────────

_WA_PORT = int(os.environ.get("WA_PORT", "3001"))
_WA_BASE = f"http://127.0.0.1:{_WA_PORT}"

def _wa(method: str, path: str, timeout: int = 15, **kwargs):
    """Llama al servidor Baileys local. Retorna dict o lanza HTTPException."""
    try:
        r = requests.request(method, f"{_WA_BASE}{path}", timeout=timeout, **kwargs)
        data = r.json()
        # Propagar errores del servidor Baileys como HTTPException
        if not r.ok:
            msg = data.get("msg") or data.get("error") or f"Error Baileys HTTP {r.status_code}"
            raise HTTPException(r.status_code, msg)
        return data
    except HTTPException:
        raise
    except requests.ConnectionError:
        raise HTTPException(503, "Servidor WhatsApp no disponible (¿está corriendo baileys-server.js?)")
    except Exception as e:
        raise HTTPException(500, f"Error WhatsApp: {e}")


def _wa_get_phones() -> list:
    """Lee todos los teléfonos registrados en JUGADORES."""
    try:
        _, rows, header_idx, headers = _read_jugadores_cached()
        phones = []
        for row in rows[header_idx + 1:]:
            d = {headers[i]: row[i].strip() for i in range(min(len(headers), len(row)))}
            phone = d.get("WHATSAPP", "") or d.get("TELEFONO", "")
            if phone:
                phones.append(phone)
        return phones
    except Exception:
        return []


@app.get("/api/admin/wa-status")
async def wa_status(ql_admin: str = Cookie(default="")):
    """Estado de la conexión WhatsApp."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return _wa("GET", "/status")


@app.post("/api/admin/wa-pair")
async def wa_pair(body: dict = None, ql_admin: str = Cookie(default="")):
    """Solicita pairing code para vincular WhatsApp sin QR."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    phone = (body or {}).get("phone", "")
    if not phone:
        raise HTTPException(400, "Falta el campo 'phone'")
    return _wa("POST", "/pair", json={"phone": phone})


@app.post("/api/admin/wa-create-group")
async def wa_create_group(ql_admin: str = Cookie(default="")):
    """Crea el grupo de WhatsApp y agrega a todos los jugadores registrados."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    cfg        = state.get("cfg", {})
    group_name = cfg.get("WA_GROUP_NAME") or cfg.get("TORNEO", "Quiniela WFC 2026 - F2") + " 🏆"
    phones     = _wa_get_phones()
    if not phones:
        return {"ok": False, "msg": "No hay jugadores con teléfono registrado"}
    # Timeout largo: verificar cada número toma ~300ms, con 30 jugadores = ~10s + creación del grupo
    n = len(phones)
    timeout_s = max(60, n * 1 + 30)  # ~1s por jugador + 30s margen
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _wa("POST", "/create-group", timeout=timeout_s, json={"name": group_name, "phones": phones})
    )


@app.post("/api/admin/wa-update-group")
async def wa_update_group(ql_admin: str = Cookie(default="")):
    """Actualiza nombre, ícono y sincroniza miembros faltantes del grupo."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    cfg        = state.get("cfg", {})
    group_name = cfg.get("WA_GROUP_NAME") or cfg.get("TORNEO", "Quiniela WFC 2026 - F2") + " 🏆"
    phones     = _wa_get_phones()
    n          = len(phones)
    timeout_s  = max(60, n * 1 + 30)
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _wa("POST", "/update-group", timeout=timeout_s, json={"name": group_name, "phones": phones})
    )


@app.post("/api/admin/wa-test")
async def wa_test(ql_admin: str = Cookie(default="")):
    """Envía un mensaje de prueba al grupo de WhatsApp."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return _wa("POST", "/send", json={"message": "🔔 Prueba de notificación WhatsApp ✅\nLas notificaciones del Mundial están funcionando."})


@app.post("/api/admin/wa-disconnect")
async def wa_disconnect(body: dict = None, ql_admin: str = Cookie(default="")):
    """Desconecta WhatsApp. Si deleteGroup=true, elimina el grupo primero."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    b = body or {}
    delete_group = b.get("deleteGroup", b.get("delete_group", False))
    return _wa("POST", "/disconnect", json={"deleteGroup": bool(delete_group)})


@app.post("/api/admin/recalc-standings")
async def admin_recalc_standings(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    t = _torneo_activo()
    if t["activo"]:
        raise HTTPException(423, f'Torneo en curso — panel bloqueado. {t["razon"]}')
    try:
        _update_standings()
        return {"ok": True, "msg": "Tabla de posiciones recalculada"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/admin/reinit-formulas")
async def admin_reinit_formulas(ql_admin: str = Cookie(default="")):
    """Actualiza las fórmulas de puntos (M-P) en todas las pestañas de jugadores,
    sin borrar los picks (F-H). Útil cuando se cambia la lógica de scoring."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    t = _torneo_activo()
    if t["activo"]:
        raise HTTPException(423, f'Torneo en curso — panel bloqueado. {t["razon"]}')
    sh  = state.get("sh")
    cfg = state.get("cfg", {})
    if not sh:
        raise HTTPException(500, "Sheet no conectado")

    total    = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total

    with _sheets_lock:
        ws_j   = sh.worksheet("JUGADORES")
        j_rows = ws_j.get_all_values()
    hi, headers = _jugadores_headers(j_rows)
    players = []
    for row in j_rows[hi + 1:]:
        if not any(c.strip() for c in row):
            continue
        d = _normalize_player({headers[k]: (row[k].strip() if k < len(row) else "")
                                for k in range(len(headers))})
        if d.get("TAB_NOMBRE"):
            players.append(d)

    updated = 0
    for p in players:
        try:
            formula_rows = []
            for i in range(1, total + 1):
                r = i + 3
                formula_rows.append([
                    f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(IF(H{r}<>"";H{r};IF(AND(F{r}<>"";G{r}<>"");IF(F{r}+0>G{r}+0;"1";IF(G{r}+0>F{r}+0;"2";"E"));""))=K{r};3;0);"")' ,
                    f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(F{r}&""=I{r}&"";1;0);"")' ,
                    f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(G{r}&""=J{r}&"";1;0);"")' ,
                    f'=IF(AND(L{r}<>"";L{r}<>"PROG");IFERROR(SUM(M{r}:O{r});0);"")' ,
                ])
            with _sheets_lock:
                ws_p = sh.worksheet(p["TAB_NOMBRE"])
                ws_p.update(formula_rows, f"M4:P{last_row}", value_input_option="USER_ENTERED")
            updated += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"[reinit-formulas] Error en {p.get('TAB_NOMBRE','?')}: {e}")

    return {"ok": True, "msg": f"Fórmulas actualizadas en {updated} pestaña(s)"}


# ── Modo Prueba: simular resultado ────────────────────────────────────────────

@app.post("/api/admin/sim-result")
async def admin_sim_result(body: SimResultBody, ql_admin: str = Cookie(default="")):
    """
    Modo Prueba: escribe un resultado manual en HORARIOS para el JGO indicado,
    luego propaga los nombres de equipos en el bracket.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    cfg          = state.get("cfg", {})
    fila_inicio  = int(cfg.get("FILA_INICIO_DATOS", 3))
    total_juegos = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    fila_fin     = fila_inicio + total_juegos - 1

    with _sheets_lock:
        ws_h  = state["sh"].worksheet("HORARIOS")
        filas = ws_h.get(f"A{fila_inicio}:K{fila_fin}")

    target_row = None
    eq1_name   = ""
    eq2_name   = ""
    for i, fila in enumerate(filas):
        def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
        if c(0) == str(body.jgo):
            target_row = fila_inicio + i
            eq1_name   = c(4)
            eq2_name   = c(5)
            break

    if not target_row:
        raise HTTPException(404, f"JGO {body.jgo} no encontrado en HORARIOS")

    if body.ganador not in ("eq1", "eq2"):
        raise HTTPException(400, "ganador debe ser 'eq1' o 'eq2'")

    ganador_name = eq1_name if body.ganador == "eq1" else eq2_name
    if not ganador_name:
        raise HTTPException(400, f"El equipo {body.ganador} no tiene nombre asignado aún")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _sheets_lock:
        ws_h.update(
            [[body.estado, str(body.gol1), str(body.gol2), ganador_name, now]],
            f"H{target_row}:L{target_row}",
            value_input_option="RAW"
        )

    _invalidate_games()

    # Propagar nombres al bracket inmediatamente
    with _sheets_lock:
        ws_h2 = state["sh"].worksheet("HORARIOS")
    changes = _propagate_bracket(ws_h=ws_h2)

    return {
        "ok":      True,
        "jgo":     body.jgo,
        "ganador": ganador_name,
        "gol1":    body.gol1,
        "gol2":    body.gol2,
        "estado":  body.estado,
        "bracket_changes": changes,
    }


@app.post("/api/admin/sim-range")
async def admin_sim_range(body: dict = None, ql_admin: str = Cookie(default="")):
    """
    Modo Prueba: simula resultados aleatorios para un rango de JGOs.
    Genera goles random y elige un ganador al azar entre eq1/eq2.
    Al final propaga el bracket.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    if body is None:
        body = {}
    try:
        return await _admin_sim_range_impl(body)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[sim-range] ERROR: {e}\n{tb}")
        raise HTTPException(500, f"Error interno: {e}")


async def _admin_sim_range_impl(body: dict):

    jgo_desde = int(body.get("jgo_desde", 1))
    jgo_hasta = int(body.get("jgo_hasta", 16))
    if jgo_desde < 1 or jgo_hasta > 64 or jgo_desde > jgo_hasta:
        raise HTTPException(400, "Rango de JGO inválido")

    import random

    cfg         = state.get("cfg", {})
    fila_inicio = int(cfg.get("FILA_INICIO_DATOS", 3))
    total       = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    fila_fin    = fila_inicio + total - 1

    with _sheets_lock:
        ws_h = state["sh"].worksheet("HORARIOS")
        filas = _sheets_retry(lambda: ws_h.get(f"A{fila_inicio}:K{fila_fin}"))

    # Marcadores para cada tipo de resultado (25% cada categoria):
    # 1) Gana eq1 (score no empate)   2) Gana eq2 (score no empate)
    # 3) Empate + gana eq1 (penales)  4) Empate + gana eq2 (penales)
    _SCORES_WIN1  = [(1,0),(2,0),(2,1),(3,0),(3,1),(3,2)]
    _SCORES_WIN2  = [(0,1),(0,2),(1,2),(0,3),(1,3),(2,3)]
    _SCORES_DRAW  = [(1,1),(2,2),(0,0),(3,3),(1,1),(2,2)]  # pesos realistas

    results = []
    batch_updates = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, fila in enumerate(filas):
        def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
        jgo_str = c(0)
        if not jgo_str or not jgo_str.isdigit():
            continue
        jgo = int(jgo_str)
        if jgo < jgo_desde or jgo > jgo_hasta:
            continue
        eq1 = c(4)
        eq2 = c(5)
        if not eq1 or not eq2 or _parse_bracket_ref(eq1) or _parse_bracket_ref(eq2):
            results.append({"jgo": jgo, "skip": True, "razon": "Equipos aún no definidos (propaga ronda anterior primero)"})
            continue

        # 25% cada resultado
        tipo = random.randint(1, 4)
        if tipo == 1:
            # Gana eq1 en 90'
            g1, g2 = random.choice(_SCORES_WIN1)
            ganador = eq1
        elif tipo == 2:
            # Gana eq2 en 90'
            g1, g2 = random.choice(_SCORES_WIN2)
            ganador = eq2
        elif tipo == 3:
            # Empate en 90' → gana eq1 por penales
            g1, g2 = random.choice(_SCORES_DRAW)
            ganador = eq1
        else:
            # Empate en 90' → gana eq2 por penales
            g1, g2 = random.choice(_SCORES_DRAW)
            ganador = eq2

        sheet_row = fila_inicio + i
        batch_updates.append({
            "range":  f"H{sheet_row}:L{sheet_row}",
            "values": [["FINAL", str(g1), str(g2), ganador, now]]
        })
        results.append({"jgo": jgo, "eq1": eq1, "eq2": eq2,
                        "g1": g1, "g2": g2, "ganador": ganador})

    if batch_updates:
        with _sheets_lock:
            ws_h2 = state["sh"].worksheet("HORARIOS")
            _sheets_retry(lambda: ws_h2.batch_update(batch_updates, value_input_option="RAW"))

    _invalidate_games()

    # Propagar bracket al final
    changes = _propagate_bracket()

    return {
        "ok":      True,
        "applied": len([r for r in results if not r.get("skip")]),
        "skipped": len([r for r in results if r.get("skip")]),
        "results": results,
        "bracket_changes": changes,
    }


@app.post("/api/admin/fix-scoring-formulas")
async def admin_fix_scoring_formulas(ql_admin: str = Cookie(default="")):
    """
    Actualiza las fórmulas de scoring (O,P,Q,R,S) en TODAS las pestañas de jugadores.
    Esquema F2: Logro(2) + Ganador(2) + Gol EQ1(1) + Gol EQ2(1) = máx 6 pts.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    try:
        RESERVED = RESERVED_TABS
        cfg       = state.get("cfg", {})
        fila_data = int(cfg.get("FILA_INICIO_DATOS", 3)) + 1
        total     = int(cfg.get("TOTAL_JUEGOS_F2", 32))
        with _sheets_lock:
            worksheets = state["sh"].worksheets()
        updated = 0
        for ws in worksheets:
            if ws.title in RESERVED:
                continue
            batch = []
            for i in range(total):
                r = fila_data + i
                f_logro = f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X"));2;0);"")'
                f_gan   = f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(J{r}=M{r};2;0);"")'
                f_gol1  = f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(G{r}&""=K{r}&"";1;0);"")'
                f_gol2  = f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(H{r}&""=L{r}&"";1;0);"")'
                f_total = f'=IF(AND(N{r}<>"";N{r}<>"PROG");IFERROR(SUM(O{r}:R{r});0);"")'
                batch += [{"range": f"O{r}", "values": [[f_logro]]},
                          {"range": f"P{r}", "values": [[f_gan]]},
                          {"range": f"Q{r}", "values": [[f_gol1]]},
                          {"range": f"R{r}", "values": [[f_gol2]]},
                          {"range": f"S{r}", "values": [[f_total]]}]
            try:
                with _sheets_lock:
                    _sheets_retry(lambda w=ws, b=batch: w.batch_update(b, value_input_option="USER_ENTERED"))
                updated += 1
                time.sleep(1.5)   # evitar 429 quota entre pestañas
            except Exception as e:
                print(f"[fix-scoring] Error en {ws.title}: {e}")
                time.sleep(2.0)
        return {"ok": True, "tabs_actualizadas": updated,
                "msg": f"✅ Fórmulas actualizadas en {updated} pestañas"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/admin/propagate-bracket")
async def admin_propagate_bracket(ql_admin: str = Cookie(default="")):
    """
    Modo Prueba: recorre HORARIOS y actualiza EQ1/EQ2 de juegos futuros
    basándose en los GANADOR actuales. Retorna lista de cambios.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    try:
        changes = _propagate_bracket()
        _invalidate_games()
        return {"ok": True, "changes": changes, "total": len(changes)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/admin/refresh-bracket-refs")
async def admin_refresh_bracket_refs(ql_admin: str = Cookie(default="")):
    """
    Resetea EQ1/EQ2 de juegos R16+ volviendo a la data de ESPN (placeholders
    originales como 'Round of 32 X Winner'), luego corre propagate-bracket.
    Necesario cuando los cruces quedaron mal por el mismatch ESPN-slot vs JGO.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    def _run():
        try:
            cfg      = state["cfg"]
            league   = cfg.get("ESPN_LEAGUE", "fifa.world")
            leagues  = [l.strip() for l in league.split(",") if l.strip()]
            fila_ini = int(cfg.get("FILA_INICIO_DATOS", 3))
            total    = int(cfg.get("TOTAL_JUEGOS_F2", 32))
            fila_fin = fila_ini + total - 1
            sh       = state["sh"]

            state["_setup_status"] = "running — leyendo HORARIOS..."
            with _sheets_lock:
                ws_h  = sh.worksheet("HORARIOS")
                filas = ws_h.get(f"A{fila_ini}:G{fila_fin}")

            summary_base = f"{ESPN_BASE}/{leagues[0]}/summary"

            # WC2026 ESPN slot → posición cronológica para R32
            _WC_R32 = {1:3, 2:6, 3:1, 4:4, 5:12, 6:11, 7:10, 8:9,
                       9:2, 10:5, 11:7, 12:8, 13:15, 14:14, 15:13, 16:16}
            _is_wc  = ("fifa"  in league.lower() or "world" in league.lower())

            batch = []
            refreshed = 0
            skipped   = 0

            for i, fila in enumerate(filas):
                def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
                ronda    = c(1)
                if ronda not in ('R16', 'QF', 'SF', '3ER', 'FINAL'):
                    continue
                espn_id = c(6)
                if not espn_id:
                    skipped += 1
                    continue

                row = fila_ini + i
                state["_setup_status"] = f"running — JGO {c(0)} ({ronda}) fetching ESPN..."

                data = (espn_get(summary_base, {"event": espn_id, "lang": "es"}) or
                        espn_get(ESPN_FALLBACK,  {"event": espn_id, "lang": "es"}))
                if not data:
                    skipped += 1
                    time.sleep(0.3)
                    continue

                try:
                    comp = data["header"]["competitions"][0]
                except (KeyError, IndexError):
                    skipped += 1
                    time.sleep(0.3)
                    continue

                competitors = comp.get("competitors", [])
                new_eq1 = new_eq2 = None

                for cx in competitors:
                    name = cx.get("team", {}).get("displayName", "") or ""
                    name = name.strip()
                    if not name:
                        continue
                    # Si ESPN devuelve "Round of 32 X Winner" y es WC2026,
                    # traducir slot ESPN → nuestra posición cronológica
                    ref = _parse_bracket_ref(name)
                    if ref and ref['ronda'] == 'R32' and _is_wc:
                        new_nth = _WC_R32.get(ref['nth'])
                        if new_nth and new_nth != ref['nth']:
                            print(f"[refresh-bracket] slot {ref['nth']} → JGO {new_nth}: {name!r}")
                            name = f"Round of 32 {new_nth} Winner"
                    # Si ESPN ya devuelve equipos reales, úsalos tal cual
                    is_home = cx.get("homeAway") == "home"
                    if is_home:
                        new_eq1 = name
                    else:
                        new_eq2 = name

                # Fallback si homeAway no está definido
                if not new_eq1 and len(competitors) > 0:
                    new_eq1 = (competitors[0].get("team", {}).get("displayName", "") or "").strip()
                if not new_eq2 and len(competitors) > 1:
                    new_eq2 = (competitors[1].get("team", {}).get("displayName", "") or "").strip()

                cur_eq1 = c(4)
                cur_eq2 = c(5)
                changed = False
                if new_eq1 and new_eq1 != cur_eq1:
                    batch.append({"range": f"E{row}", "values": [[new_eq1]]})
                    print(f"[refresh-bracket] JGO {c(0)} EQ1: {cur_eq1!r} → {new_eq1!r}")
                    changed = True
                if new_eq2 and new_eq2 != cur_eq2:
                    batch.append({"range": f"F{row}", "values": [[new_eq2]]})
                    print(f"[refresh-bracket] JGO {c(0)} EQ2: {cur_eq2!r} → {new_eq2!r}")
                    changed = True
                if changed:
                    refreshed += 1
                time.sleep(0.3)

            if batch:
                with _sheets_lock:
                    ws_h.batch_update(batch, value_input_option="RAW")
                _invalidate_games()
                print(f"[refresh-bracket] {len(batch)} celdas actualizadas en {refreshed} juegos")

            # Propagar con resolve() ya corregido (WC2026 slot mapping activo)
            state["_setup_status"] = "running — propagando bracket..."
            try:
                chgs = _propagate_bracket(ws_h=ws_h)
                print(f"[refresh-bracket] propagate: {len(chgs)} cambios")
            except Exception as _pe:
                print(f"[refresh-bracket] propagate error: {_pe}")

            state["_setup_status"] = (
                f"done — {refreshed} juegos actualizados desde ESPN, bracket propagado."
            )

        except Exception as e:
            import traceback
            print(f"[refresh-bracket] ERROR: {traceback.format_exc()}")
            state["_setup_status"] = f"ERROR: {e}"

    state["_setup_status"] = "running"
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "msg": "Refresh bracket iniciado"}


@app.post("/api/admin/sync-real-results")
async def admin_sync_real_results(ql_admin: str = Cookie(default="")):
    """
    Sincroniza los resultados reales del Mundial desde ESPN (usa ESPN_ID real,
    ignora MODO_PRUEBA y el estado FINAL para forzar la actualización).
    Actualiza HORARIOS, propaga el bracket y recalcula la tabla.
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    cfg          = state.get("cfg", {})
    fila_inicio  = int(cfg.get("FILA_INICIO_DATOS", 3))
    total_juegos = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    fila_fin     = fila_inicio + total_juegos - 1

    with _sheets_lock:
        ws_h  = state["sh"].worksheet("HORARIOS")
        filas = ws_h.get(f"A{fila_inicio}:L{fila_fin}")

    batch    = []
    updated  = []
    skipped  = []

    for i, fila in enumerate(filas):
        row = fila_inicio + i
        def cel(c, f=fila): return f[c-1].strip() if len(f) > c-1 else ""

        jgo       = cel(1)
        espn_id   = cel(7)   # col G = ESPN_ID real
        eq1_sheet = cel(5)   # col E = EQUIPO 1
        eq2_sheet = cel(6)   # col F = EQUIPO 2

        if not jgo or not espn_id:
            continue

        try:
            data = espn_get(_espn_summary_url(), {"event": espn_id}) or \
                   espn_get(ESPN_FALLBACK,       {"event": espn_id})
            if not data:
                skipped.append(f"JGO {jgo}: sin datos ESPN")
                continue

            sc = parse_score(data)
            if not sc or sc["estado"] == "PROG":
                skipped.append(f"JGO {jgo}: aún no jugado ({sc.get('estado','?') if sc else '?'})")
                continue

            ganador  = sc["ganador"]
            g1 = int(sc["gol1"]) if str(sc.get("gol1","")).isdigit() else -1
            g2 = int(sc["gol2"]) if str(sc.get("gol2","")).isdigit() else -1
            eq1_real = eq1_sheet or sc.get("eq1", "")
            eq2_real = eq2_sheet or sc.get("eq2", "")
            if eq1_real and eq2_real:
                if g1 > g2:
                    ganador = eq1_real
                elif g2 > g1:
                    ganador = eq2_real
                elif sc["ganador"]:
                    ganador = eq1_real if sc["ganador"] == sc.get("eq1","") else eq2_real

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch.append({
                "range":  f"H{row}:L{row}",
                "values": [[sc["estado"], sc["gol1"], sc["gol2"], ganador, now]]
            })
            freeze = cfg.get("FREEZE_EQUIPOS", "0").strip() not in ("", "0", "false", "no")
            if not freeze:
                eq1_espn = sc.get("eq1", "")
                eq2_espn = sc.get("eq2", "")
                if eq1_espn and not eq1_sheet:
                    batch.append({"range": f"E{row}", "values": [[eq1_espn]]})
                if eq2_espn and not eq2_sheet:
                    batch.append({"range": f"F{row}", "values": [[eq2_espn]]})

            updated.append(f"JGO {jgo}: {eq1_real or '?'} {sc['gol1']}-{sc['gol2']} {eq2_real or '?'} ({ganador})")
            time.sleep(0.3)

        except Exception as ex:
            skipped.append(f"JGO {jgo}: error — {ex}")

    if batch:
        with _sheets_lock:
            ws_h.batch_update(batch, value_input_option="RAW")
        _invalidate_games()

    try:
        changes = _propagate_bracket()
    except Exception as e:
        changes = [f"Error propagando: {e}"]

    try:
        _update_standings()
    except Exception:
        pass

    return {
        "ok":      True,
        "updated": updated,
        "skipped": skipped,
        "bracket": changes,
        "total":   len(updated),
    }


@app.post("/api/admin/clear-cache")
async def admin_clear_cache(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    t = _torneo_activo()
    if t["activo"]:
        raise HTTPException(423, f'Torneo en curso — panel bloqueado. {t["razon"]}')
    _invalidate_games()
    _cache["players"].clear()
    state["cfg"] = read_config(state["sh"])
    return {"ok": True, "msg": "Caché limpiado y config recargada"}


@app.post("/api/admin/reset-test")
async def admin_reset_test(body: dict = None, ql_admin: str = Cookie(default="")):
    """
    Modo Prueba: borra resultados en HORARIOS y picks en pestañas de jugadores
    para poder iniciar una nueva ronda de pruebas desde cero.
    body.ronda_desde: 'R32' = borrar todo | 'R16' = conservar resultados R32
    """
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    if body is None:
        body = {}

    ronda_desde = (body.get("ronda_desde") or "R32").upper()
    if ronda_desde not in ("R32", "R16", "QF", "SF"):
        raise HTTPException(400, "ronda_desde inválido. Usa: R32, R16, QF, SF")

    _RONDA_ORDER = {"R32": 0, "R16": 1, "QF": 2, "SF": 3, "3ER": 4, "FINAL": 5}
    from_idx = _RONDA_ORDER.get(ronda_desde, 0)

    cfg         = state.get("cfg", {})
    fila_inicio = int(cfg.get("FILA_INICIO_DATOS", 3))
    total       = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    fila_fin    = fila_inicio + total - 1

    sh = state["sh"]
    log = []

    with _sheets_lock:
        ws_h  = sh.worksheet("HORARIOS")
        filas = ws_h.get(f"A{fila_inicio}:M{fila_fin}")  # hasta M = ESPN_ID_TEST

    # ── Placeholders estándar WC bracket para restaurar en R16+ tras reset ──────
    # Permite que _propagate_bracket vuelva a funcionar correctamente
    _RONDA_LABEL = {
        "R32":   ("Dieciseisavos de Final", "Dieciseisavos"),
        "R16":   ("Octavos de Final",       "Octavos"),
        "QF":    ("Cuartos de Final",       "Cuartos"),
        "SF":    ("Semifinal",              "Semifinal"),
    }
    # Índice nth dentro de cada ronda (para "Ganador Ronda (nth)")
    _ronda_nth: dict = {}   # ronda → contador de slots usados

    def _placeholder(ronda_src: str, nth: int, tipo: str = "winner") -> str:
        label = _RONDA_LABEL.get(ronda_src, ("",))[0]
        if not label:
            return ""
        if tipo == "loser":
            return f"Perdedor {_RONDA_LABEL.get(ronda_src, ('',))[1]} {nth}"
        return f"Ganador {label} ({nth})"

    # Mapeo: ronda del juego → ronda de la que vienen sus equipos
    _SRC_RONDA = {"R16": "R32", "QF": "R16", "SF": "QF",
                  "3ER": "SF",  "FINAL": "SF"}

    # Pre-computar counters por ronda
    _ph_counter: dict = {}  # ronda_src → slot actual

    # ── 1. Borrar resultados en HORARIOS para las rondas elegidas ─────────────
    clear_h_ranges = []
    restore_batch  = []   # para restaurar placeholders en E:F de R16+

    for i, fila in enumerate(filas):
        def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
        ronda = c(1)
        if not c(0):
            continue
        row_idx = _RONDA_ORDER.get(ronda, -1)
        if row_idx < from_idx:
            continue   # ronda anterior al inicio → conservar
        sheet_row = fila_inicio + i
        # Borrar H:M = estado, gol1, gol2, ganador, timestamp, ESPN_ID_TEST
        clear_h_ranges.append(f"H{sheet_row}:M{sheet_row}")

        # Para R16+: restaurar placeholders en E:F en lugar de dejarlos vacíos
        # Así _propagate_bracket puede rellenarlos con equipos reales tras simular
        if ronda != "R32":
            src = _SRC_RONDA.get(ronda, "")
            if ronda == "3ER":
                # 3er puesto: perdedores de SF 1 y SF 2
                ph_eq1 = "Perdedor Semifinal 1"
                ph_eq2 = "Perdedor Semifinal 2"
            elif ronda == "FINAL":
                ph_eq1 = "Ganador Semifinal 1"
                ph_eq2 = "Ganador Semifinal 2"
            else:
                n1 = _ph_counter.get(src, 0) + 1
                n2 = n1 + 1
                _ph_counter[src] = n2
                label = _RONDA_LABEL.get(src, ("",))[0]
                ph_eq1 = f"Ganador {label} ({n1})" if label else ""
                ph_eq2 = f"Ganador {label} ({n2})" if label else ""
            if ph_eq1 and ph_eq2:
                restore_batch.append({
                    "range":  f"E{sheet_row}:F{sheet_row}",
                    "values": [[ph_eq1, ph_eq2]]
                })
        log.append(f"HORARIOS row {sheet_row} ({ronda}) → limpiado")

    if clear_h_ranges:
        with _sheets_lock:
            ws_h2 = sh.worksheet("HORARIOS")
            ws_h2.batch_clear(clear_h_ranges)

    if restore_batch:
        with _sheets_lock:
            ws_h3 = sh.worksheet("HORARIOS")
            ws_h3.batch_update(restore_batch, value_input_option="RAW")

    # ── 2. Borrar picks en todas las pestañas de jugadores ────────────────────
    reserved = RESERVED_TABS
    with _sheets_lock:
        all_ws = sh.worksheets()

    # Calcular qué filas (JGO → row en pestaña) borrar: row = jgo + 3
    # Obtener JGOs a limpiar desde HORARIOS
    jgos_a_limpiar = []
    for i, fila in enumerate(filas):
        def c(idx, f=fila): return f[idx].strip() if len(f) > idx else ""
        ronda = c(1)
        jgo_str = c(0)
        if not jgo_str:
            continue
        row_idx = _RONDA_ORDER.get(ronda, -1)
        if row_idx >= from_idx and jgo_str.isdigit():
            jgos_a_limpiar.append(int(jgo_str))

    pick_ranges = []
    for jgo in jgos_a_limpiar:
        tab_row = jgo + 3   # JGO 1 → row 4, JGO 17 → row 20
        # F:J = PICK_EQ1, PICK_GOL1, PICK_GOL2, PICK_EQ2, PICK_GANADOR
        pick_ranges.append(f"F{tab_row}:J{tab_row}")

    tabs_limpiadas = 0
    for ws in all_ws:
        if ws.title in reserved:
            continue
        try:
            with _sheets_lock:
                ws.batch_clear(pick_ranges)
            tabs_limpiadas += 1
        except Exception as e:
            log.append(f"WARN pestaña {ws.title}: {e}")

    # ── 3. Limpiar POSICIONES ─────────────────────────────────────────────────
    with _sheets_lock:
        ws_p = sh.worksheet("POSICIONES")
        ws_p.batch_clear(["A3:Z100"])

    _invalidate_games()
    _cache["players"].clear()

    msg = (f"✅ Reset desde {ronda_desde}: "
           f"{len(clear_h_ranges)} rangos en HORARIOS, "
           f"{tabs_limpiadas} pestañas de jugadores, "
           f"POSICIONES limpiada.")
    return {"ok": True, "msg": msg, "log": log[:20]}


@app.post("/api/admin/reset")
async def admin_reset(body: ArchiveResetBody, ql_admin: str = Cookie(default="")):
    """Archiva el sheet actual en Drive y resetea el original para nueva quiniela."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    cfg = state.get("cfg", {})
    reset_key = cfg.get("RESET_KEY", "RESET2026")
    if body.keyword.strip() != reset_key:
        raise HTTPException(403, "Clave incorrecta")

    sh  = state["sh"]
    torneo     = cfg.get("TORNEO", "Quiniela")
    fecha_ini  = cfg.get("FECHA_INICIO_F2", "").replace("-", "")
    fecha_fin  = cfg.get("FECHA_FIN_F2",    "").replace("-", "")
    copy_name  = f"{torneo}.{fecha_ini}.{fecha_fin}"

    # ── 1. Archivar: copiar en Drive y transferir propiedad al dueño original ──
    archive_warn = ""
    try:
        from googleapiclient.discovery import build as _gapi_build
        from google.oauth2.service_account import Credentials as _Creds
        creds = _Creds.from_service_account_file(
            os.environ.get("QL_CREDS", "credentials.json"), scopes=SCOPES)
        drive = _gapi_build("drive", "v3", credentials=creds, cache_discovery=False)

        # Obtener el dueño original del spreadsheet
        file_info = drive.files().get(fileId=sh.id, fields="owners,parents").execute()
        owner_email = (file_info.get("owners") or [{}])[0].get("emailAddress", "")
        parents = file_info.get("parents", [])

        # Crear la copia (en el mismo directorio que el original)
        body = {"name": copy_name}
        if parents:
            body["parents"] = parents
        copy_meta = drive.files().copy(
            fileId=sh.id, body=body, supportsAllDrives=True
        ).execute()
        copy_id = copy_meta.get("id")
        print(f"[reset] Copia creada: {copy_name} (id={copy_id})")

        # Transferir propiedad al dueño original para que use su cuota
        if owner_email:
            drive.permissions().create(
                fileId=copy_id,
                body={"type": "user", "role": "owner", "emailAddress": owner_email},
                transferOwnership=True,
                supportsAllDrives=True
            ).execute()
            print(f"[reset] Propiedad transferida a {owner_email}")

    except Exception as e:
        archive_warn = f"⚠️ No se pudo archivar en Drive ({e}). "
        print(f"[reset] WARN archivo Drive: {e}")

    # ── 2. Resetear original ──
    reserved = RESERVED_TABS
    with _sheets_lock:
        # Borrar pestañas de jugadores
        for ws in sh.worksheets():
            if ws.title not in reserved:
                sh.del_worksheet(ws)
            time.sleep(0.1)
        # Limpiar JUGADORES (mantener headers)
        ws_j = sh.worksheet("JUGADORES")
        rows = ws_j.get_all_values()
        hi, _ = _jugadores_headers(rows)
        first_data = hi + 2
        if len(rows) >= first_data:
            ws_j.batch_clear([f"A{first_data}:Z{len(rows) + 5}"])
        # Limpiar POSICIONES
        ws_p = sh.worksheet("POSICIONES")
        ws_p.batch_clear(["A3:Z100"])
        # Limpiar HORARIOS completo (datos + resultados) para que admin recargue con ESPN
        cfg2 = state.get("cfg", {})
        fila_ini = int(cfg2.get("FILA_INICIO_DATOS", 3))
        ws_h = sh.worksheet("HORARIOS")
        ws_h.batch_clear([f"A{fila_ini}:L1000"])

    _cache["players"].clear()
    _invalidate_games()
    msg = f"{archive_warn}Sheet reseteado exitosamente." if archive_warn else f"✅ Archivado como '{copy_name}' en tu Drive y sheet reseteado."
    return {"ok": True, "msg": msg}


# ─── Probabilidades ───────────────────────────────────────────────────────────

def _compute_probabilities() -> dict:
    """
    Calcula la probabilidad de ganar 1er y 2do lugar por jugador.
    Dos algoritmos (igual que QuinielaProbabilitiesService.cs):

    1. Simple (basado en peso):
       weight = max(0, maxPosible − liderActual + 1)
       prob_1st = weight / sum_weights * 100

    2. Universo (cada jugador como "realidad" para juegos pendientes):
       Por cada universo U, se usa el pick de U como resultado hipotético;
       se cuentan en cuántos universos cada jugador gana 1ro/2do.

    Puntuación fútbol: GAN=3pts, G1=1pt, G2=1pt (max 5 pts/partido).
    """
    import concurrent.futures

    sh = state.get("sh")
    if not sh:
        return {"players": [], "fixed_games": 0, "pending_games": 0,
                "computed_at": datetime.now().isoformat(), "error": "sin conexión"}

    cfg    = state.get("cfg", {})
    total_j = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total_j

    # ── Obtener lista de jugadores ──────────────────────────────────────────
    if not _players_cache_ok():
        _load_players_cache()

    seen_tabs: set = set()
    players = []
    for k, v in _cache["players"].items():
        if k.startswith("phone:") and v.get("TAB_NOMBRE") and v.get("NOMBRE"):
            tab = v["TAB_NOMBRE"]
            if tab not in seen_tabs:
                seen_tabs.add(tab)
                players.append(v)

    if not players:
        return {"players": [], "fixed_games": 0, "pending_games": 0,
                "computed_at": datetime.now().isoformat()}

    # ── Leer TODOS los tabs en batch (1-2 requests en vez de N) ────────────
    t_read = time.time()
    tab_data_map = _batch_read_player_tabs(sh, players, last_row)
    print(f"[prob] {len(players)} tabs leídos en {time.time()-t_read:.1f}s (batch)")

    # Estructura: player_tabs[nombre] = { jgo_str: {g1pick, g2pick, ganpick, estado, pts_total} }
    player_tabs: dict = {}

    for p in players:
        nombre = p.get("NOMBRE", p.get("TAB_NOMBRE", "?"))
        tab_data = tab_data_map.get(p["TAB_NOMBRE"], [])
        games_data: dict = {}
        for row in tab_data:
            def c(i, r=row): return r[i].strip() if len(r) > i else ""
            jgo = c(0)
            if not jgo:
                continue
            games_data[jgo] = {
                "g1pick":    c(6),   # G – PICK_GOL1
                "g2pick":    c(7),   # H – PICK_GOL2
                "ganpick":   c(9),   # J – PICK_GANADOR
                "estado":    c(13),  # N – ESTADO
                "pts_total": c(18),  # S – PTS_TOTAL F2
            }
        player_tabs[nombre] = games_data

    if not player_tabs:
        return {"players": [], "fixed_games": 0, "pending_games": 0,
                "computed_at": datetime.now().isoformat()}

    # ── Clasificar juegos: fijos vs pendientes ──────────────────────────────
    all_jgos: set = set()
    for gdata in player_tabs.values():
        all_jgos.update(gdata.keys())

    def _is_fixed(jgo) -> bool:
        for gdata in player_tabs.values():
            g = gdata.get(jgo)
            if g:
                est = g.get("estado", "")
                return bool(est) and est != "PROG"
        return False

    fixed_jgos   = {jgo for jgo in all_jgos if _is_fixed(jgo)}
    pending_jgos = all_jgos - fixed_jgos
    has_any_fixed = bool(fixed_jgos)

    # ── Puntos actuales por jugador (juegos fijos/en curso) ─────────────────
    def _current_pts(nombre) -> int:
        total = 0
        for jgo, g in player_tabs.get(nombre, {}).items():
            est = g.get("estado", "")
            if est and est != "PROG":
                try:
                    total += int(float(g.get("pts_total", "") or 0))
                except Exception:
                    pass
        return total

    MAX_PTS_GAME = 6  # F2: Logro(2) + Ganador(2) + Gol1(1) + Gol2(1) = max 6 pts

    player_names = sorted(player_tabs.keys())

    # ── Standings para probabilidad simple ──────────────────────────────────
    standings = []
    for pname in player_names:
        cur = _current_pts(pname)
        gdata = player_tabs[pname]
        rem = sum(MAX_PTS_GAME for jgo in pending_jgos if jgo in gdata)
        standings.append({
            "name":         pname,
            "current_pts":  cur,
            "remaining_max": rem,
            "max_possible": cur + rem,
            "univ_1st":     0.0,
            "univ_2nd":     0.0,
        })

    standings.sort(key=lambda x: (-x["current_pts"], -x["max_possible"], x["name"]))

    # Asignar rangos (empates = mismo rango)
    for i, s in enumerate(standings):
        if i == 0:
            s["rank"] = 1
        else:
            prev = standings[i - 1]
            same = (s["current_pts"] == prev["current_pts"] and
                    s["max_possible"] == prev["max_possible"])
            s["rank"] = prev["rank"] if same else i + 1

    # ── Probabilidad basada en equipos vivos ───────────────────────
    # Logica: si el equipo que un jugador aposte ya fue eliminado en un
    # partido terminado, ese pick ya no puede generar puntos.
    # effective_max[jugador] = pts_actuales + (juegos pendientes donde su
    #                          ganpick sigue vivo) * MAX_PTS_GAME
    # La probabilidad se distribuye proporcionalmente al effective_max.

    if has_any_fixed:
        # Obtener HORARIOS para saber eq1/eq2/ganador de cada juego
        try:
            _games_list, _ = _get_games_cache()
            horarios_map = {g["jgo"]: g for g in _games_list}
        except Exception:
            horarios_map = {}

        # Equipos eliminados = perdedor de cada juego terminado
        eliminated: set = set()
        for jgo in fixed_jgos:
            hor = horarios_map.get(jgo, {})
            winner = hor.get("ganador", "").strip()
            eq1    = hor.get("eq1", "").strip()
            eq2    = hor.get("eq2", "").strip()
            if winner and eq1 and eq2:
                loser = eq2 if winner == eq1 else eq1
                if loser:
                    eliminated.add(loser)

        # effective_max por jugador
        eff_max: dict = {}
        alive_rem: dict = {}  # cuantos juegos pendientes tienen equipo vivo
        for pname in player_names:
            cur   = _current_pts(pname)
            gdata = player_tabs[pname]
            alive_pts = 0
            alive_cnt = 0
            for jgo in pending_jgos:
                g = gdata.get(jgo)
                if not g:
                    continue
                ganpick = g.get("ganpick", "").strip()
                # El pick es viable si: hay equipo elegido Y ese equipo no fue eliminado
                if ganpick and ganpick not in eliminated:
                    alive_pts += MAX_PTS_GAME
                    alive_cnt += 1
            eff_max[pname]   = cur + alive_pts
            alive_rem[pname] = alive_cnt

        # Actualizar standings con effective_max y conteo de picks vivos
        for s in standings:
            pname = s["name"]
            s["max_possible"] = eff_max[pname]      # sobreescribir con max real
            s["alive_picks"]  = alive_rem[pname]    # cuantos picks aun vivos

        # Lider: jugador con mas puntos actuales (ya ordenado por current_pts)
        leader_pts = max((s["current_pts"] for s in standings), default=0)

        # Candidatos a 1ro: pueden alcanzar o superar al lider actual
        candidates_1st = [
            pname for pname in player_names
            if eff_max[pname] >= leader_pts
        ]

        total_eff = sum(eff_max[pname] for pname in candidates_1st)

        for s in standings:
            pname = s["name"]
            if pname in candidates_1st and total_eff > 0:
                s["univ_1st"] = round((eff_max[pname] / total_eff) * 100.0, 2)
            else:
                s["univ_1st"] = 0.0

        # 2do lugar: entre los que no ganan 1ro
        # Candidatos a 2do: los que pueden alcanzar al 2do actual
        sorted_cur = sorted(player_names, key=lambda p: -_current_pts(p))
        second_pts = _current_pts(sorted_cur[1]) if len(sorted_cur) > 1 else 0
        candidates_2nd = [
            pname for pname in player_names
            if pname not in candidates_1st and eff_max[pname] >= second_pts
        ]
        # Si no hay candidatos a 2do fuera de candidatos a 1ro, tomar el resto
        if not candidates_2nd:
            candidates_2nd = [p for p in player_names if p not in candidates_1st]

        total_eff2 = sum(eff_max[pname] for pname in candidates_2nd)
        for s in standings:
            pname = s["name"]
            if pname in candidates_2nd and total_eff2 > 0:
                s["univ_2nd"] = round((eff_max[pname] / total_eff2) * 100.0, 2)
            else:
                s["univ_2nd"] = 0.0

        # Re-ordenar por prob 1er lugar, desempate pts actuales, luego nombre
        standings.sort(key=lambda x: (-x["univ_1st"], -x["current_pts"], x["name"]))

    return {
        "players":      standings,
        "fixed_games":  len(fixed_jgos),
        "pending_games": len(pending_jgos),
        "computed_at":  datetime.now().isoformat(),
    }



@app.get("/api/teams")
async def get_teams():
    """Retorna lista de equipos que participan en F2 (tomados de las filas R32 de HORARIOS)."""
    games, _ = _get_games_cache()
    r32 = [g for g in games if g.get("ronda") == "R32"]
    teams = set()
    for g in r32:
        if g.get("eq1"): teams.add(g["eq1"])
        if g.get("eq2"): teams.add(g["eq2"])
    # Si no hay R32 cargado aun, devolver todos los equipos presentes
    if not teams:
        for g in games:
            if g.get("eq1"): teams.add(g["eq1"])
            if g.get("eq2"): teams.add(g["eq2"])
    return {"teams": sorted(list(teams))}


@app.get("/api/probabilities")
async def get_probabilities():
    """
    Retorna probabilidad de ganar 1er/2do lugar por jugador.
    Cachea el resultado por PROB_TTL segundos (3 min).
    """
    try:
        now = time.time()
        if _cache["prob"] is None or now - _cache["prob_ts"] > PROB_TTL:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _compute_probabilities)
            _cache["prob"]    = result
            _cache["prob_ts"] = time.time()
        return _cache["prob"]
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(503, f"Error calculando probabilidades: {e}")


# ── Aliases para nombres que usa el frontend ──────────────────────────────────
@app.get("/api/admin/setup-status")
async def admin_setup_status(ql_admin: str = Cookie(default="")):
    """Estado del proceso de carga de partidos ESPN."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return {"status": state.get("_setup_status", "idle")}

@app.post("/api/admin/test-telegram")
async def admin_test_telegram_alias(ql_admin: str = Cookie(default="")):
    """Alias de /api/admin/telegram-test para el frontend."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return await tg_test(ql_admin=ql_admin)

@app.post("/api/admin/wa-send")
async def admin_wa_send_alias(ql_admin: str = Cookie(default="")):
    """Alias de /api/admin/wa-test para el frontend."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return await wa_test(ql_admin=ql_admin)


@app.post("/api/admin/setup")
async def admin_setup(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    t = _torneo_activo()
    if t["activo"]:
        raise HTTPException(423, f'Torneo en curso — panel bloqueado. {t["razon"]}')

    def _run():
        try:
            from datetime import date as _date, timedelta as _td
            cfg      = state["cfg"]
            league   = cfg.get("ESPN_LEAGUE", "fifa.world")
            fecha_ini = _date.fromisoformat(cfg.get("FECHA_INICIO_F2", "2026-07-01"))
            fecha_fin = _date.fromisoformat(cfg.get("FECHA_FIN_F2",    "2026-07-19"))
            total    = int(cfg.get("TOTAL_JUEGOS_F2", 32))
            fila_ini = int(cfg.get("FILA_INICIO_DATOS", 3))

            ws_h = state["sh"].worksheet("HORARIOS")

            # ââ Limpiar HORARIOS completo A3:L — cubre todos los datos viejos ââââââ
            # Usamos una fila final generosa (200) para garantizar que se limpie todo
            _sheets_retry(lambda: ws_h.batch_clear([f"A{fila_ini}:L1000"]))
            print(f"[admin-setup] HORARIOS limpiado A{fila_ini}:L1000")

            # ââ Soporte multi-liga: ESPN_LEAGUE puede ser "spa.1,eng.1,..." ââââââ
            leagues = [l.strip() for l in league.split(",") if l.strip()]
            ligas_map = {}
            try:
                for row in state["sh"].worksheet("Ligas").get_all_values()[1:]:
                    if len(row) >= 3 and row[1].strip() and row[2].strip():
                        ligas_map[row[1].strip()] = row[2].strip()
            except Exception:
                pass

            uid_filters = set()
            for lg in leagues:
                eid = ligas_map.get(lg, "")
                if eid:
                    uid_filters.add(f"l:{eid}")
                print(f"[admin-setup] Liga: {lg} | ESPN_ID: {eid or '(sin ID — usa endpoint directo)'}")

            summary_url = (f"{ESPN_BASE}/{leagues[0]}/summary" if len(leagues) == 1
                           else f"{ESPN_BASE}/all/summary")

            # ââ Paso 1: recolectar IDs día a día ââââââââââââââââââââââââââââââââââ
            state["_setup_status"] = f"running — buscando partidos {fecha_ini} â {fecha_fin}..."
            eventos = []
            dia = fecha_ini
            # Cache del all/scoreboard por fecha para no pedirlo varias veces
            _all_cache: dict = {}
            while dia <= fecha_fin:
                fecha_str = dia.strftime("%Y%m%d")
                found = 0
                seen_ids: set = set()
                for lg in leagues:
                    eid_liga = ligas_map.get(lg, "")
                    try:
                        # 1) Intentar endpoint de liga especifica
                        r = requests.get(f"{ESPN_BASE}/{lg}/scoreboard",
                                         params={"dates": fecha_str, "limit": 500, "lang": "es"},
                                         timeout=15)
                        if r.status_code == 200:
                            ev_list = r.json().get("events", [])
                        else:
                            # 2) Fallback: all/scoreboard con filtro uid
                            if fecha_str not in _all_cache:
                                r2 = requests.get(f"{ESPN_BASE}/all/scoreboard",
                                                  params={"dates": fecha_str, "limit": 500, "lang": "es"},
                                                  timeout=15)
                                _all_cache[fecha_str] = r2.json().get("events", []) if r2.status_code == 200 else []
                                time.sleep(0.2)
                            uid_f = f"l:{eid_liga}" if eid_liga else None
                            ev_list = [e for e in _all_cache[fecha_str]
                                       if not uid_f or uid_f in e.get("uid", "")]
                        for ev in ev_list:
                            eid = ev.get("id")
                            if eid and eid not in seen_ids:
                                seen_ids.add(eid)
                                eventos.append({
                                    "id":        eid,
                                    "fecha_raw": ev.get("date", ""),
                                    "ev_data":   ev,
                                })
                                found += 1
                    except Exception as ex:
                        print(f"  [admin-setup] {dia}/{lg}: {ex}")
                    time.sleep(0.2)
                if found:
                    print(f"  [admin-setup] {dia}: {found} partidos encontrados")
                dia += _td(days=1)
                time.sleep(0.3)

            print(f"[admin-setup] Total eventos encontrados: {len(eventos)}")

            if not eventos:
                state["_setup_status"] = "ERROR: ESPN no devolvió juegos para esas fechas. Verifica liga y fechas en CONFIG."
                return

            eventos.sort(key=lambda x: x["fecha_raw"])
            print(f"[admin-setup] {len(eventos)} eventos encontrados")
            state["_setup_status"] = f"running — obteniendo info de {len(eventos)} partidos..."

            # ── Paso 2: obtener info de cada juego ───────────────────────────────
            def _fetch_summary(event_id):
                for url in [summary_url, f"{ESPN_BASE}/all/summary"]:
                    try:
                        r = requests.get(url, params={"event": event_id, "lang": "es"}, timeout=10)
                        if r.status_code == 200:
                            return r.json()
                    except Exception:
                        pass
                return None

            juegos = []
            total = len(eventos)
            for ev in eventos:
                data = _fetch_summary(ev["id"])
                if not data:
                    continue
                try:
                    comp = data["header"]["competitions"][0]
                except (KeyError, IndexError):
                    continue

                competitors = comp.get("competitors", [])
                eq1 = eq2 = ""
                for c in competitors:
                    n = c.get("team", {}).get("displayName", "")
                    if c.get("homeAway") == "home": eq1 = n
                    else: eq2 = n
                if not eq1 and len(competitors) >= 1: eq1 = competitors[0].get("team",{}).get("displayName","")
                if not eq2 and len(competitors) >= 2: eq2 = competitors[1].get("team",{}).get("displayName","")

                fecha_str = hora_str = ""
                raw = comp.get("date", "")
                if raw:
                    try:
                        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                        fecha_str = dt.strftime("%Y-%m-%d")
                        hora_str  = dt.strftime("%H:%M")
                    except Exception:
                        pass

                ronda = parse_ronda(comp, ev.get("ev_data"), eq1, eq2)

                # ── Extraer ESPN bracket slot (posición en el bracket de ESPN) ──
                # ESPN numera los juegos por posición de bracket, no por fecha.
                # Guardamos este valor para poder traducirlo al orden cronológico
                # que usamos en HORARIOS (Opción B - fix permanente de cruces).
                espn_slot = None
                for _bf in ['bracketOrder', 'bracketGameNumber', 'bracketNumber', 'bracketSequence']:
                    _v = comp.get(_bf)
                    if _v is not None:
                        espn_slot = _v
                        break
                if espn_slot is None:
                    _t = comp.get('type', {})
                    if isinstance(_t, dict):
                        espn_slot = _t.get('bracketOrder') or _t.get('bracketGameNumber')
                # Intentar también en data["competitions"] (distinto de header)
                if espn_slot is None:
                    _fc_list = data.get('competitions', [])
                    if _fc_list:
                        _fc = _fc_list[0]
                        for _bf in ['bracketOrder', 'bracketGameNumber', 'bracketNumber']:
                            _v = _fc.get(_bf)
                            if _v is not None:
                                espn_slot = _v
                                break
                # Intentar en el ev_data del scoreboard
                if espn_slot is None:
                    _ev_comps = (ev.get("ev_data") or {}).get("competitions", [{}])
                    if _ev_comps:
                        _ec = _ev_comps[0]
                        espn_slot = (_ec.get('bracketOrder') or _ec.get('bracketGameNumber')
                                     or _ec.get('bracketNumber'))
                if espn_slot is not None:
                    print(f"  [admin-setup] ESPN bracketSlot={espn_slot} para id={ev['id']} ({eq1} vs {eq2})")

                juegos.append({"id": ev["id"], "eq1": eq1, "eq2": eq2,
                               "fecha": fecha_str, "hora": hora_str,
                               "ronda": ronda,
                               "fecha_raw": ev.get("fecha_raw", ""),
                               "espn_slot": espn_slot})
                time.sleep(0.3)

            # ── Fallback por posición: si parse_ronda no pudo determinar la ronda ─
            # WC 2026: 16 juegos R32, 8 R16, 4 QF, 2 SF, 1 3ER, 1 FINAL = 32 total
            BRACKET_RONDAS = (["R32"]*16 + ["R16"]*8 + ["QF"]*4 +
                              ["SF"]*2 + ["3ER"] + ["FINAL"])
            for idx_j, j in enumerate(juegos):
                if not j["ronda"]:  # parse_ronda devolvió ""
                    j["ronda"] = BRACKET_RONDAS[idx_j] if idx_j < len(BRACKET_RONDAS) else "R32"

            # ── Opción B: traducir ESPN bracket slots → posición cronológica ────
            # ESPN numera cada juego por su posición de bracket (no por fecha).
            # "Round of 32 3 Winner" en ESPN puede referirse al JGO que nosotros
            # tenemos en la posición 1 cronológicamente.  Para que _propagate_bracket
            # resuelva correctamente, reescribimos esos refs con nuestra posición.
            #
            # Cómo funciona:
            #   1. Para cada ronda, ordenar los juegos por fecha (ya están así).
            #      El índice 1-based de cada juego en esa ronda es su posición cronológica.
            #   2. Si ESPN proporcionó bracketOrder para un juego, guardamos
            #      espn_slot → posición_cronológica en un mapa por ronda.
            #   3. Para juegos de rondas posteriores (R16, QF, …) cuyo eq1/eq2
            #      es "Round of X Y Winner", reemplazamos Y (slot ESPN) por Y' (nuestro
            #      orden cronológico), siempre que tengamos el mapa para esa ronda.

            # Mapa por ronda: {ronda_label: {espn_slot_int: chrono_pos_int}}
            _espn_to_chrono: dict = {}
            _RONDAS_BRACKET = ['R32', 'R16', 'QF', 'SF']
            _RONDA_SIZE     = {'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4}

            for _rlbl in _RONDAS_BRACKET:
                _rj = [j for j in juegos if j.get('ronda') == _rlbl]
                _sm: dict = {}
                for _chrono, _j in enumerate(_rj, start=1):
                    _slot = _j.get('espn_slot')
                    if _slot is not None:
                        try:
                            _sm[int(_slot)] = _chrono
                        except (ValueError, TypeError):
                            pass
                if _sm:
                    _espn_to_chrono[_rlbl] = _sm
                    print(f"[admin-setup] Bracket slot map {_rlbl}: {_sm}")

            if _espn_to_chrono:
                _translated = 0
                for _j in juegos:
                    for _fld in ('eq1', 'eq2'):
                        _ref = _parse_bracket_ref(_j[_fld])
                        if _ref and _ref['ronda'] in _espn_to_chrono:
                            _sm    = _espn_to_chrono[_ref['ronda']]
                            _espot = _ref['nth']
                            _cpos  = _sm.get(_espot)
                            if _cpos is not None and _cpos != _espot:
                                _rsize = _RONDA_SIZE.get(_ref['ronda'], 32)
                                _old   = _j[_fld]
                                _j[_fld] = f"Round of {_rsize} {_cpos} Winner"
                                print(f"[admin-setup] bracket-translate: {_old!r} → {_j[_fld]!r} "
                                      f"(ESPN slot {_espot} → chrono {_cpos})")
                                _translated += 1
                print(f"[admin-setup] Opción-B: {_translated} refs de bracket traducidos.")
            else:
                # Fallback WC2026 hardcodeado cuando ESPN no da bracketOrder
                _r32_cnt = sum(1 for _j in juegos if _j.get("ronda") == "R32")
                _is_wc26 = ("fifa" in league.lower() or "world" in league.lower()) and _r32_cnt == 16
                if _is_wc26:
                    _WC_FALLBACK = {1:3, 2:6, 3:1, 4:4, 5:12, 6:11, 7:10, 8:9,
                                   9:2, 10:5, 11:7, 12:8, 13:15, 14:14, 15:13, 16:16}
                    _wc_translated = 0
                    for _j in juegos:
                        for _fld in ("eq1", "eq2"):
                            _ref = _parse_bracket_ref(_j[_fld])
                            if _ref and _ref["ronda"] == "R32":
                                _cpos = _WC_FALLBACK.get(_ref["nth"])
                                if _cpos and _cpos != _ref["nth"]:
                                    _old = _j[_fld]
                                    _j[_fld] = f"Round of 32 {_cpos} Winner"
                                    print(f"[admin-setup] WC2026-fallback: {_old!r} -> {_j[_fld]!r}")
                                    _wc_translated += 1
                    print(f"[admin-setup] WC2026 fallback: {_wc_translated} refs traducidos.")
                else:
                    print("[admin-setup] AVISO: ESPN no proporcionó bracketOrder. "
                          "Los cruces de bracket quedan con el orden ESPN. "
                          "Verifica manualmente si los R16 están correctos.")

            # ── Asignar jornada a juegos sin grupo ───────────────────────────────
            jornada_base = int(cfg.get("JORNADA", 1) or 1)
            sin_grupo = all(not j["ronda"] for j in juegos)
            dia_ini_str = cfg.get("DIA_INICIO_JORNADA", "").strip()
            if sin_grupo and dia_ini_str != "":
                dia_ini = int(dia_ini_str or 1)
                from datetime import timedelta as _td2
                ref_week = (fecha_ini - _td2(days=dia_ini)).isocalendar()[1]
                for j in juegos:
                    try:
                        raw_fecha = j.get("fecha_raw", "")
                        if raw_fecha:
                            dt  = datetime.fromisoformat(raw_fecha.replace("Z", "+00:00"))
                            dt_shifted = dt - _td2(days=dia_ini)
                            semana_offset = (dt_shifted.isocalendar()[1] - ref_week)
                            if semana_offset < 0:
                                semana_offset += 52
                            j["ronda"] = str(jornada_base + semana_offset)
                        else:
                            j["ronda"] = str(jornada_base)
                    except Exception:
                        j["ronda"] = str(jornada_base)

            # ── Paso 3: escribir a HORARIOS ──────────────────────────────────────
            valores = [[i, j["ronda"], j["fecha"], j["hora"], j["eq1"], j["eq2"], j["id"]]
                       for i, j in enumerate(juegos, start=1)]

            if valores:
                rng = f"A{fila_ini}:G{fila_ini+len(valores)-1}"
                _sheets_retry(lambda v=valores, r=rng: ws_h.update(v, r, value_input_option="RAW"))
                print(f"[admin-setup] {len(valores)} filas escritas en HORARIOS")

            # ── Actualizar TOTAL_JUEGOS_F2 en CONFIG con el valor real ────────
            # ── Actualizar TOTAL_JUEGOS_F2 en CONFIG con el valor real ────────
            if valores:
                try:
                    ws_cfg = state["sh"].worksheet("CONFIG")
                    cfg_rows = ws_cfg.get_all_values()
                    for i, row in enumerate(cfg_rows):
                        if row and row[0].strip() == "TOTAL_JUEGOS_F2":
                            ws_cfg.update([[str(len(valores))]], f"B{i+1}")
                            break
                    state["cfg"] = read_config(state["sh"])
                except Exception:
                    pass

            _invalidate_games()
            state["_setup_status"] = f"done — {len(valores)} juegos cargados en HORARIOS"

        except Exception as e:
            import traceback
            err = traceback.format_exc()
            print(f"[admin-setup] ERROR: {err}")
            state["_setup_status"] = f"ERROR: {e}"

    state["_setup_status"] = "running"
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "msg": "Setup iniciado"}



# ─── Stripe ────────────────────────────────────────────────────────────────────
STRIPE_PK          = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_SK          = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SEC = os.getenv("STRIPE_WEBHOOK_SECRET", "")
QUINIELA_FEE_USD   = float(os.getenv("QUINIELA_FEE_USD", "10.00"))
_STRIPE_PCT        = 0.029
_STRIPE_FLAT       = 0.30

def _stripe_gross_up(net_usd: float) -> int:
    """Retorna centavos a cobrar para que después de comisión Stripe quede net_usd."""
    gross = (net_usd + _STRIPE_FLAT) / (1 - _STRIPE_PCT)
    return round(gross * 100)

def _mark_player_paid_internal(phone: str) -> bool:
    """Marca jugador como pagado sin requerir autenticación admin (para webhook)."""
    try:
        phone = _normalize_phone(phone)
        if not phone:
            return False
        ws, rows, header_idx, headers = _read_jugadores_cached()
        if "PAGADO" not in headers:
            col = len(headers) + 1
            with _sheets_lock:
                _sheets_retry(lambda: ws.update_cell(header_idx + 1, col, "PAGADO"))
            headers.append("PAGADO")
        pagado_col = headers.index("PAGADO") + 1
        phone_col  = (headers.index("WHATSAPP") + 1 if "WHATSAPP" in headers
                      else headers.index("TELEFONO") + 1 if "TELEFONO" in headers else None)
        if not phone_col:
            return False
        for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
            row_phone = _normalize_phone(row[phone_col - 1] if phone_col - 1 < len(row) else "")
            if row_phone == phone:
                with _sheets_lock:
                    _sheets_retry(lambda r=i, c=pagado_col: ws.update_cell(r, c, "1"))
                _invalidate_players()
                print(f"[stripe] Jugador {phone} marcado como pagado")
                return True
        return False
    except Exception as e:
        print(f"[stripe] Error marcando pagado: {e}")
        return False


# ─── Stripe endpoints ──────────────────────────────────────────────────────────

@app.get("/api/stripe/config")
async def stripe_config():
    gross = _stripe_gross_up(QUINIELA_FEE_USD)
    return {"publishable_key": STRIPE_PK, "amount_cents": gross,
            "amount_usd": round(gross / 100, 2), "net_usd": QUINIELA_FEE_USD, "currency": "usd"}

@app.post("/api/stripe/create-checkout")
async def stripe_create_checkout(body: dict):
    import stripe as _stripe
    _stripe.api_key = STRIPE_SK
    if not _stripe.api_key:
        raise HTTPException(503, "Stripe no configurado")
    phone  = body.get("phone", "")
    nombre = body.get("nombre", "")
    gross  = _stripe_gross_up(QUINIELA_FEE_USD)
    base_url = body.get("base_url", "")
    session = _stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[{"price_data": {"currency": "usd",
            "product_data": {"name": f"Quiniela WCF 2026 F2 — {nombre}"},
            "unit_amount": gross}, "quantity": 1}],
        mode="payment",
        success_url=f"{base_url}/?payment=success&phone={phone}",
        cancel_url=f"{base_url}/?payment=cancel",
        metadata={"phone": phone, "nombre": nombre},
    )
    return {"url": session.url}

@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    import stripe as _stripe, traceback, json as _json
    payload = await request.body()
    sig     = request.headers.get("stripe-signature", "")
    try:
        if STRIPE_WEBHOOK_SEC and sig:
            try:
                event = _stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SEC)
            except Exception as e:
                print(f"[stripe] Firma inválida: {e}")
                raise HTTPException(400, "Firma inválida")
        else:
            event = _json.loads(payload)
        if hasattr(event, "to_dict"):
            event = event.to_dict()
        elif not isinstance(event, dict):
            event = _json.loads(_json.dumps(event))
        event_type = event.get("type", "")
        print(f"[stripe] Evento: {event_type}")
        if event_type == "checkout.session.completed":
            session  = event.get("data", {}).get("object", {})
            metadata = session.get("metadata", {})
            phone    = metadata.get("phone", "")
            nombre   = metadata.get("nombre", "")
            amount   = session.get("amount_total", 0)
            print(f"[stripe] Pago: {nombre} ({phone}) — ${amount/100:.2f} USD")
            if phone:
                ok = _mark_player_paid_internal(phone)
                print(f"[stripe] {'✅' if ok else '⚠️'} {phone} {'marcado' if ok else 'no encontrado'}")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[stripe] ❌ Error: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"Error interno: {e}")

# ─── Entry point ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import cfg as _cfg
    args = _cfg.load("Quiniela WFC 2026 - F2 Webapp")

    os.environ["QL_CREDS"] = args.creds
    os.environ["QL_SHEET"] = args.sheet
    os.environ["QL_PORT"]  = str(args.port)

    uvicorn.run(app, host="0.0.0.0", port=args.port)
