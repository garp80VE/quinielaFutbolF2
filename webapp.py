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
from fastapi import FastAPI, HTTPException, Query, Cookie, UploadFile, File
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

# ─── Push Notifications (VAPID) ───────────────────────────────────────────────
_VAPID_FILE = Path(__file__).parent / "vapid_keys.json"
_SUBS_FILE  = Path(__file__).parent / "push_subs.json"
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
        filas = ws_h.get(f"A{fila_inicio}:K{fila_fin}")

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
            'ganador': c(10),
        }
        all_games.append(game)
        ronda_games.setdefault(c(1), []).append(game)

    # Ordenar por JGO dentro de cada ronda
    for k in ronda_games:
        ronda_games[k].sort(key=lambda g: int(g['jgo']) if g['jgo'].isdigit() else 0)

    def resolve(name, depth=0):
        if not name or depth > 8:
            return name
        ref = _parse_bracket_ref(name)
        if not ref:
            return name
        lst = ronda_games.get(ref['ronda'], [])
        nth = ref['nth'] - 1
        if nth < 0 or nth >= len(lst):
            return name
        g = lst[nth]
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

    # Actualizar placeholders que ya se pueden resolver
    batch   = []
    changes = []

    for game in all_games:
        for slot, col_letter in (('eq1', 'E'), ('eq2', 'F')):
            raw      = game[slot]
            resolved = resolve(raw)
            # Solo actualizar si cambió y el resultado ya no es un placeholder
            if resolved and resolved != raw and not _parse_bracket_ref(resolved):
                batch.append({"range": f"{col_letter}{game['row']}", "values": [[resolved]]})
                changes.append(f"JGO {game['jgo']} {slot.upper()}: {raw!r} → {resolved!r}")

    if batch:
        with _sheets_lock:
            ws_h.batch_update(batch, value_input_option="RAW")
        _invalidate_games()
        print(f"[propagate-bracket] {len(changes)} cambios: {changes}")

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
    reserved = {"HORARIOS", "JUGADORES", "POSICIONES", "CONFIG", "Ligas"}

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
    """Crea pestaña F2 desde cero con headers y fórmulas (18 columnas A-R).
    Estructura:
      A: JGO  B: RONDA  C: FECHA  D: EQ1_REAL  E: EQ2_REAL
      F: PICK_EQ1  G: PICK_GOL1  H: PICK_GOL2  I: PICK_EQ2  J: PICK_GANADOR
      K: GOL1_REAL  L: GOL2_REAL  M: GAN_REAL  N: ESTADO
      O: PTS_EQ1  P: PTS_EQ2  Q: PTS_GAN  R: PTS_TOTAL
    Usa ';' como separador (locale español de Google Sheets)."""
    headers = [
        "JGO", "RONDA", "FECHA", "EQ1 REAL", "EQ2 REAL",
        "PICK EQ1", "PICK GOL1", "PICK GOL2", "PICK EQ2", "PICK GANADOR",
        "GOL1 REAL", "GOL2 REAL", "GAN REAL", "ESTADO",
        "PTS EQ1", "PTS EQ2", "PTS GAN", "PTS TOTAL"
    ]
    ws.update([headers], "A1:R1")

    total    = int(state.get("cfg", {}).get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total

    rows = []
    for i in range(1, total + 1):
        r = i + 3
        # Fórmulas de scoring F2:
        # Liberation = al menos 1 de los 2 equipos del pick está en el partido real
        # (OR de pick_eq1 en {real_eq1,real_eq2}  O  pick_eq2 en {real_eq1,real_eq2})
        lib = f"OR(F{r}=D{r};F{r}=E{r};I{r}=D{r};I{r}=E{r})"
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
            # O: PTS_EQ1 — 1pt si pick_eq1==real_eq1 Y gol1 coincide
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND(F{r}=D{r};G{r}&""=K{r}&"");1;0);"")' ,
            # P: PTS_EQ2 — 1pt si pick_eq2==real_eq2 Y gol2 coincide
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND(I{r}=E{r};H{r}&""=L{r}&"");1;0);"")' ,
            # Q: PTS_GAN — 3pt si liberation Y pick_gan está en el partido Y ganó
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};OR(J{r}=D{r};J{r}=E{r});J{r}=M{r});3;0);"")' ,
            # R: PTS_TOTAL
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IFERROR(SUM(O{r}:Q{r});0);"")' ,
        ])
    ws.update(rows, f"A4:R{last_row}", value_input_option="USER_ENTERED")


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
                tab  = ws_p.get(f"A4:P{3 + total_j}")
                pts_day = 0
                for g in day_games:
                    row_idx = int(g["jgo"]) - 1  # jgo 1 → índice 0 en tab
                    if row_idx < len(tab) and len(tab[row_idx]) >= 16:
                        try: pts_day += float(tab[row_idx][17])  # col R = PTS_TOTAL F2
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
        ranges = [f"'{p['TAB_NOMBRE']}'!A4:P{last_row}" for p in chunk]
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
                        result_map[p["TAB_NOMBRE"]] = ws_p.get(f"A4:R{last_row}")
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
                estado = c(13)   # col N — ESTADO (F2: cols O-R = indices 14-17)
                if not estado or estado == "PROG" or not c(0):
                    continue
                jugados += 1
                # PTS por columna F2: O=PTS_EQ1(14), P=PTS_EQ2(15), Q=PTS_GAN(16), R=PTS_TOTAL(17)
                try: g1_acert  += int(float(c(14))) > 0
                except: pass
                try: g2_acert  += int(float(c(15))) > 0
                except: pass
                try: gan_acert += int(float(c(16))) > 0
                except: pass
                try: pts_total += int(float(c(17))) if c(17) else 0
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
    p = Path(__file__).parent / "icon-192.png"
    if p.exists():
        return Response(content=p.read_bytes(), media_type="image/png")
    return Response(content=_make_png(192), media_type="image/png")


@app.get("/icon-512.png")
async def icon512():
    p = Path(__file__).parent / "icon-512.png"
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
    return {"registered": True, "nombre": p.get("NOMBRE", ""),
            "tab": p.get("TAB_NOMBRE", ""),
            "phone": p.get("WHATSAPP","") or p.get("TELEFONO",""),
            "email": p.get("EMAIL","")}


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
    # R32: se bloquea partido a partido (igual que F1)
    # Rondas superiores (R16/QF/SF/3ER/FINAL): se bloquean todas juntas
    # cuando el ULTIMO partido de R32 arranca (ya no esta en PROG)
    r32_games = [g for g in games if g.get("ronda") == RONDA_BASE]
    upper_locked = False
    if r32_games:
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
            try: pts = int(float(c(17))) if c(17) else 0  # col R = PTS_TOTAL F2
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
                pts_gol1 = pts_gol2 = pts_gan = 0
            else:
                # Liberation fallback to ganador for old picks without eq1/eq2
                liberation = (pick_eq1 in (real_eq1, real_eq2) or
                              pick_eq2 in (real_eq1, real_eq2) or
                              (bool(pick_gan) and pick_gan in (real_eq1, real_eq2)))
                pts_gol1 = 1 if pick_gol1 == real_g1 else 0
                pts_gol2 = 1 if pick_gol2 == real_g2 else 0
                gan_in_match = pick_gan in (real_eq1, real_eq2)
                pts_gan = 3 if (liberation and gan_in_match and
                                gan_known and pick_gan == real_gan) else 0
                pts = (pts_gol1 + pts_gol2 + pts_gan) if liberation else 0

            game_picks.append({
                "nombre":  p.get("NOMBRE", "?"),
                "g1":      pick_gol1,
                "g2":      pick_gol2,
                "gan":     pick_gan,
                "pts":     pts,
                "g1_ok":   bool(pts_gol1 > 0),
                "g2_ok":   bool(pts_gol2 > 0),
                "gan_ok":  bool(pts_gan > 0),
                "g1_set":  bool(pick_gol1),
                "g2_set":  bool(pick_gol2),
                "gan_set": bool(pick_gan),
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
    ("FREEZE_EQUIPOS",      "Congelar nombres de equipos (1=no sobreescribir desde ESPN, 0=actualizar)"),
    ("MODO_PRUEBA",         "Modo Prueba (1=usar ESPN_ID_TEST para scores, 0=producción)"),
    ("PTS_RESULTADO",       "Puntos por ganador correcto"),
    ("PTS_GOLES1",          "Puntos por gol equipo 1 correcto"),
    ("PTS_GOLES2",          "Puntos por gol equipo 2 correcto"),
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
    """Elimina un jugador de la hoja JUGADORES (borra la fila completa). Acepta email o phone."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
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
    for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        row_email = (row[email_col - 1].strip().lower() if email_col and email_col - 1 < len(row) else "")
        row_phone = _normalize_phone(row[phone_col - 1] if phone_col and phone_col - 1 < len(row) else "")
        if (email and row_email == email) or (phone and row_phone == phone):
            with _sheets_lock:
                _sheets_retry(lambda r=i: ws.delete_rows(r))
            # Limpiar cache
            if email: _cache["players"].pop(f"email:{email}", None)
            if phone: _cache["players"].pop(f"phone:{phone}", None)
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
        base  = Path(__file__).parent

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
    p = Path(__file__).parent / "logo.png"
    if p.exists():
        return Response(content=p.read_bytes(), media_type="image/png")
    # Fallback al icon-192
    p2 = Path(__file__).parent / "icon-192.png"
    if p2.exists():
        return Response(content=p2.read_bytes(), media_type="image/png")
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
    reserved = {"HORARIOS", "JUGADORES", "POSICIONES", "CONFIG", "Ligas", "CHAT"}
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
                "pick_eq1":  c(5),   # F – PICK EQ1
                "g2pick":    c(6),   # G – G2 PICK
                "ganpick":   c(7),   # H – GAN.PICK
                "estado":    c(11),  # L – ESTADO
                "pts_total": c(17),  # R – PTS TOTAL F2 (OK)
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

    MAX_PTS_GAME = 5  # GAN=3 + G1=1 + G2=1

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

    # ── Probabilidad universo ───────────────────────────────────────────────
    if has_any_fixed:
        univ_first  = {pname: 0.0 for pname in player_names}
        univ_second = {pname: 0.0 for pname in player_names}
        n_universes = len(player_names)

        def _score_hyp(pick: dict, owner: dict) -> int:
            """Puntúa pick contra los picks del dueño del universo (partidos pendientes)."""
            pts = 0
            if pick.get("ganpick") and owner.get("ganpick") and pick["ganpick"] == owner["ganpick"]:
                pts += 3
            if pick.get("g1pick") and owner.get("g1pick") and pick["g1pick"] == owner["g1pick"]:
                pts += 1
            if pick.get("g2pick") and owner.get("g2pick") and pick["g2pick"] == owner["g2pick"]:
                pts += 1
            return pts

        for owner_name in player_names:
            owner_gdata = player_tabs[owner_name]
            points = {pname: 0 for pname in player_names}

            for jgo in all_jgos:
                owner_g = owner_gdata.get(jgo)
                if not owner_g:
                    continue
                is_fixed_game = jgo in fixed_jgos

                for pname in player_names:
                    player_g = player_tabs[pname].get(jgo)
                    if not player_g:
                        continue
                    if is_fixed_game:
                        try:
                            points[pname] += int(float(player_g.get("pts_total", "") or 0))
                        except Exception:
                            pass
                    else:
                        points[pname] += _score_hyp(player_g, owner_g)

            # Clasificar en este universo
            ranked = sorted(points.items(), key=lambda kv: (-kv[1], kv[0]))
            if ranked:
                best_pts = ranked[0][1]
                first_grp = [x for x in ranked if x[1] == best_pts]
                share1 = 1.0 / len(first_grp)
                for pname, _ in first_grp:
                    univ_first[pname] += share1

                remaining = [x for x in ranked if x[1] < best_pts]
                if remaining:
                    sec_pts = remaining[0][1]
                    sec_grp = [x for x in remaining if x[1] == sec_pts]
                    share2 = 1.0 / len(sec_grp)
                    for pname, _ in sec_grp:
                        univ_second[pname] += share2

        # Convertir conteos a porcentajes
        for s in standings:
            pname = s["name"]
            s["univ_1st"] = round((univ_first[pname]  / n_universes) * 100.0, 2)
            s["univ_2nd"] = round((univ_second[pname] / n_universes) * 100.0, 2)

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
            ligas_sin_id = []
            for lg in leagues:
                eid = ligas_map.get(lg, "")
                if eid:
                    uid_filters.add(f"l:{eid}")
                else:
                    ligas_sin_id.append(lg)
                print(f"[admin-setup] Liga: {lg} | ESPN_ID: {eid or '(NO CONFIGURADO)'}")

            if ligas_sin_id:
                state["_setup_status"] = (
                    f"ERROR: Faltan ESPN_ID en la pestaña Ligas para: {', '.join(ligas_sin_id)}. "
                    f"Agrega la columna ESPN_ID con el número correspondiente."
                )
                return

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

                juegos.append({"id": ev["id"], "eq1": eq1, "eq2": eq2,
                               "fecha": fecha_str, "hora": hora_str,
                               "ronda": ronda,
                               "fecha_raw": ev.get("fecha_raw", "")})
                time.sleep(0.3)

            # ── Fallback por posición: si parse_ronda no pudo determinar la ronda ─
            # WC 2026: 16 juegos R32, 8 R16, 4 QF, 2 SF, 1 3ER, 1 FINAL = 32 total
            BRACKET_RONDAS = (["R32"]*16 + ["R16"]*8 + ["QF"]*4 +
                              ["SF"]*2 + ["3ER"] + ["FINAL"])
            for idx_j, j in enumerate(juegos):
                if not j["ronda"]:  # parse_ronda devolvió ""
                    j["ronda"] = BRACKET_RONDAS[idx_j] if idx_j < len(BRACKET_RONDAS) else "R32"

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


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import cfg as _cfg
    args = _cfg.load("Quiniela WFC 2026 - F2 Webapp")

    os.environ["QL_CREDS"] = args.creds
    os.environ["QL_SHEET"] = args.sheet
    os.environ["QL_PORT"]  = str(args.port)

    uvicorn.run(app, host="0.0.0.0", port=args.port)
