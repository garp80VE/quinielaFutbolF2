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
from starlette.middleware.base import BaseHTTPMiddleware
_Req = Request
from google.oauth2.service_account import Credentials
from pydantic import BaseModel
import db as _db

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

def parse_grupo(comp: dict, event: dict = None) -> str:
    """
    Para F2 (fase eliminatoria WC2026): detecta la ronda desde ESPN y la mapea
    a los nombres internos de F2 (R32, R16, QF, SF, 3ER, FINAL).
    Fallback: retorna el valor bruto de ESPN o cadena vacía.
    """
    import re as _re

    _ROUND_MAP = [
        (_re.compile(r"round of 32|dieciseisavos|32avos",           _re.I), "R32"),
        (_re.compile(r"round of 16|octavos|round of sixteen",       _re.I), "R16"),
        (_re.compile(r"quarter.?final|cuartos",                     _re.I), "QF"),
        (_re.compile(r"third.?place|tercer.?lugar|3.?er",           _re.I), "3ER"),
        (_re.compile(r"semi.?final",                                 _re.I), "SF"),
        (_re.compile(r"\bfinal\b",                                   _re.I), "FINAL"),
    ]

    # Fuentes donde ESPN suele poner el nombre de la ronda
    fuentes = []
    if comp.get("notes"):
        for n in comp["notes"]:
            fuentes.append(n.get("headline", ""))
            fuentes.append(n.get("type",     ""))
    fuentes.append(comp.get("series", {}).get("summary", ""))
    groups = comp.get("groups", {})
    if isinstance(groups, dict):
        fuentes.append(groups.get("name", ""))
    elif isinstance(groups, list) and groups:
        fuentes.append(groups[0].get("name", ""))
    if event:
        for c in event.get("competitions", [{}])[:1]:
            for n in c.get("notes", []):
                fuentes.append(n.get("headline", ""))
        fuentes.append(str(event.get("name", "")))
        fuentes.append(str(event.get("shortName", "")))

    for raw in fuentes:
        if not raw:
            continue
        for pat, ronda in _ROUND_MAP:
            if pat.search(raw):
                return ronda

    # Fallback: grupo de liga (por si hay fase de grupos mezclada)
    for raw in fuentes:
        m = _re.search(r"Grup[oa]\s+([A-L])", raw, _re.IGNORECASE)
        if m:
            return m.group(1).upper()

    return ""

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
        return code not in (400, 404, 410)
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
    "data_change_ts": 0.0,  # timestamp del último cambio real de datos (para clientes)
    "prob":      None, # resultado de _compute_probabilities()
    "prob_ts":   0.0,  # timestamp del último cálculo de probabilidades
    "top5_text": "",   # top 5 en texto plano (para notificaciones)
    "top3_text": "",   # top 3 compacto (para push)
}
GAMES_TTL    = 5    # segundos antes de refrescar juegos
PLAYERS_TTL  = 120  # segundos antes de refrescar lista de jugadores
PROB_TTL     = 180  # segundos antes de recalcular probabilidades (3 min)


def _load_players_cache():
    """Carga jugadores desde SQLite, indexados por email y telefono."""
    jugadores = _db.db_get_jugadores()
    new_cache = {}
    for p in jugadores:
        d = _jugador_db_to_cache(p)
        if d.get("EMAIL"):
            new_cache["email:" + d["EMAIL"].lower()] = d
        phone_val = _normalize_phone(d.get("WHATSAPP", "") or d.get("TELEFONO", ""))
        if phone_val:
            new_cache["phone:" + phone_val] = d
    _cache["players"]    = new_cache
    _cache["players_ts"] = time.time()
    print(f"[players-cache] {len([k for k in new_cache if k.startswith('email:')])} jugadores desde SQLite")


def _players_cache_ok():
    return bool(_cache["players"]) and (time.time() - _cache["players_ts"]) < PLAYERS_TTL

def _get_games_cache():
    """Retorna (games_list, estados_dict) desde cache o SQLite si expiro."""
    now = time.time()
    if _cache["games"] is None or now - _cache["games_ts"] > GAMES_TTL:
        horarios = _db.db_get_horarios()
        games, estados = [], {}
        for h in horarios:
            dt_utc = (
                f"{h['fecha']}T{h['hora']}:00Z"
                if h.get("fecha") and h.get("hora") else ""
            )
            games.append({
                "jgo":          str(h["jgo"]),
                "ronda":        h.get("grupo", ""),   # db usa "grupo", F2 usa "ronda"
                "fecha":        h.get("fecha", ""),
                "hora":         h.get("hora", ""),
                "datetime_utc": dt_utc,
                "eq1":          h.get("eq1", ""),
                "eq2":          h.get("eq2", ""),
                "espn_id":      h.get("espn_id", ""),
                "estado":       h.get("estado", "PROG"),
                "gol1":         h.get("gol1", ""),
                "gol2":         h.get("gol2", ""),
                "ganador":      h.get("ganador", ""),
            })
            estados[str(h["jgo"])] = h.get("estado", "PROG")
        _cache["games"]    = games
        _cache["estados"]  = estados
        _cache["games_ts"] = now
    return _cache["games"], _cache["estados"]


def _invalidate_games():
    _cache["games_ts"] = 0
    _cache["data_change_ts"] = time.time()

def _invalidate_players():
    _cache["players_ts"] = 0
    _cache["standings_rows"] = None  # forzar recompute en próximo /api/standings

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


def _propagate_bracket() -> list:
    """
    Lee HORARIOS desde SQLite y actualiza EQ1/EQ2 de juegos futuros cuyo nombre
    sea un placeholder de bracket resoluble con los GANADOR actuales.
    """
    cfg      = state.get("cfg", {})
    horarios = _db.db_get_horarios()

    all_games   = []
    ronda_games = {}

    for h in horarios:
        jgo = str(h["jgo"])
        if not jgo:
            continue
        game = {
            "jgo":     jgo,
            "ronda":   h.get("grupo", ""),
            "eq1":     h.get("eq1", ""),
            "eq2":     h.get("eq2", ""),
            "estado":  h.get("estado", "PROG"),
            "gol1":    h.get("gol1", ""),
            "gol2":    h.get("gol2", ""),
            "ganador": h.get("ganador", ""),
        }
        all_games.append(game)
        ronda_games.setdefault(h.get("grupo", ""), []).append(game)

    for k in ronda_games:
        ronda_games[k].sort(key=lambda g: int(g["jgo"]) if g["jgo"].isdigit() else 0)

    # Mapas verificados por Gio contra excel template WC2026
    # R32: slot secuencial N → slot real del juego R32 (1-16)
    _WC2026_R32 = {
        1:1,  2:4,  3:3,  4:6,
        5:2,  6:5,  7:7,  8:8,
        9:12, 10:11, 11:10, 12:9,
        13:15, 14:14, 15:13, 16:16
    }
    # R16: slot secuencial N → slot real del juego R16 (1-8)
    _WC2026_R16 = {1:1, 2:2, 3:5, 4:6, 5:3, 6:4, 7:7, 8:8}
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
        lst = ronda_games.get(ref["ronda"], [])
        nth = ref["nth"]
        if ref["ronda"] == "R32" and _use_wc2026 and len(lst) == 16:
            nth = _WC2026_R32.get(nth, nth)
        elif ref["ronda"] == "R16" and _use_wc2026 and len(lst) == 8:
            nth = _WC2026_R16.get(nth, nth)
        idx = nth - 1
        if idx < 0 or idx >= len(lst):
            return name
        g = lst[idx]
        if not g["ganador"]:
            return name
        if ref["type"] == "loser":
            eq1 = resolve(g["eq1"], depth + 1)
            eq2 = resolve(g["eq2"], depth + 1)
            if g["ganador"] == eq1:
                return eq2 or name
            if g["ganador"] == eq2:
                return eq1 or name
            return name
        return resolve(g["ganador"], depth + 1)

    db_updates      = []
    changes         = []
    placeholder_map = {}

    # Paso 1: resolver placeholders en EQ1/EQ2 de juegos futuros
    for game in all_games:
        for slot in ("eq1", "eq2"):
            val = game[slot]
            if not val or not _parse_bracket_ref(val):
                continue
            resolved = resolve(val)
            if resolved and resolved != val:
                if game["estado"] in ("", "PROG"):
                    db_updates.append((game["jgo"], slot, resolved))
                    placeholder_map[val] = resolved
                    changes.append(
                        f"JGO {game['jgo']} {slot.upper()}: {val!r} -> {resolved!r}"
                    )
                    game[slot] = resolved

    # Paso 2: SF -> FINAL (ganadores) y SF -> 3ER (perdedores)
    def sorted_by_jgo(lst):
        return sorted(lst, key=lambda g: int(g["jgo"]) if str(g["jgo"]).isdigit() else 0)

    sf_lst  = sorted_by_jgo(ronda_games.get("SF",    []))
    fin_lst = sorted_by_jgo(ronda_games.get("FINAL", []))
    ter_lst = sorted_by_jgo(ronda_games.get("3ER",   []))

    for si, slot in enumerate(("eq1", "eq2")):
        if si >= len(sf_lst):
            continue
        sf_g = sf_lst[si]
        gan  = sf_g["ganador"]
        if not gan or _parse_bracket_ref(gan):
            continue
        if fin_lst and fin_lst[0][slot] != gan:
            if fin_lst[0][slot] and _parse_bracket_ref(fin_lst[0][slot]):
                placeholder_map[fin_lst[0][slot]] = gan
            db_updates.append((fin_lst[0]["jgo"], slot, gan))
            changes.append(
                f"JGO {fin_lst[0]['jgo']} {slot.upper()} (FINAL): "
                f"{fin_lst[0][slot]!r} -> {gan!r}"
            )
            fin_lst[0][slot] = gan
        if ter_lst:
            eq1_sf = resolve(sf_g["eq1"])
            eq2_sf = resolve(sf_g["eq2"])
            loser  = eq2_sf if gan == eq1_sf else (eq1_sf if gan == eq2_sf else None)
            # Siempre actualizar 3ER con el perdedor (igual que FINAL con el ganador)
            # La condicion anterior "not ter_lst[0][slot]" era incorrecta: si el slot
            # ya tenia un valor incorrecto (ej. el ganador), nunca se corregía.
            if loser and not _parse_bracket_ref(loser) and ter_lst[0][slot] != loser:
                if ter_lst[0][slot] and _parse_bracket_ref(ter_lst[0][slot]):
                    placeholder_map[ter_lst[0][slot]] = loser
                db_updates.append((ter_lst[0]["jgo"], slot, loser))
                changes.append(
                    f"JGO {ter_lst[0]['jgo']} {slot.upper()} (3ER-loser): "
                    f"{ter_lst[0][slot]!r} -> {loser!r}"
                )
                ter_lst[0][slot] = loser

    if db_updates:
        conn = _db.get_conn()
        with conn:
            for jgo_u, col_u, val_u in db_updates:
                if col_u == "eq1":
                    conn.execute("UPDATE horarios SET eq1=? WHERE jgo=?", (val_u, str(jgo_u)))
                else:
                    conn.execute("UPDATE horarios SET eq2=? WHERE jgo=?", (val_u, str(jgo_u)))
        conn.close()
        _invalidate_games()
        print(f"[propagate-bracket] {len(changes)} cambios SQLite: {changes}")

    # Paso 3: PICK_COLS = [] -- picks del jugador intocables
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
        ws.update([["TABLA DE POSICIONES"]], "A1")
        print("[init] Hoja POSICIONES creada")

    # Eliminar Sheet1 / Hoja1 vacía inicial si existe
    for default_name in ("Sheet1", "Hoja 1", "Hoja1"):
        if default_name in existing and len(sh.worksheets()) > 1:
            try:
                sh.del_worksheet(sh.worksheet(default_name))
                print(f"[init] Hoja por defecto '{default_name}' eliminada")
            except Exception:
                pass


def read_config(sh=None) -> dict:
    """Lee config desde SQLite (primario). Si se pasa sh, sincroniza primero."""
    if sh is not None:
        try:
            ws = sh.worksheet("CONFIG")
            rows_cfg = {
                r[0].strip(): r[1].strip()
                for r in ws.get_all_values()
                if len(r) >= 2 and r[0].strip()
            }
            if rows_cfg:
                _db.db_save_config(rows_cfg)
        except Exception as _e:
            print(f"[config] Error sync desde Sheets: {_e}")
    return _db.db_get_config()


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


def _jugador_db_to_cache(p: dict) -> dict:
    """Convierte fila SQLite (lowercase keys) al formato de cache (UPPERCASE keys)."""
    return {
        "NOMBRE":         p.get("nombre", ""),
        "EMAIL":          p.get("email", ""),
        "WHATSAPP":       p.get("whatsapp", ""),
        "TELEFONO":       p.get("whatsapp", ""),
        "TAB_NOMBRE":     p.get("tab_nombre", ""),
        "PAGADO":         "1" if p.get("pagado") else "",
        "REGLAS_OK":      1 if p.get("reglas_ok") else 0,
        "FECHA REG.":     p.get("fecha_reg", ""),
        "FECHA_REGISTRO": p.get("fecha_reg", ""),
        "#":              str(p.get("num", "")),
        "_id":            p.get("id"),
    }


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
    base  = f"{parts[0]} {parts[1][0]}." if len(parts) >= 2 else parts[0]
    existing = {p.get("tab_nombre", "") for p in _db.db_get_jugadores()}
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
        new_ws.batch_clear([f"F4:J{last_row}"])  # limpiar TODOS los picks del template (F-J)
    else:
        new_ws = sh.add_worksheet(title=tab_name, rows=last_row + 10, cols=16)
        _init_player_tab(new_ws)

    return new_ws


def _init_player_tab(ws, cfg=None):
    """Crea pestaña F2 desde cero con headers y fórmulas (20 columnas A-T).
    Estructura:
      A: JGO  B: RONDA  C: FECHA  D: EQ1_REAL  E: EQ2_REAL
      F: PICK_EQ1  G: PICK_GOL1  H: PICK_GOL2  I: PICK_EQ2  J: PICK_GANADOR
      K: GOL1_REAL  L: GOL2_REAL  M: GAN_REAL  N: ESTADO
      O: PTS_LOGRO  P: PTS_GAN  Q: PTS_GOL1  R: PTS_GOL2
      S: PTS_CAMPEON  T: PTS_TOTAL
    Puntuación F2: configurable via cfg (PTS_LOGRO/PTS_GAN/PTS_GOL1/PTS_GOL2/PTS_CAMPEON)
    Usa ';' como separador (locale español de Google Sheets)."""
    if cfg is None:
        cfg = state.get("cfg", {})
    # Fórmulas dinámicas — leen el valor de CONFIG en tiempo real (no hardcoded)
    _VL = 'IFERROR(VLOOKUP("PTS_LOGRO";CONFIG!$A:$B;2;0)*1;1)'
    _VG = 'IFERROR(VLOOKUP("PTS_GAN";CONFIG!$A:$B;2;0)*1;2)'
    _V1 = 'IFERROR(VLOOKUP("PTS_GOL1";CONFIG!$A:$B;2;0)*1;1)'
    _V2 = 'IFERROR(VLOOKUP("PTS_GOL2";CONFIG!$A:$B;2;0)*1;1)'
    _VC = 'IFERROR(VLOOKUP("PTS_CAMPEON";CONFIG!$A:$B;2;0)*1;0)'
    headers = [
        "JGO", "RONDA", "FECHA", "EQ1 REAL", "EQ2 REAL",
        "PICK EQ1", "PICK GOL1", "PICK GOL2", "PICK EQ2", "PICK GANADOR",
        "GOL1 REAL", "GOL2 REAL", "GAN REAL", "ESTADO",
        "PTS LOGRO", "PTS GAN", "PTS GOL1", "PTS GOL2", "PTS CAMPEON", "PTS TOTAL"
    ]
    ws.update([headers], "A1:T1")

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
            # O: PTS_LOGRO — pts si resultado coincide Y al menos 1 equipo pick sigue vivo
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X")));{_VL};0);"")' ,
            # P: PTS_GAN — pts si ganador coincide Y al menos 1 equipo pick sigue vivo
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};J{r}=M{r});{_VG};0);"")' ,
            # Q: PTS_GOL1 — pts si gol EQ1 coincide Y al menos 1 equipo pick sigue vivo
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};G{r}&""=K{r}&"");{_V1};0);"")' ,
            # R: PTS_GOL2 — pts si gol EQ2 coincide Y al menos 1 equipo pick sigue vivo
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};H{r}&""=L{r}&"");{_V2};0);"")' ,
            # S: PTS_CAMPEON — bono FINAL si ganador coincide Y al menos 1 equipo pick sigue vivo
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(B{r}="FINAL";IF(AND({lib};J{r}=M{r});{_VC};0);0);"")' ,
            # T: PTS_TOTAL = suma de O:S
            f'=IF(AND(N{r}<>"";N{r}<>"PROG");IFERROR(SUM(O{r}:S{r});0);"")' ,
        ])
    ws.update(rows, f"A4:T{last_row}", value_input_option="USER_ENTERED")


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
                        try: pts_day += float(tab[row_idx][19])  # col T = PTS_TOTAL F2
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

def _check_reminders(games, cfg):
    """Envía recordatorio 15/10/5/3/1 min antes a jugadores que no apostaron."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _tdt
    now_utc = _dt.now(_tz.utc)

    for game in games:
        espn_id = str(game.get("espn_id", "") or "")
        estado  = str(game.get("estado", "") or "")
        if not espn_id or (estado != "PROG" and estado != ""):
            continue

        # Buscar la fecha/hora del partido
        game = game
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
        jgo = str(game.get("jgo", ""))
        if not jgo:
            continue

        sin_pick = []        # nombres
        sin_pick_phones = [] # telefonos para push personalizado
        sin_pick_emails = [] # emails para push personalizado
        try:
            sin_pick_rows = _db.db_get_picks_without_pick(jgo)
            for sp in sin_pick_rows:
                sin_pick.append(sp.get("nombre", "?"))
                wa = sp.get("whatsapp", "")
                sin_pick_phones.append(wa)
                p_cached = (
                    _cache["players"].get("phone:" + _normalize_phone(wa), {})
                    if wa else {}
                )
                sin_pick_emails.append(p_cached.get("EMAIL", ""))
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
        ranges = [f"'{p['TAB_NOMBRE']}'!A4:T{last_row}" for p in chunk]
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
                        result_map[p["TAB_NOMBRE"]] = ws_p.get(f"A4:T{last_row}")
                    time.sleep(0.2)
                except Exception as e2:
                    print(f"[batch-read] {p['TAB_NOMBRE']}: {e2}")
                    result_map[p["TAB_NOMBRE"]] = []

    return result_map


def _compute_probabilities():
    """Placeholder — probabilidades no implementadas aún."""
    return {}


def _update_standings():
    """Calcula posiciones desde SQLite y actualiza caches en memoria."""
    cfg       = state.get("cfg", {})
    standings = _db.db_compute_standings(cfg)

    if not standings:
        return

    lider_pts = standings[0]["pts"] if standings else 0
    rows_out  = []
    pos = 1
    for i, s in enumerate(standings):
        if i > 0:
            prev = standings[i - 1]
            same = (s["pts"] == prev["pts"] and
                    s["gan"] == prev["gan"] and
                    s["g1"] + s["g2"] == prev["g1"] + prev["g2"])
            if not same:
                pos = i + 1
        diferencia = s["pts"] - lider_pts
        rows_out.append([pos, s["nombre"], s["pts"], diferencia])

    _cache["top5_text"] = "\n".join(
        f"  {r[0]}. {r[1]}" for r in rows_out[:5]
    )
    _cache["top3_text"] = " \u00b7 ".join(
        f"{r[0]}. {r[1]} ({r[2]}pts)" for r in rows_out[:3] if len(r) >= 3
    )
    _cache["standings_rows"] = [["POS", "NOMBRE", "Ptos", "Diferencia"]] + rows_out

    sh = state.get("sh")
    if sh:
        try:
            ws_pos = sh.worksheet("POSICIONES")
            fila_fin_clear = max(len(standings) + 10, 50)
            with _sheets_lock:
                ws_pos.batch_clear([f"A2:Z{fila_fin_clear}"])
                ws_pos.update([["POS", "NOMBRE", "Ptos", "Diferencia"]], "A2:D2",
                              value_input_option="RAW")
                if rows_out:
                    ws_pos.update(rows_out, f"A3:D{2 + len(rows_out)}",
                                  value_input_option="RAW")
        except Exception as _e:
            print(f"[standings] Error escribiendo POSICIONES en Sheets: {_e}")

    print(f"[standings] {len(standings)} jugador(es) desde SQLite")


def _updater_loop():
    """Loop de actualizacion de scores desde ESPN. Lee/escribe en SQLite."""
    print("[updater] Iniciando en segundo plano")
    while True:
        try:
            t0          = time.time()
            cfg         = state.get("cfg", {})
            interval    = int(cfg.get("INTERVAL_SEGS", 60))
            modo_prueba = cfg.get("MODO_PRUEBA", "0").strip() not in ("", "0", "false", "no")

            _invalidate_games()
            games, _ = _get_games_cache()

            try:
                _check_reminders(games, cfg)
            except Exception as e:
                print(f"[updater-reminder] {e}")

            if modo_prueba:
                time.sleep(max(0, interval - (time.time() - t0)))
                continue

            n = 0
            for game in games:
                jgo         = str(game.get("jgo", ""))
                espn_id     = game.get("espn_id", "")
                estado_prev = game.get("estado", "PROG")

                if not espn_id or estado_prev == "FINAL":
                    continue

                data = espn_get(_espn_summary_url(), {"event": espn_id}) or \
                       espn_get(ESPN_FALLBACK,       {"event": espn_id})
                if not data:
                    continue
                sc = parse_score(data)
                if not sc:
                    continue

                nuevo_minuto = sc.get("minuto", "")
                if nuevo_minuto:
                    if _live_clocks.get(espn_id) != nuevo_minuto:
                        _live_clocks[espn_id] = nuevo_minuto
                        _invalidate_games()
                elif sc["estado"] == "FINAL":
                    _live_clocks.pop(espn_id, None)

                eq1_sheet  = game.get("eq1", "")
                eq2_sheet  = game.get("eq2", "")
                eq1_espn   = sc.get("eq1", "")
                eq2_espn   = sc.get("eq2", "")
                freeze     = cfg.get("FREEZE_EQUIPOS", "0").strip() not in ("", "0", "false", "no")
                _ronda_cur = game.get("ronda", "")
                freeze = freeze or _ronda_cur in ("R16", "QF", "SF", "3ER", "FINAL")
                teams_changed = not freeze and (
                    (eq1_espn and eq1_espn != eq1_sheet) or
                    (eq2_espn and eq2_espn != eq2_sheet)
                )

                if (sc["estado"] == estado_prev and
                        sc["gol1"] == game.get("gol1", "") and
                        sc["gol2"] == game.get("gol2", "") and
                        sc["ganador"] == game.get("ganador", "") and
                        not teams_changed):
                    time.sleep(0.3)
                    continue

                eq1  = eq1_sheet or eq1_espn
                eq2  = eq2_sheet or eq2_espn
                prev = _prev_states.get(espn_id, {})

                if sc["estado"] != "PROG" and estado_prev == "PROG":
                    _tg_send(f"\U0001f7e1 <b>INICIO:</b> {eq1} vs {eq2}\nJornada")
                    _send_push_all("\u26bd Partido iniciado", f"{eq1} vs {eq2}",
                                   {"tipo": "inicio", "eq1": eq1, "eq2": eq2})
                    try:
                        _wa("POST", "/send", json={"message": f"\U0001f7e1 INICIO: {eq1} vs {eq2}\n"})
                    except Exception as e:
                        print(f"[WA] Error inicio: {e}")
                elif sc["estado"] in ("EN VIVO", "MEDIO TIEMPO", "PRORROGA", "PENALES"):
                    if prev and (sc["gol1"] != prev.get("gol1", "") or
                                 sc["gol2"] != prev.get("gol2", "")):
                        minuto  = _live_clocks.get(espn_id, "")
                        min_txt = (f" ({minuto}')" if minuto and minuto != "MT"
                                   else (" (MT)" if sc["estado"] == "MEDIO TIEMPO" else ""))
                        _pending_notifs.append({
                            "tipo": "gol", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                            "min_txt": min_txt, "minuto": minuto,
                        })
                elif sc["estado"] == "FINAL" and estado_prev != "FINAL":
                    if prev and (sc["gol1"] != prev.get("gol1", "") or
                                 sc["gol2"] != prev.get("gol2", "")):
                        _pending_notifs.append({
                            "tipo": "gol", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                            "min_txt": "", "minuto": "",
                        })
                    gan_eq = sc["ganador"] if sc["ganador"] else "Sin definir"
                    _pending_notifs.append({
                        "tipo": "final", "eq1": eq1, "eq2": eq2,
                        "gol1": sc["gol1"], "gol2": sc["gol2"],
                        "ganador": sc["ganador"], "gan_eq": gan_eq,
                    })

                _prev_states[espn_id] = {
                    "estado": sc["estado"],
                    "gol1":   sc["gol1"],
                    "gol2":   sc["gol2"],
                }

                ult_act = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _db.db_update_game_result(
                    jgo, sc["estado"], sc["gol1"], sc["gol2"], sc["ganador"], ult_act
                )

                if teams_changed:
                    conn = _db.get_conn()
                    with conn:
                        if eq1_espn and eq1_espn != eq1_sheet:
                            conn.execute("UPDATE horarios SET eq1=? WHERE jgo=?",
                                         (eq1_espn, jgo))
                            print(f"[updater] JGO {jgo} EQ1: {eq1_sheet!r} -> {eq1_espn!r}")
                        if eq2_espn and eq2_espn != eq2_sheet:
                            conn.execute("UPDATE horarios SET eq2=? WHERE jgo=?",
                                         (eq2_espn, jgo))
                            print(f"[updater] JGO {jgo} EQ2: {eq2_sheet!r} -> {eq2_espn!r}")
                    conn.close()

                _invalidate_games()
                n += 1
                time.sleep(0.3)

            if n > 0:
                print(f"[updater] {n} partido(s) actualizados en SQLite")
                try:
                    _propagate_bracket()
                except Exception as _pe:
                    print(f"[updater] propagate-bracket error: {_pe}")

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
                                f"\u26bd <b>MARCADOR:</b> {eq1n} {g1} \u2013 {g2} {eq2n}{mt}\n"
                                + (f"\n\U0001f3c6 <b>Top 5:</b>\n{top}" if top else "")
                            )
                            push_body = f"{eq1n} {g1} \u2013 {g2} {eq2n}{mt}"
                            if top3: push_body += f"\n\U0001f3c6 {top3}"
                            _send_push_all("\u26bd Gol!", push_body,
                                {"tipo": "gol", "eq1": eq1n, "eq2": eq2n,
                                 "gol1": g1, "gol2": g2, "minuto": minuto_n})
                            try:
                                wa_msg = f"\u26bd GOL: {eq1n} {g1} \u2013 {g2} {eq2n}{mt}"
                                if top3: wa_msg += f"\n\U0001f3c6 {top3}"
                                _wa("POST", "/send", json={"message": wa_msg})
                            except Exception as e:
                                print(f"[WA] Error gol: {e}")
                        elif notif["tipo"] == "final":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            g1, g2     = notif["gol1"], notif["gol2"]
                            gan        = notif["ganador"]
                            gan_eq_n   = notif["gan_eq"]
                            # F2: ganador es nombre real del equipo (no "1"/"2")
                            gan_txt    = (f"\U0001f3c5 Gana <b>{gan_eq_n}</b>"
                                          if gan else "\U0001f91d <b>Empate</b>")
                            _tg_send(
                                f"\U0001f3c1 <b>FINAL:</b> {eq1n} {g1} \u2013 {g2} {eq2n}\n"
                                f"{gan_txt}\n"
                                + (f"\n\U0001f3c6 <b>Top 5:</b>\n{top}" if top else "")
                            )
                            push_body = f"{eq1n} {g1} \u2013 {g2} {eq2n} \u00b7 {gan_eq_n}"
                            if top3: push_body += f"\n\U0001f3c6 {top3}"
                            _send_push_all("\U0001f3c1 Partido finalizado", push_body,
                                {"tipo": "final", "eq1": eq1n, "eq2": eq2n,
                                 "gol1": g1, "gol2": g2, "ganador": gan})
                            try:
                                gan_wa = f"\U0001f3c5 Gana {gan_eq_n}" if gan else "\U0001f91d Empate"
                                wa_msg = f"\U0001f3c1 FINAL: {eq1n} {g1} \u2013 {g2} {eq2n}\n{gan_wa}"
                                if top3: wa_msg += f"\n\n\U0001f3c6 Top 3:\n{top3}"
                                _wa("POST", "/send", json={"message": wa_msg})
                            except Exception as e:
                                print(f"[WA] Error final: {e}")
                    except Exception as e:
                        print(f"[updater] notif-flush ERROR: {e}")
                _pending_notifs.clear()

            global _standings_last_update
            time_since_last = time.time() - _standings_last_update
            should_update   = n > 0 or (time_since_last >= _STANDINGS_MIN_INTERVAL)
            if should_update:
                def _standings_async():
                    if not _standings_lock.acquire(blocking=False):
                        return
                    try:
                        _update_standings()
                        global _standings_last_update
                        _standings_last_update = time.time()
                        try:
                            result = _compute_probabilities()
                            _cache["prob"]    = result
                            _cache["prob_ts"] = time.time()
                        except Exception as ep:
                            print(f"[standings-async] prob ERROR: {ep}")
                        try:
                            _compute_compare_picks()
                        except Exception as ec:
                            print(f"[standings-async] compare ERROR: {ec}")
                    except Exception as e:
                        print(f"[standings-async] ERROR: {e}")
                    finally:
                        _standings_lock.release()
                threading.Thread(target=_standings_async, daemon=True, name="standings").start()

            try:
                games_now, _ = _get_games_cache()
                _check_day_end_notif(games_now)
            except Exception as e:
                print(f"[updater] day-end ERROR: {e}")

        except Exception as e:
            print(f"[updater] ERROR: {e}")

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

    # 1. Inicializar SQLite
    _db.init_db()
    print("[webapp] SQLite inicializado")

    # 2. Verificar si la DB es nueva (sin jugadores)
    db_is_new = False
    try:
        _tmp = _db.get_conn()
        _cnt = _tmp.execute("SELECT COUNT(*) FROM jugadores").fetchone()[0]
        _tmp.close()
        db_is_new = (_cnt == 0)
    except Exception:
        db_is_new = True

    # 3. Cargar config y estado desde SQLite (arranque inmediato)
    state["cfg"] = _db.db_get_config()
    print(f"[webapp] Config SQLite: {len(state['cfg'])} campos")

    global _vapid_keys
    _vapid_keys = _load_vapid()
    _subs_load()

    # 4. Restaurar logo desde Volume /data
    _data_dir = Path("/data")
    if _data_dir.exists():
        import shutil as _shutil
        _app_dir = Path(__file__).parent
        for _fname in ["logo.png", "icon-192.png", "icon-512.png"]:
            _src2 = _data_dir / _fname
            if _src2.exists():
                _shutil.copy2(_src2, _app_dir / _fname)
                print(f"[webapp] Logo restaurado: {_fname}")

    print(f"[webapp] Corriendo en http://localhost:{os.environ.get('QL_PORT', 8000)}")

    # 5. Hilo Sheets: conectar en background, migrar si es nueva, sync cada 60s
    def _sheets_connect_and_sync():
        import time as _time
        _sh = None
        if not sheet_id:
            print("[sheets] Sin QL_SHEET -- app corre solo con SQLite")
            return
        if not os.path.exists(creds_path):
            gc_env = os.environ.get("GOOGLE_CREDENTIALS", "")
            if gc_env:
                with open(creds_path, "w", encoding="utf-8") as _f:
                    _f.write(gc_env)
                print("[sheets] credentials.json recreado desde env")
            else:
                print("[sheets] Sin credenciales -- app corre solo con SQLite")
                return
        try:
            creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
            gc    = gspread.authorize(creds)
            for _att in range(4):
                try:
                    _sh = gc.open_by_key(sheet_id)
                    break
                except gspread.exceptions.APIError as _e:
                    if "429" in str(_e) and _att < 3:
                        _w = 30 * (_att + 1)
                        print(f"[sheets] 429 -- reintentando en {_w}s...")
                        _time.sleep(_w)
                    else:
                        print(f"[sheets] No accesible: {_e}")
                        break
                except Exception as _e2:
                    print(f"[sheets] No accesible: {_e2}")
                    break
            if _sh:
                state["sh"] = _sh
                _ensure_base_sheets(_sh)
                print(f"[sheets] Conectado: {_sh.title}")
                if db_is_new:
                    print("[sheets] SQLite vacio -- migrando datos desde Sheets...")
                    try:
                        cfg_sh = {r[0].strip(): r[1].strip()
                                  for r in _sh.worksheet("CONFIG").get_all_values()
                                  if len(r) >= 2 and r[0].strip()}
                        _db.migrate_from_sheets(_sh, cfg_sh)
                        state["cfg"] = _db.db_get_config()
                        _cnt2 = _db.get_conn().execute(
                            "SELECT COUNT(*) FROM jugadores").fetchone()[0]
                        print(f"[sheets] Migracion completada -- {_cnt2} jugadores")
                        _invalidate_players()
                    except Exception as _em:
                        print(f"[sheets] Error migracion: {_em}")
                else:
                    read_config(_sh)
                    state["cfg"] = _db.db_get_config()
            else:
                print("[sheets] No disponible -- app corre solo con SQLite")
        except Exception as _ec:
            print(f"[sheets] Error de conexion (no fatal): {_ec}")

        # Loop de sync cada 60s
        while True:
            _time.sleep(60)
            try:
                if state.get("sh"):
                    _db.sync_to_sheets(state["sh"], state.get("cfg", {}))
            except Exception as _se:
                print(f"[sync] Error: {_se}")

    threading.Thread(target=_sheets_connect_and_sync, daemon=True, name="sheets").start()

    # 6. Arrancar updater
    t = threading.Thread(target=_updater_loop, daemon=True)
    t.start()

    # 7. Pre-calentar caches
    def _warmup_caches():
        import time as _wtime
        _wtime.sleep(3)
        try:
            _update_standings()
            global _standings_last_update
            _standings_last_update = _wtime.time()
            print("[webapp] standings iniciales calculados")
        except Exception as e:
            print(f"[webapp] standings startup error: {e}")
        try:
            result = _compute_probabilities()
            _cache["prob"]    = result
            _cache["prob_ts"] = _wtime.time()
            print("[webapp] probabilidades iniciales calculadas")
        except Exception as e:
            print(f"[webapp] prob startup error: {e}")
        try:
            _compute_compare_picks()
            print("[webapp] comparar iniciales calculado")
        except Exception as e:
            print(f"[webapp] compare startup error: {e}")
    threading.Thread(target=_warmup_caches, daemon=True, name="warmup").start()

    yield


app = FastAPI(lifespan=lifespan)


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
            "reglas_ok": bool(p.get("REGLAS_OK")),
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
    tab      = generate_tab_name(body.nombre)
    fecha_reg = datetime.now().strftime("%Y-%m-%d %H:%M")
    email_clean = body.email.strip().lower() if body.email else ""

    # ── 1. SQLite (primario — siempre) ───────────────────────────────────────
    _db.db_register_player(email_clean, body.nombre.strip(), phone_norm, fecha_reg, tab)
    _db.db_init_picks_for_player(
        _db.db_find_player(email=email_clean, phone=phone_norm)["id"]
    )
    _invalidate_players()
    _cache["players"].clear()

    # ── 2. Sheets (async best-effort) ────────────────────────────────────────
    def _sheets_register():
        try:
            sh = state.get("sh")
            if not sh:
                return
            create_player_tab(tab)
            ws   = sh.worksheet("JUGADORES")
            rows = ws.get_all_values()
            header_idx, headers = _jugadores_headers(rows)
            next_row = len(rows) + 1
            def col(*names):
                for n in names:
                    if n in headers:
                        return headers.index(n)
                return -1
            num_cols  = len(headers) if headers else 6
            nueva_fila = [""] * num_cols
            def set_col(val, *names):
                idx = col(*names)
                if 0 <= idx < num_cols:
                    nueva_fila[idx] = val
            set_col(str(len(rows) - header_idx), "#")
            set_col(email_clean,       "EMAIL")
            set_col(body.nombre.strip(), "NOMBRE")
            set_col(phone_norm,        "WHATSAPP", "TELEFONO")
            set_col(fecha_reg,         "FECHA REG.", "FECHA_REGISTRO")
            set_col(tab,               "TAB SHEET", "TAB_NOMBRE", "TAB_SHEET")
            last_col = chr(ord("A") + num_cols - 1)
            with _sheets_lock:
                ws.update([nueva_fila], f"A{next_row}:{last_col}{next_row}")
        except Exception as _e:
            print(f"[register-sheets] Error (no bloqueante): {_e}")
    threading.Thread(target=_sheets_register, daemon=True).start()

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


@app.post("/api/player/self-delete")
async def player_self_delete(
    response: Response,
    phone: str = Query(""),
    email: str = Query(""),
    ql_session: str = Cookie(default=""),
):
    """El propio jugador se retira - solo permitido si el torneo NO esta activo.
    Identifica al jugador por la cookie de sesion (ql_session = telefono o email);
    phone/email en query se mantienen como fallback para compatibilidad."""
    if _torneo_activo().get("activo"):
        raise HTTPException(403, "No puedes retirarte mientras el torneo esta activo")
    p = (find_player_any(phone=ql_session, email=ql_session) if ql_session else None) \
        or find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    player_id = p.get("_id") or p.get("id")
    if not player_id:
        raise HTTPException(404, "Jugador sin ID")

    # Cancelar suscripciones push de este jugador
    phone_norm  = p.get("WHATSAPP") or p.get("TELEFONO") or ""
    email_clean = p.get("EMAIL") or ""
    if _push_subs:
        _push_subs[:] = [
            s for s in _push_subs
            if not (s.get("_phone") == phone_norm or s.get("_email") == email_clean)
        ]
        _subs_save()

    # Eliminar picks + jugador de SQLite
    _db.db_delete_player(int(player_id))
    _invalidate_players()
    _cache["players"].clear()
    _cache["standings_rows"] = None

    # Limpiar cookie de sesion
    response.delete_cookie("ql_session", path="/")
    return {"ok": True}


@app.post("/api/player/accept-rules")
async def player_accept_rules(phone: str = Query(""), email: str = Query(""),
                              ql_session: str = Cookie(default="")):
    """Marca que el jugador leyo y acepto el reglamento (modal obligatorio)."""
    p = (find_player_any(phone=ql_session, email=ql_session) if ql_session else None) \
        or find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    player_id = p.get("_id") or p.get("id")
    if not player_id:
        raise HTTPException(404, "Jugador sin ID")
    _db.db_set_reglas_ok(int(player_id), True)
    _invalidate_players()
    return {"ok": True}


# ── Resolucion de bracket para el PDF ────────────────────────────────────────
# Port del frontend (_parseBracketRef / resolveTeamName / _inferBracketSlot).
# En eliminatorias HORARIOS trae placeholders secuenciales ("Round of 32 1 Winner")
# que NO reflejan el cruce real del Mundial 2026; el cruce correcto se infiere con
# WC2026_MAP a partir de los ganadores que predijo el jugador en rondas anteriores.
_RONDA_MAP_EN = {32: "R32", 16: "R16", 8: "QF", 4: "SF"}
_FEED_RONDA   = {"R16": "R32", "QF": "R16", "SF": "QF", "3ER": "SF", "FINAL": "SF"}
_WC2026_MAP   = {
    "R16":   [[0, 3], [2, 5], [1, 4], [6, 7], [11, 10], [9, 8], [14, 13], [12, 15]],
    "QF":    [[0, 1], [4, 5], [2, 3], [7, 6]],
    "SF":    [[0, 1], [2, 3]],
    "FINAL": [[0, 1]],
}

def _parse_bracket_ref(raw: str):
    import re as _re
    if not raw:
        return None
    m = _re.match(r"Round of (\d+) (\d+) Winner", raw, _re.I)
    if m:
        ronda = _RONDA_MAP_EN.get(int(m.group(1)))
        if ronda:
            return {"ronda": ronda, "nth": int(m.group(2)), "type": "winner", "jgo": None}
    typ = "loser" if _re.match(r"^Perdedor", raw, _re.I) else "winner"
    if _re.match(r"^(Ganador|Perdedor)", raw, _re.I):
        for pat, ronda in ((r"Dieciseisavos", "R32"), (r"Octavos", "R16"),
                           (r"Cuartos", "QF"), (r"Semifinal", "SF")):
            if _re.search(pat, raw, _re.I):
                mp = _re.search(r"\((\d+)\)", raw)
                me = _re.search(r"\s(\d+)$", raw)
                nth = int(mp.group(1)) if mp else (int(me.group(1)) if me else None)
                if nth is not None:
                    return {"ronda": ronda, "nth": nth, "type": typ, "jgo": None}
    mg = _re.search(r"(?:Winner\s+)?Game\s+(\d+)(?:\s+Winner)?", raw, _re.I)
    if mg:
        return {"ronda": None, "nth": None, "type": "winner", "jgo": int(mg.group(1))}
    return None

def _is_placeholder(name: str) -> bool:
    return _parse_bracket_ref(name) is not None

def _resolve_team_name(raw, by_jgo, by_ronda, picks, depth=0):
    if not raw or depth > 8:
        return raw
    ref = _parse_bracket_ref(raw)
    if not ref:
        return raw  # ya es un nombre real
    if ref["jgo"]:
        ref_game = by_jgo.get(str(ref["jgo"]))
    else:
        rg  = by_ronda.get(ref["ronda"], [])
        idx = ref["nth"] - 1
        ref_game = rg[idx] if 0 <= idx < len(rg) else None
    if not ref_game:
        return raw
    pk  = picks.get(str(ref_game["jgo"])) or {}
    gan = pk.get("gan", "")
    if not gan:
        return raw  # sin pick → mantener placeholder
    if ref["type"] == "loser":
        r1 = _resolve_team_name(ref_game.get("eq1", ""), by_jgo, by_ronda, picks, depth + 1)
        r2 = _resolve_team_name(ref_game.get("eq2", ""), by_jgo, by_ronda, picks, depth + 1)
        if gan == r1 or gan == pk.get("eq1"):
            return r2 or pk.get("eq2") or raw
        if gan == r2 or gan == pk.get("eq2"):
            return r1 or pk.get("eq1") or raw
        return raw
    return _resolve_team_name(gan, by_jgo, by_ronda, picks, depth + 1)

def _infer_bracket_slot(game, slot, by_jgo, by_ronda, picks):
    feed_ronda = _FEED_RONDA.get(game.get("ronda"))
    if not feed_ronda:
        return None  # R32 / fase de grupos: no se infiere
    this_round = by_ronda.get(game.get("ronda"), [])
    my_idx = next((i for i, g in enumerate(this_round)
                   if str(g["jgo"]) == str(game["jgo"])), -1)
    if my_idx < 0:
        return None
    feed_games = by_ronda.get(feed_ronda, [])

    if game.get("ronda") == "3ER":  # perdedor SF1 vs perdedor SF2
        sf_idx  = 0 if slot == "eq1" else 1
        sf_game = feed_games[sf_idx] if sf_idx < len(feed_games) else None
        if not sf_game:
            return None
        pk = picks.get(str(sf_game["jgo"])) or {}
        if not pk.get("gan"):
            return f"Perdedor SF{sf_idx + 1}"
        gan    = _resolve_team_name(pk["gan"], by_jgo, by_ronda, picks) or pk["gan"]
        sf_eq1 = _resolve_team_name(sf_game.get("eq1", ""), by_jgo, by_ronda, picks) or sf_game.get("eq1", "")
        sf_eq2 = _resolve_team_name(sf_game.get("eq2", ""), by_jgo, by_ronda, picks) or sf_game.get("eq2", "")
        if not sf_eq1 or sf_eq1 == "TBD":
            sf_eq1 = _resolve_team_name(pk.get("eq1", ""), by_jgo, by_ronda, picks) or pk.get("eq1", "")
        if not sf_eq2 or sf_eq2 == "TBD":
            sf_eq2 = _resolve_team_name(pk.get("eq2", ""), by_jgo, by_ronda, picks) or pk.get("eq2", "")
        if not sf_eq1 or sf_eq1 == "TBD":
            inf = _infer_bracket_slot(sf_game, "eq1", by_jgo, by_ronda, picks)
            if inf and not inf.startswith("Gan. ") and not inf.startswith("Perdedor "):
                sf_eq1 = inf
        if not sf_eq2 or sf_eq2 == "TBD":
            inf = _infer_bracket_slot(sf_game, "eq2", by_jgo, by_ronda, picks)
            if inf and not inf.startswith("Gan. ") and not inf.startswith("Perdedor "):
                sf_eq2 = inf
        loser = sf_eq2 if gan == sf_eq1 else (sf_eq1 if gan == sf_eq2 else "")
        return loser or f"Perdedor SF{sf_idx + 1}"

    slot_idx  = 0 if slot == "eq1" else 1
    ronda_map = _WC2026_MAP.get(game.get("ronda"))
    if ronda_map and my_idx < len(ronda_map):
        feed_idx = ronda_map[my_idx][slot_idx]
    else:
        feed_idx = my_idx * 2 if slot == "eq1" else my_idx * 2 + 1  # fallback secuencial
    feed_game = feed_games[feed_idx] if 0 <= feed_idx < len(feed_games) else None
    if not feed_game:
        return None
    pk = picks.get(str(feed_game["jgo"])) or {}
    if not pk.get("gan"):
        return f"Gan. JGO {feed_game['jgo']}"
    resolved = _resolve_team_name(pk["gan"], by_jgo, by_ronda, picks) or pk["gan"]
    return f"Gan. JGO {feed_game['jgo']}" if _is_placeholder(resolved) else resolved

def _disp_team_pdf(game, slot, by_jgo, by_ronda, picks):
    """Equipo a mostrar en el PDF para un slot: el que predijo el jugador."""
    raw = game.get(slot) or ""
    if game.get("ronda") not in _FEED_RONDA:
        return raw  # R32 / grupos: equipo real directo
    inf = _infer_bracket_slot(game, slot, by_jgo, by_ronda, picks)
    if inf and not inf.startswith("Gan. ") and not inf.startswith("Perdedor "):
        return inf
    # Fallbacks: pick guardado (si es nombre real) → resolver placeholder → inf/raw
    stored = (picks.get(str(game["jgo"])) or {}).get(slot) or ""
    if stored and not _is_placeholder(stored):
        return stored
    resolved = _resolve_team_name(raw, by_jgo, by_ronda, picks)
    return resolved or inf or raw


@app.get("/api/picks/pdf")
async def picks_pdf(phone: str = Query(""), email: str = Query(""),
                    ql_session: str = Cookie(default="")):
    """Genera un PDF con todos los picks del jugador.
    Identifica al jugador por la cookie de sesion; phone/email son fallback."""
    from fpdf import FPDF
    from io import BytesIO
    from datetime import datetime as _dt
    import re as _re

    p = (find_player_any(phone=ql_session, email=ql_session) if ql_session else None) \
        or find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    player_id = p.get("_id") or p.get("id")
    if not player_id:
        raise HTTPException(404, "Jugador sin ID")

    nombre    = p.get("NOMBRE", "Jugador")
    tel       = phone or p.get("WHATSAPP") or p.get("TELEFONO") or "sin-tel"
    tel_clean = _re.sub(r"[^\d]", "", tel)

    raw_picks = _db.db_get_picks(int(player_id))
    games     = _db.db_get_horarios()

    # HORARIOS guarda la ronda en la columna 'grupo' (R32/R16/QF/SF/3ER/FINAL).
    # Normalizar a 'ronda' para que la resolucion de bracket la lea correctamente.
    for g in games:
        if not g.get("ronda"):
            g["ronda"] = g.get("grupo", "") or ""

    RONDA_ORDER   = ["R32", "R16", "QF", "SF", "3ER", "FINAL"]
    RONDA_LABEL   = {
        "R32": "Dieciseisavos",
        "R16": "Octavos de Final",
        "QF":  "Cuartos de Final",
        "SF":  "Semifinal",
        "3ER": "Tercer Puesto",
        "FINAL": "Final",
    }
    RONDA_ORD_MAP = {k: i for i, k in enumerate(RONDA_ORDER)}

    sorted_games = sorted(
        games,
        key=lambda g: (
            RONDA_ORD_MAP.get(g.get("ronda") or g.get("grupo", ""), 99),
            g.get("fecha", ""),
            g.get("hora", ""),
            int(g.get("jgo", 0)) if str(g.get("jgo", "")).isdigit() else 0,
        ),
    )

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=14)

    # ── Encabezado (estilo F1) ─────────────────────────────────────────────
    torneo = state.get("cfg", {}).get("TORNEO", "Quiniela")
    pdf.set_text_color(0, 40, 104)
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 11, torneo, new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_text_color(40, 40, 40)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Jugador: {nombre}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 6, f"Telefono: {tel}",   new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 6, f"Generado: {_dt.now().strftime('%d/%m/%Y %H:%M')}",
             new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)

    # Anchos de columna (estilo F1: goles local/visitante + resultado 1/E/2)
    W_NUM, W_LOCAL, W_VIS, W_GL, W_GV, W_RES = 9, 44, 44, 15, 15, 14

    # Indices para resolver cruces de bracket segun los picks del jugador
    by_jgo   = {str(g["jgo"]): g for g in games}
    by_ronda = {}
    for g in games:
        by_ronda.setdefault(g.get("ronda") or g.get("grupo") or "?", []).append(g)
    for k in by_ronda:
        by_ronda[k].sort(key=lambda g: int(g["jgo"]) if str(g["jgo"]).isdigit() else 0)

    # Contenido por ronda
    total_count = filled_count = 0
    current_ronda = None
    for g in sorted_games:
        ronda = g.get("ronda") or g.get("grupo") or "?"
        eq1   = g.get("eq1") or ""
        eq2   = g.get("eq2") or ""
        if not eq1 or not eq2:
            continue
        jgo_str = str(g["jgo"])
        pk      = raw_picks.get(jgo_str) or raw_picks.get(int(g["jgo"]), {})
        gol1    = str(pk.get("g1", "")) if pk else ""
        gol2    = str(pk.get("g2", "")) if pk else ""
        ganador = pk.get("gan", "") if pk else ""

        # Resultado del marcador predicho (el ganador ya lo da la columna Ganador):
        # E = empate, G = hay ganador por marcador (logro / PTS_LOGRO)
        if gol1 != "" and gol2 != "" and gol1.lstrip("-").isdigit() and gol2.lstrip("-").isdigit():
            res_sign = "E" if int(gol1) == int(gol2) else "G"
        else:
            res_sign = "-"

        # En eliminatorias el horario trae placeholders ("Round of 32 1 Winner")
        # con emparejamiento secuencial que NO refleja el bracket real. Inferir el
        # cruce que predijo el jugador con WC2026_MAP + sus ganadores previos.
        disp_eq1 = _disp_team_pdf(g, "eq1", by_jgo, by_ronda, raw_picks)
        disp_eq2 = _disp_team_pdf(g, "eq2", by_jgo, by_ronda, raw_picks)

        total_count += 1
        if ganador:
            filled_count += 1

        if ronda != current_ronda:
            current_ronda = ronda
            label = RONDA_LABEL.get(ronda, ronda)
            pdf.set_fill_color(245, 184, 0)
            pdf.set_text_color(26, 26, 26)
            pdf.set_font("Helvetica", "B", 11)
            pdf.cell(0, 8, f"  {label}", new_x="LMARGIN", new_y="NEXT", fill=True)
            pdf.ln(1)
            # Cabecera de columnas (azul con texto blanco, estilo F1)
            pdf.set_fill_color(0, 40, 104)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("Helvetica", "B", 8)
            pdf.cell(W_NUM,   6, "#",         border=0, fill=True, align="C")
            pdf.cell(W_LOCAL, 6, "Local",     border=0, fill=True)
            pdf.cell(W_VIS,   6, "Visitante", border=0, fill=True)
            pdf.cell(W_GL,    6, "G.Local",   border=0, fill=True, align="C")
            pdf.cell(W_GV,    6, "G.Visit.",  border=0, fill=True, align="C")
            pdf.cell(W_RES,   6, "Res.",      border=0, fill=True, align="C")
            pdf.cell(0,       6, "Ganador",   border=0, fill=True, align="C",
                     new_x="LMARGIN", new_y="NEXT")

        if int(g["jgo"]) % 2 == 0:
            pdf.set_fill_color(255, 255, 255)
        else:
            pdf.set_fill_color(245, 247, 250)
        pdf.set_text_color(30, 30, 30)
        pdf.set_font("Helvetica", "", 8)
        pdf.cell(W_NUM,   6, jgo_str,       border=0, fill=True, align="C")
        pdf.cell(W_LOCAL, 6, disp_eq1[:26], border=0, fill=True)
        pdf.cell(W_VIS,   6, disp_eq2[:26], border=0, fill=True)
        pdf.cell(W_GL,    6, gol1 if gol1 != "" else "-", border=0, fill=True, align="C")
        pdf.cell(W_GV,    6, gol2 if gol2 != "" else "-", border=0, fill=True, align="C")
        # Columna Resultado (G/E): empate en magenta, ganador en azul
        pdf.set_font("Helvetica", "B", 8)
        if res_sign == "E":
            pdf.set_text_color(190, 24, 93)
        elif res_sign == "G":
            pdf.set_text_color(37, 99, 235)
        else:
            pdf.set_text_color(180, 180, 180)
        pdf.cell(W_RES, 6, res_sign, border=0, fill=True, align="C")
        # Columna Ganador (nombre del equipo)
        if ganador:
            pdf.set_text_color(0, 104, 71)
            pdf.set_font("Helvetica", "B", 8)
        else:
            pdf.set_text_color(180, 180, 180)
            pdf.set_font("Helvetica", "", 8)
        pdf.cell(0, 6, ganador[:22] if ganador else "-",
                 border=0, fill=True, align="C", new_x="LMARGIN", new_y="NEXT")

    # ── Pie (estilo F1) ─────────────────────────────────────────────────────
    pdf.ln(4)
    pdf.set_text_color(120, 120, 120)
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 5, f"Picks completados: {filled_count} / {total_count}",
             align="R", new_x="LMARGIN", new_y="NEXT")

    ts_str    = _dt.now().strftime("%Y%m%d%H%M")
    nom_clean = _re.sub(r"[^\w]", "_", nombre)[:20]
    filename  = f"{nom_clean}_{tel_clean}_{ts_str}.pdf"

    buf = BytesIO()
    buf.write(pdf.output())
    buf.seek(0)

    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



# ââ Partidos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/games")
async def get_games():
    games, _ = _get_games_cache()
    has_finals = any(g["estado"] == "FINAL" for g in games)
    # Adjuntar minuto desde cache en memoria (sin llamada extra a Sheets)
    for g in games:
        g["minuto"] = _live_clocks.get(g.get("espn_id", ""), "")
    return {"games": games, "has_finals": has_finals,
            "data_ts": int(_cache["data_change_ts"] * 1000)}


# ââ Picks âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/picks")
async def get_picks(email: str = Query(""), phone: str = Query("")):
    p = find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    player_id = p.get("_id") or p.get("id")
    if not player_id:
        return {"picks": {}}
    raw = _db.db_get_picks(int(player_id))
    # Formato esperado por el frontend: {jgo: {eq1,gol1,gol2,eq2,ganador}}
    games_map = {str(g["jgo"]): g for g in _db.db_get_horarios()}
    picks = {}
    for jgo, pk in raw.items():
        gm = games_map.get(str(jgo), {})
        picks[str(jgo)] = {
            "eq1":     gm.get("eq1", ""),
            "gol1":    pk.get("g1", ""),
            "gol2":    pk.get("g2", ""),
            "eq2":     gm.get("eq2", ""),
            "ganador": pk.get("gan", ""),
        }
    return {"picks": picks}


@app.post("/api/picks")
async def save_picks(body: SavePicksBody):
    p = find_player_any(phone=body.phone, email=body.email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")

    player_id = p.get("_id") or p.get("id")
    if not player_id:
        raise HTTPException(404, "Jugador sin ID")

    games, _ = _get_games_cache()
    modo_prueba = state.get("cfg", {}).get("MODO_PRUEBA", "") in ("1", "true", "True")

    r32_games = [g for g in games if g.get("ronda") == RONDA_BASE]
    upper_locked = False
    if r32_games and not modo_prueba:
        last_r32 = max(r32_games, key=lambda g: int(g.get("jgo", 0) or 0))
        upper_locked = bool(last_r32.get("estado", "") not in ("", "PROG"))

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
            else:
                bloq = bool(estado and estado != "PROG")
            if bloq:
                bloqueados += 1
                continue

        _db.db_save_pick(
            int(player_id), str(pick.jgo),
            str(pick.gol1), str(pick.gol2), str(pick.ganador),
            eq1=str(pick.eq1 or ""), eq2=str(pick.eq2 or "")
        )
        guardados += 1

    if guardados:
        _cache["standings_rows"] = None  # invalidar standings al guardar picks

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
        "pts_logro":   int(cfg.get("PTS_LOGRO",   1) or 1),
        "pts_gan":     int(cfg.get("PTS_GAN",     2) or 2),
        "pts_gol1":    int(cfg.get("PTS_GOL1",    1) or 1),
        "pts_gol2":    int(cfg.get("PTS_GOL2",    1) or 1),
        "pts_campeon": int(cfg.get("PTS_CAMPEON",  0) or 0),
    }


@app.get("/api/standings")
async def get_standings():
    try:
        # Calcular SIEMPRE en vivo desde SQLite (igual que /api/my-points). La cache
        # se mantiene solo para otros consumidores (top3/top5), pero el endpoint no
        # debe servir una cache potencialmente vieja (bug: tabla en 0 tras simular).
        cfg = state.get("cfg", {})
        st  = _db.db_compute_standings(cfg)
        if not st:
            return {"rows": []}
        lider = st[0]["pts"]
        rows, pos = [], 1
        for i, s in enumerate(st):
            if i > 0:
                prev = st[i - 1]
                if not (s["pts"] == prev["pts"] and s["gan"] == prev["gan"] and
                        s["g1"] + s["g2"] == prev["g1"] + prev["g2"]):
                    pos = i + 1
            rows.append([pos, s["nombre"], s["pts"], s["pts"] - lider])
        result = [["POS", "NOMBRE", "Ptos", "Diferencia"]] + rows
        _cache["standings_rows"] = result
        return {"rows": result}
    except Exception:
        return {"rows": []}


# ââ Mis Puntos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/my-points")
async def get_my_points(email: str = Query(""), phone: str = Query("")):
    try:
        player = find_player_any(phone=phone, email=email)
        if not player:
            raise HTTPException(404, "Jugador no encontrado")

        cfg    = state.get("cfg", {})
        games, _ = _get_games_cache()

        _v_logro   = int(cfg.get("PTS_LOGRO", 1) or 1)
        _v_gan     = int(cfg.get("PTS_GAN",   2) or 2)
        _v_gol1    = int(cfg.get("PTS_GOL1",  1) or 1)
        _v_gol2    = int(cfg.get("PTS_GOL2",  1) or 1)
        _v_campeon = int(cfg.get("PTS_CAMPEON", 0) or 0)

        def _res(g1, g2):
            try: return "1" if int(g1) > int(g2) else ("2" if int(g1) < int(g2) else "X")
            except: return ""

        picks_raw = _db.db_get_picks(player["_id"])  # {jgo_str: {g1,g2,gan}}

        by_day    = {}
        total_pts = 0

        for jgo_str, pk in picks_raw.items():
            game = next((g for g in games if g["jgo"] == jgo_str), None)
            if not game:
                continue
            estado = game.get("estado", "")
            if not estado or estado == "PROG":
                continue

            real_g1  = game.get("gol1",    "")
            real_g2  = game.get("gol2",    "")
            real_gan = game.get("ganador", "")
            eq1_real = game.get("eq1", "")
            eq2_real = game.get("eq2", "")
            pick_g1  = pk.get("g1", "") or ""
            pick_g2  = pk.get("g2", "") or ""
            pick_gan = pk.get("gan", "") or ""
            eq1_pick = pk.get("eq1", "") or ""
            eq2_pick = pk.get("eq2", "") or ""

            # teamAlive: al menos 1 equipo predicho debe estar en el partido real
            team_alive = True
            if eq1_real and eq2_real:
                real_teams = {eq1_real.strip(), eq2_real.strip()}
                pred_teams = {eq1_pick.strip(), eq2_pick.strip(), pick_gan.strip()}
                pred_teams = {t for t in pred_teams if t and not t.startswith("Gan. ")}
                if pred_teams and not (pred_teams & real_teams):
                    team_alive = False

            if not pick_g1 or not pick_g2 or not pick_gan or not team_alive:
                pts = pts_logro = pts_gan = pts_gol1 = pts_gol2 = 0
            else:
                # Detectar orden invertido (eq1_pick es eq2 en horarios) y cruzar goles
                _inverted = bool(
                    (eq1_pick and eq2_real and eq1_pick.strip() == eq2_real.strip()) or
                    (eq2_pick and eq1_real and eq2_pick.strip() == eq1_real.strip())
                )
                g1r_eff = real_g2 if _inverted else real_g1
                g2r_eff = real_g1 if _inverted else real_g2
                pts_logro = _v_logro if (g1r_eff != "" and g2r_eff != "" and
                                          _res(pick_g1, pick_g2) == _res(g1r_eff, g2r_eff)) else 0
                pts_gan   = _v_gan  if (real_gan and pick_gan == real_gan) else 0
                pts_gol1  = _v_gol1 if (g1r_eff != "" and pick_g1 == g1r_eff) else 0
                pts_gol2  = _v_gol2 if (g2r_eff != "" and pick_g2 == g2r_eff) else 0
                pts_campeon = _v_campeon if (
                    _v_campeon and game.get("ronda","").upper() == "FINAL" and
                    real_gan and pick_gan == real_gan
                ) else 0
                pts = pts_logro + pts_gan + pts_gol1 + pts_gol2 + pts_campeon

            fecha = game.get("fecha", "")
            if fecha not in by_day:
                by_day[fecha] = {"fecha": fecha, "pts": 0, "games": []}
            by_day[fecha]["pts"] += pts
            by_day[fecha]["games"].append({
                "jgo": jgo_str, "ronda": game.get("ronda",""),
                "eq1_real": eq1_real, "eq2_real": eq2_real,
                "pick_eq1": eq1_pick, "pick_eq2": eq2_pick,
                "pick_gol1": pick_g1, "pick_gol2": pick_g2,
                "pick_ganador": pick_gan,
                "gol1_real": real_g1, "gol2_real": real_g2, "gan_real": real_gan,
                "pts": pts, "estado": estado,
                "team_alive": team_alive,
                "ok_logro": pts_logro > 0, "ok_gan": pts_gan > 0,
                "ok_gol1": pts_gol1 > 0,   "ok_gol2": pts_gol2 > 0,
            })
            total_pts += pts

        days = sorted(by_day.values(), key=lambda x: x["fecha"], reverse=True)
        return {"total": total_pts, "days": days}
    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
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

    cfg      = state.get("cfg", {})
    real_g1  = game.get("gol1",    "")
    real_g2  = game.get("gol2",    "")
    real_gan = game.get("ganador", "")
    estado   = game.get("estado",  "")

    _v_logro   = int(cfg.get("PTS_LOGRO", 1) or 1)
    _v_gan     = int(cfg.get("PTS_GAN",   2) or 2)
    _v_gol1    = int(cfg.get("PTS_GOL1",  1) or 1)
    _v_gol2    = int(cfg.get("PTS_GOL2",  1) or 1)
    _v_campeon = int(cfg.get("PTS_CAMPEON", 0) or 0)

    def _res(g1, g2):
        try: return "1" if int(g1) > int(g2) else ("2" if int(g1) < int(g2) else "X")
        except: return ""

    all_picks = _db.db_get_all_picks_for_game(str(jgo))
    game_picks = []
    for pk in all_picks:
        pick_gol1 = pk.get("g1_pick", "") or ""
        pick_gol2 = pk.get("g2_pick", "") or ""
        pick_gan  = pk.get("gan_pick", "") or ""

        if not pick_gol1 or not pick_gol2 or not pick_gan:
            pts = pts_logro = pts_gan = pts_gol1 = pts_gol2 = 0
        else:
            pts_logro = _v_logro if (real_g1 != "" and real_g2 != "" and
                                      _res(pick_gol1, pick_gol2) == _res(real_g1, real_g2)) else 0
            pts_gan   = _v_gan  if (real_gan and pick_gan == real_gan) else 0
            pts_gol1  = _v_gol1 if (real_g1 != "" and pick_gol1 == real_g1) else 0
            pts_gol2  = _v_gol2 if (real_g2 != "" and pick_gol2 == real_g2) else 0
            pts_campeon = _v_campeon if (
                _v_campeon and game.get("ronda","").upper() == "FINAL" and
                real_gan and pick_gan == real_gan
            ) else 0
            pts = pts_logro + pts_gan + pts_gol1 + pts_gol2 + pts_campeon

        game_picks.append({
            "nombre":    pk.get("nombre", ""),
            "pick_gol1": pick_gol1, "pick_gol2": pick_gol2,
            "pick_gan":  pick_gan,  "pts": pts,
            "ok_logro":  pts_logro > 0, "ok_gan": pts_gan > 0,
            "ok_gol1":   pts_gol1  > 0, "ok_gol2": pts_gol2 > 0,
        })

    game_picks.sort(key=lambda x: -x["pts"])
    is_final = estado == "FINAL"
    data = {"game": game, "picks": game_picks}
    _game_picks_cache[key] = {"data": data, "ts": now, "final": is_final}
    return data


# ── Comparar picks de todos los jugadores (partidos iniciados) ─────────────

_compare_cache: dict = {"data": None, "ts": 0.0, "all_final": False}
COMPARE_TTL       = 30   # segundos — partidos en vivo
COMPARE_TTL_FINAL = 300  # segundos — cuando todos son FINAL


def _compute_compare_picks() -> dict:
    """Devuelve partidos INICIADOS con picks de cada jugador, desde SQLite."""
    games, _ = _get_games_cache()
    started  = [g for g in games if g.get("estado") and g["estado"] != "PROG"]
    if not started:
        return {"games": []}

    cfg = state.get("cfg", {})
    all_final    = True
    result_games = []

    for game in started:
        jgo_str  = str(game.get("jgo", ""))
        real_gan = game.get("ganador", "")
        real_g1  = game.get("gol1",    "")
        real_g2  = game.get("gol2",    "")
        estado   = game.get("estado",  "")

        if estado != "FINAL":
            all_final = False

        g1_known  = real_g1  != ""
        g2_known  = real_g2  != ""
        gan_known = real_gan != ""

        all_picks  = _db.db_get_all_picks_for_game(jgo_str)
        game_picks = []

        for pk in all_picks:
            pick_gol1 = pk.get("g1_pick", "") or ""
            pick_gol2 = pk.get("g2_pick", "") or ""
            pick_gan  = pk.get("gan_pick", "") or ""

            real_eq1   = game.get("eq1", "")
            real_eq2   = game.get("eq2", "")
            team_alive = (
                not real_eq1 or not real_eq2 or not pick_gan or
                pick_gan == real_eq1 or pick_gan == real_eq2
            )

            if not pick_gol1 or not pick_gol2 or not pick_gan or not team_alive:
                pts = pts_logro = pts_gan = pts_gol1 = pts_gol2 = 0
            else:
                _v_logro   = int(cfg.get("PTS_LOGRO", 1) or 1)
                _v_gan     = int(cfg.get("PTS_GAN",   2) or 2)
                _v_gol1    = int(cfg.get("PTS_GOL1",  1) or 1)
                _v_gol2    = int(cfg.get("PTS_GOL2",  1) or 1)

                def _res(g1, g2):
                    try: return "1" if int(g1) > int(g2) else ("2" if int(g1) < int(g2) else "X")
                    except: return ""

                pts_logro = _v_logro if (g1_known and g2_known and
                                         _res(pick_gol1, pick_gol2) == _res(real_g1, real_g2)) else 0
                pts_gan   = _v_gan  if (gan_known and pick_gan == real_gan) else 0
                pts_gol1  = _v_gol1 if (g1_known and pick_gol1 == real_g1) else 0
                pts_gol2  = _v_gol2 if (g2_known and pick_gol2 == real_g2) else 0
                _v_campeon = int(cfg.get("PTS_CAMPEON", 0) or 0)
                pts_campeon = _v_campeon if (
                    _v_campeon and
                    game.get("ronda", "").upper() == "FINAL" and
                    gan_known and pick_gan == real_gan
                ) else 0
                pts = pts_logro + pts_gan + pts_gol1 + pts_gol2 + pts_campeon

            game_picks.append({
                "nombre":    pk.get("nombre", ""),
                "pick_eq1":  "",
                "pick_gol1": pick_gol1,
                "pick_gol2": pick_gol2,
                "pick_eq2":  "",
                "pick_gan":  pick_gan,
                "pts":       pts,
                "pts_logro": pts_logro,
                "pts_gan":   pts_gan,
                "pts_gol1":  pts_gol1,
                "pts_gol2":  pts_gol2,
                "ok_logro":  pts_logro > 0,
                "ok_gan":    pts_gan   > 0,
                "ok_gol1":   pts_gol1  > 0,
                "ok_gol2":   pts_gol2  > 0,
            })

        game_picks.sort(key=lambda x: -x["pts"])
        result_games.append({
            "jgo":     jgo_str,
            "ronda":   game.get("ronda", ""),
            "fecha":   game.get("fecha", ""),
            "eq1":     game.get("eq1", ""),
            "eq2":     game.get("eq2", ""),
            "estado":  estado,
            "gol1":    real_g1,
            "gol2":    real_g2,
            "ganador": real_gan,
            "picks":   game_picks,
        })

    result = {"games": result_games, "all_final": all_final}
    _cache["compare"]    = result
    _cache["compare_ts"] = time.time()
    return result


@app.get("/api/probabilities")
async def get_probabilities():
    """Distribucion de picks + standings para pantalla Probabilidades."""
    games, _ = _get_games_cache()
    cfg = state.get("cfg", {})

    all_games   = games
    fixed_games = [g for g in all_games if g.get("estado") == "FINAL"]
    prog_games  = [g for g in all_games if not g.get("estado") or g["estado"] == "PROG"]

    # Pick distribution por partido pendiente
    game_dist = []
    for game in prog_games:
        jgo_str   = str(game.get("jgo", ""))
        eq1       = game.get("eq1", "")
        eq2       = game.get("eq2", "")
        if not eq1 or not eq2 or eq1.startswith("Gan.") or eq2.startswith("Gan."):
            continue
        all_picks = _db.db_get_all_picks_for_game(jgo_str)
        total = len(all_picks)
        c1 = sum(1 for p in all_picks if (p.get("gan_pick") or "") == eq1)
        c2 = sum(1 for p in all_picks if (p.get("gan_pick") or "") == eq2)
        game_dist.append({
            "jgo": jgo_str, "ronda": game.get("ronda",""),
            "eq1": eq1, "eq2": eq2, "total": total,
            "picks_eq1": c1, "picks_eq2": c2,
            "pct_eq1": round(c1/total*100) if total else 0,
            "pct_eq2": round(c2/total*100) if total else 0,
        })

    # Standings actuales para construir lista de jugadores
    standings = _db.db_compute_standings(cfg)
    if not standings:
        return {"players": [], "fixed_games": len(fixed_games),
                "pending_games": len(prog_games), "games": game_dist}

    pts_logro_val   = int(cfg.get("PTS_LOGRO",   1) or 1)
    pts_gan_val     = int(cfg.get("PTS_GAN",     2) or 2)
    pts_gol1_val    = int(cfg.get("PTS_GOL1",    1) or 1)
    pts_gol2_val    = int(cfg.get("PTS_GOL2",    1) or 1)
    pts_campeon_val = int(cfg.get("PTS_CAMPEON", 0) or 0)
    max_per_game    = pts_logro_val + pts_gan_val + pts_gol1_val + pts_gol2_val
    pending_count   = len(prog_games)

    lider_pts = standings[0]["pts"] if standings else 0
    players_out = []
    for rank_i, s in enumerate(standings):
        cur_pts  = s["pts"]
        max_add  = pending_count * max_per_game
        # +PTS_CAMPEON si hay FINAL pendiente
        has_final_pending = any(g.get("ronda","").upper()=="FINAL" for g in prog_games)
        if has_final_pending:
            max_add += pts_campeon_val
        max_possible = cur_pts + max_add
        # Probabilidad simplificada: basada en ranking relativo al lider
        # (se actualiza a medida que avanza el torneo)
        if pending_count == 0:
            prob_1st = 100.0 if rank_i == 0 else 0.0
            univ_1st = 100 if rank_i == 0 else 0
            univ_2nd = 100 if rank_i == 1 else 0
        else:
            gap_to_leader = lider_pts - cur_pts
            can_catch = max_possible >= lider_pts
            # Probabilidad estimada: proporcional a pts relativos
            total_pts_all = sum(ss["pts"] for ss in standings) or 1
            raw_prob = cur_pts / total_pts_all * 100 if total_pts_all else 0
            prob_1st = round(raw_prob, 1) if can_catch else 0.0
            univ_1st = round(prob_1st) if can_catch else 0
            univ_2nd = round(raw_prob * 0.8, 0) if can_catch else 0
        players_out.append({
            "name":         s["nombre"],
            "rank":         rank_i + 1,
            "current_pts":  cur_pts,
            "max_possible": max_possible,
            "prob_1st":     prob_1st,
            "univ_1st":     univ_1st,
            "univ_2nd":     univ_2nd,
        })

    return {
        "players":      players_out,
        "fixed_games":  len(fixed_games),
        "pending_games": pending_count,
        "games":        game_dist,
    }

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
    """Retorna los partidos PROXIMOS (PROG) con picks de cada jugador -- SQLite."""
    games, _ = _get_games_cache()
    upcoming  = [g for g in games if not g.get("estado") or g["estado"] == "PROG"]
    if not upcoming:
        return {"games": []}

    result_games = []
    for game in upcoming:
        jgo_str = str(game.get("jgo", ""))
        eq1     = game.get("eq1", "")
        eq2     = game.get("eq2", "")
        # Saltar si equipos aun no definidos (placeholder)
        if not eq1 or not eq2 or eq1.startswith("Gan.") or eq2.startswith("Gan."):
            continue

        all_picks = _db.db_get_all_picks_for_game(jgo_str)
        game_picks = []
        for pk in all_picks:
            game_picks.append({
                "nombre": pk.get("nombre", "?"),
                "gol1":   pk.get("g1_pick", "") or "",
                "gol2":   pk.get("g2_pick", "") or "",
                "gan":    pk.get("gan_pick", "") or "",
            })

        game_picks.sort(key=lambda x: (not bool(x["gan"]), x["nombre"]))

        result_games.append({
            "jgo":          jgo_str,
            "ronda":        game.get("ronda",  ""),
            "eq1":          eq1,
            "eq2":          eq2,
            "fecha":        game.get("fecha",  ""),
            "hora":         game.get("hora",   ""),
            "datetime_utc": game.get("datetime_utc", ""),
            "picks":        game_picks,
        })

    result_games.sort(key=lambda g: g.get("datetime_utc") or g.get("fecha") or "")
    out = {"games": result_games, "computed_at": datetime.now().isoformat()}
    _upcoming_cache["data"] = out
    _upcoming_cache["ts"]   = time.time()
    return out

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
    ("PTS_CAMPEON",         "Bono por acertar al campeón del torneo (ganador de la Final)"),
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
        jugadores = _db.db_get_jugadores()
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
    # Guardar en SQLite
    _db.db_save_config(body.fields)
    # Refrescar config en memoria
    state["cfg"] = _db.db_get_config()
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
    """Lee jugadores desde SQLite. API compatible con codigo legacy."""
    jugadores   = _db.db_get_jugadores()
    headers     = ["EMAIL", "NOMBRE", "WHATSAPP", "TELEFONO", "TAB_NOMBRE",
                   "PAGADO", "FECHA_REGISTRO", "#"]
    rows_compat = [_jugador_db_to_cache(p) for p in jugadores]
    return None, rows_compat, 0, headers


@app.delete("/api/admin/players/{jugador_id}")
async def admin_delete_player(jugador_id: int, ql_admin: str = Cookie(default="")):
    """Elimina un jugador y todos sus picks (útil para pruebas)."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    _db.db_delete_player(jugador_id)
    return {"ok": True, "deleted": jugador_id}


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
        for row in rows:
            # rows son dicts (formato SQLite via _jugador_db_to_cache)
            if not row:
                continue
            d = _normalize_player(row)
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
    result["torneo_activo"] = _torneo_activo().get("activo", False)
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


@app.get("/api/admin/debug-picks")
async def admin_debug_picks(jgo_desde: int = 17, jgo_hasta: int = 24, ql_admin: str = Cookie(default="")):
    """Diagnóstico: muestra picks de todos los jugadores para un rango de JGO."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    conn = _db.get_conn()
    try:
        rows = conn.execute("""
            SELECT j.nombre, p.jgo, p.g1_pick, p.g2_pick, p.gan_pick
            FROM picks p JOIN jugadores j ON j.id = p.jugador_id
            WHERE CAST(p.jgo AS INTEGER) BETWEEN ? AND ?
            ORDER BY j.nombre, CAST(p.jgo AS INTEGER)
        """, (jgo_desde, jgo_hasta)).fetchall()
        result = {}
        for r in rows:
            nombre = r["nombre"]
            if nombre not in result:
                result[nombre] = []
            result[nombre].append({
                "jgo": r["jgo"],
                "g1": r["g1_pick"] or "",
                "g2": r["g2_pick"] or "",
                "gan": r["gan_pick"] or "",
                "tiene_pick": bool(r["g1_pick"] or r["g2_pick"] or r["gan_pick"])
            })
        return {"jgo_desde": jgo_desde, "jgo_hasta": jgo_hasta, "jugadores": result}
    finally:
        conn.close()

@app.get("/api/admin/player-points")
async def admin_player_points(q: str = Query(""), ql_admin: str = Cookie(default="")):
    """Diagnostico: desglose de puntos por juego de UN jugador, con la MISMA
    funcion de calculo que la tabla de posiciones (_calc_pts). Util para auditar.
    Uso: /api/admin/player-points?q=eudi  (q = nombre parcial, telefono, email o id)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    if not q.strip():
        raise HTTPException(400, "Falta ?q= (nombre, telefono, email o id)")
    ql = q.strip().lower()
    jugadores = _db.db_get_jugadores()
    match = next((j for j in jugadores if (
        ql in (j.get("nombre", "").lower())
        or ql == (j.get("whatsapp", "").lower())
        or ql == (j.get("email", "").lower())
        or ql == str(j.get("id", "")))), None)
    if not match:
        raise HTTPException(404, f"Jugador no encontrado para '{q}'")

    cfg = state.get("cfg", {})
    vL = int(cfg.get("PTS_LOGRO", 1) or 1); vG = int(cfg.get("PTS_GAN", 2) or 2)
    v1 = int(cfg.get("PTS_GOL1", 1) or 1);  v2 = int(cfg.get("PTS_GOL2", 1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)

    picks    = _db.db_get_picks(match["id"])
    by_jgo   = {str(h["jgo"]): h for h in _db.db_get_horarios()}

    juegos = []
    tot = {"logro": 0, "gan": 0, "gol1": 0, "gol2": 0, "campeon": 0, "total": 0}
    for jgo_str in sorted(by_jgo, key=lambda x: int(x) if x.isdigit() else 0):
        h = by_jgo[jgo_str]
        estado = h.get("estado", "")
        if not estado or estado == "PROG":
            continue
        pk = picks.get(jgo_str, {})
        pl, pg, pg1, pg2, total = _db._calc_pts(
            pk.get("g1", ""), pk.get("g2", ""), pk.get("gan", ""),
            h.get("gol1", ""), h.get("gol2", ""), h.get("ganador", ""), estado,
            vL, vG, v1, v2, vC, h.get("grupo", ""),
            eq1_pick=pk.get("eq1", ""), eq2_pick=pk.get("eq2", ""),
            eq1_real=h.get("eq1", ""), eq2_real=h.get("eq2", ""),
        )
        pc = total - (pl + pg + pg1 + pg2)  # campeon = lo que _calc_pts sumo aparte
        # teamAlive (mismo criterio que _calc_pts) para mostrarlo explicito
        team_alive = True
        e1r = (h.get("eq1", "") or "").strip(); e2r = (h.get("eq2", "") or "").strip()
        if e1r and e2r:
            real = {e1r, e2r}
            pred = {(pk.get("eq1", "") or "").strip(), (pk.get("eq2", "") or "").strip(),
                    (pk.get("gan", "") or "").strip()}
            pred = {t for t in pred if t and not t.startswith("Gan. ")}
            if pred and not (pred & real):
                team_alive = False
        juegos.append({
            "jgo": jgo_str, "ronda": h.get("grupo", ""),
            "eq1_real": h.get("eq1", ""), "eq2_real": h.get("eq2", ""),
            "marcador_real": f"{h.get('gol1','')}-{h.get('gol2','')}", "gan_real": h.get("ganador", ""),
            "pick_marcador": f"{pk.get('g1','')}-{pk.get('g2','')}", "pick_gan": pk.get("gan", ""),
            "eq1_pick": pk.get("eq1", ""), "eq2_pick": pk.get("eq2", ""),
            "team_alive": team_alive,
            "pts_logro": pl, "pts_gan": pg, "pts_gol1": pg1, "pts_gol2": pg2, "pts_campeon": pc,
            "pts": total,
        })
        tot["logro"] += pl; tot["gan"] += pg; tot["gol1"] += pg1
        tot["gol2"] += pg2; tot["campeon"] += pc; tot["total"] += total

    return {
        "jugador": match.get("nombre", ""), "jugador_id": match["id"],
        "valores_pts": {"logro": vL, "gan": vG, "gol1": v1, "gol2": v2, "campeon": vC},
        "totales": tot,
        "juegos_finalizados": len(juegos),
        "juegos": juegos,
    }


@app.post("/api/admin/fix-bracket")
async def admin_fix_bracket(ql_admin: str = Cookie(default="")):
    """Fuerza re-propagacion del bracket completo. Util para corregir 3ER u otros slots
    que quedaron con equipos incorrectos (ej. ganadores en vez de perdedores)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    try:
        changes = _propagate_bracket()
        _invalidate_games()
        return {"changes": changes, "total": len(changes)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/admin/backfill-eq-picks")
async def admin_backfill_eq_picks(ql_admin: str = Cookie(default="")):
    """Rellena eq1_pick/eq2_pick retroactivamente para picks existentes que los tengan vacios.
    Usa _inferBracketSlot (misma logica que el frontend) para deducir los equipos predichos
    a partir del gan_pick en rondas anteriores.
    Solo afecta filas donde eq1_pick='' Y eq2_pick=''.
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    games, _ = _get_games_cache()
    games_map = {g["jgo"]: g for g in games}

    # Mapa WC2026: misma logica que webapp.py _propagate_bracket y frontend _inferBracketSlot
    WC2026_MAP = {
        "R16": [[0,1],[4,5],[2,3],[7,6],[8,9],[12,13],[10,11],[15,14]],
        "QF":  [[0,1],[4,5],[2,3],[7,6]],
        "SF":  [[0,1],[2,3]],
        "FINAL": [[0,1]],
        "3ER":   [[0,1]],
    }
    # Juegos R32 y R16 ordenados por jgo
    def _sorted_ronda(ronda):
        return sorted([g for g in games if g.get("ronda") == ronda],
                      key=lambda g: int(g["jgo"]) if str(g["jgo"]).isdigit() else 0)

    r32 = _sorted_ronda("R32")
    r16 = _sorted_ronda("R16")
    qf  = _sorted_ronda("QF")
    sf  = _sorted_ronda("SF")

    ronda_lists = {"R32": r32, "R16": r16, "QF": qf, "SF": sf}

    def _infer_teams(jgo_str: str, picks_for_player: dict):
        """Dado un jgo y los picks del jugador, infiere los equipos que el jugador
        esperaba que jugaran ese partido (igual que _inferBracketSlot en frontend)."""
        game = games_map.get(jgo_str)
        if not game:
            return "", ""
        ronda = game.get("ronda", "")
        if ronda == "R32":
            return game.get("eq1",""), game.get("eq2","")
        slot_map = WC2026_MAP.get(ronda)
        if not slot_map:
            return game.get("eq1",""), game.get("eq2","")
        # Encontrar el indice de este juego en su ronda
        ronda_games = ronda_lists.get(ronda, [])
        try:
            idx = next(i for i,g in enumerate(ronda_games) if g["jgo"] == jgo_str)
        except StopIteration:
            return game.get("eq1",""), game.get("eq2","")
        if idx >= len(slot_map):
            return game.get("eq1",""), game.get("eq2","")
        prev_indices = slot_map[idx]  # [i1, i2] indices de juegos de ronda anterior
        # Ronda anterior
        prev_ronda = {"R16":"R32", "QF":"R16", "SF":"QF", "FINAL":"SF", "3ER":"SF"}.get(ronda)
        if not prev_ronda:
            return game.get("eq1",""), game.get("eq2","")
        prev_games = ronda_lists.get(prev_ronda, [])

        def _get_predicted_winner(prev_idx):
            if prev_idx >= len(prev_games):
                return ""
            prev_g = prev_games[prev_idx]
            pk = picks_for_player.get(prev_g["jgo"]) or picks_for_player.get(str(prev_g["jgo"])) or {}
            return pk.get("gan", "") or ""

        eq1 = _get_predicted_winner(prev_indices[0]) if len(prev_indices) > 0 else ""
        eq2 = _get_predicted_winner(prev_indices[1]) if len(prev_indices) > 1 else ""
        return eq1, eq2

    conn = _db.get_conn()
    try:
        jugadores = conn.execute("SELECT id FROM jugadores").fetchall()
        updated = 0
        for j in jugadores:
            jid = j["id"]
            pk_rows = conn.execute(
                "SELECT jgo, g1_pick, g2_pick, gan_pick, eq1_pick, eq2_pick FROM picks WHERE jugador_id=?",
                (jid,)
            ).fetchall()
            # Construir mapa de picks del jugador para lookup rapido
            picks_map = {r["jgo"]: {"g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"]}
                         for r in pk_rows}
            for pk in pk_rows:
                # Solo actualizar si eq1_pick y eq2_pick estan vacios
                if pk["eq1_pick"] or pk["eq2_pick"]:
                    continue
                # Solo para rondas eliminatorias (no R32, que usa equipos reales directamente)
                game = games_map.get(pk["jgo"])
                if not game or game.get("ronda") == "R32":
                    continue
                eq1p, eq2p = _infer_teams(pk["jgo"], picks_map)
                if eq1p or eq2p:
                    conn.execute(
                        "UPDATE picks SET eq1_pick=?, eq2_pick=? WHERE jugador_id=? AND jgo=?",
                        (eq1p, eq2p, jid, pk["jgo"])
                    )
                    updated += 1
        conn.commit()
        return {"updated": updated, "msg": f"Backfill completado: {updated} picks actualizados"}
    finally:
        conn.close()


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
    group_name = cfg.get("WA_GROUP_NAME") or cfg.get("TORNEO", "Quiniela WFC 2026") + " 🏆"
    phones     = _wa_get_phones()
    if not phones:
        return {"ok": False, "msg": "No hay jugadores con telefono registrado"}
    n         = len(phones)
    timeout_s = max(60, n * 1 + 30)
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _wa("POST", "/create-group", timeout=timeout_s, json={"name": group_name, "phones": phones})
    )


@app.post("/api/admin/wa-add-member")
async def wa_add_member(body: dict = None, ql_admin: str = Cookie(default="")):
    """Agrega un jugador individual al grupo de WhatsApp."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    phone = (body or {}).get("phone", "")
    if not phone:
        raise HTTPException(400, "Falta el campo 'phone'")
    return _wa("POST", "/add-member", json={"phone": phone})


@app.post("/api/admin/wa-update-group")
async def wa_update_group(ql_admin: str = Cookie(default="")):
    """Actualiza nombre, icono y sincroniza miembros faltantes del grupo."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    cfg        = state.get("cfg", {})
    group_name = cfg.get("WA_GROUP_NAME") or cfg.get("TORNEO", "Quiniela WFC 2026") + " 🏆"
    phones     = _wa_get_phones()
    n          = len(phones)
    timeout_s  = max(60, n * 1 + 30)
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _wa("POST", "/update-group", timeout=timeout_s, json={"name": group_name, "phones": phones})
    )


@app.post("/api/admin/wa-test")
async def wa_test(ql_admin: str = Cookie(default="")):
    """Envia un mensaje de prueba al grupo de WhatsApp."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return _wa("POST", "/send", json={"message": "🔔 Prueba de notificacion WhatsApp ✅\nLas notificaciones del Mundial estan funcionando."})


@app.post("/api/admin/wa-disconnect")
async def wa_disconnect(body: dict = None, ql_admin: str = Cookie(default="")):
    """Desconecta WhatsApp. Si deleteGroup=true, elimina el grupo primero."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    b = body or {}
    delete_group = b.get("deleteGroup", b.get("delete_group", False))
    return _wa("POST", "/disconnect", json={"deleteGroup": bool(delete_group)})



# ─── Admin: db-dump (vista general SQLite) ────────────────────────────────────

@app.get("/api/admin/db-dump")
async def admin_db_dump(ql_admin: str = Cookie(default="")):
    """Devuelve un snapshot completo de SQLite para el panel admin."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    conn = _db.get_conn()

    # Horarios
    horarios = [dict(r) for r in conn.execute(
        "SELECT jgo, grupo, fecha, hora, eq1, eq2, estado, gol1, gol2, ganador FROM horarios ORDER BY CAST(jgo AS INTEGER)"
    ).fetchall()]

    # Jugadores
    jugadores = [dict(r) for r in conn.execute(
        "SELECT id, nombre, whatsapp, email, fecha_reg FROM jugadores ORDER BY id"
    ).fetchall()]

    # Picks por jugador (join con jugadores para nombre)
    picks_raw = conn.execute("""
        SELECT j.nombre, j.whatsapp, COUNT(*) as total,
               SUM(CASE WHEN p.gan_pick IS NOT NULL AND p.gan_pick != '' THEN 1 ELSE 0 END) as con_ganador
        FROM picks p
        JOIN jugadores j ON j.id = p.jugador_id
        GROUP BY p.jugador_id
    """).fetchall()
    picks_summary = [dict(r) for r in picks_raw]

    # Config
    cfg_raw = conn.execute("SELECT key, value FROM config ORDER BY key").fetchall()
    config = {r["key"]: r["value"] for r in cfg_raw}

    conn.close()

    return {
        "horarios":      horarios,
        "jugadores":     jugadores,
        "picks_summary": picks_summary,
        "config":        config,
        "totals": {
            "horarios":  len(horarios),
            "jugadores": len(jugadores),
            "picks":     sum(p["total"] for p in picks_summary),
        }
    }


# ─── Admin: setup ESPN / bracket / test-mode ──────────────────────────────────

@app.get("/api/admin/setup-status")
async def admin_setup_status(ql_admin: str = Cookie(default="")):
    """Estado del proceso de carga de partidos desde ESPN."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return {"status": state.get("_setup_status", "idle")}


@app.post("/api/admin/setup")
async def admin_setup(ql_admin: str = Cookie(default="")):
    """Recarga los partidos de F2 desde ESPN y los escribe en SQLite."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    t = _torneo_activo()
    if t["activo"]:
        raise HTTPException(423, f'Torneo en curso — panel bloqueado. {t["razon"]}')

    def _run():
        try:
            from datetime import date as _date, timedelta as _td
            cfg       = state["cfg"]
            league    = cfg.get("ESPN_LEAGUE", "fifa.world")
            fecha_ini = _date.fromisoformat(cfg.get("FECHA_INICIO_F2", "2026-07-01"))
            fecha_fin = _date.fromisoformat(cfg.get("FECHA_FIN_F2",    "2026-07-19"))

            conn_h = _db.get_conn()
            with conn_h:
                conn_h.execute("DELETE FROM horarios")
            conn_h.close()
            print("[admin-setup-f2] HORARIOS SQLite limpiado")

            leagues   = [l.strip() for l in league.split(",") if l.strip()]
            ligas_map = {}
            try:
                for liga in _db.db_get_ligas():
                    if liga.get("codigo") and liga.get("espn_id"):
                        ligas_map[liga["codigo"]] = liga["espn_id"]
            except Exception:
                pass

            ligas_sin_id = []
            for lg in leagues:
                eid = ligas_map.get(lg, "")
                if not eid:
                    ligas_sin_id.append(lg)
                print(f"[admin-setup-f2] Liga: {lg} | ESPN_ID: {eid or '(NO CONFIGURADO)'}")

            if ligas_sin_id:
                state["_setup_status"] = (
                    f"ERROR: Faltan ESPN_ID en Ligas para: {', '.join(ligas_sin_id)}."
                )
                return

            summary_url = (f"{ESPN_BASE}/{leagues[0]}/summary" if len(leagues) == 1
                           else f"{ESPN_BASE}/all/summary")

            state["_setup_status"] = f"running — buscando partidos {fecha_ini} -> {fecha_fin}..."
            eventos   = []
            _all_cache: dict = {}
            dia = fecha_ini
            while dia <= fecha_fin:
                fecha_str = dia.strftime("%Y%m%d")
                found     = 0
                seen_ids: set = set()
                for lg in leagues:
                    eid_liga = ligas_map.get(lg, "")
                    try:
                        r = requests.get(f"{ESPN_BASE}/{lg}/scoreboard",
                                         params={"dates": fecha_str, "limit": 500, "lang": "es"},
                                         timeout=15)
                        if r.status_code == 200:
                            ev_list = r.json().get("events", [])
                        else:
                            if fecha_str not in _all_cache:
                                r2 = requests.get(f"{ESPN_BASE}/all/scoreboard",
                                                  params={"dates": fecha_str, "limit": 500, "lang": "es"},
                                                  timeout=15)
                                _all_cache[fecha_str] = (r2.json().get("events", [])
                                                         if r2.status_code == 200 else [])
                                time.sleep(0.2)
                            uid_f   = f"l:{eid_liga}" if eid_liga else None
                            ev_list = [e for e in _all_cache[fecha_str]
                                       if not uid_f or uid_f in e.get("uid", "")]
                        for ev in ev_list:
                            eid = ev.get("id")
                            if eid and eid not in seen_ids:
                                seen_ids.add(eid)
                                eventos.append({"id": eid,
                                                "fecha_raw": ev.get("date", ""),
                                                "ev_data":   ev})
                                found += 1
                    except Exception as ex:
                        print(f"  [admin-setup-f2] {dia}/{lg}: {ex}")
                    time.sleep(0.2)
                if found:
                    print(f"  [admin-setup-f2] {dia}: {found} partidos")
                dia += _td(days=1)
                time.sleep(0.3)

            if not eventos:
                state["_setup_status"] = "ERROR: ESPN no devolvio juegos. Verifica liga y fechas en CONFIG."
                return

            eventos.sort(key=lambda x: x["fecha_raw"])
            state["_setup_status"] = f"running — obteniendo info de {len(eventos)} partidos..."

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
                    else:                           eq2 = n
                if not eq1 and len(competitors) >= 1:
                    eq1 = competitors[0].get("team", {}).get("displayName", "")
                if not eq2 and len(competitors) >= 2:
                    eq2 = competitors[1].get("team", {}).get("displayName", "")
                fecha_str = hora_str = ""
                raw = comp.get("date", "")
                if raw:
                    try:
                        dt        = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                        fecha_str = dt.strftime("%Y-%m-%d")
                        hora_str  = dt.strftime("%H:%M")
                    except Exception:
                        pass
                grupo = parse_grupo(comp, ev.get("ev_data"))
                juegos.append({"id": ev["id"], "eq1": eq1, "eq2": eq2,
                               "fecha": fecha_str, "hora": hora_str,
                               "grupo": grupo, "fecha_raw": ev.get("fecha_raw", "")})
                time.sleep(0.3)

            if juegos:
                for i, j in enumerate(juegos, start=1):
                    _db.db_upsert_horario({
                        "jgo":     str(i),
                        "grupo":   j.get("grupo", ""),
                        "fecha":   j.get("fecha", ""),
                        "hora":    j.get("hora", ""),
                        "eq1":     j.get("eq1", ""),
                        "eq2":     j.get("eq2", ""),
                        "espn_id": j.get("id", ""),
                        "estado":  "PROG",
                        "gol1":    "",
                        "gol2":    "",
                        "ganador": "",
                    })
                _db.db_save_config({"TOTAL_JUEGOS_F2": str(len(juegos))})
                state["cfg"] = _db.db_get_config()

            # Aplicar mapeo de rondas por JGO (WC2026: JGO 1-16=R32, 17-24=R16, etc.)
            _grupo_map = (
                [(str(j), "R32")   for j in range(1,  17)] +
                [(str(j), "R16")   for j in range(17, 25)] +
                [(str(j), "QF")    for j in range(25, 29)] +
                [(str(j), "SF")    for j in range(29, 31)] +
                [("31",   "3ER"), ("32", "FINAL")]
            )
            conn_fix = _db.get_conn()
            with conn_fix:
                for jgo_f, grp_f in _grupo_map:
                    conn_fix.execute("UPDATE horarios SET grupo=? WHERE jgo=?", (grp_f, jgo_f))
            conn_fix.close()
            print("[admin-setup-f2] Rondas WC2026 aplicadas automáticamente")

            _invalidate_games()
            state["_setup_status"] = f"done — {len(juegos)} juegos cargados con rondas asignadas"

        except Exception as e:
            import traceback
            print(f"[admin-setup-f2] ERROR: {traceback.format_exc()}")
            state["_setup_status"] = f"ERROR: {e}"

    state["_setup_status"] = "running"
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "msg": "Setup F2 iniciado"}


@app.post("/api/admin/refresh-bracket-refs")
async def admin_refresh_bracket_refs(ql_admin: str = Cookie(default="")):
    """Re-carga partidos desde ESPN para refrescar los placeholders de bracket."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return await admin_setup(ql_admin=ql_admin)


@app.post("/api/admin/set-game-result")
async def admin_set_game_result(body: dict, ql_admin: str = Cookie(default="")):
    """
    Escribe directamente en SQLite los campos de un partido.
    Util en modo prueba: {jgo, eq1?, eq2?, estado?, gol1?, gol2?, ganador?}
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    jgo = str(body.get("jgo", "")).strip()
    if not jgo:
        raise HTTPException(400, "jgo requerido")

    conn = _db.get_conn()
    with conn:
        if "eq1" in body:
            conn.execute("UPDATE horarios SET eq1=? WHERE jgo=?", (str(body["eq1"]), jgo))
        if "eq2" in body:
            conn.execute("UPDATE horarios SET eq2=? WHERE jgo=?", (str(body["eq2"]), jgo))
        if "grupo" in body:
            conn.execute("UPDATE horarios SET grupo=? WHERE jgo=?", (str(body["grupo"]), jgo))
    conn.close()

    if any(k in body for k in ("estado", "gol1", "gol2", "ganador")):
        _db.db_update_game_result(
            jgo,
            body.get("estado",  "PROG"),
            body.get("gol1",    ""),
            body.get("gol2",    ""),
            body.get("ganador", ""),
        )

    _invalidate_games()
    return {"ok": True, "jgo": jgo}


@app.post("/api/admin/propagate-bracket")
async def admin_propagate_bracket(ql_admin: str = Cookie(default="")):
    """Propaga ganadores actuales a los cruces de la siguiente ronda."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    changes = _propagate_bracket()
    return {"ok": True, "changes": changes}


@app.post("/api/admin/sim-range")
async def admin_sim_range(body: dict, ql_admin: str = Cookie(default="")):
    """
    Simula resultados aleatorios para JGO desde..hasta.
    Genera marcadores realistas, determina ganador (si empate → aleatorio en eliminatorias)
    y propaga el bracket automáticamente.
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    import random, datetime as _dt
    jgo_desde = int(body.get("jgo_desde", 1))
    jgo_hasta  = int(body.get("jgo_hasta", 16))

    horarios = _db.db_get_horarios()
    games_map = {str(h["jgo"]): h for h in horarios}

    # Distribución de goles realista (0-3 por equipo, sesgada hacia lo bajo)
    _GOALS = [0,0,0,1,1,1,1,2,2,3]

    results  = []
    applied  = 0
    skipped  = 0

    for jgo_n in range(jgo_desde, jgo_hasta + 1):
        jgo = str(jgo_n)
        h   = games_map.get(jgo)
        if not h:
            results.append({"jgo": jgo_n, "skip": True, "reason": "no encontrado"})
            skipped += 1
            continue

        eq1 = (h.get("eq1") or "").strip()
        eq2 = (h.get("eq2") or "").strip()

        # Saltar si algún equipo no está definido o es placeholder
        def _is_ph(s): return not s or s.startswith("Round of") or s.startswith("Gan. ") or s.startswith("Perdedor ")
        if _is_ph(eq1) or _is_ph(eq2):
            results.append({"jgo": jgo_n, "eq1": eq1, "eq2": eq2, "skip": True, "reason": "equipos sin definir"})
            skipped += 1
            continue

        g1 = random.choice(_GOALS)
        g2 = random.choice(_GOALS)

        # Eliminatorias (R32...FINAL): el marcador PUEDE quedar empatado (1-1) y
        # se resuelve por penales -> uno avanza. Conservamos el empate en el
        # marcador (para puntuar PTS_LOGRO) y elegimos al ganador que avanza.
        ronda = h.get("grupo", "")
        knockout = ronda in ("R32", "R16", "QF", "SF", "3ER", "FINAL")
        empate = (g1 == g2)
        if g1 > g2:
            ganador = eq1
        elif g2 > g1:
            ganador = eq2
        elif knockout:
            ganador = eq1 if random.random() < 0.5 else eq2  # penales
        else:
            ganador = ""  # fase de grupos: empate sin ganador

        ult_act = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        _db.db_update_game_result(jgo, "FINAL", str(g1), str(g2), ganador, ult_act)

        results.append({"jgo": jgo_n, "eq1": eq1, "eq2": eq2, "g1": g1, "g2": g2,
                        "ganador": ganador or "Empate",
                        "penales": bool(empate and knockout and ganador), "skip": False})
        applied += 1

    # Propagar bracket (actualiza EQ1/EQ2 de rondas siguientes)
    _invalidate_games()
    bracket_changes = _propagate_bracket()
    _invalidate_games()

    return {"applied": applied, "skipped": skipped, "results": results, "bracket_changes": bracket_changes}


@app.post("/api/admin/fix-grupos-wc2026")
async def admin_fix_grupos_wc2026(ql_admin: str = Cookie(default="")):
    """
    Corrige el campo 'grupo' (ronda) de los 32 partidos WC2026
    según el número de JGO, ignorando lo que ESPN haya devuelto:
      JGO 1-16  → R32
      JGO 17-24 → R16
      JGO 25-28 → QF
      JGO 29-30 → SF
      JGO 31    → 3ER
      JGO 32    → FINAL
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    mapping = []
    for jgo in range(1, 17):   mapping.append((str(jgo), "R32"))
    for jgo in range(17, 25):  mapping.append((str(jgo), "R16"))
    for jgo in range(25, 29):  mapping.append((str(jgo), "QF"))
    for jgo in range(29, 31):  mapping.append((str(jgo), "SF"))
    mapping.append(("31", "3ER"))
    mapping.append(("32", "FINAL"))

    conn = _db.get_conn()
    updated = 0
    with conn:
        for jgo, grupo in mapping:
            cur = conn.execute("UPDATE horarios SET grupo=? WHERE jgo=?", (grupo, jgo))
            updated += cur.rowcount
    conn.close()
    _invalidate_games()
    return {"ok": True, "updated": updated, "msg": f"{updated} partidos actualizados con ronda correcta"}


@app.post("/api/admin/load-test-teams")
async def admin_load_test_teams(ql_admin: str = Cookie(default="")):
    """Carga los equipos reales WC2026 en JGO 1-16 (R32) para modo prueba."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    teams = [
        (1,  "Checa",          "Suiza"),
        (2,  "Brasil",         "Japon"),
        (3,  "Alemania",       "Turquia"),
        (4,  "Paises Bajos",   "Marruecos"),
        (5,  "Costa de Marfil","Noruega"),
        (6,  "Francia",        "Suecia"),
        (7,  "Mexico",         "Arabia"),
        (8,  "Inglaterra",     "Ecuador"),
        (9,  "Belgica",        "Corea"),
        (10, "USA",            "Bosnia"),
        (11, "España",         "Austria"),
        (12, "COlombia",       "Croacia"),
        (13, "Canada",         "Nueva Zelanda"),
        (14, "Paraguay",       "Egipto"),
        (15, "Argentina",      "Uruguay"),
        (16, "Portugal",       "Ghana"),
    ]
    conn = _db.get_conn()
    updated = 0
    with conn:
        for jgo, eq1, eq2 in teams:
            cur = conn.execute(
                "UPDATE horarios SET eq1=?, eq2=? WHERE jgo=? AND grupo='R32'",
                (eq1, eq2, str(jgo)))
            updated += cur.rowcount
    conn.close()
    _invalidate_games()
    return {"ok": True, "updated": updated, "msg": f"{updated} equipos R32 cargados (WC2026)"}


@app.post("/api/admin/fix-bracket-wc2026")
async def admin_fix_bracket_wc2026(ql_admin: str = Cookie(default="")):
    """
    Restablece los placeholders de bracket WC2026 en R16, QF, SF, 3ER y FINAL.
    Escribe 'Round of X N Winner' secuencialmente para que _propagate_bracket()
    los resuelva con el mapa WC2026.
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    horarios = _db.db_get_horarios()
    ronda_games: dict = {}
    for h in horarios:
        ronda = h.get("grupo", "")
        ronda_games.setdefault(ronda, []).append(h)
    for k in ronda_games:
        ronda_games[k].sort(
            key=lambda h: int(str(h["jgo"])) if str(h["jgo"]).isdigit() else 0)

    # rof_num must be 32/16/8/4 so _parse_bracket_ref can decode them
    round_pairs = [
        ("R16",   32),
        ("QF",    16),
        ("SF",     8),
        ("3ER",    4),
        ("FINAL",  4),
    ]

    changes = []
    updates = []
    for target_ronda, rof_num in round_pairs:
        games = ronda_games.get(target_ronda, [])
        for i, h in enumerate(games):
            n1      = 2 * i + 1
            n2      = 2 * i + 2
            eq1_new = f"Round of {rof_num} {n1} Winner"
            eq2_new = f"Round of {rof_num} {n2} Winner"
            jgo     = str(h["jgo"])
            if h.get("eq1") != eq1_new:
                updates.append((jgo, "eq1", eq1_new))
                changes.append(f"JGO {jgo} ({target_ronda}) EQ1: {h.get('eq1','')!r} -> {eq1_new!r}")
            if h.get("eq2") != eq2_new:
                updates.append((jgo, "eq2", eq2_new))
                changes.append(f"JGO {jgo} ({target_ronda}) EQ2: {h.get('eq2','')!r} -> {eq2_new!r}")

    if updates:
        conn = _db.get_conn()
        with conn:
            for jgo_u, col_u, val_u in updates:
                if col_u == "eq1":
                    conn.execute("UPDATE horarios SET eq1=? WHERE jgo=?", (val_u, jgo_u))
                else:
                    conn.execute("UPDATE horarios SET eq2=? WHERE jgo=?", (val_u, jgo_u))
        conn.close()
        _invalidate_games()

    return {"ok": True, "total": len(updates), "changes": changes}


@app.post("/api/admin/setup-all")
async def admin_setup_all(ql_admin: str = Cookie(default="")):
    """
    Repara todo en orden:
    1. Fix rondas (grupo) por JGO
    2. Reset placeholders bracket (Round of X N Winner)
    3. Propagar bracket con mapas WC2026
    4. Recalcular standings
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    log    = []
    errors = []

    # 1. Fix rondas por JGO
    try:
        _grupo_map = (
            [(str(j), "R32") for j in range(1, 17)] +
            [(str(j), "R16") for j in range(17, 25)] +
            [(str(j), "QF")  for j in range(25, 29)] +
            [(str(j), "SF")  for j in range(29, 31)] +
            [("31", "3ER"), ("32", "FINAL")]
        )
        conn_g = _db.get_conn()
        updated_g = 0
        with conn_g:
            for jgo_f, grp_f in _grupo_map:
                cur = conn_g.execute("UPDATE horarios SET grupo=? WHERE jgo=?", (grp_f, jgo_f))
                updated_g += cur.rowcount
        conn_g.close()
        _invalidate_games()
        log.append(f"Rondas asignadas: {updated_g} partido(s)")
    except Exception as e:
        errors.append(f"Error asignando rondas: {e}")

    # 2. Reset placeholders bracket
    try:
        horarios_pb = _db.db_get_horarios()
        ronda_games_pb: dict = {}
        for h in horarios_pb:
            ronda_games_pb.setdefault(h.get("grupo", ""), []).append(h)
        for k in ronda_games_pb:
            ronda_games_pb[k].sort(key=lambda h: int(str(h["jgo"])) if str(h["jgo"]).isdigit() else 0)
        round_pairs = [("R16", 32), ("QF", 16), ("SF", 8), ("3ER", 4), ("FINAL", 4)]
        updates_pb = []
        for target_ronda, rof_num in round_pairs:
            for i, h in enumerate(ronda_games_pb.get(target_ronda, [])):
                eq1_new = f"Round of {rof_num} {2*i+1} Winner"
                eq2_new = f"Round of {rof_num} {2*i+2} Winner"
                updates_pb.append((str(h["jgo"]), eq1_new, eq2_new))
        if updates_pb:
            conn_pb = _db.get_conn()
            with conn_pb:
                for jgo_u, e1, e2 in updates_pb:
                    conn_pb.execute("UPDATE horarios SET eq1=?, eq2=? WHERE jgo=?", (e1, e2, jgo_u))
            conn_pb.close()
            _invalidate_games()
        log.append(f"Placeholders bracket: {len(updates_pb)} juego(s) reseteados")
    except Exception as e:
        errors.append(f"Error reseteando placeholders: {e}")

    # 3. Propagar bracket
    try:
        changes = _propagate_bracket()
        if changes:
            log.append(f"Bracket propagado: {len(changes)} cambio(s)")
            log.extend(changes[:20])
        else:
            log.append("Bracket: nada que propagar (sin ganadores aún)")
    except Exception as e:
        errors.append(f"Error propagando bracket: {e}")

    # 4. Recalcular standings
    try:
        _update_standings()
        log.append("Standings recalculados")
    except Exception as e:
        errors.append(f"Error recalculando standings: {e}")

    return {"ok": not errors, "log": log, "errors": errors}



@app.post("/api/admin/reset")
async def admin_reset(body: ArchiveResetBody, ql_admin: str = Cookie(default="")):
    """Archiva el sheet actual en Drive y resetea jugadores/horarios/picks para nueva quiniela."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")

    cfg = state.get("cfg", {})
    reset_key = cfg.get("RESET_KEY", "RESET2026")
    if body.keyword.strip() != reset_key:
        raise HTTPException(403, "Clave incorrecta")

    torneo    = cfg.get("TORNEO", "QuinielaF2")
    fecha_ini = cfg.get("FECHA_INICIO_F2", "").replace("-", "")
    fecha_fin = cfg.get("FECHA_FIN_F2",    "").replace("-", "")
    copy_name = f"{torneo}.{fecha_ini}.{fecha_fin}"

    # 1. Archivar copia en Drive (best effort)
    archive_warn = ""
    sh = state.get("sh")
    if sh:
        try:
            from googleapiclient.discovery import build as _gapi_build
            from google.oauth2.service_account import Credentials as _Creds
            creds = _Creds.from_service_account_file(
                os.environ.get("QL_CREDS", "credentials.json"), scopes=SCOPES)
            drive = _gapi_build("drive", "v3", credentials=creds, cache_discovery=False)
            file_info  = drive.files().get(fileId=sh.id, fields="owners,parents").execute()
            owner_email = (file_info.get("owners") or [{}])[0].get("emailAddress", "")
            parents    = file_info.get("parents", [])
            copy_body  = {"name": copy_name}
            if parents:
                copy_body["parents"] = parents
            copy_meta = drive.files().copy(
                fileId=sh.id, body=copy_body, supportsAllDrives=True).execute()
            copy_id = copy_meta.get("id")
            print(f"[reset] Copia creada: {copy_name} (id={copy_id})")
            if owner_email:
                drive.permissions().create(
                    fileId=copy_id,
                    body={"role": "writer", "type": "user", "emailAddress": owner_email},
                    sendNotificationEmail=False
                ).execute()
        except Exception as e_drive:
            archive_warn = f"[WARN] No se pudo archivar en Drive: {e_drive}. "
            print(f"[reset] Drive error: {e_drive}")

    # 2. Limpiar Sheets (best effort)
    if sh:
        try:
            reserved = {"HORARIOS", "JUGADORES", "POSICIONES", "CONFIG", "INSTRUCCIONES"}
            for ws in sh.worksheets():
                if ws.title not in reserved:
                    sh.del_worksheet(ws)
                time.sleep(0.1)
            ws_j = sh.worksheet("JUGADORES")
            rows_j = ws_j.get_all_values()
            hi, _ = _jugadores_headers(rows_j)
            first_data = hi + 2
            if len(rows_j) >= first_data:
                ws_j.batch_clear([f"A{first_data}:Z{len(rows_j) + 5}"])
            sh.worksheet("POSICIONES").batch_clear(["A3:Z100"])
            sh.worksheet("HORARIOS").batch_clear(["A3:L1000"])
        except Exception as e:
            print(f"[reset] WARN Sheets clear: {e}")

    # 3. Limpiar SQLite
    try:
        conn_r = _db.get_conn()
        with conn_r:
            conn_r.execute("DELETE FROM picks")
            conn_r.execute("DELETE FROM jugadores")
            conn_r.execute("DELETE FROM horarios")
            conn_r.execute("DELETE FROM chat")
        conn_r.close()
        print("[reset] SQLite limpiado")
    except Exception as e_sql:
        print(f"[reset] SQLite error: {e_sql}")

    _cache["players"].clear()
    _invalidate_games()
    state["cfg"] = _db.db_get_config()

    msg = (f"{archive_warn}SQLite reseteado."
           if archive_warn else
           f"Archivado como '{copy_name}' y todo reseteado.")
    return {"ok": True, "msg": msg}


@app.post("/api/admin/reset-test")
async def admin_reset_test(body: dict, ql_admin: str = Cookie(default="")):
    """
    Modo prueba: borra resultados (ganador/goles/estado) desde una ronda en adelante
    y resetea los eq1/eq2 de esas rondas a placeholders de bracket.
    Body: { ronda_desde: "R32" | "R16" | "QF" | "SF" | "FINAL" }
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    ronda_desde = str(body.get("ronda_desde", "R32")).strip().upper()
    RONDAS_ORDER = ["R32", "R16", "QF", "SF", "3ER", "FINAL"]
    ROF_MAP = {"R16": 32, "QF": 16, "SF": 8, "3ER": 4, "FINAL": 4}

    if ronda_desde not in RONDAS_ORDER:
        raise HTTPException(400, f"ronda_desde invalida. Usar: {RONDAS_ORDER}")

    idx_desde = RONDAS_ORDER.index(ronda_desde)
    rondas_a_limpiar = RONDAS_ORDER[idx_desde:]

    horarios = _db.db_get_horarios()
    ronda_games: dict = {}
    for h in horarios:
        ronda = h.get("grupo", "")
        ronda_games.setdefault(ronda, []).append(h)
    for k in ronda_games:
        ronda_games[k].sort(key=lambda h: int(str(h["jgo"])) if str(h["jgo"]).isdigit() else 0)

    conn = _db.get_conn()
    cleared = 0
    with conn:
        for ronda in rondas_a_limpiar:
            games = ronda_games.get(ronda, [])
            for h in games:
                jgo = str(h["jgo"])
                conn.execute(
                    "UPDATE horarios SET ganador='', gol1='', gol2='', estado='PROG' WHERE jgo=?",
                    (jgo,))
                cleared += 1
                if ronda in ROF_MAP:
                    rof = ROF_MAP[ronda]
                    games_sorted = ronda_games.get(ronda, [])
                    i = games_sorted.index(h)
                    eq1_new = f"Round of {rof} {2*i+1} Winner"
                    eq2_new = f"Round of {rof} {2*i+2} Winner"
                    conn.execute("UPDATE horarios SET eq1=?, eq2=? WHERE jgo=?",
                                 (eq1_new, eq2_new, jgo))

    conn.close()

    # Borrar picks correspondientes a las rondas limpiadas
    jgos_limpiados = []
    for ronda in rondas_a_limpiar:
        for h in ronda_games.get(ronda, []):
            jgos_limpiados.append(str(h["jgo"]))

    if jgos_limpiados:
        conn2 = _db.get_conn()
        with conn2:
            placeholders = ",".join("?" * len(jgos_limpiados))
            conn2.execute(f"DELETE FROM picks WHERE jgo IN ({placeholders})", jgos_limpiados)
        conn2.close()

    _invalidate_games()
    return {
        "ok":  True,
        "msg": f"Reseteados {cleared} partido(s) desde {ronda_desde} "
               f"({', '.join(rondas_a_limpiar)}) y picks eliminados."
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quiniela Futbol F2 - Backend")
    parser.add_argument("--port",  type=int, default=int(os.environ.get("PORT", 8080)),
                        help="Puerto HTTP (default: $PORT o 8080)")
    parser.add_argument("--sheet", type=str, default="",
                        help="ID del Google Sheet (opcional)")
    parser.add_argument("--creds", type=str, default="credentials.json",
                        help="Ruta al credentials.json")
    args = parser.parse_args()

    if args.sheet:
        os.environ["QL_SHEET"] = args.sheet
    if args.creds:
        os.environ["QL_CREDS"] = args.creds
    uvicorn.run(app, host="0.0.0.0", port=args.port)
