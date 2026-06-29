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
import glob
import json
import os
import random
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

    # Marcador de la tanda de penales (shootoutScore) — solo si ESPN lo trae
    def _pen(ci):
        v = ci.get("shootoutScore")
        if v in (None, ""):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None
    pen0 = _pen(competitors[0]); pen1v = _pen(competitors[1])

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
            "eq1": eq1_name, "eq2": eq2_name,
            "pen1": ("" if pen0 is None else str(pen0)),
            "pen2": ("" if pen1v is None else str(pen1v))}

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

# _parse_bracket_ref se define como alias de _db._parse_bracket_ref mas abajo
# (fuente unica en db.py). _propagate_bracket lo usa en runtime.


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
_aviso15:     set  = set()  # 16.4: aviso "~15 min" de inicio de partido (tras el cierre)
_aviso5:      set  = set()  # 16.4: aviso "~5 min" de inicio de partido (tras el cierre)
_aviso1pago:  set  = set()  # 16.4: recordatorio de pago (~1-3 min antes, 2da mitad del torneo)
_aviso_cierre: set = set()  # aviso ~15 min antes del ÚLTIMO 16vo (cierra toda la quiniela)
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

def _top12_text(inline=False) -> str:
    """Lista a TODOS los de 1° y 2° lugar (respetando empates), desde SQLite.
    inline=False → multilínea (Telegram); inline=True → ' · ' (push/WA)."""
    try:
        standings = _db.db_compute_standings(state.get("cfg", {}))
        if not standings:
            return ""
        parts = []; pos = 1
        for i, s in enumerate(standings):
            # 16.6: puesto SOLO por puntos (mismos pts = mismo puesto)
            if i > 0 and s["pts"] != standings[i - 1]["pts"]:
                pos = i + 1
            if pos > 2:
                break
            parts.append(f"{pos}. {s['nombre']} ({s['pts']}pts)" if inline
                         else f"  {pos}. {s['nombre']} ({s['pts']}pts)")
        return (" · " if inline else "\n").join(parts)
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

_BANDERAS = {
    # CONMEBOL
    "argentina": "🇦🇷", "brasil": "🇧🇷", "uruguay": "🇺🇾", "colombia": "🇨🇴",
    "chile": "🇨🇱", "perú": "🇵🇪", "peru": "🇵🇪", "ecuador": "🇪🇨",
    "paraguay": "🇵🇾", "bolivia": "🇧🇴", "venezuela": "🇻🇪",
    # CONCACAF
    "méxico": "🇲🇽", "mexico": "🇲🇽", "estados unidos": "🇺🇸", "ee.uu.": "🇺🇸",
    "canadá": "🇨🇦", "canada": "🇨🇦", "costa rica": "🇨🇷", "panamá": "🇵🇦",
    "panama": "🇵🇦", "honduras": "🇭🇳", "jamaica": "🇯🇲", "curazao": "🇨🇼",
    "curacao": "🇨🇼", "haití": "🇭🇹", "haiti": "🇭🇹",
    # UEFA
    "españa": "🇪🇸", "espana": "🇪🇸", "francia": "🇫🇷", "alemania": "🇩🇪",
    "italia": "🇮🇹", "portugal": "🇵🇹", "países bajos": "🇳🇱", "paises bajos": "🇳🇱",
    "holanda": "🇳🇱", "bélgica": "🇧🇪", "belgica": "🇧🇪", "croacia": "🇭🇷",
    "suiza": "🇨🇭", "polonia": "🇵🇱", "dinamarca": "🇩🇰", "serbia": "🇷🇸",
    "austria": "🇦🇹", "ucrania": "🇺🇦", "suecia": "🇸🇪", "noruega": "🇳🇴",
    "chequia": "🇨🇿", "república checa": "🇨🇿", "republica checa": "🇨🇿",
    "turquía": "🇹🇷", "turquia": "🇹🇷", "hungría": "🇭🇺", "hungria": "🇭🇺",
    "grecia": "🇬🇷", "rumanía": "🇷🇴", "rumania": "🇷🇴", "eslovenia": "🇸🇮",
    "eslovaquia": "🇸🇰", "bosnia y herzegovina": "🇧🇦", "irlanda": "🇮🇪",
    "inglaterra": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "escocia": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "gales": "🏴󠁧󠁢󠁷󠁬󠁳󠁿", "rusia": "🇷🇺",
    # CAF
    "marruecos": "🇲🇦", "senegal": "🇸🇳", "túnez": "🇹🇳", "tunez": "🇹🇳",
    "argelia": "🇩🇿", "egipto": "🇪🇬", "nigeria": "🇳🇬", "ghana": "🇬🇭",
    "camerún": "🇨🇲", "camerun": "🇨🇲", "costa de marfil": "🇨🇮",
    "sudáfrica": "🇿🇦", "sudafrica": "🇿🇦", "malí": "🇲🇱", "mali": "🇲🇱",
    # AFC + OFC
    "japón": "🇯🇵", "japon": "🇯🇵", "corea del sur": "🇰🇷", "irán": "🇮🇷",
    "iran": "🇮🇷", "arabia saudita": "🇸🇦", "arabia saudí": "🇸🇦", "australia": "🇦🇺",
    "catar": "🇶🇦", "qatar": "🇶🇦", "irak": "🇮🇶", "emiratos árabes unidos": "🇦🇪",
    "uzbekistán": "🇺🇿", "uzbekistan": "🇺🇿", "nueva zelanda": "🇳🇿", "jordania": "🇯🇴",
}

def _eq(nombre):
    """Devuelve 'bandera nombre' si conocemos la selección; si no, el nombre tal cual."""
    b = _BANDERAS.get((nombre or "").strip().lower(), "")
    return f"{b} {nombre}".strip() if b else (nombre or "")


def _send_recordatorio_pago() -> dict:
    """Recuerda a los jugadores morosos que paguen (si quedan). Reutilizado por el
    aviso automático del loop (16.4) y por el endpoint manual /api/admin/recordar-pago."""
    try:
        n_sin = sum(1 for p in _db.db_get_jugadores()
                    if not p.get("pagado") and not p.get("excluido"))
    except Exception:
        n_sin = 0
    if n_sin <= 0:
        return {"enviado": False, "n_sin": 0}
    pl = "es" if n_sin != 1 else ""
    msg = ("💰 Se les agradece a los jugadores. Por favor, realicen el pago de la quiniela "
           "para garantizar el premio cuando termine la ronda.\n"
           f"Faltan {n_sin} jugador{pl} por realizar su pago. ¡Muchas gracias! 🙏")
    try: _wa("POST", "/send", json={"message": msg})
    except Exception: pass
    try: _tg_send(msg)
    except Exception: pass
    try: _send_push_all("💰 Recordatorio de pago",
                        f"Faltan {n_sin} jugador{pl} por pagar la quiniela", {"tipo": "pago"})
    except Exception: pass
    return {"enviado": True, "n_sin": n_sin}


def _check_aviso_partidos(games_db, cfg):
    """Aviso ~15 min antes del PRIMER 16vo (cierra TODA la quiniela) y —tras el primer
    partido— avisos de ~15/~5 min de cada partido + recordatorio de pago."""
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc)
    # Aviso ~15 min antes del PRIMER 16vo (R32), que CIERRA toda la quiniela. Debe poder
    # salir ANTES del primer partido → va ANTES del gate de _torneo_activo.
    dt_cierre = _r32_cierre_dt(games_db)
    if dt_cierre and not _quiniela_cerrada(games_db):
        ck = dt_cierre.isoformat()
        mins_c = (dt_cierre - now).total_seconds() / 60
        if 10 <= mins_c <= 20 and ck not in _aviso_cierre:
            _aviso_cierre.add(ck)
            prim = None
            for h in games_db:
                if (h.get("ronda") or h.get("grupo") or "") != "R32":
                    continue
                f, hh = (h.get("fecha") or "").strip(), (h.get("hora") or "").strip()
                if not f or not hh:
                    continue
                try:
                    if _dt.fromisoformat(f"{f}T{hh}:00+00:00") == dt_cierre:
                        prim = h; break
                except Exception:
                    pass
            par = f"{_eq(prim.get('eq1',''))} vs {_eq(prim.get('eq2',''))}" if prim else ""
            msg = ("🔒 ¡ÚLTIMO LLAMADO! En ~15 min comienza el PRIMER partido de 16vos"
                   + (f" ({par})" if par else "")
                   + " y con él se CIERRAN TODOS LOS PICKS de todas las rondas.\n"
                   "Revisa y completa tus picks AHORA. ¡Mucha suerte a todos! 🍀🏆")
            try: _wa("POST", "/send", json={"message": msg})
            except Exception: pass
            try: _tg_send(msg)
            except Exception: pass
            try: _send_push_all("🔒 Último llamado",
                                "El primer 16vo cierra todos los picks en ~15 min. ¡Revisa los tuyos!",
                                {"tipo": "cierre"})
            except Exception: pass
    # Los avisos de partido y el recordatorio de pago solo aplican una vez iniciado el torneo.
    if not _torneo_activo().get("activo"):
        return
    # El recordatorio de pago se gatilla por el NÚMERO de partido (jgo >= total//2+1),
    # NO por cuántos van jugados (1 min antes del #37 solo hay 36 finalizados).
    total       = len(games_db)
    jugados     = sum(1 for h in games_db if h.get("estado", "PROG") not in ("PROG", ""))
    umbral_pago = (total // 2) + 1
    for h in games_db:
        if h.get("estado", "PROG") not in ("PROG", ""):
            continue
        if not h.get("fecha") or not h.get("hora"):
            continue
        key = h.get("espn_id", "") or str(h.get("jgo", ""))
        try:
            mins = (_dt.fromisoformat(f"{h['fecha']}T{h['hora']}:00+00:00") - now).total_seconds() / 60
        except Exception:
            continue
        b1, b2 = _eq(h.get("eq1", "")), _eq(h.get("eq2", ""))
        # Aviso de que VA A COMENZAR (~15 y ~5 min)
        lbl = None
        if 13 <= mins <= 17 and key not in _aviso15:
            lbl = "15"; _aviso15.add(key)
        elif 4 <= mins <= 6 and key not in _aviso5:
            lbl = "5"; _aviso5.add(key)
        if lbl:
            try: _wa("POST", "/send", json={"message": f"⏰ En ~{lbl} min comienza: {b1} vs {b2}"})
            except Exception: pass
            try: _tg_send(f"⏰ <b>En ~{lbl} min:</b> {b1} vs {b2}")
            except Exception: pass
            try: _send_push_all(f"⏰ En ~{lbl} min", f"{b1} vs {b2}", {"tipo": "aviso_partido"})
            except Exception: pass
        # Recordatorio de PAGO (~1-3 min antes) — desde el partido (mitad+1) y si faltan pagos.
        try: jgo_num = int(str(h.get("jgo", "")).strip() or "0")
        except Exception: jgo_num = 0
        en_2da_mitad = (jgo_num >= umbral_pago) if jgo_num else (jugados >= total // 2)
        if en_2da_mitad and 0.5 <= mins <= 3.5 and key not in _aviso1pago:
            _aviso1pago.add(key)
            _send_recordatorio_pago()


def _check_reminders(games, cfg):
    """Envía recordatorio 15/10/5/3/1 min antes a jugadores que no apostaron."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _tdt
    # 15.2: tras el cierre las apuestas están bloqueadas → no recordar partidos
    # siguientes. Se mantienen inicio/gol/medio tiempo/final por otras vías.
    if _torneo_activo().get("activo"):
        return
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


def _pick_completo(p):
    """Criterio ÚNICO de 'pick completo' en F2: requiere AMBOS goles Y el ganador.
    Un marcador a medias (un solo gol) o sin ganador NO es un resultado válido.
    Debe coincidir EXACTO con el criterio del frontend (hasPick)."""
    return (str(p.get("g1", "")).strip() != "" and
            str(p.get("g2", "")).strip() != "" and
            str(p.get("gan", "")).strip() != "")


def _juego_pickable(g):
    """Un juego cuenta para la completitud solo si tiene ambos equipos definidos
    (mismo criterio que el frontend: g.eq1 && g.eq2)."""
    return bool((g.get("eq1") or "").strip()) and bool((g.get("eq2") or "").strip())


def _picks_faltantes(jugador_id, games):
    """Cuenta picks completos vs total (solo juegos pickables). Usa _pick_completo
    como criterio único. Retorna (faltan, total, llenos)."""
    picks = _db.db_get_picks(jugador_id)
    pickables = [g for g in games if _juego_pickable(g)]
    total = len(pickables)
    llenos = sum(1 for g in pickables
                 if _pick_completo(picks.get(str(g.get("jgo", "")), {})))
    return (total - llenos), total, llenos


def _primer_partido_dt(games):
    """Datetime UTC del primer partido programado (menor fecha+hora). None si no hay."""
    from datetime import datetime as _dt
    mejor = None
    for g in games:
        f = (g.get("fecha") or "").strip()
        h = (g.get("hora") or "").strip()
        if not f or not h:
            continue
        try:
            dt = _dt.fromisoformat(f"{f}T{h}:00+00:00")
        except Exception:
            continue
        if mejor is None or dt < mejor:
            mejor = dt
    return mejor


def _avisar_picks_faltantes(games_db, cfg):
    """~6h antes del CIERRE de la quiniela (primer 16vo): push DIRIGIDO a cada jugador
    con pendientes + un resumen SIN nombres al grupo (Telegram/WhatsApp). NO se usan DMs
    individuales de WhatsApp (disparan bloqueos). Dedup por config ligado al cierre."""
    from datetime import datetime as _dt, timezone as _tz
    if not games_db:
        return
    dt0 = _r32_cierre_dt(games_db)   # el cierre real es el primer 16vo
    if not dt0:
        return
    horas = (dt0 - _dt.now(_tz.utc)).total_seconds() / 3600.0
    if not (5.5 <= horas <= 6.5):
        return  # fuera de la ventana de ~6h
    target_iso = dt0.isoformat()
    if cfg.get("AVISO_6H_DONE", "") == target_iso:
        return  # ya enviado para este cierre
    pendientes = 0
    for j in _db.db_get_jugadores():
        if j.get("excluido"):
            continue
        faltan, total, _ = _picks_faltantes(j["id"], games_db)
        if faltan <= 0:
            continue
        pendientes += 1
        try:
            _send_push_players(
                [j.get("whatsapp") or ""], [j.get("email") or ""],
                "⏳ Te faltan picks por llenar",
                f"Tienes {faltan} de {total} picks sin completar. Complétalos antes de que "
                f"empiece la quiniela (en ~6 h) o quedarás FUERA automáticamente.",
                {"tipo": "picks_faltantes", "faltan": faltan, "total": total})
        except Exception as e:
            print(f"[avisar-picks] push {j.get('nombre')}: {e}")
    if pendientes > 0:
        plural = "es" if pendientes != 1 else ""
        verbo  = "tienen" if pendientes != 1 else "tiene"
        msg = (f"⚠️ {pendientes} jugador{plural} aún {verbo} picks pendientes. "
               f"¡Complétalos antes del cierre (~6 h) o quedarás fuera de la quiniela!")
        try: _tg_send(msg)
        except Exception as e: print(f"[avisar-picks] TG: {e}")
        try: _wa("POST", "/send", json={"message": msg})
        except Exception as e: print(f"[avisar-picks] WA: {e}")
    _db.db_save_config({"AVISO_6H_DONE": target_iso})
    state.setdefault("cfg", {})["AVISO_6H_DONE"] = target_iso
    print(f"[avisar-picks] aviso 6h enviado: {pendientes} pendiente(s)")


def _check_exclusiones(games_db, cfg):
    """Al cerrar (primer partido ya arrancó: algún estado != PROG), excluye a quien no
    completó sus picks. Reversible (marca flag, NO borra picks). Dedup con EXCL_DONE
    ligado al primer partido (se resetea si cambia el partido 1)."""
    from datetime import datetime as _dt
    if not games_db:
        return
    if not _quiniela_cerrada(games_db):
        return  # aún no cierra (la quiniela cierra al arrancar el último 16vo)
    dt0 = _r32_cierre_dt(games_db)
    target_iso = dt0.isoformat() if dt0 else "cerrada"
    if cfg.get("EXCL_TARGET", "") != target_iso:
        _db.db_save_config({"EXCL_TARGET": target_iso, "EXCL_DONE": ""})
        state.setdefault("cfg", {}).update({"EXCL_TARGET": target_iso, "EXCL_DONE": ""})
        cfg = state.get("cfg", {})
    if cfg.get("EXCL_DONE", ""):
        return
    fecha = _dt.now().strftime("%d/%m/%Y %H:%M")
    excluidos = 0
    for j in _db.db_get_jugadores():
        if j.get("excluido"):
            continue
        faltan, total, llenos = _picks_faltantes(j["id"], games_db)
        if faltan <= 0:
            continue
        if _db.db_set_excluido(j["id"], True,
                f"No completó picks ({llenos}/{total}) al cierre", fecha):
            excluidos += 1
            try:
                _send_push_players(
                    [j.get("whatsapp") or ""], [j.get("email") or ""],
                    "\U0001f6ab Quedaste fuera de la quiniela",
                    f"No completaste tus picks ({llenos}/{total}) antes del cierre. "
                    f"Si crees que es un error, contacta al administrador para reactivarte.",
                    {"tipo": "excluido"})
            except Exception as e:
                print(f"[exclusiones] push: {e}")
    if excluidos > 0:
        try:
            _tg_send(f"\U0001f6ab <b>Exclusión automática al cierre:</b> {excluidos} jugador(es) "
                     f"sin picks completos fueron sacados (reversible en el admin).")
        except Exception as e:
            print(f"[exclusiones] TG: {e}")
    _invalidate_games()
    # PDF con TODAS las apuestas al Telegram del admin (snapshot definitivo del cierre).
    tg_admin = (cfg.get("TELEGRAM_ADMIN_CHAT_ID", "") or "").strip()
    if tg_admin:
        try:
            from datetime import datetime as _dtp
            torneo = cfg.get("TORNEO", "Quiniela")
            pdf_bytes = _pdf_todos_jugadores(games_db, cfg)
            pdf_name = f"picks_todos_{torneo}_{_dtp.now().strftime('%Y%m%d_%H%M')}.pdf".replace(" ", "_")
            _tg_send_document(tg_admin, pdf_name, pdf_bytes,
                f"\U0001f4c4 Picks de TODOS los jugadores — {torneo}\nReenvíalo al grupo de WhatsApp 👍")
        except Exception as e:
            print(f"[exclusiones] PDF todos: {e}")
    _db.db_save_config({"EXCL_DONE": "1"})
    state.setdefault("cfg", {})["EXCL_DONE"] = "1"
    print(f"[exclusiones] {excluidos} jugador(es) excluido(s) al cierre")


def _stats_picks(games_db):
    """Avance (solo jugadores NO excluidos): total, con >=1 pick completo, sin nada,
    completos. Usa _picks_faltantes (criterio único _pick_completo)."""
    total = con_alguno = sin_ninguno = completos = 0
    for j in _db.db_get_jugadores():
        if j.get("excluido"):
            continue
        total += 1
        faltan, tot, llenos = _picks_faltantes(j["id"], games_db)
        if llenos >= 1:
            con_alguno += 1
        else:
            sin_ninguno += 1
        if faltan == 0 and tot > 0:
            completos += 1
    return {"total": total, "con_alguno": con_alguno,
            "sin_ninguno": sin_ninguno, "completos": completos}


def _texto_avance(games_db):
    """Bloque de texto con el avance de la quiniela (para grupo WA/TG)."""
    st = _stats_picks(games_db)
    return (f"\U0001f4ca Avance ({st['total']} jugadores):"
            f"\n\U0001f7e2 Ya llenaron al menos 1 pick: {st['con_alguno']}"
            f"\n\U0001f534 Aún no han llenado nada: {st['sin_ninguno']}"
            f"\n✅ Completaron TODOS sus picks: {st['completos']}")


def _health_snapshot() -> dict:
    """Snapshot de salud de los componentes (DB, juegos, updater, ESPN, WA, TG, push)."""
    import datetime as _dt
    cfg = state.get("cfg", {}); comps = {}
    try:
        comps["db"] = {"ok": True, "detalle": f"{len(_db.db_get_jugadores())} jugadores"}
    except Exception as e:
        comps["db"] = {"ok": False, "detalle": f"Error: {e}"}
    try:
        games = _db.db_get_horarios()
        sin_id = sum(1 for g in games if not g.get("espn_id"))
        comps["juegos"] = {"ok": len(games) > 0 and sin_id == 0,
                           "detalle": f"{len(games)} juegos, {sin_id} sin espn_id"}
    except Exception as e:
        comps["juegos"] = {"ok": False, "detalle": f"Error: {e}"}
    last = state.get("updater_last_tick", 0)
    comps["updater"] = {"ok": bool(last) and (time.time() - last) < 180,
                        "detalle": (f"último tick hace {int(time.time()-last)}s" if last else "sin latido aún")}
    try:
        league = (cfg.get("ESPN_LEAGUE", "fifa.world") or "fifa.world").split(",")[0].strip()
        r = requests.get(f"{ESPN_BASE}/{league}/scoreboard", timeout=8)
        comps["espn"] = {"ok": r.ok, "detalle": f"HTTP {r.status_code} (liga {league})"}
    except Exception as e:
        comps["espn"] = {"ok": False, "detalle": f"Error: {e}"}
    if str(os.environ.get("WA_ENABLED", "true")).lower() == "false":
        comps["whatsapp"] = {"ok": True, "detalle": "deshabilitado"}
    else:
        try:
            st = _wa("GET", "/status", timeout=8); conn = bool(st.get("connected"))
            comps["whatsapp"] = {"ok": conn,
                                 "detalle": ("conectado " + (st.get("phone") or "")) if conn else "no vinculado"}
        except Exception as e:
            comps["whatsapp"] = {"ok": False, "detalle": str(getattr(e, "detail", e))[:90]}
    token = (cfg.get("TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not token:
        comps["telegram"] = {"ok": False, "detalle": "sin token"}
    else:
        try:
            j = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8).json()
            comps["telegram"] = {"ok": bool(j.get("ok")),
                                 "detalle": ("@" + (j.get("result", {}) or {}).get("username", "")) if j.get("ok") else "getMe falló"}
        except Exception as e:
            comps["telegram"] = {"ok": False, "detalle": f"Error: {e}"}
    comps["push"] = {"ok": bool(_vapid_keys), "detalle": f"{len(_push_subs)} suscriptores"}
    return {"ok": all(c["ok"] for c in comps.values()),
            "fecha": _dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "componentes": comps}


def _health_alert(snap):
    """Avisa por Telegram SOLO las transiciones (caída/recuperación), no spamea."""
    prev = state.get("_health_prev", {}); comps = snap["componentes"]; cambios = []
    for nombre, c in comps.items():
        antes, ahora = prev.get(nombre), c["ok"]
        if antes is None:
            if not ahora:
                cambios.append(f"\U0001f534 {nombre.upper()} con problema — {c['detalle']}")
        elif antes and not ahora:
            cambios.append(f"\U0001f534 ALERTA: {nombre.upper()} CAÍDO — {c['detalle']}")
        elif (not antes) and ahora:
            cambios.append(f"\U0001f7e2 {nombre.upper()} recuperado — {c['detalle']}")
    state["_health_prev"] = {k: v["ok"] for k, v in comps.items()}
    if cambios:
        tg_admin = (state.get("cfg", {}).get("TELEGRAM_ADMIN_CHAT_ID", "") or "").strip()
        msg = "\U0001fa7a <b>Salud F2</b> — " + snap["fecha"] + "\n" + "\n".join(cambios)
        try:
            (_tg_send_personal(tg_admin, msg) if tg_admin else _tg_send(msg))
        except Exception as e:
            print(f"[health] TG: {e}")


def _check_health_monitor(cfg):
    """Corre el snapshot de salud cada ~5 min y avisa transiciones."""
    now = time.time()
    if now - state.get("_health_last_run", 0) < 300:
        return
    state["_health_last_run"] = now
    _health_alert(_health_snapshot())


def _tg_send_document(chat_id_user: str, filename: str, content: bytes, caption: str = ""):
    """Envía un archivo (bytes) por Telegram a un chat (sendDocument)."""
    cfg   = state.get("cfg", {})
    token = cfg.get("TELEGRAM_BOT_TOKEN", "")
    if not token or not chat_id_user:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id_user, "caption": caption[:1024], "parse_mode": "HTML"},
            files={"document": (filename, content, "application/json")},
            timeout=15,
        )
    except Exception as e:
        print(f"[telegram-doc] {e}")


def _jugador_picks_payload(player_id, nombre, telefono, email, games,
                           by_jgo=None, by_ronda=None):
    """Dict con los picks de un jugador (sobre los juegos pickables).
    En rondas superiores resuelve eq1/eq2 al equipo que el propio jugador
    dedujo con sus picks (no el placeholder "Round of 32 1 Winner").
    Retorna (data, llenos)."""
    picks = _db.db_get_picks(player_id)
    if by_jgo is None or by_ronda is None:
        by_jgo, by_ronda = _db.build_bracket_index(games)
    lista = []; llenos = 0
    for g in games:
        if not _juego_pickable(g):
            continue
        p = picks.get(str(g.get("jgo", "")), {})
        g1, g2, gan = p.get("g1", ""), p.get("g2", ""), p.get("gan", "")
        if _pick_completo(p):
            llenos += 1
        eq1 = _db._disp_team(g, "eq1", by_jgo, by_ronda, picks) or g.get("eq1", "")
        eq2 = _db._disp_team(g, "eq2", by_jgo, by_ronda, picks) or g.get("eq2", "")
        lista.append({
            "jgo": str(g.get("jgo", "")), "ronda": g.get("grupo", "") or g.get("ronda", ""),
            "eq1": eq1, "eq2": eq2,
            "g1": g1, "g2": g2, "ganador": gan,
        })
    total = sum(1 for g in games if _juego_pickable(g))
    return {"id": player_id, "nombre": nombre or "", "telefono": telefono or "",
            "email": email or "", "completados": llenos, "total": total,
            "picks": lista}, llenos


def _todos_los_picks_json(games):
    """JSON con TODOS los jugadores y sus picks. Retorna (data, bytes)."""
    jugadores = []
    by_jgo, by_ronda = _db.build_bracket_index(games)
    for j in _db.db_get_jugadores():
        d, _ = _jugador_picks_payload(j["id"], j.get("nombre", ""),
                                      j.get("whatsapp", ""), j.get("email", ""), games,
                                      by_jgo, by_ronda)
        d["excluido"] = bool(j.get("excluido"))
        jugadores.append(d)
    out = {"n_jugadores": len(jugadores), "total_juegos": sum(1 for g in games if _juego_pickable(g)),
           "jugadores": jugadores}
    return out, json.dumps(out, ensure_ascii=False, indent=2).encode("utf-8")


def _backup_picks_telegram(player_id, nombre, telefono, email, games, motivo):
    """Envía a Telegram (chat privado admin) el JSON con TODOS los jugadores y sus
    picks, como respaldo ante reclamos. Solo si TELEGRAM_ADMIN_CHAT_ID está configurado."""
    from datetime import datetime as _dt
    cfg = state.get("cfg", {})
    tg_admin = (cfg.get("TELEGRAM_ADMIN_CHAT_ID", "") or "").strip()
    if not tg_admin:
        return
    data, payload = _todos_los_picks_json(games)
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    fname = f"picks_todos_{ts}.json"
    caption = (f"{motivo}\n\U0001f464 Disparado por: {nombre} ({telefono})\n"
               f"\U0001f4cb {data['n_jugadores']} jugadores · {_dt.now().strftime('%d/%m/%Y %H:%M')}")
    try:
        _tg_send_document(tg_admin, fname, payload, caption)
    except Exception as e:
        print(f"[backup-picks] TG: {e}")


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
    """Cálculo de probabilidades (Monte Carlo del bracket). Usado para
    precalentar el caché desde el updater y al arranque."""
    return _build_probabilities()


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


_PLACEHOLDER_KW = ("winner", "round of", "gan.", "ganador de", "perdedor",
                   "2nd place", "place", "vencedor", "por definir", "tbd")

def _es_placeholder(nombre: str) -> bool:
    """True si el nombre del equipo es un marcador de bracket sin resolver."""
    n = (nombre or "").strip().lower()
    return not n or any(k in n for k in _PLACEHOLDER_KW)


def _resolver_empates_sin_ganador(games) -> int:
    """En eliminatoria, TODO empate FINAL debe tener un ganador. Si ESPN no lo
    definió (ej. amistosos de prueba que terminan empatados), el sistema elige
    uno AL AZAR automáticamente y propaga el bracket. Idempotente: solo actúa
    sobre partidos FINAL empatados que aún no tienen ganador."""
    cambiado = 0
    for g in games:
        if (g.get("estado") or "").strip() != "FINAL":
            continue
        if (g.get("ganador") or "").strip():
            continue
        g1 = (g.get("gol1") or "").strip(); g2 = (g.get("gol2") or "").strip()
        if not g1 or not g2 or g1 != g2:
            continue  # sin marcador o no es empate
        e1 = (g.get("eq1") or "").strip(); e2 = (g.get("eq2") or "").strip()
        if _es_placeholder(e1) or _es_placeholder(e2):
            continue  # equipos aún no resueltos
        gan = random.choice([e1, e2])
        _db.db_update_game_result(
            str(g["jgo"]), "FINAL", g1, g2, gan,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print(f"[empate-azar] JGO {g['jgo']}: {e1} {g1}-{g2} {e2} -> {gan}")
        cambiado += 1
    if cambiado:
        _invalidate_games()
        _cache["standings_rows"] = None
        try:
            _propagate_bracket()
        except Exception as e:
            print(f"[empate-azar] propagate: {e}")
    return cambiado


def _updater_loop():
    """Loop de actualizacion de scores desde ESPN. Lee/escribe en SQLite."""
    print("[updater] Iniciando en segundo plano")
    while True:
        try:
            t0          = time.time()
            state["updater_last_tick"] = t0   # latido para el monitor de salud
            cfg         = state.get("cfg", {})
            interval    = int(cfg.get("INTERVAL_SEGS", 60))
            modo_prueba = cfg.get("MODO_PRUEBA", "0").strip() not in ("", "0", "false", "no")

            _invalidate_games()
            games, _ = _get_games_cache()

            # Polling acelerado SOLO cuando hay algún partido en vivo: consulta
            # ESPN cada ~20s en vez de 60s para reducir el delay percibido.
            _vivos = {"EN VIVO", "MEDIO TIEMPO", "PRORROGA", "PENALES"}
            if any(g.get("estado", "") in _vivos for g in games):
                interval = min(interval, int(cfg.get("INTERVAL_LIVE_SEGS", 20) or 20))

            try:
                _check_reminders(games, cfg)
            except Exception as e:
                print(f"[updater-reminder] {e}")

            try:
                _check_aviso_partidos(games, cfg)
            except Exception as e:
                print(f"[updater-aviso-partido] {e}")

            if not modo_prueba:
                try:
                    _avisar_picks_faltantes(games, cfg)
                except Exception as e:
                    print(f"[updater-avisar-picks] {e}")
                try:
                    _check_exclusiones(games, cfg)
                except Exception as e:
                    print(f"[updater-exclusiones] {e}")

            try:
                _write_daily_backup()
            except Exception as e:
                print(f"[updater-backup] {e}")

            try:
                if _resolver_empates_sin_ganador(games):
                    games, _ = _get_games_cache()  # recargar tras resolver
            except Exception as e:
                print(f"[updater-empate] {e}")

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
                    _b1, _b2 = _eq(eq1), _eq(eq2)
                    _tg_send(f"\U0001f7e1 <b>INICIO:</b> {_b1} vs {_b2}\nJornada")
                    _send_push_all("\u26bd Partido iniciado", f"{_b1} vs {_b2}",
                                   {"tipo": "inicio", "eq1": eq1, "eq2": eq2})
                    try:
                        _wa("POST", "/send", json={"message": f"\U0001f7e1 INICIO: {_b1} vs {_b2}\n"})
                    except Exception as e:
                        print(f"[WA] Error inicio: {e}")
                elif sc["estado"] in ("EN VIVO", "MEDIO TIEMPO", "PRORROGA", "PENALES"):
                    # 12.1: comparar el marcador contra la BD (game), no contra el estado
                    # en memoria (_prev_states) — así sobrevive a reinicios del server.
                    if sc["gol1"] != game.get("gol1", "") or sc["gol2"] != game.get("gol2", ""):
                        minuto  = _live_clocks.get(espn_id, "")
                        min_txt = (f" ({minuto}')" if minuto and minuto != "MT"
                                   else (" (MT)" if sc["estado"] == "MEDIO TIEMPO" else ""))
                        _pending_notifs.append({
                            "tipo": "gol", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                            "min_txt": min_txt, "minuto": minuto,
                        })
                    # 12.2: aviso de MEDIO TIEMPO (una sola vez, al entrar a esa fase)
                    if sc["estado"] == "MEDIO TIEMPO" and estado_prev != "MEDIO TIEMPO":
                        _pending_notifs.append({
                            "tipo": "medio_tiempo", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                        })
                    # Inicio de TIEMPO EXTRA (prórroga) — una sola vez
                    if sc["estado"] == "PRORROGA" and estado_prev != "PRORROGA":
                        _pending_notifs.append({
                            "tipo": "prorroga", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                        })
                    # Inicio de la TANDA DE PENALES — una sola vez
                    if sc["estado"] == "PENALES" and estado_prev != "PENALES":
                        _pending_notifs.append({
                            "tipo": "penales", "eq1": eq1, "eq2": eq2,
                            "gol1": sc["gol1"], "gol2": sc["gol2"],
                        })
                elif sc["estado"] == "FINAL" and estado_prev != "FINAL":
                    if sc["gol1"] != game.get("gol1", "") or sc["gol2"] != game.get("gol2", ""):
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
                        "pen1": sc.get("pen1", ""), "pen2": sc.get("pen2", ""),
                    })

                _prev_states[espn_id] = {
                    "estado": sc["estado"],
                    "gol1":   sc["gol1"],
                    "gol2":   sc["gol2"],
                }

                # Con equipos congelados (FREEZE_EQUIPOS=1 o rondas superiores), el
                # GANADOR debe derivarse del marcador aplicado a los equipos de la
                # QUINIELA, NO copiarse de ESPN (que trae el equipo del partido real).
                ganador_final = sc["ganador"]
                if freeze and (eq1_sheet or eq2_sheet) and sc["estado"] not in ("", "PROG"):
                    try:
                        _g1 = int(sc["gol1"] or 0); _g2 = int(sc["gol2"] or 0)
                    except (ValueError, TypeError):
                        _g1 = _g2 = 0
                    if _g1 > _g2:
                        ganador_final = eq1_sheet or eq1
                    elif _g2 > _g1:
                        ganador_final = eq2_sheet or eq2
                    elif sc["ganador"]:
                        # Empate a 90 resuelto por penales/prórroga → mapear por lado
                        ganador_final = (eq2_sheet or eq2) if sc["ganador"] == sc.get("eq2", "") \
                                        else (eq1_sheet or eq1)
                    else:
                        ganador_final = ""

                ult_act = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _db.db_update_game_result(
                    jgo, sc["estado"], sc["gol1"], sc["gol2"], ganador_final, ult_act
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
                top  = _top12_text()             # 1° y 2° lugar (con empates), multilínea
                top3 = _top12_text(inline=True)   # versión inline para push/WA
                for notif in _pending_notifs:
                    try:
                        if notif["tipo"] == "gol":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            b1n, b2n   = _eq(eq1n), _eq(eq2n)
                            g1, g2     = notif["gol1"], notif["gol2"]
                            mt         = notif["min_txt"]
                            minuto_n   = notif["minuto"]
                            _tg_send(
                                f"\u26bd <b>MARCADOR:</b> {b1n} {g1} \u2013 {g2} {b2n}{mt}\n"
                                + (f"\n\U0001f3c6 <b>1\u00b0 y 2\u00b0 lugar:</b>\n{top}" if top else "")
                            )
                            push_body = f"{b1n} {g1} \u2013 {g2} {b2n}{mt}"
                            if top3: push_body += f"\n\U0001f3c6 {top3}"
                            _send_push_all("\u26bd Gol!", push_body,
                                {"tipo": "gol", "eq1": eq1n, "eq2": eq2n,
                                 "gol1": g1, "gol2": g2, "minuto": minuto_n})
                            try:
                                wa_msg = f"\u26bd GOL: {b1n} {g1} \u2013 {g2} {b2n}{mt}"
                                if top3: wa_msg += f"\n\U0001f3c6 {top3}"
                                _wa("POST", "/send", json={"message": wa_msg})
                            except Exception as e:
                                print(f"[WA] Error gol: {e}")
                        elif notif["tipo"] == "medio_tiempo":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            b1n, b2n   = _eq(eq1n), _eq(eq2n)
                            g1, g2     = notif["gol1"], notif["gol2"]
                            _tg_send(
                                f"\u23f8\ufe0f <b>MEDIO TIEMPO:</b> {b1n} {g1} \u2013 {g2} {b2n}\n"
                                + (f"\n\U0001f3c6 <b>1\u00b0 y 2\u00b0 lugar:</b>\n{top}" if top else "")
                            )
                            _send_push_all("\u23f8\ufe0f Medio tiempo",
                                f"{b1n} {g1} \u2013 {g2} {b2n}",
                                {"tipo": "medio_tiempo", "eq1": eq1n, "eq2": eq2n,
                                 "gol1": g1, "gol2": g2})
                            try:
                                _wa("POST", "/send", json={"message":
                                    f"\u23f8\ufe0f MEDIO TIEMPO: {b1n} {g1} \u2013 {g2} {b2n}"})
                            except Exception as e:
                                print(f"[WA] Error MT: {e}")
                        elif notif["tipo"] == "prorroga":
                            b1n, b2n = _eq(notif["eq1"]), _eq(notif["eq2"])
                            g1, g2   = notif["gol1"], notif["gol2"]
                            _tg_send(f"\u23f1\ufe0f <b>TIEMPO EXTRA:</b> {b1n} {g1} \u2013 {g2} {b2n}\n"
                                     f"Empate a los 90' \u2014 se juega pr\u00f3rroga.")
                            _send_push_all("\u23f1\ufe0f Tiempo extra",
                                f"{b1n} {g1} \u2013 {g2} {b2n} \u00b7 pr\u00f3rroga",
                                {"tipo": "prorroga", "eq1": notif["eq1"], "eq2": notif["eq2"]})
                            try:
                                _wa("POST", "/send", json={"message":
                                    f"\u23f1\ufe0f TIEMPO EXTRA: {b1n} {g1} \u2013 {g2} {b2n}\nEmpate a los 90', se juega pr\u00f3rroga."})
                            except Exception as e:
                                print(f"[WA] Error pr\u00f3rroga: {e}")
                        elif notif["tipo"] == "penales":
                            b1n, b2n = _eq(notif["eq1"]), _eq(notif["eq2"])
                            g1, g2   = notif["gol1"], notif["gol2"]
                            _tg_send(f"\U0001f945 <b>PENALES:</b> {b1n} {g1} \u2013 {g2} {b2n}\n"
                                     f"Se define en la tanda de penales.")
                            _send_push_all("\U0001f945 Tanda de penales",
                                f"{b1n} vs {b2n} \u00b7 se define en penales",
                                {"tipo": "penales", "eq1": notif["eq1"], "eq2": notif["eq2"]})
                            try:
                                _wa("POST", "/send", json={"message":
                                    f"\U0001f945 PENALES: {b1n} {g1} \u2013 {g2} {b2n}\nSe define en la tanda de penales."})
                            except Exception as e:
                                print(f"[WA] Error penales: {e}")
                        elif notif["tipo"] == "final":
                            eq1n, eq2n = notif["eq1"], notif["eq2"]
                            b1n, b2n   = _eq(eq1n), _eq(eq2n)
                            g1, g2     = notif["gol1"], notif["gol2"]
                            gan        = notif["ganador"]
                            gan_eq_n   = notif["gan_eq"]
                            _p1n, _p2n = notif.get("pen1", ""), notif.get("pen2", "")
                            pen_txt    = (f" (penales {_p1n}-{_p2n})"
                                          if _p1n != "" and _p2n != "" else "")
                            # F2: en eliminatorias siempre AVANZA un equipo (no "1"/"2")
                            gan_txt    = (f"\U0001f3c5 Avanza <b>{_eq(gan_eq_n)}</b>{pen_txt}"
                                          if gan else "\U0001f91d <b>Empate</b>")
                            _tg_send(
                                f"\U0001f3c1 <b>FINAL:</b> {b1n} {g1} \u2013 {g2} {b2n}\n"
                                f"{gan_txt}\n"
                                + (f"\n\U0001f3c6 <b>1\u00b0 y 2\u00b0 lugar:</b>\n{top}" if top else "")
                            )
                            push_body = f"{b1n} {g1} \u2013 {g2} {b2n} \u00b7 {gan_eq_n}{pen_txt}"
                            if top3: push_body += f"\n\U0001f3c6 {top3}"
                            _send_push_all("\U0001f3c1 Partido finalizado", push_body,
                                {"tipo": "final", "eq1": eq1n, "eq2": eq2n,
                                 "gol1": g1, "gol2": g2, "ganador": gan})
                            try:
                                gan_wa = f"\U0001f3c5 Avanza {_eq(gan_eq_n)}{pen_txt}" if gan else "\U0001f91d Empate"
                                wa_msg = f"\U0001f3c1 FINAL: {b1n} {g1} \u2013 {g2} {b2n}\n{gan_wa}"
                                if top3: wa_msg += f"\n\n\U0001f3c6 1° y 2°:\n{top3}"
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

        try:
            _check_health_monitor(state.get("cfg", {}))
        except Exception as e:
            print(f"[updater] health-monitor ERROR: {e}")

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
    # Estado de aprobación (control de invitador) — leído fresco de SQLite para reflejar
    # al instante cuando el admin lo libera.
    _fresh = _db.db_find_player(
        phone=_normalize_phone(body.phone) if body.phone else "",
        email=(body.email or "").strip().lower()) or {}
    aprobado  = bool(_fresh.get("aprobado", 1))
    invitador = _fresh.get("invitador", "") or ""
    return {"registered": True, "nombre": p.get("NOMBRE", ""),
            "tab": p.get("TAB_NOMBRE", ""),
            "phone": p.get("WHATSAPP","") or p.get("TELEFONO",""),
            "email": p.get("EMAIL",""),
            "pagado": is_paid,
            "reglas_ok": bool(p.get("REGLAS_OK")),
            "aprobado": aprobado,
            "invitador": invitador,
            "stripe_activo": stripe_activo}


@app.post("/api/auth/register")
async def auth_register(body: RegisterBody, response: Response):
    # 15.1: una vez iniciado el torneo no se admiten registros nuevos (un jugador
    # nuevo no podría llenar los picks de partidos ya jugados). El admin sí puede
    # seguir agregando jugadores a mano (su flujo no pasa por este endpoint).
    if _quiniela_cerrada():
        raise HTTPException(403, "La quiniela ya cerró (arrancó el último 16vo). Los registros están cerrados.")
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

    # Aviso al admin: nuevo registro PENDIENTE de asignar invitador (en hilo aparte).
    def _aviso_pendiente():
        try:
            tg_admin = (state.get("cfg", {}).get("TELEGRAM_ADMIN_CHAT_ID", "") or "").strip()
            msg = ("🆕 <b>Nuevo registro pendiente</b>\n"
                   f"👤 {body.nombre.strip()} ({phone_norm})"
                   + (f" · {email_clean}" if email_clean else "")
                   + "\nAsígnale quién lo invitó en el panel para liberarlo.")
            (_tg_send_personal(tg_admin, msg) if tg_admin else _tg_send(msg))
        except Exception as e:
            print(f"[registro] aviso admin: {e}")
    threading.Thread(target=_aviso_pendiente, daemon=True).start()

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
    games, _ = _get_games_cache()
    if _quiniela_cerrada(games):
        raise HTTPException(403, "No puedes retirarte: la quiniela ya cerró (arrancó el último 16vo)")
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


# ── Inferencia de bracket: vive en db.py (fuente unica). Aliases para mantener
#    las referencias existentes (PDF, _propagate_bracket, scoring). ──
_parse_bracket_ref  = _db._parse_bracket_ref
_is_placeholder     = _db._is_placeholder
_resolve_team_name  = _db._resolve_team_name
_infer_bracket_slot = _db._infer_bracket_slot
_disp_team_pdf      = _db._disp_team


def _pdf_todos_jugadores(games, cfg):
    """Genera UN PDF con TODOS los jugadores y sus picks (uno por página).
    F2: g1/g2 = goles, gan = nombre del equipo ganador."""
    from fpdf import FPDF
    import datetime as _dt
    _lat = lambda s: str(s or "").encode("latin-1", "replace").decode("latin-1")
    torneo = _lat(cfg.get("TORNEO", "Quiniela"))
    now = _dt.datetime.now()
    # Normalizar 'ronda' (HORARIOS guarda la ronda en 'grupo')
    for g in games:
        if not g.get("ronda"):
            g["ronda"] = g.get("grupo", "") or ""
    col_w   = [12, 42, 42, 18, 18, 24]
    headers = ["#", "Local", "Visitante", "G.Loc", "G.Vis", "Ganador"]
    pdf = FPDF(); pdf.set_margins(15, 15, 15); pdf.set_auto_page_break(auto=True, margin=15)
    for jug in _db.db_get_jugadores():
        try:
            picks = _db.db_get_picks(jug["id"]); pdf.add_page()
            pdf.set_font("Helvetica", "B", 16); pdf.cell(0, 10, torneo, ln=True, align="C")
            pdf.set_font("Helvetica", "", 11)
            pdf.cell(0, 7, f"Jugador: {_lat(jug.get('nombre','Jugador'))}", ln=True, align="C")
            if jug.get("whatsapp"):
                pdf.cell(0, 6, f"Telefono: {_lat(jug.get('whatsapp'))}", ln=True, align="C")
            if jug.get("excluido"):
                pdf.set_text_color(200, 0, 0); pdf.cell(0, 6, "** EXCLUIDO **", ln=True, align="C")
                pdf.set_text_color(0, 0, 0)
            pdf.cell(0, 6, f"Generado: {now.strftime('%d/%m/%Y %H:%M')}", ln=True, align="C"); pdf.ln(6)
            pdf.set_fill_color(30, 64, 175); pdf.set_text_color(255, 255, 255); pdf.set_font("Helvetica", "B", 9)
            for i, h in enumerate(headers):
                pdf.cell(col_w[i], 8, h, border=1, align="C", fill=True)
            pdf.ln(); pdf.set_text_color(0, 0, 0); fill = False
            total = len(games); llenos = 0
            for g in games:
                p = picks.get(str(g["jgo"]), {})
                g1, g2, gan = p.get("g1", ""), p.get("g2", ""), p.get("gan", "")
                completo = _pick_completo(p)
                if completo:
                    llenos += 1
                pdf.set_fill_color(240, 244, 255) if fill else pdf.set_fill_color(255, 255, 255)
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(0, 0, 0) if completo else pdf.set_text_color(180, 180, 180)
                pdf.cell(col_w[0], 7, str(g["jgo"]), border=1, align="C", fill=True)
                pdf.cell(col_w[1], 7, _lat(g.get("eq1", ""))[:20], border=1, align="L", fill=True)
                pdf.cell(col_w[2], 7, _lat(g.get("eq2", ""))[:20], border=1, align="L", fill=True)
                pdf.cell(col_w[3], 7, g1 if g1 else "-", border=1, align="C", fill=True)
                pdf.cell(col_w[4], 7, g2 if g2 else "-", border=1, align="C", fill=True)
                pdf.cell(col_w[5], 7, (_lat(gan)[:12] if gan else "-"), border=1, align="C", fill=True)
                pdf.ln(); fill = not fill
            pdf.set_text_color(0, 0, 0); pdf.set_font("Helvetica", "I", 9); pdf.ln(3)
            pdf.cell(0, 6, f"Picks completados: {llenos} / {total}", ln=True, align="R")
        except Exception as e:
            print(f"[pdf-todos] jugador {jug.get('id')}: {e}")
    return bytes(pdf.output())


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

    # No permitir PDF con picks incompletos: el comprobante debe reflejar TODOS los
    # picks válidos (ambos marcadores + ganador). Evita PDFs "a medias".
    _faltan = sum(1 for g in games
                  if _juego_pickable(g) and not _pick_completo(raw_picks.get(str(g["jgo"]), {}) or {}))
    if _faltan > 0:
        raise HTTPException(400, f"Completa todos tus picks (marcador y ganador) antes de "
                                 f"descargar el PDF. Te faltan {_faltan}.")

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
        if gol1 != "" and gol2 != "" and ganador:   # criterio único de pick completo
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

    pdf_bytes = pdf.output()
    # Archivar copia para auditoria/respaldo en /data/picks_pdf/.
    # Nombre: {jugador_id}_{nombre_limpio}_{timestamp}.pdf (el parser separa por el ULTIMO "_").
    try:
        _pdf_dir = DATA_DIR / "picks_pdf"
        _pdf_dir.mkdir(parents=True, exist_ok=True)
        (_pdf_dir / f"{player_id}_{nom_clean}_{ts_str}.pdf").write_bytes(bytes(pdf_bytes))
    except Exception as _e:
        print(f"[pdf] Error guardando copia: {_e}")

    buf = BytesIO()
    buf.write(pdf_bytes)
    buf.seek(0)

    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/admin/picks-pdfs")
async def admin_list_picks_pdfs(ql_admin: str = Cookie(default="")):
    """Lista los PDFs de picks archivados, agrupados por jugador (auditoria)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    import datetime as _dt
    _pdf_dir = DATA_DIR / "picks_pdf"
    if not _pdf_dir.exists():
        return {"pdfs": [], "total": 0}
    jugadores = {str(j.get("id", "")): j for j in _db.db_get_jugadores()}
    archivos = sorted(_pdf_dir.glob("*.pdf"),
                      key=lambda p: p.stat().st_mtime, reverse=True)
    result = []
    for f in archivos:
        stem = f.stem  # "15_Gio_Ramirez_202206081530" o legacy "15_202206081530"
        stem_parts = stem.rsplit("_", 1)           # separar por el ULTIMO "_"
        ts_str = stem_parts[1] if len(stem_parts) > 1 else ""
        rest   = stem_parts[0] if stem_parts else stem
        rest_parts = rest.split("_", 1)            # "{id}_{nom}" o "{id}"
        jug_id = rest_parts[0]
        nom_from_file = rest_parts[1].replace("_", " ") if len(rest_parts) > 1 else ""
        jug = jugadores.get(jug_id, {})
        try:
            fecha = _dt.datetime.strptime(ts_str, "%Y%m%d%H%M").strftime("%d/%m/%Y %H:%M")
        except Exception:
            fecha = ts_str
        result.append({
            "filename":   f.name,
            "jugador_id": jug_id,
            "nombre":     jug.get("nombre") or nom_from_file or f"Jugador {jug_id}",
            "telefono":   jug.get("whatsapp") or "",
            "fecha":      fecha,
            "size_kb":    round(f.stat().st_size / 1024, 1),
        })
    return {"pdfs": result, "total": len(result)}


@app.get("/api/admin/picks-pdf/{filename}")
async def admin_download_picks_pdf(filename: str, ql_admin: str = Cookie(default="")):
    """Descarga un PDF archivado especifico (con proteccion anti path-traversal)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    from fastapi.responses import FileResponse
    if ("/" in filename or "\\" in filename or ".." in filename
            or not filename.lower().endswith(".pdf")):
        raise HTTPException(400, "Nombre de archivo no válido")
    _pdf_dir = DATA_DIR / "picks_pdf"
    filepath = (_pdf_dir / filename).resolve()
    if _pdf_dir.resolve() not in filepath.parents:
        raise HTTPException(400, "Ruta no permitida")
    if not filepath.exists() or not filepath.is_file():
        raise HTTPException(404, "PDF no encontrado")
    return FileResponse(path=str(filepath), media_type="application/pdf", filename=filename)


@app.get("/api/admin/picks-pdf-todos")
async def admin_picks_pdf_todos(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Genera y descarga UN PDF con las apuestas de TODOS los jugadores.
    Acceso: ?key=CLAVE_ADMIN o sesión admin (cookie)."""
    from fastapi.responses import Response as _Resp
    import datetime as _dt
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    games = _db.db_get_horarios()
    pdf_bytes = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _pdf_todos_jugadores(games, cfg))
    torneo = (cfg.get("TORNEO", "Quiniela") or "Quiniela").replace(" ", "_")
    fname  = f"apuestas_todos_{torneo}_{_dt.datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return _Resp(content=pdf_bytes, media_type="application/pdf",
                 headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.get("/api/admin/picks-status")
async def admin_picks_status(ql_admin: str = Cookie(default="")):
    """Estado de picks por jugador: completos / pendientes / excluidos."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    games = _db.db_get_horarios()
    total = len(games)
    jugadores = []; n_c = n_p = n_e = 0
    for j in _db.db_get_jugadores():
        faltan, _t, llenos = _picks_faltantes(j["id"], games)
        excl = bool(j.get("excluido"))
        if excl: n_e += 1
        elif faltan > 0: n_p += 1
        else: n_c += 1
        jugadores.append({
            "id": j["id"], "nombre": j.get("nombre") or f"Jugador {j['id']}",
            "telefono": j.get("whatsapp") or "", "llenos": llenos, "total": total,
            "faltan": faltan, "excluido": excl,
            "excluido_fecha": j.get("excluido_fecha") or "",
            "excluido_motivo": j.get("excluido_motivo") or "",
        })
    jugadores.sort(key=lambda x: (not x["excluido"], x["faltan"] == 0, x["nombre"].lower()))
    return {"jugadores": jugadores, "total_juegos": total,
            "completos": n_c, "pendientes": n_p, "excluidos": n_e}


@app.post("/api/admin/excluir/{jugador_id}")
async def admin_excluir_jugador(jugador_id: int, ql_admin: str = Cookie(default="")):
    """Excluye manualmente a un jugador (reversible, no borra picks)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    import datetime as _dt
    ok = _db.db_set_excluido(jugador_id, True, "Excluido manualmente por el admin",
                             _dt.datetime.now().strftime("%d/%m/%Y %H:%M"))
    if not ok: raise HTTPException(404, "Jugador no encontrado")
    _invalidate_games()
    _cache["standings_rows"] = None
    return {"ok": True, "msg": "Jugador excluido"}


@app.post("/api/admin/reactivar/{jugador_id}")
async def admin_reactivar_jugador(jugador_id: int, ql_admin: str = Cookie(default="")):
    """Reactiva a un jugador excluido (vuelve a la quiniela con sus picks intactos)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    ok = _db.db_set_excluido(jugador_id, False)
    if not ok: raise HTTPException(404, "Jugador no encontrado")
    _invalidate_games()
    _cache["standings_rows"] = None
    return {"ok": True, "msg": "Jugador reactivado"}


@app.post("/api/admin/set-invitador")
async def admin_set_invitador(body: dict, ql_admin: str = Cookie(default="")):
    """Asigna quién invitó al jugador y lo APRUEBA (lo libera para registrar picks).
    invitador vacío → vuelve a pendiente. Body: {id|phone, invitador}."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    jid       = body.get("id") or 0
    phone     = (body.get("phone") or "").strip()
    invitador = (body.get("invitador") or "").strip()
    if not jid and phone:
        p = find_player_any(phone=phone)
        if p:
            jid = p.get("_id") or p.get("id")
    if not jid:
        raise HTTPException(404, "Jugador no encontrado")
    ok = _db.db_set_invitador(int(jid), invitador)
    if not ok:
        raise HTTPException(404, "Jugador no encontrado")
    _invalidate_players()
    _cache["players"].clear()
    # Avisar al jugador (push) que fue liberado, si se aprobó.
    if invitador:
        try:
            j = next((x for x in _db.db_get_jugadores() if x.get("id") == int(jid)), {})
            _send_push_players([j.get("whatsapp") or ""], [j.get("email") or ""],
                "✅ ¡Acceso liberado!",
                "Ya puedes registrar tus picks y ver la quiniela. ¡Mucha suerte!",
                {"tipo": "aprobado"})
        except Exception as e:
            print(f"[set-invitador] push: {e}")
    return {"ok": True, "aprobado": bool(invitador), "invitador": invitador}


@app.post("/api/admin/test-avance")
async def admin_test_avance(ql_admin: str = Cookie(default="")):
    """Envía AHORA al grupo (WhatsApp + Telegram) el resumen de avance de picks."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    games = _db.db_get_horarios()
    txt = "\U0001f4cb Recordatorio de la quiniela\n\n" + _texto_avance(games)
    res = {"wa": False, "tg": False}
    try:
        _wa("POST", "/send", json={"message": txt}); res["wa"] = True
    except Exception as e:
        print(f"[test-avance] WA: {e}")
    try:
        _tg_send(txt); res["tg"] = True
    except Exception as e:
        print(f"[test-avance] TG: {e}")
    canales = [c.upper() for c, ok in res.items() if ok]
    return {"ok": bool(canales), "enviado_a": canales,
            "msg": ("Enviado a " + ", ".join(canales)) if canales
                   else "No se pudo enviar (revisa WhatsApp/Telegram)"}


@app.get("/api/admin/health")
async def admin_health(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Estado de salud de los componentes. Acceso: ?key=CLAVE_ADMIN o sesión admin."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    snap = _health_snapshot()
    try:
        _health_alert(snap)
    except Exception as e:
        print(f"[health-endpoint] alert: {e}")
    return snap


@app.get("/api/admin/diag-sync")
async def admin_diag_sync(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Diagnóstico de la actualización automática de equipos desde ESPN.
    Muestra los flags que la controlan y el estado de cada juego. ?key=CLAVE_ADMIN."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    mp_raw = str(cfg.get("MODO_PRUEBA", "")).strip()
    fz_raw = str(cfg.get("FREEZE_EQUIPOS", "")).strip()
    modo_prueba    = mp_raw not in ("", "0", "false", "no")
    freeze_equipos = fz_raw not in ("", "0", "false", "no")
    games = _db.db_get_horarios()
    _R_CONGELADAS = ("R16", "QF", "SF", "3ER", "FINAL")
    juegos, sin_id = [], 0
    for g in games:
        eid   = g.get("espn_id", "") or ""
        ronda = g.get("grupo", "") or ""
        if not eid:
            sin_id += 1
        # ¿Este juego se actualizaría desde ESPN ahora mismo?
        congelado_ronda = ronda in _R_CONGELADAS
        actualizaria = (not modo_prueba) and bool(eid) and (not freeze_equipos) and (not congelado_ronda)
        juegos.append({
            "jgo": g.get("jgo"), "ronda": ronda,
            "eq1": g.get("eq1", ""), "eq2": g.get("eq2", ""),
            "estado": g.get("estado", ""), "espn_id": eid or None,
            "se_actualiza_de_espn": actualizaria,
        })
    motivos = []
    if modo_prueba:    motivos.append("MODO_PRUEBA está activo (1) → el updater NO consulta ESPN")
    if freeze_equipos: motivos.append("FREEZE_EQUIPOS está activo (1) → los equipos no se sobreescriben")
    if sin_id:         motivos.append(f"{sin_id} juego(s) sin espn_id → recarga partidos de ESPN")
    return {
        "MODO_PRUEBA": mp_raw or "(vacío=0)",
        "FREEZE_EQUIPOS": fz_raw or "(vacío=0)",
        "actualizacion_automatica_de_equipos_R32": (not modo_prueba and not freeze_equipos),
        "juegos_sin_espn_id": sin_id,
        "total_juegos": len(games),
        "problemas": motivos or ["Config OK: R32 se actualiza desde ESPN (R16+ se propagan del bracket)"],
        "nota": "R16/QF/SF/3ER/FINAL nunca se copian de ESPN (se propagan del bracket cuando avanzan los ganadores reales).",
        "juegos": juegos,
    }


@app.get("/api/admin/unpaid")
async def admin_unpaid(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Lista de jugadores que NO han pagado (excluye a los marcados como excluidos).
    Acceso: ?key=CLAVE_ADMIN o sesión admin."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN o inicia sesión como admin.")
    jugadores = _db.db_get_jugadores()
    unpaid = [{"id": p.get("id"), "nombre": p.get("nombre", ""), "whatsapp": p.get("whatsapp", ""),
               "email": p.get("email", ""), "fecha_reg": p.get("fecha_reg", "")}
              for p in jugadores if not p.get("pagado") and not p.get("excluido")]
    unpaid.sort(key=lambda x: (x["nombre"] or "").lower())
    return {"count": len(unpaid), "total_jugadores": len(jugadores), "unpaid": unpaid}


@app.get("/api/admin/recordar-pago")
async def admin_recordar_pago(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Envía AHORA el recordatorio de pago (WA+TG+push) si quedan morosos. Acceso:
    ?key=CLAVE_ADMIN o sesión admin."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN o inicia sesión como admin.")
    res = _send_recordatorio_pago()
    return {"ok": True, "enviado": res["enviado"], "faltan": res["n_sin"]}


@app.get("/api/admin/telegram-updates")
async def admin_telegram_updates(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Ayuda a obtener TU chat_id de Telegram: escríbele algo a tu bot y abre esto.
    Muestra los chats que han escrito al bot. ?key=CLAVE_ADMIN."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    token = (cfg.get("TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not token:
        return {"ok": False, "error": "No hay TELEGRAM_BOT_TOKEN configurado en el panel."}
    try:
        j = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=10).json()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    chats = {}
    for upd in j.get("result", []):
        msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post") or {}
        ch  = msg.get("chat") or {}
        if ch.get("id"):
            chats[str(ch["id"])] = {
                "chat_id": ch.get("id"),
                "tipo":    ch.get("type"),
                "nombre":  (ch.get("title")
                            or " ".join([ch.get("first_name", ""), ch.get("last_name", "")]).strip()
                            or ch.get("username", "")),
            }
    return {
        "ok": True,
        "instrucciones": ("1) En Telegram escríbele algo a tu bot (un '/start' o 'hola'). "
                          "2) Recarga esta página. 3) Copia el chat_id del tipo 'private' (el tuyo) "
                          "y pégalo en TELEGRAM_ADMIN_CHAT_ID en el panel → Guardar."),
        "chats": list(chats.values()),
    }


@app.get("/api/admin/player-picks")
async def admin_player_picks(key: str = Query(""), phone: str = Query(""),
                             id: int = Query(0), ql_admin: str = Cookie(default="")):
    """Picks de UN jugador (auditoría). Acceso: ?key=CLAVE_ADMIN o sesión admin.
    Identifícalo con ?phone= (con o sin +) o ?id=N."""
    cfg = state.get("cfg", {})
    admin_pass = cfg.get("ADMIN_PASS", "quiniela2026")
    if not (_admin_check(ql_admin) or (key and key == admin_pass)):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN o inicia sesión como admin.")
    import re as _re
    jug = None
    if id:
        jug = next((j for j in _db.db_get_jugadores() if j.get("id") == id), None)
    elif phone:
        # El '+' en la URL llega como espacio; comparamos solo por dígitos (tolerante al código de país)
        target = _re.sub(r"\D", "", phone)
        if target:
            for j in _db.db_get_jugadores():
                jph = _re.sub(r"\D", "", j.get("whatsapp", "") or "")
                if jph and (jph == target or jph.endswith(target) or target.endswith(jph)):
                    jug = j; break
    if not jug:
        raise HTTPException(404, "Jugador no encontrado. Usa ?phone=NUMERO (con o sin +) o ?id=N.")
    games = _db.db_get_horarios()
    data, _ = _jugador_picks_payload(jug["id"], jug.get("nombre", ""),
                                     jug.get("whatsapp", ""), jug.get("email", ""), games)
    data["excluido"] = bool(jug.get("excluido"))
    return data



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
    # Formato esperado por el frontend: {jgo: {eq1,gol1,gol2,eq2,ganador,pts}}
    # pts lo calcula el backend con inferencia de bracket (fuente unica de verdad).
    by_jgo, by_ronda = _db.build_bracket_index(_db.db_get_horarios())
    cfg = state.get("cfg", {})
    vL = int(cfg.get("PTS_LOGRO", 1) or 1); vG = int(cfg.get("PTS_GAN", 2) or 2)
    v1 = int(cfg.get("PTS_GOL1", 1) or 1);  v2 = int(cfg.get("PTS_GOL2", 1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)
    picks = {}
    for jgo, pk in raw.items():
        gm     = by_jgo.get(str(jgo), {})
        estado = gm.get("estado", "")
        pts    = None
        if estado and estado != "PROG":
            _, _, _, _, pts = _db.calc_pts_inferred(
                gm, pk, by_jgo, by_ronda, raw, vL, vG, v1, v2, vC)
        picks[str(jgo)] = {
            "eq1":     gm.get("eq1", ""),
            "gol1":    pk.get("g1", ""),
            "gol2":    pk.get("g2", ""),
            "eq2":     gm.get("eq2", ""),
            # peq1/peq2: equipos que el jugador PREDIJO para este cruce (inferidos
            # del bracket). En eliminatorias difieren de eq1/eq2 (reales). El front
            # los usa para mostrar el pick del jugador, no el partido real.
            "peq1":    _db._disp_team(gm, "eq1", by_jgo, by_ronda, raw) if gm else "",
            "peq2":    _db._disp_team(gm, "eq2", by_jgo, by_ronda, raw) if gm else "",
            "ganador": pk.get("gan", ""),
            "pts":     pts,
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

    # Control de invitador: si el jugador no está aprobado, no puede guardar picks.
    _fresh_j = _db.db_find_player(phone=body.phone or "", email=body.email or "") or {}
    if not bool(_fresh_j.get("aprobado", 1)):
        raise HTTPException(403, "Tu registro está pendiente de aprobación por el administrador.")

    games, _ = _get_games_cache()
    modo_prueba = state.get("cfg", {}).get("MODO_PRUEBA", "") in ("1", "true", "True")

    # Cierre de la quiniela: se bloquean TODOS los picks cuando ARRANCA el PRIMER
    # 16vo (R32). El bloqueo por partido de abajo queda redundante (todo se cierra
    # de golpe), pero es inocuo.
    cerrada = (not modo_prueba) and _quiniela_cerrada(games)

    guardados = bloqueados = medias = protegidos = 0

    # Estado previo: para proteger picks ya llenos de un guardado vacío (anti-borrado)
    # y para detectar pérdidas de completitud (alerta de respaldo).
    picks_antes  = _db.db_get_picks(int(player_id))
    total_games  = sum(1 for g in games if _juego_pickable(g))
    llenos_antes = sum(1 for g in games
                       if _juego_pickable(g) and _pick_completo(picks_antes.get(str(g["jgo"]), {})))

    def _tiene_datos(d):
        return bool(d.get("g1") or d.get("g2") or d.get("gan"))

    for pick in body.picks:
        game = next((g for g in games if g["jgo"] == str(pick.jgo)), None)
        if not game:
            bloqueados += 1
            continue

        # Cierre total (arrancó el último 16vo) → ningún pick se puede cambiar.
        if cerrada:
            bloqueados += 1
            continue

        # Bloqueo por partido: un partido que ya inició no se puede editar
        # (aunque la quiniela siga abierta para los demás).
        if (not modo_prueba) and (game.get("estado") or "PROG") not in ("PROG", ""):
            bloqueados += 1
            continue

        g1v = (str(pick.gol1) if pick.gol1 is not None else "") != ""
        g2v = (str(pick.gol2) if pick.gol2 is not None else "") != ""

        # REGLA: un pick a medias (un solo marcador) NO es válido → no se guarda.
        if g1v != g2v:
            medias += 1
            continue

        # ANTI-BORRADO: un pick entrante vacío nunca pisa uno que ya tiene datos.
        # (Para cambiar un pick se editan los números; vaciar ya no borra.)
        entrante_vacio = not (str(pick.gol1 or "") or str(pick.gol2 or "") or str(pick.ganador or ""))
        if entrante_vacio and _tiene_datos(picks_antes.get(str(pick.jgo), {})):
            protegidos += 1
            continue

        _db.db_save_pick(
            int(player_id), str(pick.jgo),
            str(pick.gol1), str(pick.gol2), str(pick.ganador),
            eq1=str(pick.eq1 or ""), eq2=str(pick.eq2 or "")
        )
        guardados += 1

    if guardados:
        _cache["standings_rows"] = None  # invalidar standings al guardar picks

    # Respaldo automático a Telegram (chat privado admin) con TODOS los jugadores,
    # en hilo aparte para no demorar al jugador. Solo si hubo cambios relevantes.
    if (guardados or protegidos) and total_games > 0:
        try:
            _, _, llenos_now = _picks_faltantes(int(player_id), games)
            nombre   = p.get("NOMBRE") or p.get("nombre") or ""
            telefono = p.get("WHATSAPP") or p.get("TELEFONO") or p.get("whatsapp") or ""
            email    = p.get("EMAIL") or p.get("email") or ""
            motivo = None
            if protegidos > 0:
                motivo = f"\U0001f6e1️ Se evitó borrar {protegidos} pick(s) ya llenos (intento de guardado vacío)"
            elif llenos_now == total_games:
                motivo = "✅ Picks COMPLETOS (respaldo)"
            elif llenos_antes == total_games and llenos_now < total_games:
                motivo = f"⚠️ ALERTA: bajó de {total_games} a {llenos_now} picks (posible pérdida)"
            if motivo:
                threading.Thread(target=_backup_picks_telegram,
                    args=(int(player_id), nombre, telefono, email, games, motivo),
                    daemon=True).start()
        except Exception as e:
            print(f"[save-picks] backup: {e}")

    return {"guardados": guardados, "bloqueados": bloqueados,
            "medias": medias, "protegidos": protegidos}


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
            # 16.6: el PUESTO se define SOLO por puntos (mismos pts = mismo puesto).
            # El desempate por aciertos solo ordena visualmente (ya viene ordenado).
            if i > 0 and s["pts"] != st[i - 1]["pts"]:
                pos = i + 1
            rows.append([pos, s["nombre"], s["pts"], s["pts"] - lider])
        result = [["POS", "NOMBRE", "Ptos", "Diferencia"]] + rows
        _cache["standings_rows"] = result
        return {"rows": result}
    except Exception:
        return {"rows": []}


@app.get("/api/mi-cuadro")
async def get_mi_cuadro(phone: str = Query(""), email: str = Query("")):
    """Cuadro (bracket) PREDICHO por el jugador, con el estado vivo/muerto de cada
    equipo según el torneo real. Solo el propio jugador (su sesión)."""
    p = find_player_any(phone=phone, email=email) if (phone or email) else None
    if not p:
        raise HTTPException(404, "Jugador no encontrado. Inicia sesión de nuevo.")
    pid    = p.get("_id") or p.get("id")
    nombre = p.get("nombre") or p.get("NOMBRE") or ""

    games = _db.db_get_horarios()
    by_jgo, by_ronda = _db.build_bracket_index(games)
    vivos = _db._equipos_vivos(games)          # equipos aún no eliminados
    picks = _db.db_get_picks(pid)

    cfg = state.get("cfg", {})
    vL = int(cfg.get("PTS_LOGRO", 1) or 1); vG = int(cfg.get("PTS_GAN", 2) or 2)
    v1 = int(cfg.get("PTS_GOL1", 1) or 1); v2 = int(cfg.get("PTS_GOL2", 1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)

    def _is_ph(n):
        n = (n or "").strip()
        return (not n) or _db._is_placeholder(n) or n.startswith("Gan. ") or n.startswith("Perdedor ")

    def _status(name):
        if _is_ph(name):
            return "unknown"
        return "alive" if name in vivos else "dead"

    ROND = [("R32", "Dieciseisavos"), ("R16", "Octavos"), ("QF", "Cuartos"),
            ("SF", "Semis"), ("3ER", "Tercer puesto"), ("FINAL", "Final")]
    by_r = {}
    for g in games:
        r = (g.get("grupo") or g.get("ronda") or "")
        by_r.setdefault(r, []).append(g)

    rondas = []
    for key, label in ROND:
        gl = sorted(by_r.get(key, []),
                    key=lambda x: int(x["jgo"]) if str(x["jgo"]).isdigit() else 0)
        if not gl:
            continue
        matches = []
        for g in gl:
            js = str(g["jgo"]); pk = picks.get(js) or {}
            e1 = _db._disp_team(g, "eq1", by_jgo, by_ronda, picks)
            e2 = _db._disp_team(g, "eq2", by_jgo, by_ronda, picks)
            ganp = (_db._resolve_team_name((pk.get("gan") or "").strip(), by_jgo, by_ronda, picks)
                    or (pk.get("gan") or "").strip())
            estado = g.get("estado", "")
            jugado = bool(estado) and estado != "PROG"
            real_gan = (g.get("ganador") or "").strip()
            st1, st2, stw = _status(e1), _status(e2), _status(ganp)
            es_final = key == "FINAL"
            # Puntos: si ya se jugó → ganados reales; si está pendiente → máximo
            # aún disponible según qué equipos predichos siguen vivos.
            if jugado:
                _, _, _, _, ganado = _db.calc_pts_inferred(
                    g, pk, by_jgo, by_ronda, picks, vL, vG, v1, v2, vC)
                max_disp = ganado
            else:
                ganado = None
                if st1 != "alive" and st2 != "alive":
                    max_disp = 0   # ambos equipos muertos → no suma nada
                else:
                    max_disp = vL                                  # logro (no-empate)
                    if stw == "alive": max_disp += vG              # ganador
                    if st1 == "alive": max_disp += v1              # gol equipo 1
                    if st2 == "alive": max_disp += v2              # gol equipo 2
                    if es_final and stw == "alive": max_disp += vC # campeón
            real = None
            if jugado:
                real = {
                    "eq1": (g.get("eq1") or "").strip(),
                    "eq2": (g.get("eq2") or "").strip(),
                    "g1":  g.get("gol1", ""), "g2": g.get("gol2", ""),
                    "gan": real_gan,
                }
            matches.append({
                "jgo":    js,
                "eq1":    {"name": ("?" if _is_ph(e1) else e1), "status": st1},
                "eq2":    {"name": ("?" if _is_ph(e2) else e2), "status": st2},
                "winner": {"name": ("?" if _is_ph(ganp) else ganp), "status": stw},
                "g1":     pk.get("g1", ""), "g2": pk.get("g2", ""),
                "jugado": jugado,
                "acerto": bool(jugado and ganp and real_gan and ganp == real_gan),
                "max":    max_disp,
                "ganado": ganado,
                "real":   real,
            })
        rondas.append({"key": key, "label": label, "matches": matches})

    camp = {"name": "?", "status": "unknown"}
    fin = [g for g in games if (g.get("grupo") or g.get("ronda") or "").upper() == "FINAL"]
    if fin:
        pkf = picks.get(str(fin[0]["jgo"])) or {}
        c = (_db._resolve_team_name((pkf.get("gan") or "").strip(), by_jgo, by_ronda, picks)
             or (pkf.get("gan") or "").strip())
        camp = {"name": ("?" if _is_ph(c) else c), "status": _status(c)}

    # Reordenar cada ronda en ORDEN DE CUADRO (no por fecha/jgo de FIFA): se deriva
    # desde la Final hacia abajo con _WC2026_MAP, para que los 2 alimentadores de
    # cada cruce queden adyacentes y alineados con su ronda superior.
    WCMAP = _db._WC2026_MAP
    def _expand(parent_order, child_map):
        out = []
        for p in parent_order:
            if 0 <= p < len(child_map):
                out.extend(child_map[p])
        return out
    disp = {"FINAL": [0]}
    disp["SF"]  = _expand(disp["FINAL"], WCMAP.get("FINAL", []))
    disp["QF"]  = _expand(disp["SF"],    WCMAP.get("SF", []))
    disp["R16"] = _expand(disp["QF"],    WCMAP.get("QF", []))
    disp["R32"] = _expand(disp["R16"],   WCMAP.get("R16", []))
    for rd in rondas:
        o = disp.get(rd["key"])
        if not o:
            continue
        ms = rd["matches"]; used = set()
        nuevo = []
        for i in o:
            if 0 <= i < len(ms) and i not in used:
                nuevo.append(ms[i]); used.add(i)
        for i in range(len(ms)):           # cualquier sobrante, por seguridad
            if i not in used:
                nuevo.append(ms[i])
        rd["matches"] = nuevo

    return {"nombre": nombre, "rondas": rondas, "campeon": camp}


@app.get("/api/recorrido")
async def get_recorrido(phone: str = Query(""), email: str = Query("")):
    """Evolución de la posición de cada jugador partido a partido (gráfica Recorrido)."""
    try:
        data = _db.db_compute_recorrido(state.get("cfg", {}))
        me = None
        if phone or email:
            p = find_player_any(phone=phone, email=email)
            if p:
                me = p.get("_id") or p.get("id")
        for j in data.get("jugadores", []):
            j["yo"] = (j["id"] == me)
        return data
    except Exception as e:
        print(f"[recorrido] {e}")
        return {"labels": [], "total": 0, "jugadores": []}


# ââ Mis Puntos ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.get("/api/my-points")
async def get_my_points(email: str = Query(""), phone: str = Query("")):
    try:
        player = find_player_any(phone=phone, email=email)
        if not player:
            raise HTTPException(404, "Jugador no encontrado")

        cfg    = state.get("cfg", {})
        games, _ = _get_games_cache()
        by_jgo, by_ronda = _db.build_bracket_index(games)

        _v_logro   = int(cfg.get("PTS_LOGRO", 1) or 1)
        _v_gan     = int(cfg.get("PTS_GAN",   2) or 2)
        _v_gol1    = int(cfg.get("PTS_GOL1",  1) or 1)
        _v_gol2    = int(cfg.get("PTS_GOL2",  1) or 1)
        _v_campeon = int(cfg.get("PTS_CAMPEON", 0) or 0)

        picks_raw = _db.db_get_picks(player["_id"])  # {jgo_str: {g1,g2,gan,eq1,eq2}}

        by_day    = {}
        total_pts = 0

        for jgo_str, pk in picks_raw.items():
            game = by_jgo.get(jgo_str)
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

            # teamAlive (solo para mostrar el flag; _calc_pts lo aplica internamente)
            team_alive = True
            if eq1_real and eq2_real:
                real_teams = {eq1_real.strip(), eq2_real.strip()}
                pred_teams = {eq1_pick.strip(), eq2_pick.strip(), pick_gan.strip()}
                pred_teams = {t for t in pred_teams if t and not t.startswith("Gan. ")}
                if pred_teams and not (pred_teams & real_teams):
                    team_alive = False

            # Calculo centralizado con inferencia de bracket (fuente unica de verdad)
            pts_logro, pts_gan, pts_gol1, pts_gol2, pts = _db.calc_pts_inferred(
                game, pk, by_jgo, by_ronda, picks_raw,
                _v_logro, _v_gan, _v_gol1, _v_gol2, _v_campeon)

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

    # Picks de cada jugador con inferencia de bracket (fuente unica de verdad)
    by_jgo, by_ronda = _db.build_bracket_index(games)
    grouped = _db.db_get_all_picks_grouped()
    game_picks = []
    for _pid, data in grouped.items():
        if data.get("excluido"):
            continue   # los jugadores excluidos no aparecen en la comparación
        pp = data["picks"]
        pk = pp.get(str(jgo))
        if not pk:
            continue
        pick_gol1 = pk.get("g1", "") or ""
        pick_gol2 = pk.get("g2", "") or ""
        pick_gan  = pk.get("gan", "") or ""
        pts_logro, pts_gan, pts_gol1, pts_gol2, pts = _db.calc_pts_inferred(
            game, pk, by_jgo, by_ronda, pp,
            _v_logro, _v_gan, _v_gol1, _v_gol2, _v_campeon)
        game_picks.append({
            "nombre":    data.get("nombre", ""),
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
    vL = int(cfg.get("PTS_LOGRO", 1) or 1); vG = int(cfg.get("PTS_GAN", 2) or 2)
    v1 = int(cfg.get("PTS_GOL1", 1) or 1);  v2 = int(cfg.get("PTS_GOL2", 1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)
    by_jgo, by_ronda = _db.build_bracket_index(games)
    grouped = _db.db_get_all_picks_grouped()
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

        game_picks = []
        for _pid, data in grouped.items():
            if data.get("excluido"):
                continue   # los jugadores excluidos no aparecen en la comparación
            pp = data["picks"]
            pk = pp.get(jgo_str)
            if not pk:
                continue
            pick_gol1 = pk.get("g1", "") or ""
            pick_gol2 = pk.get("g2", "") or ""
            pick_gan  = pk.get("gan", "") or ""

            # Calculo centralizado con inferencia de bracket (fuente unica de verdad)
            pts_logro, pts_gan, pts_gol1, pts_gol2, pts = _db.calc_pts_inferred(
                game, pk, by_jgo, by_ronda, pp, vL, vG, v1, v2, vC)

            game_picks.append({
                "nombre":    data.get("nombre", ""),
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
    """Probabilidades (Monte Carlo del bracket) + distribución de picks."""
    now = time.time()
    if _cache.get("prob") is not None and now - _cache.get("prob_ts", 0) < PROB_TTL:
        return _cache["prob"]
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _build_probabilities)
    _cache["prob"]    = result
    _cache["prob_ts"] = time.time()
    return result


def _build_probabilities():
    """Cálculo pesado (corre en executor): Monte Carlo + distribución de picks."""
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

    # Probabilidades por jugador con Monte Carlo del bracket (16.12).
    mc = _db.db_compute_probabilities_mc(cfg)
    # Conservar info de "equipos con vida / por cobrar" (cálculo realista barato),
    # fusionada por nombre en los jugadores del Monte Carlo.
    try:
        extra = {e["nombre"]: e for e in _db.db_compute_probabilities(cfg)}
        for p in mc.get("players", []):
            e = extra.get(p["name"])
            if e:
                p["equipos_vivos"]    = e.get("equipos_vivos", 0)
                p["equipos_lista"]    = e.get("equipos_lista", [])
                p["por_cobrar"]       = e.get("por_cobrar", 0)
                p["por_cobrar_lista"] = e.get("por_cobrar_lista", [])
    except Exception as _ex:
        print(f"[prob] merge equipos_vivos: {_ex}")
    mc["games"] = game_dist
    return mc


@app.get("/api/universos")
async def get_universos():
    """Vista de 'universos' (proyección perfecta por jugador) derivada del
    mismo cálculo de probabilidades."""
    data = await get_probabilities()
    estado_es = {"opcion_1": "puede_1", "solo_2": "solo_2", "eliminado": "eliminado"}
    out = []
    for p in data.get("players", []):
        out.append({"nombre": p.get("name"), "pts": p.get("current_pts"),
                    "techo": p.get("techo"), "univ_1ro": p.get("univ_win"),
                    "univ_top2": p.get("univ_top2"),
                    "dif_2do_en_su_universo": p.get("univ_dif2"),
                    "estado": estado_es.get(p.get("chance"), p.get("chance"))})
    out.sort(key=lambda x: (-(x["univ_top2"] or 0), -(x["univ_1ro"] or 0), -(x["pts"] or 0)))
    return {"universos": len(data.get("players", [])),
            "fixed_games": data.get("fixed_games"),
            "pending_games": data.get("pending_games"), "jugadores": out}


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
    ("TELEGRAM_ADMIN_CHAT_ID", "Tu chat personal de Telegram (DM al admin: nuevos registros, respaldos, salud)"),
    ("TELEGRAM_ENABLED",     "Notificaciones Telegram activas (1=sí, 0=no)"),
    ("TELEGRAM_INVITE_LINK", "Enlace de invitación al grupo (https://t.me/+...)"),
    ("RESET_KEY",           "Clave para resetear el torneo"),
    ("ADMIN_USER",          "Usuario del panel admin (para iniciar sesión)"),
    ("ADMIN_PASS",          "Contraseña del panel admin (para iniciar sesión)"),
    ("DIA_INICIO_JORNADA",  "Día inicio de jornada (0=Lun, 1=Mar, 2=Mié, 3=Jue, 4=Vie, 5=Sáb, 6=Dom)"),
    ("COLOR_SCHEME",        "Esquema de colores de la app"),
    ("PREMIOS_REGLAS",      "Premios y reglas (texto libre, saltos de línea permitidos)"),
    ("SORTEO_FECHA",        "Fecha del sorteo en vivo — activa la pestaña sorteo"),
    ("SORTEO_HORA",         "Hora del sorteo en vivo (en UTC — España verano = UTC+2, réstale 2h)"),
    ("SORTEO_ANIM",         "Animación del sorteo"),
    ("STRIPE_ACTIVO",       "Pago con tarjeta activo (1=sí, 0=no)"),
    ("FREEZE_EQUIPOS",      "Congelar nombres de equipos (1=no sobreescribir desde ESPN, 0=actualizar)"),
    ("MODO_PRUEBA",         "Modo Prueba (1=usar ESPN_ID_TEST para scores, 0=producción)"),
    ("PTS_LOGRO",           "Puntos por acertar empate/no-empate a los 90min"),
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


def _r32_cierre_dt(games=None):
    """Datetime UTC del PRIMER partido de 16vos (R32). Cuando ese partido arranca,
    se cierra TODA la quiniela (todos los picks de todas las rondas)."""
    from datetime import datetime as _dt
    if games is None:
        games, _ = _get_games_cache()
    dts = []
    for g in games:
        ronda = (g.get("ronda") or g.get("grupo") or "")
        if ronda != "R32":
            continue
        f, h = (g.get("fecha") or "").strip(), (g.get("hora") or "").strip()
        if not f or not h:
            continue
        try:
            dts.append(_dt.fromisoformat(f"{f}T{h}:00+00:00"))
        except Exception:
            pass
    return min(dts) if dts else None


def _quiniela_cerrada(games=None) -> bool:
    """True cuando ya comenzó el PRIMER 16vo (R32): a partir de ahí se bloquean TODOS
    los picks de todas las rondas (cierre justo, nadie edita viendo resultados)."""
    from datetime import datetime as _dt, timezone as _tz
    if games is None:
        games, _ = _get_games_cache()
    # Algún partido ya inició (R32 es la 1ª ronda → su primer partido es el del torneo).
    if any((g.get("estado") or "PROG") not in ("PROG", "") for g in games):
        return True
    # Respaldo: la hora del primer R32 ya pasó aunque ESPN no haya marcado el inicio.
    dt = _r32_cierre_dt(games)
    return dt is not None and _dt.now(_tz.utc) >= dt


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
    # Fuente primaria: SQLite (igual que el resto de F2). Antes leía de Google
    # Sheets, que en F2 está vacío → por eso el frontend caía al input de texto
    # en vez de mostrar el listado de ligas con checkboxes.
    try:
        ligas_db = _db.db_get_ligas()
        if ligas_db:
            return {"ligas": [{"nombre": l.get("nombre", ""), "codigo": l.get("codigo", ""),
                               "espn_id": l.get("espn_id", "")} for l in ligas_db]}
    except Exception as e:
        print(f"[ligas] SQLite: {e}")
    # Fallback a Google Sheets (compatibilidad con instalaciones antiguas)
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


@app.post("/api/admin/seed-ligas")
async def admin_seed_ligas(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Siembra el catálogo de ligas que falte (no toca las existentes). Acceso:
    ?key=CLAVE_ADMIN o sesión admin. Útil para llenar la tabla sin redeploy."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    insertadas = _db.db_seed_ligas_default()
    ligas = _db.db_get_ligas()
    return {"ok": True, "insertadas": insertadas, "total": len(ligas), "ligas": ligas}


@app.get("/api/admin/config")
async def admin_get_config(ql_admin: str = Cookie(default="")):
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    cfg = state.get("cfg", {})
    fields = {k: cfg.get(k, "") for k, _ in ADMIN_CONFIG_FIELDS}
    # Mostrar las credenciales reales del admin (con su default) aunque aún no se
    # hayan guardado en config, para que el campo no aparezca vacío.
    if not fields.get("ADMIN_USER"): fields["ADMIN_USER"] = "admin"
    if not fields.get("ADMIN_PASS"): fields["ADMIN_PASS"] = "quiniela2026"
    return {"fields":   fields,
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
async def admin_get_players(key: str = Query(""), ql_admin: str = Cookie(default="")):
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN o inicia sesión como admin.")
    # F2 es SQLite-primario (antes leía de Sheets, vacío). Incluye el responsable
    # (invitador) y el estado de aprobación de cada jugador.
    players = []
    for j in _db.db_get_jugadores():
        players.append({
            "id":        j.get("id"),
            "nombre":    j.get("nombre", ""),
            "email":     j.get("email", ""),
            "phone":     j.get("whatsapp", ""),
            "fecha":     j.get("fecha_reg", ""),
            "tab":       j.get("tab_nombre", ""),
            "pagado":    bool(j.get("pagado")),
            "aprobado":  bool(j.get("aprobado", 1)),
            "invitador": j.get("invitador", "") or "",
            "excluido":  bool(j.get("excluido")),
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
        # Estado de aprobación/invitador fresco de SQLite (por teléfono normalizado).
        _jdb  = _db.db_get_jugadores()
        _amap = {_normalize_phone(j.get("whatsapp", "") or ""): j for j in _jdb if j.get("whatsapp")}
        # Jugadores ya aprobados (para el dropdown de invitador en el admin).
        aprobados = sorted(
            [j.get("nombre", "") for j in _jdb
             if j.get("aprobado", 1) and not j.get("excluido") and j.get("nombre")],
            key=lambda s: s.lower())
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
            _jrec = _amap.get(_normalize_phone(d.get("WHATSAPP", d.get("TELEFONO", "")) or ""), {})
            players.append({
                "nombre": d.get("NOMBRE", ""),
                "email":  d.get("EMAIL", ""),
                "phone":  d.get("WHATSAPP", d.get("TELEFONO", "")),
                "fecha":  d.get("FECHA REG.", d.get("FECHA_REGISTRO", "")),
                "tab":    d.get("TAB_NOMBRE", ""),
                "pagado": is_paid,
                "id":        _jrec.get("id"),
                "aprobado":  bool(_jrec.get("aprobado", 1)),
                "invitador": _jrec.get("invitador", "") or "",
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
        return {"prize": prize, "players": players, "aprobados": aprobados}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[prize-and-players] ERROR: {e}\n{traceback.format_exc()}")
        raise HTTPException(503, f"Error: {e}")


def _get_tie_counts() -> tuple:
    """Retorna (tie_1st, tie_2nd): cuántos jugadores comparten el 1° y el 2° lugar.
    16.6: el puesto se cuenta SOLO por puntos (mismos pts = mismo puesto), igual que
    la tabla, para que el reparto de premios en empates sea correcto. Lee de SQLite
    (antes leía POSICIONES de Sheets, vacío en F2 → siempre 1,1)."""
    try:
        st = _db.db_compute_standings(state.get("cfg", {}))
        if not st:
            return 1, 1
        from collections import Counter
        pos_counter: Counter = Counter()
        pos = 1
        for i, s in enumerate(st):
            if i > 0 and s["pts"] != st[i - 1]["pts"]:
                pos = i + 1
            pos_counter[pos] += 1
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
    # Mismo criterio que el Pozo público: cuenta aprobados no excluidos.
    paid = sum(1 for j in _db.db_get_jugadores()
               if j.get("aprobado", 1) and not j.get("excluido"))
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
    p = find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    pid = p.get("_id") or p.get("id")
    if not pid:
        raise HTTPException(404, "Jugador sin ID")
    conn = _db.get_conn()
    with conn:
        conn.execute("UPDATE jugadores SET pagado=? WHERE id=?",
                     (1 if paid else 0, int(pid)))
    conn.close()
    _invalidate_players()
    _cache["players"].clear()
    return {"ok": True, "paid": paid}


@app.post("/api/admin/player-delete")
async def admin_player_delete(body: dict, ql_admin: str = Cookie(default="")):
    """Elimina un jugador (SQLite) y todos sus picks, por telefono o email."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    email = (body.get("email") or "").strip()
    phone = (body.get("phone") or "").strip()
    if not email and not phone:
        raise HTTPException(400, "email o teléfono requerido")
    p = find_player_any(phone=phone, email=email)
    if not p:
        raise HTTPException(404, "Jugador no encontrado")
    player_id = p.get("_id") or p.get("id")
    if not player_id:
        raise HTTPException(404, "Jugador sin ID")

    # Cancelar suscripciones push del jugador (si las hay)
    phone_norm  = p.get("WHATSAPP") or p.get("TELEFONO") or ""
    email_clean = p.get("EMAIL") or ""
    if _push_subs:
        _push_subs[:] = [
            s for s in _push_subs
            if not (s.get("_phone") == phone_norm or s.get("_email") == email_clean)
        ]
        _subs_save()

    _db.db_delete_player(int(player_id))
    _invalidate_players()
    _cache["players"].clear()
    _cache["standings_rows"] = None
    return {"ok": True, "deleted": player_id}


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
    # El pozo cuenta a los jugadores APROBADOS y no excluidos: como el acceso ya
    # se controla por invitador/aprobación, entrar = estar en el pozo (en F2 no
    # hay cobro automático que marque "pagado").
    paid = sum(1 for j in _db.db_get_jugadores()
               if j.get("aprobado", 1) and not j.get("excluido"))
    tie_1st, tie_2nd = _get_tie_counts()
    result = _calc_prize(paid, cost, cat_a_max=cat_a, cat_b_max=cat_b,
                         pct_1=pct_1, sorteo_cant=sorteo_cant,
                         sorteo_ganadores=ganadores,
                         tie_1st=tie_1st, tie_2nd=tie_2nd,
                         fee_pct=fee_pct)
    result["costo"] = cost
    result["torneo_activo"] = _torneo_activo().get("activo", False)
    # quiniela_cerrada: la quiniela se cierra al arrancar el ÚLTIMO 16vo (R32).
    # El frontend lo usa para ocultar Comparar/Probabilidades/Sorteo y el botón
    # de retirarse (mientras siga abierta, nadie debe ver picks ajenos).
    try:
        _dtc = _r32_cierre_dt()
        result["quiniela_cerrada"] = _quiniela_cerrada()
        result["cierre_dt"] = _dtc.strftime("%Y-%m-%dT%H:%M:%SZ") if _dtc else ""
    except Exception:
        result["quiniela_cerrada"] = False
        result["cierre_dt"] = ""
    result["sorteo_ganadores"] = [g for g in ganadores if g and g.strip()]
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
    for row in rows:  # rows son dicts (SQLite via _jugador_db_to_cache)
        if not row:
            continue
        d = _normalize_player(row)
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
            # Ventana de gracia tras la hora del sorteo: si la fecha ya pasó pero
            # hace poco (≤6 h), seguimos en lobby esperando que el admin lance. Si
            # pasó hace mucho (fecha vieja/obsoleta), NO activamos el sorteo: se
            # mantiene 'idle' y la pestaña permanece oculta.
            _GRACIA_MIN = 6 * 60
            if fase_actual == "idle":
                if -_GRACIA_MIN <= minutos <= 0:
                    _sorteo["fase"] = "lobby"
                    fase_actual = "lobby"
                elif 0 < minutos <= 15:
                    fase_actual = "lobby"  # mostrar tab pero no cambiar estado
                # minutos < -gracia → fecha pasada obsoleta → seguir idle (tab oculto)
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
async def admin_player_points(q: str = Query(""), key: str = Query(""),
                              ql_admin: str = Cookie(default="")):
    """Diagnostico: desglose de puntos por juego de UN jugador, con la MISMA
    funcion de calculo que la tabla de posiciones. Util para auditar.
    Uso: /api/admin/player-points?q=eudi          (logueado como admin), o
         /api/admin/player-points?q=eudi&key=CLAVE (CLAVE = tu contraseña admin),
    para abrirlo directo en el navegador sin login."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
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
    by_jgo, by_ronda = _db.build_bracket_index(_db.db_get_horarios())

    juegos = []
    tot = {"logro": 0, "gan": 0, "gol1": 0, "gol2": 0, "campeon": 0, "total": 0}
    for jgo_str in sorted(by_jgo, key=lambda x: int(x) if x.isdigit() else 0):
        h = by_jgo[jgo_str]
        estado = h.get("estado", "")
        if not estado or estado == "PROG":
            continue
        pk = picks.get(jgo_str, {})
        # Equipos que predijo el jugador (inferidos del bracket) y puntos
        eq1_inf = _db._disp_team(h, "eq1", by_jgo, by_ronda, picks)
        eq2_inf = _db._disp_team(h, "eq2", by_jgo, by_ronda, picks)
        pl, pg, pg1, pg2, total = _db.calc_pts_inferred(
            h, pk, by_jgo, by_ronda, picks, vL, vG, v1, v2, vC)
        pc = total - (pl + pg + pg1 + pg2)  # campeon = lo que sumo aparte
        # teamAlive con los equipos inferidos (para mostrarlo explicito)
        team_alive = True
        e1r = (h.get("eq1", "") or "").strip(); e2r = (h.get("eq2", "") or "").strip()
        if e1r and e2r:
            real = {e1r, e2r}
            pred = {(eq1_inf or "").strip(), (eq2_inf or "").strip(),
                    (pk.get("gan", "") or "").strip()}
            pred = {t for t in pred if t and not t.startswith("Gan. ")}
            if pred and not (pred & real):
                team_alive = False
        juegos.append({
            "jgo": jgo_str, "ronda": h.get("grupo", ""),
            "eq1_real": h.get("eq1", ""), "eq2_real": h.get("eq2", ""),
            "marcador_real": f"{h.get('gol1','')}-{h.get('gol2','')}", "gan_real": h.get("ganador", ""),
            "pick_marcador": f"{pk.get('g1','')}-{pk.get('g2','')}", "pick_gan": pk.get("gan", ""),
            "eq1_pick_inferido": eq1_inf, "eq2_pick_inferido": eq2_inf,
            "eq1_pick_guardado": pk.get("eq1", ""), "eq2_pick_guardado": pk.get("eq2", ""),
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


@app.get("/api/admin/picks-summary")
async def admin_picks_summary(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Diagnostico: por jugador, cuantos picks tiene completos y CUALES jgos le
    faltan ganador o marcador. Lee directo de la tabla picks (misma fuente que
    el contador de la vista SQLite). Uso: ?key=TU_CLAVE_ADMIN."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    grouped = _db.db_get_all_picks_grouped()
    out = []
    for pid, data in grouped.items():
        pp = data["picks"]
        def _ints(jgos):
            return sorted(int(j) for j in jgos if str(j).isdigit())
        sin_gan = _ints(j for j, pk in pp.items() if not (pk.get("gan") or "").strip())
        sin_marc = _ints(j for j, pk in pp.items()
                         if not (pk.get("g1") or "").strip() or not (pk.get("g2") or "").strip())
        out.append({
            "jugador":           data.get("nombre", ""),
            "jugador_id":        pid,
            "total_filas":       len(pp),
            "con_ganador":       sum(1 for pk in pp.values() if (pk.get("gan") or "").strip()),
            "con_marcador":      sum(1 for pk in pp.values()
                                     if (pk.get("g1") or "").strip() and (pk.get("g2") or "").strip()),
            "jgos_sin_ganador":  sin_gan,
            "jgos_sin_marcador": sin_marc,
        })
    out.sort(key=lambda x: x["con_ganador"])  # los incompletos primero
    return {"n_jugadores": len(out), "jugadores": out}


@app.get("/api/admin/all-player-points")
async def admin_all_player_points(key: str = Query(""), q: str = Query(""),
                                  ql_admin: str = Cookie(default=""),
                                  solo_jugados: int = Query(1)):
    """Diagnostico: desglose de puntos/apuestas por juego de TODOS los jugadores
    (mismo calculo que la tabla, con inferencia de bracket).
    Uso: /api/admin/all-player-points?key=TU_CLAVE_ADMIN
         &q=angibell       -> filtra solo ese jugador (nombre parcial)
         &solo_jugados=0   -> incluye tambien partidos no finalizados (pts=null)."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")

    cfg = state.get("cfg", {})
    vL = int(cfg.get("PTS_LOGRO", 1) or 1); vG = int(cfg.get("PTS_GAN", 2) or 2)
    v1 = int(cfg.get("PTS_GOL1", 1) or 1);  v2 = int(cfg.get("PTS_GOL2", 1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)

    by_jgo, by_ronda = _db.build_bracket_index(_db.db_get_horarios())
    grouped = _db.db_get_all_picks_grouped()
    jgo_orden = sorted(by_jgo, key=lambda x: int(x) if x.isdigit() else 0)
    ql = q.strip().lower()

    jugadores_out = []
    for pid, data in grouped.items():
        if ql and ql not in (data.get("nombre", "").lower()):
            continue
        pp = data["picks"]
        juegos = []
        tot = {"logro": 0, "gan": 0, "gol1": 0, "gol2": 0, "campeon": 0, "total": 0}
        for jgo_str in jgo_orden:
            h = by_jgo[jgo_str]
            estado = h.get("estado", "")
            jugado = bool(estado and estado != "PROG")
            if solo_jugados and not jugado:
                continue
            pk = pp.get(jgo_str, {})
            eq1_inf = _db._disp_team(h, "eq1", by_jgo, by_ronda, pp)
            eq2_inf = _db._disp_team(h, "eq2", by_jgo, by_ronda, pp)
            if jugado:
                pl, pg, pg1, pg2, total = _db.calc_pts_inferred(
                    h, pk, by_jgo, by_ronda, pp, vL, vG, v1, v2, vC)
                pc = total - (pl + pg + pg1 + pg2)
            else:
                pl = pg = pg1 = pg2 = pc = 0
                total = None
            team_alive = True
            e1r = (h.get("eq1", "") or "").strip(); e2r = (h.get("eq2", "") or "").strip()
            if jugado and e1r and e2r:
                real = {e1r, e2r}
                pred = {(eq1_inf or "").strip(), (eq2_inf or "").strip(), (pk.get("gan", "") or "").strip()}
                pred = {t for t in pred if t and not t.startswith("Gan. ")}
                if pred and not (pred & real):
                    team_alive = False
            juegos.append({
                "jgo": jgo_str, "ronda": h.get("grupo", ""), "estado": estado,
                "eq1_real": h.get("eq1", ""), "eq2_real": h.get("eq2", ""),
                "marcador_real": f"{h.get('gol1','')}-{h.get('gol2','')}", "gan_real": h.get("ganador", ""),
                "pick_marcador": f"{pk.get('g1','')}-{pk.get('g2','')}", "pick_gan": pk.get("gan", ""),
                "eq1_pick_inferido": eq1_inf, "eq2_pick_inferido": eq2_inf,
                "eq1_pick_guardado": pk.get("eq1", ""), "eq2_pick_guardado": pk.get("eq2", ""),
                "team_alive": team_alive,
                "pts_logro": pl, "pts_gan": pg, "pts_gol1": pg1, "pts_gol2": pg2,
                "pts_campeon": pc, "pts": total,
            })
            if jugado:
                tot["logro"] += pl; tot["gan"] += pg; tot["gol1"] += pg1
                tot["gol2"] += pg2; tot["campeon"] += pc; tot["total"] += total
        jugadores_out.append({
            "jugador": data.get("nombre", ""), "jugador_id": pid,
            "totales": tot, "juegos": juegos,
        })

    jugadores_out.sort(key=lambda x: -x["totales"]["total"])
    return {
        "valores_pts": {"logro": vL, "gan": vG, "gol1": v1, "gol2": v2, "campeon": vC},
        "n_jugadores": len(jugadores_out),
        "jugadores": jugadores_out,
    }


# ── Respaldos automáticos (JSON en disco persistente /data) ──────────────────
BACKUP_DIR  = os.path.join(os.path.dirname(_db.DB_PATH) or ".", "backups")
BACKUP_KEEP = 14  # cuántos backups diarios conservar

def _write_daily_backup(force: bool = False) -> str | None:
    """Escribe un dump JSON (jugadores+picks+horarios+config) una vez al día.
    Rota conservando los últimos BACKUP_KEEP. Devuelve la ruta si escribió."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    fname = os.path.join(BACKUP_DIR, f"quiniela_{today}.json")
    if os.path.exists(fname) and not force:
        return None
    dump = _db.db_dump_all()
    dump["_ts"] = datetime.now().isoformat(timespec="seconds")
    tmp = fname + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dump, f, ensure_ascii=False)
    os.replace(tmp, fname)  # escritura atómica
    # Rotación
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, "quiniela_*.json")))
    for old in files[:-BACKUP_KEEP]:
        try: os.remove(old)
        except Exception: pass
    print(f"[backup] {os.path.basename(fname)} -> "
          f"{len(dump.get('jugadores', []))} jug, {len(dump.get('picks', []))} picks")
    return fname

def _list_backups() -> list:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    out = []
    for p in sorted(glob.glob(os.path.join(BACKUP_DIR, "quiniela_*.json")), reverse=True):
        try:
            st = os.stat(p)
            out.append({"archivo": os.path.basename(p), "bytes": st.st_size})
        except Exception:
            pass
    return out


@app.get("/api/admin/backups")
async def admin_backups_list(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Lista los respaldos disponibles."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    return {"backups": _list_backups(), "dir": BACKUP_DIR}


@app.post("/api/admin/backup-now")
async def admin_backup_now(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Fuerza la creación de un respaldo inmediato (sobrescribe el de hoy)."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    path = _write_daily_backup(force=True)
    return {"ok": True, "archivo": os.path.basename(path) if path else None,
            "backups": _list_backups()}


@app.get("/api/admin/backup-download")
async def admin_backup_download(f: str = Query(...), key: str = Query(""),
                                ql_admin: str = Cookie(default="")):
    """Descarga el contenido de un respaldo (para guardarlo fuera de Railway)."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    fname = os.path.basename(f)  # evita path traversal
    path = os.path.join(BACKUP_DIR, fname)
    if not fname.startswith("quiniela_") or not os.path.exists(path):
        raise HTTPException(404, "Backup no encontrado")
    with open(path, encoding="utf-8") as fh:
        contenido = fh.read()
    return Response(content=contenido, media_type="application/json",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.post("/api/admin/restore-backup")
async def admin_restore_backup(body: dict, key: str = Query(""),
                               ql_admin: str = Cookie(default="")):
    """Restaura jugadores+picks desde un backup. Acepta:
       { archivo: "quiniela_YYYYMMDD.json" }  -> usa uno guardado en /data
       { data: {<dump>} }                     -> usa un JSON pegado a mano
    No borra nada: recrea jugadores faltantes y repone picks (UPSERT)."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    dump = body.get("data")
    if not dump:
        archivo = os.path.basename(body.get("archivo", ""))
        path = os.path.join(BACKUP_DIR, archivo)
        if not archivo.startswith("quiniela_") or not os.path.exists(path):
            raise HTTPException(404, "Backup no encontrado")
        with open(path, encoding="utf-8") as fh:
            dump = json.load(fh)
    if not isinstance(dump, dict) or "jugadores" not in dump:
        raise HTTPException(400, "Dump inválido (falta 'jugadores')")
    res = _db.db_restore_all(dump)
    _cache["players"].clear(); _cache["standings_rows"] = None
    _invalidate_players(); _invalidate_games()
    return {"ok": True, "resultado": res}


@app.post("/api/admin/import-picks")
async def admin_import_picks(body: dict, key: str = Query(""),
                            ql_admin: str = Cookie(default="")):
    """Restaura los picks de jugadores desde un JSON de all-player-points.
    Los jugadores deben EXISTIR ya (no los recrea, solo repone sus apuestas).
    Body: { data: <json de all-player-points>, excluir: ["Angibell"], dry_run: false }
    - Empareja por nombre exacto (case-insensitive).
    - Salta picks vacios (sin marcador ni ganador).
    - dry_run=true solo reporta que haria, sin escribir."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")

    data    = body.get("data") or {}
    excluir = {str(x).strip().lower() for x in (body.get("excluir") or [])}
    dry_run = bool(body.get("dry_run", False))
    jugadores = data.get("jugadores") or []
    if not jugadores:
        raise HTTPException(400, "JSON sin 'jugadores' (¿pegaste el de all-player-points?)")

    # Indice nombre->id de los jugadores ACTUALES en BD
    actuales = {}
    for p in _db.db_get_jugadores():
        nm = (p.get("nombre", "") or "").strip().lower()
        if nm:
            actuales[nm] = p.get("id")

    resumen = []
    for jug in jugadores:
        nombre = (jug.get("jugador") or "").strip()
        low    = nombre.lower()
        if not nombre or low in excluir:
            resumen.append({"jugador": nombre, "estado": "excluido"})
            continue
        pid = actuales.get(low)
        if not pid:
            resumen.append({"jugador": nombre, "estado": "NO existe en BD (saltado)"})
            continue
        n_ok = n_vacios = 0
        for jg in (jug.get("juegos") or []):
            jgo = str(jg.get("jgo", "")).strip()
            if not jgo:
                continue
            marc = (jg.get("pick_marcador") or "").strip()
            g1 = g2 = ""
            if "-" in marc:
                a, b = marc.split("-", 1)
                g1, g2 = a.strip(), b.strip()
            gan = (jg.get("pick_gan") or "").strip()
            eq1 = (jg.get("eq1_pick_guardado") or "").strip()
            eq2 = (jg.get("eq2_pick_guardado") or "").strip()
            if not g1 and not g2 and not gan:
                n_vacios += 1
                continue
            if not dry_run:
                _db.db_save_pick(pid, jgo, g1, g2, gan, eq1, eq2)
            n_ok += 1
        resumen.append({
            "jugador": nombre, "id": pid,
            "picks_restaurados": n_ok, "picks_vacios_saltados": n_vacios,
        })

    if not dry_run:
        _cache["standings_rows"] = None
        _invalidate_games()
    return {"ok": True, "dry_run": dry_run, "resumen": resumen}


@app.get("/api/admin/bracket")
async def admin_bracket(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Devuelve los cruces del bracket (R32 -> FINAL) para revisar/corregir.
    Uso: /api/admin/bracket?key=TU_CLAVE
    Cada cruce trae jgo, ronda, eq1 vs eq2, estado, marcador, ganador y espn_id.
    Para corregir: edita los eq1/eq2 (o ganador) y mándalos a /bracket-fix."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    ORDEN = {"R32": 1, "R16": 2, "QF": 3, "SF": 4, "3ER": 5, "FINAL": 6}
    def _k(h):
        j = str(h.get("jgo", ""))
        return (ORDEN.get(h.get("grupo", ""), 9), int(j) if j.isdigit() else 0)
    cruces = []
    for h in sorted(_db.db_get_horarios(), key=_k):
        cruces.append({
            "jgo":      str(h.get("jgo", "")),
            "ronda":    h.get("grupo", ""),
            "eq1":      h.get("eq1", ""),
            "eq2":      h.get("eq2", ""),
            "estado":   h.get("estado", ""),
            "marcador": f'{h.get("gol1","")}-{h.get("gol2","")}',
            "ganador":  h.get("ganador", ""),
            "espn_id":  h.get("espn_id", ""),
        })
    return {"total": len(cruces), "cruces": cruces}


@app.post("/api/admin/bracket-fix")
async def admin_bracket_fix(body: dict, key: str = Query(""),
                            ql_admin: str = Cookie(default="")):
    """Corrige cruces por jgo. Solo toca los campos que mandes.
    Body: { cruces: [{jgo, eq1?, eq2?, ganador?}, ...] }
    Devuelve qué jgos se actualizaron."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    cruces = body.get("cruces") or []
    if not cruces:
        raise HTTPException(400, "Falta 'cruces' (lista)")
    conn = _db.get_conn()
    cambios = []
    with conn:
        for c in cruces:
            jgo = str(c.get("jgo", "")).strip()
            if not jgo:
                continue
            sets, params = [], []
            for campo in ("eq1", "eq2", "ganador"):
                if campo in c:
                    sets.append(f"{campo}=?")
                    params.append(str(c.get(campo, "")).strip())
            if not sets:
                continue
            params.append(jgo)
            conn.execute(f"UPDATE horarios SET {','.join(sets)} WHERE jgo=?", params)
            cambios.append(jgo)
    conn.close()
    _invalidate_games()
    _cache["standings_rows"] = None
    return {"ok": True, "actualizados": cambios, "total": len(cambios)}


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
async def admin_test_espn(fecha: str = "", liga: str = Query(""), key: str = Query(""),
                          ql_admin: str = Cookie(default="")):
    """Diagnostico: consulta ESPN y devuelve los partidos encontrados para una fecha.
    Uso directo: /api/admin/test-espn?fecha=20260602&liga=fifa.friendly&key=TU_CLAVE
    'liga' (opcional) sobreescribe ESPN_LEAGUE solo para esta consulta (no guarda nada)."""
    _admin_pass = state.get("cfg", {}).get("ADMIN_PASS", "quiniela2026")
    if not _admin_check(ql_admin) and key != _admin_pass:
        raise HTTPException(403, "No autorizado")
    from datetime import date as _date
    cfg    = state.get("cfg", {})
    league = liga.strip() or cfg.get("ESPN_LEAGUE", "fifa.world")
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
                    "espn_id": ev.get("id", ""),   # <-- ID que va en el campo ESPN ID
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


@app.post("/api/admin/test-notif-juego")
async def admin_test_notif_juego(key: str = Query(""), ql_admin: str = Cookie(default="")):
    """Envía AHORA una notificación con el estado REAL del partido en vivo
    (marcador y minuto actuales) por push + Telegram + WhatsApp. Si no hay
    ninguno en vivo, usa el primer partido no finalizado. ?key=CLAVE_ADMIN."""
    cfg = state.get("cfg", {})
    if not (_admin_check(ql_admin) or (key and key == cfg.get("ADMIN_PASS", "quiniela2026"))):
        raise HTTPException(403, "No autorizado. Usa ?key=CLAVE_ADMIN.")
    games, _ = _get_games_cache()
    _vivos = {"EN VIVO", "MEDIO TIEMPO", "PRORROGA", "PENALES"}
    g = (next((x for x in games if x.get("estado", "") in _vivos), None)
         or next((x for x in games if x.get("estado", "") not in ("FINAL",) and x.get("espn_id")), None)
         or (games[0] if games else None))
    if not g:
        return {"ok": False, "msg": "No hay partidos cargados."}

    eq1, eq2 = g.get("eq1", ""), g.get("eq2", "")
    b1, b2   = _eq(eq1), _eq(eq2)
    g1, g2   = g.get("gol1", "") or "0", g.get("gol2", "") or "0"
    estado   = g.get("estado", "PROG")
    minuto   = _live_clocks.get(g.get("espn_id", ""), "")
    if estado in _vivos:
        min_txt = f" ({minuto}')" if minuto and minuto not in ("MT",) else (" (MT)" if estado == "MEDIO TIEMPO" else "")
        encab   = "⚽ MARCADOR EN VIVO"
        cuerpo  = f"{b1} {g1} – {g2} {b2}{min_txt}"
    elif estado == "FINAL":
        encab  = "🏁 PARTIDO FINALIZADO"
        cuerpo = f"{b1} {g1} – {g2} {b2}"
    else:
        encab  = "🟡 PRÓXIMO PARTIDO"
        cuerpo = f"{b1} vs {b2}"

    titulo = f"🧪 PRUEBA · {encab}"
    results = {}
    # Push
    if _push_subs:
        try:
            _send_push_all(titulo, cuerpo, {"tipo": "test", "url": "/"})
            results["push"] = f"{len(_push_subs)} enviado(s)"
        except Exception as e:
            results["push"] = f"Error: {e}"
    else:
        results["push"] = "Sin suscriptores"
    # Telegram
    try:
        _tg_send(f"🧪 <b>{encab}</b>\n{cuerpo}")
        results["telegram"] = "Enviado"
    except Exception as e:
        results["telegram"] = f"Error: {e}"
    # WhatsApp
    try:
        _wa("POST", "/send", json={"message": f"🧪 {encab}\n{cuerpo}"})
        results["whatsapp"] = "Enviado"
    except Exception as e:
        results["whatsapp"] = f"Error: {e}"

    return {"ok": True, "juego": {"jgo": g.get("jgo"), "estado": estado, "texto": cuerpo}, "results": results}


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
        for d in rows:  # rows son dicts (SQLite via _jugador_db_to_cache)
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


@app.get("/api/admin/wa-groups")
async def wa_groups(ql_admin: str = Cookie(default="")):
    """Lista los grupos de WhatsApp donde el número ya es miembro (para elegir uno)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    return _wa("GET", "/list-groups")


@app.post("/api/admin/wa-select-group")
async def wa_select_group(body: dict = None, ql_admin: str = Cookie(default="")):
    """Fija como destino de notificaciones un grupo EXISTENTE (por su id)."""
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")
    gid = (body or {}).get("groupId", "")
    if not gid:
        raise HTTPException(400, "Falta el campo 'groupId'")
    return _wa("POST", "/select-group", json={"groupId": gid})


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
    Util en modo prueba: {jgo, eq1?, eq2?, espn_id?, estado?, gol1?, gol2?, ganador?}
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
        if "espn_id" in body:
            conn.execute("UPDATE horarios SET espn_id=? WHERE jgo=?", (str(body["espn_id"]).strip(), jgo))
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
    Body: { ronda_desde: "R32"|"R16"|"QF"|"SF"|"FINAL", mantener_picks: bool }
    Si mantener_picks=True, conserva las apuestas de los jugadores (solo borra los
    resultados reales). Por defecto False (comportamiento clasico: borra picks).
    """
    if not _admin_check(ql_admin): raise HTTPException(403, "No autorizado")

    # Red de seguridad: si el frontend no manda el flag, CONSERVAR picks por
    # defecto (antes era False y borraba apuestas de TODOS los jugadores).
    mantener_picks = bool(body.get("mantener_picks", True))
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

    # Borrar picks de las rondas limpiadas (salvo que se pida mantenerlos)
    jgos_limpiados = []
    for ronda in rondas_a_limpiar:
        for h in ronda_games.get(ronda, []):
            jgos_limpiados.append(str(h["jgo"]))

    if jgos_limpiados and not mantener_picks:
        conn2 = _db.get_conn()
        with conn2:
            placeholders = ",".join("?" * len(jgos_limpiados))
            conn2.execute(f"DELETE FROM picks WHERE jgo IN ({placeholders})", jgos_limpiados)
        conn2.close()

    _invalidate_games()
    _cache["standings_rows"] = None
    return {
        "ok":  True,
        "msg": f"Reseteados {cleared} partido(s) desde {ronda_desde} "
               f"({', '.join(rondas_a_limpiar)}). "
               + ("Apuestas CONSERVADAS." if mantener_picks else "Picks eliminados.")
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
