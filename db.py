"""
db.py -- SQLite como base de datos primaria para Quiniela WFC 2026
Reemplaza Google Sheets en todas las operaciones de lectura/escritura.
Sheets se mantiene como espejo de backup (sync cada 60s).
"""

import sqlite3, os, threading, time
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", "/data/quiniela.db")
_db_lock = threading.Lock()

def get_conn():
    """Conexion SQLite con WAL mode para concurrencia de lectura."""
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    """Crea todas las tablas si no existen."""
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS config (
            key   TEXT PRIMARY KEY,
            value TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS jugadores (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            num        INTEGER DEFAULT 0,
            email      TEXT    DEFAULT '',
            nombre     TEXT    NOT NULL DEFAULT '',
            whatsapp   TEXT    DEFAULT '',
            fecha_reg  TEXT    DEFAULT '',
            tab_nombre TEXT    DEFAULT '',
            pagado     INTEGER DEFAULT 0,
            reglas_ok  INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_jug_whatsapp ON jugadores(whatsapp);
        CREATE INDEX IF NOT EXISTS idx_jug_email    ON jugadores(email);
        CREATE TABLE IF NOT EXISTS horarios (
            jgo     TEXT PRIMARY KEY,
            grupo   TEXT DEFAULT '',
            fecha   TEXT DEFAULT '',
            hora    TEXT DEFAULT '',
            eq1     TEXT DEFAULT '',
            eq2     TEXT DEFAULT '',
            espn_id TEXT DEFAULT '',
            estado  TEXT DEFAULT 'PROG',
            gol1    TEXT DEFAULT '',
            gol2    TEXT DEFAULT '',
            ganador TEXT DEFAULT '',
            ult_act TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS picks (
            jugador_id INTEGER NOT NULL,
            jgo        TEXT    NOT NULL,
            g1_pick    TEXT DEFAULT '',
            g2_pick    TEXT DEFAULT '',
            gan_pick   TEXT DEFAULT '',
            eq1_pick   TEXT DEFAULT '',
            eq2_pick   TEXT DEFAULT '',
            PRIMARY KEY (jugador_id, jgo)
        );
        CREATE TABLE IF NOT EXISTS ligas (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre  TEXT DEFAULT '',
            codigo  TEXT DEFAULT '',
            espn_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS chat (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT '',
            ident     TEXT DEFAULT '',
            nombre    TEXT DEFAULT '',
            mensaje   TEXT DEFAULT ''
        );
    """)
    # Migracion incremental: agregar columnas nuevas si no existen (para DBs ya desplegadas)
    for col_sql in [
        "ALTER TABLE picks ADD COLUMN eq1_pick TEXT DEFAULT ''",
        "ALTER TABLE picks ADD COLUMN eq2_pick TEXT DEFAULT ''",
        "ALTER TABLE jugadores ADD COLUMN reglas_ok INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(col_sql)
            conn.commit()
        except Exception:
            pass  # columna ya existe
    conn.close()

# == Config ====================================================================

def db_get_config() -> dict:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT key, value FROM config").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()

def db_save_config(updates: dict):
    conn = get_conn()
    with _db_lock:
        with conn:
            for k, v in updates.items():
                conn.execute(
                    "INSERT INTO config(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(k), str(v))
                )
    conn.close()

# == Jugadores =================================================================

def db_get_jugadores() -> list:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, num, email, nombre, whatsapp, fecha_reg, tab_nombre, pagado, reglas_ok "
            "FROM jugadores ORDER BY num, id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def db_find_player(email: str = "", phone: str = ""):
    """Busca jugador por telefono o email. Retorna dict o None."""
    conn = get_conn()
    try:
        if phone:
            phone = phone.strip()
            r = conn.execute(
                "SELECT * FROM jugadores WHERE whatsapp=? LIMIT 1", (phone,)
            ).fetchone()
            if r:
                return dict(r)
            # Intentar sin prefijo 1
            alt = phone.lstrip("1") if phone.startswith("1") and len(phone) > 10 else ("1" + phone)
            r = conn.execute(
                "SELECT * FROM jugadores WHERE whatsapp=? LIMIT 1", (alt,)
            ).fetchone()
            if r:
                return dict(r)
        if email:
            r = conn.execute(
                "SELECT * FROM jugadores WHERE lower(trim(email))=lower(trim(?)) LIMIT 1",
                (email,)
            ).fetchone()
            if r:
                return dict(r)
        return None
    finally:
        conn.close()

def db_register_player(email, nombre, whatsapp, fecha_reg, tab_nombre) -> int:
    """Inserta nuevo jugador y retorna su id."""
    conn = get_conn()
    with _db_lock:
        with conn:
            cur = conn.execute("SELECT COALESCE(MAX(num),0)+1 FROM jugadores")
            num = cur.fetchone()[0]
            conn.execute(
                "INSERT INTO jugadores(num,email,nombre,whatsapp,fecha_reg,tab_nombre,pagado) "
                "VALUES(?,?,?,?,?,?,0)",
                (num, email or "", nombre, whatsapp or "", fecha_reg, tab_nombre)
            )
            jid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return jid

def db_mark_paid(phone: str, paid: bool = True) -> bool:
    """Marca o desmarca jugador como pagado. Retorna True si encontro el jugador."""
    conn = get_conn()
    try:
        with _db_lock:
            with conn:
                r = conn.execute(
                    "UPDATE jugadores SET pagado=? WHERE whatsapp=?",
                    (1 if paid else 0, phone.strip())
                )
        return r.rowcount > 0
    finally:
        conn.close()

def db_set_reglas_ok(jugador_id: int, ok: bool = True) -> bool:
    """Marca que el jugador ya leyo/acepto el reglamento. Retorna True si existia."""
    conn = get_conn()
    try:
        with _db_lock:
            with conn:
                r = conn.execute(
                    "UPDATE jugadores SET reglas_ok=? WHERE id=?",
                    (1 if ok else 0, int(jugador_id))
                )
        return r.rowcount > 0
    finally:
        conn.close()

def db_delete_player(jugador_id: int):
    """Elimina jugador y todos sus picks."""
    conn = get_conn()
    with _db_lock:
        with conn:
            conn.execute("DELETE FROM picks WHERE jugador_id=?", (jugador_id,))
            conn.execute("DELETE FROM jugadores WHERE id=?", (jugador_id,))
    conn.close()


def db_dump_all() -> dict:
    """Dump completo para respaldo: jugadores, picks, horarios y config."""
    conn = get_conn()
    try:
        def rows(sql):
            return [dict(r) for r in conn.execute(sql).fetchall()]
        return {
            "jugadores": rows("SELECT * FROM jugadores"),
            "picks":     rows("SELECT * FROM picks"),
            "horarios":  rows("SELECT * FROM horarios"),
            "config":    rows("SELECT * FROM config"),
        }
    finally:
        conn.close()


def db_restore_all(dump: dict) -> dict:
    """Restaura jugadores y picks desde un dump de db_dump_all().
    - Recrea jugadores que no existan (match por whatsapp/email); no duplica.
    - Repone TODOS los picks (UPSERT). No borra nada que ya exista.
    Devuelve un resumen con conteos."""
    res = {"jugadores_creados": 0, "jugadores_existentes": 0, "picks_restaurados": 0}
    conn = get_conn()
    with _db_lock:
        with conn:
            id_map = {}  # id_viejo -> id_actual
            for j in dump.get("jugadores", []):
                old_id = j.get("id")
                wa = (j.get("whatsapp") or "").strip()
                em = (j.get("email") or "").strip()
                row = conn.execute(
                    "SELECT id FROM jugadores WHERE (whatsapp=? AND whatsapp!='') "
                    "OR (lower(email)=lower(?) AND email!='') LIMIT 1",
                    (wa or "_x_", em or "_x_")
                ).fetchone()
                if row:
                    id_map[old_id] = row[0]
                    res["jugadores_existentes"] += 1
                else:
                    conn.execute(
                        "INSERT INTO jugadores(num,email,nombre,whatsapp,fecha_reg,"
                        "tab_nombre,pagado,reglas_ok) VALUES(?,?,?,?,?,?,?,?)",
                        (j.get("num", 0), em, j.get("nombre", ""), wa,
                         j.get("fecha_reg", ""), j.get("tab_nombre", ""),
                         j.get("pagado", 0), j.get("reglas_ok", 0))
                    )
                    id_map[old_id] = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    res["jugadores_creados"] += 1
            for p in dump.get("picks", []):
                nid = id_map.get(p.get("jugador_id"))
                if not nid:
                    continue
                conn.execute("""
                    INSERT INTO picks(jugador_id,jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(jugador_id,jgo) DO UPDATE SET
                        g1_pick=excluded.g1_pick, g2_pick=excluded.g2_pick,
                        gan_pick=excluded.gan_pick, eq1_pick=excluded.eq1_pick,
                        eq2_pick=excluded.eq2_pick
                """, (nid, str(p.get("jgo", "")), p.get("g1_pick", ""),
                      p.get("g2_pick", ""), p.get("gan_pick", ""),
                      p.get("eq1_pick", ""), p.get("eq2_pick", "")))
                res["picks_restaurados"] += 1
    conn.close()
    return res

# == Horarios ==================================================================

def db_get_horarios() -> list:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT jgo,grupo,fecha,hora,eq1,eq2,espn_id,estado,gol1,gol2,ganador,ult_act "
            "FROM horarios ORDER BY fecha,hora,CAST(jgo AS INTEGER)"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def db_upsert_horario(game: dict):
    """Insert o update de un partido (usado en admin_setup y updater)."""
    conn = get_conn()
    with conn:
        conn.execute("""
            INSERT INTO horarios(jgo,grupo,fecha,hora,eq1,eq2,espn_id,estado,gol1,gol2,ganador,ult_act)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(jgo) DO UPDATE SET
                grupo=excluded.grupo, fecha=excluded.fecha, hora=excluded.hora,
                eq1=excluded.eq1, eq2=excluded.eq2, espn_id=excluded.espn_id,
                estado=excluded.estado, gol1=excluded.gol1, gol2=excluded.gol2,
                ganador=excluded.ganador, ult_act=excluded.ult_act
        """, (
            str(game.get("jgo","")), game.get("grupo",""), game.get("fecha",""),
            game.get("hora",""), game.get("eq1",""), game.get("eq2",""),
            game.get("espn_id",""), game.get("estado","PROG"),
            str(game.get("gol1","")), str(game.get("gol2","")),
            game.get("ganador",""), game.get("ult_actualizacion","")
        ))
    conn.close()

def db_update_game_result(jgo: str, estado: str, gol1: str, gol2: str, ganador: str, ult_act: str = ""):
    """Actualiza solo el resultado de un partido (llamado por el updater ESPN)."""
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE horarios SET estado=?,gol1=?,gol2=?,ganador=?,ult_act=? WHERE jgo=?",
            (estado, str(gol1), str(gol2), ganador, ult_act, str(jgo))
        )
    conn.close()

# == Picks =====================================================================

def db_get_picks(jugador_id: int) -> dict:
    """Retorna {jgo_str: {g1, g2, gan, eq1, eq2}} para un jugador."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT jgo, g1_pick, g2_pick, gan_pick, eq1_pick, eq2_pick FROM picks WHERE jugador_id=?",
            (jugador_id,)
        ).fetchall()
        return {r["jgo"]: {"g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"],
                            "eq1": r["eq1_pick"] or "", "eq2": r["eq2_pick"] or ""} for r in rows}
    finally:
        conn.close()

def db_save_pick(jugador_id: int, jgo: str, g1: str, g2: str, gan: str, eq1: str = "", eq2: str = ""):
    """Upsert de un pick individual."""
    conn = get_conn()
    with conn:
        conn.execute("""
            INSERT INTO picks(jugador_id,jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(jugador_id,jgo) DO UPDATE SET
                g1_pick=excluded.g1_pick,
                g2_pick=excluded.g2_pick,
                gan_pick=excluded.gan_pick,
                eq1_pick=excluded.eq1_pick,
                eq2_pick=excluded.eq2_pick
        """, (jugador_id, str(jgo), g1, g2, gan, eq1 or "", eq2 or ""))
    conn.close()

def db_init_picks_for_player(jugador_id: int):
    """Crea filas de picks vacias para todos los juegos existentes."""
    conn = get_conn()
    with conn:
        horarios = conn.execute("SELECT jgo FROM horarios").fetchall()
        conn.executemany(
            "INSERT OR IGNORE INTO picks(jugador_id,jgo,g1_pick,g2_pick,gan_pick) VALUES(?,?,?,?,?)",
            [(jugador_id, h["jgo"], "", "", "") for h in horarios]
        )
    conn.close()

def db_get_pick_for_game(jugador_id: int, jgo: str) -> dict:
    """Retorna {g1, g2, gan} para un jugador/juego especifico."""
    conn = get_conn()
    try:
        r = conn.execute(
            "SELECT g1_pick, g2_pick, gan_pick FROM picks WHERE jugador_id=? AND jgo=?",
            (jugador_id, str(jgo))
        ).fetchone()
        if r:
            return {"g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"]}
        return {"g1": "", "g2": "", "gan": ""}
    finally:
        conn.close()

def db_get_all_picks_for_game(jgo: str) -> list:
    """Todos los picks de todos los jugadores para un partido."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT j.nombre, p.g1_pick, p.g2_pick, p.gan_pick, p.eq1_pick, p.eq2_pick
            FROM picks p JOIN jugadores j ON j.id = p.jugador_id
            WHERE p.jgo=?
        """, (str(jgo),)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def db_get_all_picks_grouped() -> dict:
    """Todos los picks de todos los jugadores agrupados por jugador:
    {jugador_id: {"nombre": str, "picks": {jgo_str: {g1,g2,gan,eq1,eq2}}}}.
    Usado para calcular puntos con inferencia de bracket en vistas por-partido."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT p.jugador_id, j.nombre, p.jgo,
                   p.g1_pick, p.g2_pick, p.gan_pick, p.eq1_pick, p.eq2_pick
            FROM picks p JOIN jugadores j ON j.id = p.jugador_id
        """).fetchall()
        out: dict = {}
        for r in rows:
            pid = r["jugador_id"]
            if pid not in out:
                out[pid] = {"nombre": r["nombre"], "picks": {}}
            out[pid]["picks"][str(r["jgo"])] = {
                "g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"],
                "eq1": r["eq1_pick"] or "", "eq2": r["eq2_pick"] or "",
            }
        return out
    finally:
        conn.close()

def db_get_picks_without_pick(jgo: str) -> list:
    """Jugadores que NO tienen pick para un partido (para recordatorios)."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT j.nombre, j.whatsapp
            FROM jugadores j
            LEFT JOIN picks p ON p.jugador_id=j.id AND p.jgo=?
            WHERE (p.g1_pick IS NULL OR p.g1_pick='')
               OR (p.g2_pick IS NULL OR p.g2_pick='')
        """, (str(jgo),)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

# == Scoring ===================================================================

# == Inferencia de bracket (equipos que predijo el jugador en eliminatorias) ===
# En eliminatorias, HORARIOS trae placeholders ("Round of 32 1 Winner") con
# emparejamiento secuencial que NO refleja el bracket real del Mundial 2026. Los
# equipos que el jugador predijo para cada cruce se infieren con WC2026_MAP a
# partir de los ganadores que eligio en rondas anteriores. (Misma logica que el
# frontend y el PDF; fuente unica aqui para que el calculo de puntos sea correcto.)
import re as _re_bracket
_RONDA_MAP_EN = {32: "R32", 16: "R16", 8: "QF", 4: "SF"}
_FEED_RONDA   = {"R16": "R32", "QF": "R16", "SF": "QF", "3ER": "SF", "FINAL": "SF"}
_WC2026_MAP   = {
    "R16":   [[0, 3], [2, 5], [1, 4], [6, 7], [11, 10], [9, 8], [14, 13], [12, 15]],
    "QF":    [[0, 1], [4, 5], [2, 3], [7, 6]],
    "SF":    [[0, 1], [2, 3]],
    "FINAL": [[0, 1]],
}

def _parse_bracket_ref(raw):
    if not raw:
        return None
    m = _re_bracket.match(r"Round of (\d+) (\d+) Winner", raw, _re_bracket.I)
    if m:
        ronda = _RONDA_MAP_EN.get(int(m.group(1)))
        if ronda:
            return {"ronda": ronda, "nth": int(m.group(2)), "type": "winner", "jgo": None}
    typ = "loser" if _re_bracket.match(r"^Perdedor", raw, _re_bracket.I) else "winner"
    if _re_bracket.match(r"^(Ganador|Perdedor)", raw, _re_bracket.I):
        for pat, ronda in ((r"Dieciseisavos", "R32"), (r"Octavos", "R16"),
                           (r"Cuartos", "QF"), (r"Semifinal", "SF")):
            if _re_bracket.search(pat, raw, _re_bracket.I):
                mp = _re_bracket.search(r"\((\d+)\)", raw)
                me = _re_bracket.search(r"\s(\d+)$", raw)
                nth = int(mp.group(1)) if mp else (int(me.group(1)) if me else None)
                if nth is not None:
                    return {"ronda": ronda, "nth": nth, "type": typ, "jgo": None}
    mg = _re_bracket.search(r"(?:Winner\s+)?Game\s+(\d+)(?:\s+Winner)?", raw, _re_bracket.I)
    if mg:
        return {"ronda": None, "nth": None, "type": "winner", "jgo": int(mg.group(1))}
    return None

def _is_placeholder(name):
    return _parse_bracket_ref(name) is not None

def _resolve_team_name(raw, by_jgo, by_ronda, picks, depth=0):
    if not raw or depth > 8:
        return raw
    ref = _parse_bracket_ref(raw)
    if not ref:
        return raw
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
        return raw
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
        return None
    this_round = by_ronda.get(game.get("ronda"), [])
    my_idx = next((i for i, g in enumerate(this_round)
                   if str(g["jgo"]) == str(game["jgo"])), -1)
    if my_idx < 0:
        return None
    feed_games = by_ronda.get(feed_ronda, [])

    if game.get("ronda") == "3ER":
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
        feed_idx = my_idx * 2 if slot == "eq1" else my_idx * 2 + 1
    feed_game = feed_games[feed_idx] if 0 <= feed_idx < len(feed_games) else None
    if not feed_game:
        return None
    pk = picks.get(str(feed_game["jgo"])) or {}
    if not pk.get("gan"):
        return f"Gan. JGO {feed_game['jgo']}"
    resolved = _resolve_team_name(pk["gan"], by_jgo, by_ronda, picks) or pk["gan"]
    return f"Gan. JGO {feed_game['jgo']}" if _is_placeholder(resolved) else resolved

def _disp_team(game, slot, by_jgo, by_ronda, picks):
    """Equipo que el jugador predijo para un slot (eq1/eq2) de un partido."""
    raw = game.get(slot) or ""
    if game.get("ronda") not in _FEED_RONDA:
        return raw  # R32 / grupos: equipo real directo
    inf = _infer_bracket_slot(game, slot, by_jgo, by_ronda, picks)
    if inf and not inf.startswith("Gan. ") and not inf.startswith("Perdedor "):
        return inf
    stored = (picks.get(str(game["jgo"])) or {}).get(slot) or ""
    if stored and not _is_placeholder(stored):
        return stored
    resolved = _resolve_team_name(raw, by_jgo, by_ronda, picks)
    return resolved or inf or raw

def build_bracket_index(games):
    """games: lista de horarios. Normaliza ronda<-grupo y arma (by_jgo, by_ronda)."""
    by_jgo, by_ronda = {}, {}
    for g in games:
        if not g.get("ronda"):
            g["ronda"] = g.get("grupo", "") or ""
        by_jgo[str(g["jgo"])] = g
        by_ronda.setdefault(g["ronda"], []).append(g)
    for k in by_ronda:
        by_ronda[k].sort(key=lambda x: int(x["jgo"]) if str(x["jgo"]).isdigit() else 0)
    return by_jgo, by_ronda

def calc_pts_inferred(game, pick, by_jgo, by_ronda, player_picks,
                      v_logro=1, v_gan=2, v_g1=1, v_g2=1, v_campeon=0):
    """Calcula puntos resolviendo los equipos predichos via inferencia de bracket
    (no usa los eq_pick guardados, que son poco confiables en eliminatorias)."""
    eq1_pick = _disp_team(game, "eq1", by_jgo, by_ronda, player_picks)
    eq2_pick = _disp_team(game, "eq2", by_jgo, by_ronda, player_picks)
    return _calc_pts(
        pick.get("g1", ""), pick.get("g2", ""), pick.get("gan", ""),
        game.get("gol1", ""), game.get("gol2", ""), game.get("ganador", ""),
        game.get("estado", ""),
        v_logro, v_gan, v_g1, v_g2, v_campeon,
        game.get("ronda", "") or game.get("grupo", ""),
        eq1_pick=eq1_pick, eq2_pick=eq2_pick,
        eq1_real=game.get("eq1", ""), eq2_real=game.get("eq2", ""),
    )


def _calc_pts(g1_pick, g2_pick, gan_pick, gol1, gol2, ganador, estado,
              pts_logro_val=1, pts_gan_val=2, pts_g1_val=1, pts_g2_val=1,
              pts_campeon_val=0, ronda="",
              eq1_pick="", eq2_pick="", eq1_real="", eq2_real=""):
    """Calcula puntos para un pick vs resultado real.
    Retorna (pts_logro, pts_gan, pts_g1, pts_g2, total).

    Reglas (F2):
    - pts_logro:  acertar si el partido a los 90' quedo EMPATE o NO-EMPATE
                  (no importa quien gane, solo empate vs no-empate).
    - pts_gan:    acertar el equipo que avanza (ganador, por nombre).
    - pts_g1/g2:  acertar el marcador exacto de CADA equipo real, por NOMBRE:
                  solo cuenta si el jugador predijo a ese equipo con ese marcador.
                  Si el equipo predicho no esta en el partido, su gol no cuenta
                  aunque el numero coincida por posicion.
    - pts_campeon: bonus si acierta el campeon en ronda FINAL.

    Regla teamAlive (R16+): si conocemos los equipos reales (eq1_real/eq2_real),
    el jugador debe haber predicho al menos 1 de ellos (via eq1_pick, eq2_pick
    o gan_pick). Si ningun equipo predicho esta en el partido real -> 0 pts.
    """
    if not estado or estado == "PROG":
        return 0, 0, 0, 0, 0

    _eq1r = (eq1_real or "").strip()
    _eq2r = (eq2_real or "").strip()
    e1p   = (eq1_pick or "").strip()
    e2p   = (eq2_pick or "").strip()
    gan_p = (gan_pick or "").strip()
    gan_real = (ganador or "").strip()

    # teamAlive: al menos 1 equipo predicho debe estar jugando el partido real
    if _eq1r and _eq2r:
        real_teams = {_eq1r, _eq2r}
        pred_teams = {e1p, e2p, gan_p}
        pred_teams = {t for t in pred_teams if t and not t.startswith("Gan. ")}
        if pred_teams and not (pred_teams & real_teams):
            return 0, 0, 0, 0, 0

    def _num(x):
        try: return int(str(x).strip())
        except: return None

    n_g1r, n_g2r = _num(gol1), _num(gol2)
    n_g1p, n_g2p = _num(g1_pick), _num(g2_pick)

    # ── Resultado 90': empate vs no-empate (no importa quien gane) ──
    pl = 0
    if None not in (n_g1r, n_g2r, n_g1p, n_g2p):
        if (n_g1r == n_g2r) == (n_g1p == n_g2p):
            pl = pts_logro_val

    # ── Ganador (equipo que avanza, por nombre) ──
    pg = pts_gan_val if (gan_p and gan_real and gan_p == gan_real) else 0

    # ── Goles por NOMBRE de equipo (no por posicion) ──
    def _gol_predicho_para(equipo):
        """Gol que el jugador asigno a 'equipo' segun el slot donde lo puso."""
        if e1p and e1p == equipo: return n_g1p
        if e2p and e2p == equipo: return n_g2p
        return None

    pg1 = pg2 = 0
    if _eq1r and _eq2r and (e1p or e2p):
        gp_eq1 = _gol_predicho_para(_eq1r)
        gp_eq2 = _gol_predicho_para(_eq2r)
        if gp_eq1 is not None and n_g1r is not None and gp_eq1 == n_g1r:
            pg1 = pts_g1_val
        if gp_eq2 is not None and n_g2r is not None and gp_eq2 == n_g2r:
            pg2 = pts_g2_val
    else:
        # Sin nombres de equipo en el pick (datos antiguos): comparar por posicion
        if n_g1p is not None and n_g1r is not None and n_g1p == n_g1r:
            pg1 = pts_g1_val
        if n_g2p is not None and n_g2r is not None and n_g2p == n_g2r:
            pg2 = pts_g2_val

    # ── Bono campeon ──
    pc = pts_campeon_val if (pts_campeon_val and ronda.upper() == "FINAL"
                             and gan_p and gan_real and gan_p == gan_real) else 0
    return pl, pg, pg1, pg2, pl + pg + pg1 + pg2 + pc

def db_compute_standings(cfg: dict = None) -> list:
    """Calcula posiciones completas desde picks + horarios. Retorna lista ordenada.
    Los equipos predichos en eliminatorias se infieren del bracket (calc_pts_inferred)."""
    cfg = cfg or {}
    vL = int(cfg.get("PTS_LOGRO",   1) or 1)
    vG = int(cfg.get("PTS_GAN",     2) or 2)
    v1 = int(cfg.get("PTS_GOL1",    1) or 1)
    v2 = int(cfg.get("PTS_GOL2",    1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)

    conn = get_conn()
    try:
        # TODOS los partidos (la inferencia de bracket necesita los juegos feeder)
        games_rows = conn.execute(
            "SELECT jgo,grupo,eq1,eq2,gol1,gol2,ganador,estado FROM horarios"
        ).fetchall()
        games_list = [dict(r) for r in games_rows]
        by_jgo, by_ronda = build_bracket_index(games_list)

        jugadores = conn.execute("SELECT id, nombre FROM jugadores ORDER BY num, id").fetchall()

        standings = []
        for j in jugadores:
            pk_rows = conn.execute(
                "SELECT jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick FROM picks WHERE jugador_id=?",
                (j["id"],)
            ).fetchall()
            player_picks = {
                str(r["jgo"]): {"g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"],
                                "eq1": r["eq1_pick"] or "", "eq2": r["eq2_pick"] or ""}
                for r in pk_rows
            }

            pts_total = gan_acert = g1_acert = g2_acert = jugados = 0
            for jgo_str, pk in player_picks.items():
                game   = by_jgo.get(jgo_str)
                if not game:
                    continue
                estado = game.get("estado", "")
                if not estado or estado == "PROG":
                    continue
                jugados += 1
                pl, pg, pg1, pg2, ptot = calc_pts_inferred(
                    game, pk, by_jgo, by_ronda, player_picks, vL, vG, v1, v2, vC)
                pts_total += ptot
                if pg  > 0: gan_acert += 1
                if pg1 > 0: g1_acert  += 1
                if pg2 > 0: g2_acert  += 1

            standings.append({
                "jugador_id": j["id"],
                "nombre":     j["nombre"],
                "pts":        pts_total,
                "jugados":    jugados,
                "gan":        gan_acert,
                "g1":         g1_acert,
                "g2":         g2_acert,
            })

        standings.sort(key=lambda x: (-x["pts"], -x["gan"], -(x["g1"]+x["g2"]), x["nombre"]))
        return standings
    finally:
        conn.close()


def _equipos_vivos(games) -> set:
    """Equipos reales que NO han sido eliminados (no perdieron ningun FINAL)."""
    eliminados, todos = set(), set()
    for g in games:
        e1 = (g.get("eq1") or "").strip()
        e2 = (g.get("eq2") or "").strip()
        for e in (e1, e2):
            if e and not _is_placeholder(e):
                todos.add(e)
        if (g.get("estado") or "") == "FINAL":
            gan = (g.get("ganador") or "").strip()
            if gan:
                for e in (e1, e2):
                    if e and e != gan and not _is_placeholder(e):
                        eliminados.add(e)
    return todos - eliminados


def db_compute_probabilities(cfg: dict = None) -> list:
    """Por jugador: pts actuales, MAX REALISTA (solo equipos aun con vida) y
    cuantos de sus equipos predichos siguen vivos.

    El max realista por cada partido PENDIENTE suma:
      - el punto de no-empate SIEMPRE (no depende del equipo),
      - ganador + goles SOLO si el equipo que predijo ganador sigue vivo,
      - bono campeon SOLO si su campeon predicho sigue vivo (final pendiente).
    """
    cfg = cfg or {}
    vL = int(cfg.get("PTS_LOGRO",   1) or 1)
    vG = int(cfg.get("PTS_GAN",     2) or 2)
    v1 = int(cfg.get("PTS_GOL1",    1) or 1)
    v2 = int(cfg.get("PTS_GOL2",    1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)

    conn = get_conn()
    try:
        games_list = [dict(r) for r in conn.execute(
            "SELECT jgo,grupo,eq1,eq2,gol1,gol2,ganador,estado FROM horarios").fetchall()]
        by_jgo, by_ronda = build_bracket_index(games_list)
        vivos = _equipos_vivos(games_list)

        pendientes = [g for g in games_list
                      if not g.get("estado") or g["estado"] == "PROG"]
        final_pend = [g for g in pendientes
                      if (g.get("ronda") or g.get("grupo") or "").upper() == "FINAL"]

        jugadores = conn.execute(
            "SELECT id, nombre FROM jugadores ORDER BY num, id").fetchall()

        def _gan_real(pk, picks):
            """Nombre real del ganador que predijo el jugador (resuelto)."""
            raw = (pk.get("gan") or "").strip()
            return _resolve_team_name(raw, by_jgo, by_ronda, picks) or raw

        out = []
        for j in jugadores:
            pk_rows = conn.execute(
                "SELECT jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick "
                "FROM picks WHERE jugador_id=?", (j["id"],)).fetchall()
            _pp = {str(r["jgo"]): {"g1": r["g1_pick"], "g2": r["g2_pick"],
                                   "gan": r["gan_pick"], "eq1": r["eq1_pick"] or "",
                                   "eq2": r["eq2_pick"] or ""} for r in pk_rows}

            # Puntos actuales (partidos ya jugados)
            pts = 0
            for jgo_str, pk in _pp.items():
                game = by_jgo.get(jgo_str)
                if not game:
                    continue
                est = game.get("estado", "")
                if not est or est == "PROG":
                    continue
                _, _, _, _, ptot = calc_pts_inferred(
                    game, pk, by_jgo, by_ronda, _pp, vL, vG, v1, v2, vC)
                pts += ptot

            # Equipos que predijo (ganador) y siguen vivos
            equipos_vivos_jug = set()
            for pk in _pp.values():
                gr = _gan_real(pk, _pp)
                if gr and not _is_placeholder(gr) and gr in vivos:
                    equipos_vivos_jug.add(gr)

            # Max realista
            max_add = 0
            for g in pendientes:
                pk = _pp.get(str(g["jgo"])) or {}
                max_add += vL  # no-empate siempre alcanzable
                gr = _gan_real(pk, _pp)
                if gr and not _is_placeholder(gr) and gr in vivos:
                    max_add += vG + v1 + v2
            if final_pend and vC:
                pkf = _pp.get(str(final_pend[0]["jgo"])) or {}
                camp = _gan_real(pkf, _pp)
                if camp and not _is_placeholder(camp) and camp in vivos:
                    max_add += vC

            out.append({
                "jugador_id":     j["id"],
                "nombre":         j["nombre"],
                "pts":            pts,
                "max_realista":   pts + max_add,
                "equipos_vivos":  len(equipos_vivos_jug),
                "equipos_lista":  sorted(equipos_vivos_jug),
            })

        out.sort(key=lambda x: (-x["pts"], -x["max_realista"], x["nombre"]))
        return out
    finally:
        conn.close()

# == Ligas =====================================================================

def db_get_ligas() -> list:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT nombre,codigo,espn_id FROM ligas ORDER BY id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def db_set_ligas(ligas: list):
    """Reemplaza toda la tabla de ligas."""
    conn = get_conn()
    with conn:
        conn.execute("DELETE FROM ligas")
        conn.executemany(
            "INSERT INTO ligas(nombre,codigo,espn_id) VALUES(?,?,?)",
            [(l.get("nombre",""), l.get("codigo",""), l.get("espn_id","")) for l in ligas]
        )
    conn.close()

# == Chat ======================================================================

def db_get_chat(limit: int = 60) -> list:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT timestamp,ident,nombre,mensaje FROM chat ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]
    finally:
        conn.close()

def db_add_chat(ident: str, nombre: str, mensaje: str):
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    with conn:
        conn.execute(
            "INSERT INTO chat(timestamp,ident,nombre,mensaje) VALUES(?,?,?,?)",
            (ts, ident, nombre, mensaje)
        )
    conn.close()

# == Migration from Sheets =====================================================

def migrate_from_sheets(sh, cfg: dict):
    """Migracion unica: lee todo de Google Sheets y escribe en SQLite."""
    print("[db] Iniciando migracion desde Google Sheets...")

    # 1. Config
    if cfg:
        db_save_config(cfg)
        print(f"[db] Config: {len(cfg)} claves")

    # 2. Ligas
    try:
        ws_ligas = sh.worksheet("Ligas")
        rows_l = ws_ligas.get_all_values()
        ligas = []
        for r in rows_l[1:]:
            if len(r) >= 2 and r[0].strip():
                ligas.append({"nombre": r[0].strip(),
                              "codigo": r[1].strip() if len(r)>1 else "",
                              "espn_id": r[2].strip() if len(r)>2 else ""})
        db_set_ligas(ligas)
        print(f"[db] Ligas: {len(ligas)}")
    except Exception as e:
        print(f"[db] Ligas error: {e}")

    # 3. HORARIOS
    try:
        ws_h     = sh.worksheet("HORARIOS")
        fila_ini = int(cfg.get("FILA_INICIO_DATOS", 3))
        total    = int(cfg.get("TOTAL_JUEGOS_F1", 72))
        filas    = ws_h.get(f"A{fila_ini}:L{fila_ini+total-1}")
        conn = get_conn()
        with conn:
            for row in filas:
                def c(j, r=row): return r[j].strip() if len(r)>j else ""
                if not c(0): continue
                conn.execute("""
                    INSERT OR REPLACE INTO horarios
                    (jgo,grupo,fecha,hora,eq1,eq2,espn_id,estado,gol1,gol2,ganador,ult_act)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """, (c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7),c(8),c(9),c(10),c(11)))
        conn.close()
        print(f"[db] HORARIOS: {len(filas)} juegos")
    except Exception as e:
        print(f"[db] HORARIOS error: {e}")

    # 4. JUGADORES + picks de cada tab
    try:
        ws_j   = sh.worksheet("JUGADORES")
        j_rows = ws_j.get_all_values()
        # Encontrar fila de headers
        hi, headers = 0, []
        for idx, row in enumerate(j_rows):
            if any("EMAIL" in c.upper() or "NOMBRE" in c.upper() for c in row):
                hi = idx
                headers = [c.strip().upper() for c in row]
                break

        def hget(row, *keys):
            for k in keys:
                try:
                    i = headers.index(k)
                    if i < len(row) and row[i].strip():
                        return row[i].strip()
                except ValueError:
                    pass
            return ""

        horarios_list = db_get_horarios()
        migrated = 0

        for row in j_rows[hi+1:]:
            if not any(c.strip() for c in row): continue
            nombre = hget(row, "NOMBRE")
            if not nombre: continue

            email    = hget(row, "EMAIL")
            whatsapp = hget(row, "WHATSAPP", "TELEFONO")
            fecha    = hget(row, "FECHA REG.", "FECHA_REGISTRO")
            tab      = hget(row, "TAB_NOMBRE", "TAB SHEET", "TAB_SHEET")
            pag_raw  = hget(row, "PAGADO").upper()
            pagado   = 1 if pag_raw in ("1","SI","SI","YES","TRUE","X") else 0
            try:   num = int(hget(row, "#") or 0)
            except: num = 0

            conn = get_conn()
            ex = conn.execute(
                "SELECT id FROM jugadores WHERE (whatsapp=? AND whatsapp!='') "
                "OR (lower(email)=lower(?) AND email!='') LIMIT 1",
                (whatsapp or "_x_", email or "_x_")
            ).fetchone()

            if ex:
                jid = ex[0]
                with conn:
                    conn.execute(
                        "UPDATE jugadores SET nombre=?,tab_nombre=?,pagado=?,num=? WHERE id=?",
                        (nombre, tab, pagado, num, jid)
                    )
            else:
                with conn:
                    conn.execute(
                        "INSERT INTO jugadores(num,email,nombre,whatsapp,fecha_reg,tab_nombre,pagado) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (num, email, nombre, whatsapp, fecha, tab, pagado)
                    )
                    jid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    conn.executemany(
                        "INSERT OR IGNORE INTO picks(jugador_id,jgo) VALUES(?,?)",
                        [(jid, h["jgo"]) for h in horarios_list]
                    )
            conn.close()

            # Leer picks desde la tab del jugador en Sheets
            if tab:
                try:
                    ws_p  = sh.worksheet(tab)
                    total = int(cfg.get("TOTAL_JUEGOS_F1", 72))
                    tab_d = ws_p.get(f"A4:H{3+total}")
                    conn2 = get_conn()
                    with conn2:
                        for t_row in tab_d:
                            def tc(j, r=t_row): return r[j].strip() if len(r)>j else ""
                            jgo = tc(0)
                            if not jgo: continue
                            conn2.execute("""
                                INSERT INTO picks(jugador_id,jgo,g1_pick,g2_pick,gan_pick)
                                VALUES(?,?,?,?,?)
                                ON CONFLICT(jugador_id,jgo) DO UPDATE SET
                                    g1_pick=excluded.g1_pick,
                                    g2_pick=excluded.g2_pick,
                                    gan_pick=excluded.gan_pick
                            """, (jid, jgo, tc(5), tc(6), tc(7)))
                    conn2.close()
                    time.sleep(0.3)  # anti-429
                except Exception as e_tab:
                    print(f"[db] Picks tab '{tab}': {e_tab}")
            migrated += 1

        print(f"[db] JUGADORES: {migrated} migrados con picks")
    except Exception as e:
        print(f"[db] JUGADORES error: {e}")

    # 5. Chat (opcional)
    try:
        ws_chat = sh.worksheet("CHAT")
        chat_rows = ws_chat.get_all_values()
        conn = get_conn()
        with conn:
            for r in chat_rows[1:]:
                if len(r) >= 4:
                    conn.execute(
                        "INSERT INTO chat(timestamp,ident,nombre,mensaje) VALUES(?,?,?,?)",
                        (r[0], r[1], r[2], r[3])
                    )
        conn.close()
        print(f"[db] CHAT: {len(chat_rows)-1} mensajes")
    except Exception:
        pass  # CHAT puede no existir

    print("[db] Migracion completada.")

# == Sheets sync (SQLite -> Sheets backup) =====================================

def sync_to_sheets(sh, cfg: dict):
    """Sincroniza datos SQLite -> Sheets como backup. Llamar cada 60s."""
    try:
        _sync_jugadores(sh)
    except Exception as e:
        print(f"[sync] jugadores: {e}")
    try:
        _sync_posiciones(sh, cfg)
    except Exception as e:
        print(f"[sync] posiciones: {e}")
    try:
        _sync_picks_tabs(sh, cfg)
    except Exception as e:
        print(f"[sync] picks_tabs: {e}")

def _sync_jugadores(sh):
    """Escribe tabla jugadores -> hoja JUGADORES.
    GUARD: si SQLite tiene 0 jugadores, NO sobrescribe Sheets (evita borrado accidental)."""
    jugs = db_get_jugadores()
    if not jugs:
        print("[sync] _sync_jugadores: SQLite vacio — NO se sobrescribe JUGADORES en Sheets")
        return
    rows = [["#", "EMAIL", "NOMBRE", "WHATSAPP", "FECHA REG.", "TAB_NOMBRE", "PAGADO"]]
    for j in jugs:
        rows.append([
            j.get("num",""), j.get("email",""), j.get("nombre",""),
            j.get("whatsapp",""), j.get("fecha_reg",""), j.get("tab_nombre",""),
            "1" if j.get("pagado") else ""
        ])
    ws = sh.worksheet("JUGADORES")
    end_row = max(len(rows)+5, 30)
    ws.batch_clear([f"A1:G{end_row}"])
    if rows:
        ws.update(rows, f"A1:G{len(rows)}")

def _sync_posiciones(sh, cfg):
    """Escribe posiciones calculadas -> hoja POSICIONES."""
    standings = db_compute_standings(cfg)
    ws_pos = sh.worksheet("POSICIONES")
    ws_pos.batch_clear(["A2:D100"])
    ws_pos.update([["POS","NOMBRE","Ptos","Diferencia"]], "A2:D2")
    if not standings:
        return
    lider = standings[0]["pts"]
    rows_out = []
    pos = 1
    for i, s in enumerate(standings):
        if i > 0:
            pv = standings[i-1]
            same = (s["pts"]==pv["pts"] and s["gan"]==pv["gan"]
                    and (s["g1"]+s["g2"])==(pv["g1"]+pv["g2"]))
            if not same: pos = i+1
        rows_out.append([pos, s["nombre"], s["pts"], s["pts"]-lider])
    ws_pos.update(rows_out, f"A3:D{2+len(rows_out)}")


def _init_player_tab_db(ws, total: int):
    headers = ["JGO","GRUPO","FECHA","EQUIPO 1","EQUIPO 2","G1 PICK","G2 PICK","GAN.PICK","GOL1 REAL","GOL2 REAL","GAN.REAL","ESTADO","PTS GAN","PTS G1","PTS G2","PTS TOTAL"]
    ws.update([headers], "A1:P1")
    rows = []
    for i in range(1, total + 1):
        r = i + 3
        rows.append([i,f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;2;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;3;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;5;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;6;FALSE);"")',"","","",f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;9;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;10;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;11;FALSE);"")',f'=IFERROR(VLOOKUP(A{r};HORARIOS!$A:$L;8;FALSE);"")',f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(IF(H{r}<>"";H{r};IF(AND(F{r}<>"";G{r}<>"");IF(F{r}+0>G{r}+0;"1";IF(G{r}+0>F{r}+0;"2";"E"));""))=K{r};3;0);"")',f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(F{r}&""=I{r}&"";1;0);"")',f'=IF(AND(L{r}<>"";L{r}<>"PROG");IF(G{r}&""=J{r}&"";1;0);"")',f'=IF(AND(L{r}<>"";L{r}<>"PROG");IFERROR(SUM(M{r}:O{r});0);"")'])
    ws.update(rows, f"A4:P{3+total}", value_input_option="USER_ENTERED")

def _sync_picks_tabs(sh, cfg: dict):
    import time as _t
    total = int(cfg.get("TOTAL_JUEGOS_F1", 72))
    jugs = db_get_jugadores()
    if not jugs: return
    try:
        existing = {ws.title: ws for ws in sh.worksheets()}
    except Exception as e:
        print(f"[sync] picks: error listando tabs: {e}"); return
    for j in jugs:
        tab_name = j.get("tab_nombre",""); jug_id = j.get("id")
        if not tab_name or not jug_id: continue
        if tab_name not in existing:
            try:
                ws = sh.add_worksheet(title=tab_name, rows=total+10, cols=16)
                _init_player_tab_db(ws, total)
                existing[tab_name] = ws
                print(f"[sync] Tab creado: {tab_name}")
                _t.sleep(1)
            except Exception as e:
                print(f"[sync] Error creando tab {tab_name}: {e}"); continue
        else:
            ws = existing[tab_name]
        picks = db_get_picks(jug_id)
        if picks is None: continue
        rows = []
        for n in range(1, total+1):
            p = picks.get(str(n), picks.get(n, {}))
            rows.append([p.get("g1","") or "", p.get("g2","") or "", p.get("gan","") or ""])
        try:
            ws.update(rows, f"F4:H{3+total}")
        except Exception as e:
            print(f"[sync] Error picks {tab_name}: {e}")
