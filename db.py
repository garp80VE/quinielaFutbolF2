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
        "ALTER TABLE jugadores ADD COLUMN excluido INTEGER DEFAULT 0",
        "ALTER TABLE jugadores ADD COLUMN excluido_fecha TEXT DEFAULT ''",
        "ALTER TABLE jugadores ADD COLUMN excluido_motivo TEXT DEFAULT ''",
        # Control de acceso por invitador. aprobado DEFAULT 1 → los jugadores ya
        # registrados quedan liberados; los NUEVOS registros se insertan con aprobado=0.
        "ALTER TABLE jugadores ADD COLUMN aprobado INTEGER DEFAULT 1",
        "ALTER TABLE jugadores ADD COLUMN invitador TEXT DEFAULT ''",
        # Pago por comprobante: imagen subida, fecha y número de depósito/voucher.
        "ALTER TABLE jugadores ADD COLUMN comprobante TEXT DEFAULT ''",
        "ALTER TABLE jugadores ADD COLUMN pago_fecha TEXT DEFAULT ''",
        "ALTER TABLE jugadores ADD COLUMN voucher TEXT DEFAULT ''",
    ]:
        try:
            conn.execute(col_sql)
            conn.commit()
        except Exception:
            pass  # columna ya existe
    conn.close()
    # Sembrar catálogo de ligas si faltan (no toca las existentes ni su espn_id).
    try:
        n = db_seed_ligas_default()
        if n:
            print(f"[db] Ligas sembradas (catálogo): {n} nuevas")
    except Exception as e:
        print(f"[db] seed ligas: {e}")

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
            "SELECT id, num, email, nombre, whatsapp, fecha_reg, tab_nombre, pagado, reglas_ok, "
            "excluido, excluido_fecha, excluido_motivo, aprobado, invitador, "
            "comprobante, pago_fecha, voucher "
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
            # aprobado=0: el registro público queda PENDIENTE hasta que el admin le
            # asigne quién lo invitó (control de acceso por invitador).
            conn.execute(
                "INSERT INTO jugadores(num,email,nombre,whatsapp,fecha_reg,tab_nombre,pagado,aprobado) "
                "VALUES(?,?,?,?,?,?,0,0)",
                (num, email or "", nombre, whatsapp or "", fecha_reg, tab_nombre)
            )
            jid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return jid

def db_set_invitador(jugador_id: int, invitador: str) -> bool:
    """Asigna el invitador y APRUEBA al jugador (lo libera). Si invitador queda vacío,
    revierte a pendiente (aprobado=0). Retorna True si encontró al jugador."""
    inv = (invitador or "").strip()
    conn = get_conn()
    try:
        with _db_lock:
            with conn:
                r = conn.execute(
                    "UPDATE jugadores SET invitador=?, aprobado=? WHERE id=?",
                    (inv, 1 if inv else 0, int(jugador_id)))
        return r.rowcount > 0
    finally:
        conn.close()

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

def db_set_comprobante(jugador_id: int, filename=None, fecha: str = "", voucher=None) -> bool:
    """Registra el comprobante (imagen) y/o el número de voucher de pago del jugador
    (queda en revisión). Solo actualiza los campos que se pasen (no nulos)."""
    sets, vals = [], []
    if filename is not None:
        sets.append("comprobante=?"); vals.append(filename)
    if voucher is not None:
        sets.append("voucher=?"); vals.append(voucher)
    sets.append("pago_fecha=?"); vals.append(fecha)
    vals.append(jugador_id)
    conn = get_conn()
    try:
        with _db_lock:
            with conn:
                r = conn.execute(
                    f"UPDATE jugadores SET {', '.join(sets)} WHERE id=?", vals
                )
        return r.rowcount > 0
    finally:
        conn.close()

def db_get_pago_info(jugador_id: int) -> dict:
    """Estado de pago de un jugador: pagado, comprobante subido, fecha y voucher."""
    conn = get_conn()
    try:
        r = conn.execute(
            "SELECT pagado, comprobante, pago_fecha, voucher FROM jugadores WHERE id=? LIMIT 1",
            (jugador_id,)
        ).fetchone()
        if not r:
            return {"pagado": False, "comprobante": "", "pago_fecha": "", "voucher": ""}
        return {"pagado": bool(r["pagado"]),
                "comprobante": r["comprobante"] or "",
                "pago_fecha": r["pago_fecha"] or "",
                "voucher": r["voucher"] or ""}
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

def db_set_excluido(jugador_id: int, excluido: bool = True, motivo: str = "", fecha: str = "") -> bool:
    """Excluye (o reactiva) a un jugador. Reversible: NO borra sus picks."""
    conn = get_conn()
    try:
        with _db_lock:
            with conn:
                if excluido:
                    r = conn.execute(
                        "UPDATE jugadores SET excluido=1, excluido_motivo=?, excluido_fecha=? WHERE id=?",
                        (motivo, fecha, int(jugador_id)))
                else:
                    r = conn.execute(
                        "UPDATE jugadores SET excluido=0, excluido_motivo='', excluido_fecha='' WHERE id=?",
                        (int(jugador_id),))
        return r.rowcount > 0
    finally:
        conn.close()

def db_get_excluidos() -> list:
    """Lista de jugadores actualmente excluidos."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, num, nombre, whatsapp, excluido_fecha, excluido_motivo "
            "FROM jugadores WHERE COALESCE(excluido,0)=1 ORDER BY num, id").fetchall()
        return [dict(r) for r in rows]
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
            SELECT p.jugador_id, j.nombre, j.excluido, p.jgo,
                   p.g1_pick, p.g2_pick, p.gan_pick, p.eq1_pick, p.eq2_pick
            FROM picks p JOIN jugadores j ON j.id = p.jugador_id
        """).fetchall()
        out: dict = {}
        for r in rows:
            pid = r["jugador_id"]
            if pid not in out:
                out[pid] = {"nombre": r["nombre"],
                            "excluido": bool(r["excluido"]),
                            "picks": {}}
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
        gan = _resolve_team_name(pk["gan"], by_jgo, by_ronda, picks) or pk["gan"]
        # El 3er puesto son los PERDEDORES de las semis. Se usan los equipos que
        # EL JUGADOR predijo para su semifinal (no los reales), para que sea
        # coherente con el resto de las rondas: si predijo mal a los semifinalistas,
        # su equipo del 3er puesto también será "equivocado" (y se marcará muerto),
        # en vez de reescribirse con el perdedor real que él nunca eligió.
        sf_eq1 = _disp_team(sf_game, "eq1", by_jgo, by_ronda, picks)
        sf_eq2 = _disp_team(sf_game, "eq2", by_jgo, by_ronda, picks)
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

    # Ganador EFECTIVO: si el gan guardado quedo HUERFANO (no es ninguno de los dos
    # equipos que el jugador predijo para ESTE cruce, p.ej. 'Francia' guardado en el
    # 3er puesto cuando sus equipos ahi son Portugal/Brasil), se infiere de SU
    # marcador (2-1 -> gana el local). Asi el gan huerfano no cuela por el gate
    # teamAlive ni suma ganador. Coherente con _gan_efectivo de la vista.
    if gan_p and e1p and e2p and gan_p not in (e1p, e2p) \
       and not e1p.startswith("Gan. ") and not e2p.startswith("Gan. "):
        try:
            _ap, _bp = int(str(g1_pick).strip()), int(str(g2_pick).strip())
            gan_p = e1p if _ap > _bp else (e2p if _bp > _ap else "")
        except (ValueError, TypeError):
            gan_p = ""

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

        jugadores = conn.execute(
            "SELECT id, nombre FROM jugadores WHERE COALESCE(excluido,0)=0 ORDER BY num, id"
        ).fetchall()

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


def db_compute_recorrido(cfg: dict = None) -> dict:
    """Evolución de la posición de cada jugador tras cada partido finalizado
    (orden cronológico). Usa la MISMA inferencia de bracket y desempate que la tabla.
    Retorna {labels:[jgo...], total, jugadores:[{id,nombre,rank_final,posiciones:[..]}]}."""
    cfg = cfg or {}
    vL = int(cfg.get("PTS_LOGRO",   1) or 1)
    vG = int(cfg.get("PTS_GAN",     2) or 2)
    v1 = int(cfg.get("PTS_GOL1",    1) or 1)
    v2 = int(cfg.get("PTS_GOL2",    1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)
    conn = get_conn()
    try:
        all_games = [dict(r) for r in conn.execute(
            "SELECT jgo,grupo,eq1,eq2,gol1,gol2,ganador,estado,fecha,hora FROM horarios").fetchall()]
        by_jgo, by_ronda = build_bracket_index(all_games)
        fin = [g for g in all_games if g.get("estado") and g["estado"] != "PROG"]
        fin.sort(key=lambda g: (f"{g.get('fecha','')} {g.get('hora','')}",
                                int(str(g["jgo"])) if str(g["jgo"]).isdigit() else 0))
        if not fin:
            return {"labels": [], "total": 0, "jugadores": []}
        jugs = [dict(j) for j in conn.execute(
            "SELECT id, nombre FROM jugadores WHERE COALESCE(excluido,0)=0 ORDER BY num, id").fetchall()]
        picks_by: dict = {}
        for r in conn.execute(
                "SELECT jugador_id,jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick FROM picks").fetchall():
            picks_by.setdefault(r["jugador_id"], {})[str(r["jgo"])] = {
                "g1": r["g1_pick"], "g2": r["g2_pick"], "gan": r["gan_pick"],
                "eq1": r["eq1_pick"] or "", "eq2": r["eq2_pick"] or ""}
        acc    = {j["id"]: {"pts": 0, "gan": 0, "g1": 0, "g2": 0} for j in jugs}
        series = {j["id"]: [] for j in jugs}
        labels = []
        for g in fin:
            jgo  = str(g["jgo"]); labels.append(jgo)
            game = by_jgo.get(jgo)
            for j in jugs:
                pp = picks_by.get(j["id"], {})
                pk = pp.get(jgo)
                if pk and game:
                    _pl, pg, pg1, pg2, ptot = calc_pts_inferred(
                        game, pk, by_jgo, by_ronda, pp, vL, vG, v1, v2, vC)
                    a = acc[j["id"]]; a["pts"] += ptot
                    if pg  > 0: a["gan"] += 1
                    if pg1 > 0: a["g1"]  += 1
                    if pg2 > 0: a["g2"]  += 1
            def _keyf(j):
                a = acc[j["id"]]; return (a["pts"], a["gan"], a["g1"] + a["g2"])
            orden = sorted(jugs, key=lambda j: (-_keyf(j)[0], -_keyf(j)[1],
                                                -_keyf(j)[2], (j["nombre"] or "").lower()))
            # 16.6: el puesto cambia SOLO cuando cambian los puntos (el desempate
            # por aciertos solo ordena visualmente dentro del mismo puesto).
            prev_pts, prk = None, 0
            for i, j in enumerate(orden):
                pts_j = acc[j["id"]]["pts"]
                if pts_j != prev_pts:
                    prk = i + 1; prev_pts = pts_j
                series[j["id"]].append(prk)
        return {"labels": labels, "total": len(jugs),
                "jugadores": [{"id": j["id"], "nombre": j["nombre"],
                               "rank_final": series[j["id"]][-1] if series[j["id"]] else 0,
                               "posiciones": series[j["id"]]} for j in jugs]}
    finally:
        conn.close()


def _equipos_vivos(games) -> set:
    """Equipos que AÚN tienen partido por jugar: los que aparecen en los partidos
    PENDIENTES (no finalizados). Son los únicos que todavía pueden dar puntos.
    (Si quedan 2 partidos, esto devuelve a lo sumo 4 equipos.)"""
    vivos = set()
    for g in games:
        estado = (g.get("estado") or "").strip()
        if estado and estado != "PROG":
            continue  # partido ya jugado -> sus equipos no suman más por aquí
        for slot in ("eq1", "eq2"):
            e = (g.get(slot) or "").strip()
            if e and not _is_placeholder(e):
                vivos.add(e)
    return vivos


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
            "SELECT id, nombre FROM jugadores WHERE COALESCE(excluido,0)=0 ORDER BY num, id"
        ).fetchall()

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

            # Puntos actuales (partidos ya jugados) + desglose por puntaje (16.10)
            pts = 0
            breakdown: dict = {}
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
                breakdown[ptot] = breakdown.get(ptot, 0) + 1

            # Equipos vivos del jugador = equipos de SUS líneas en partidos PENDIENTES
            # (lo que muestra su cuadro) que además siguen vivos en la realidad.
            # Idéntico a los verdes de "Mi cuadro": NO cuenta equipos que predijo en
            # rondas ya jugadas ni equipos ajenos a su línea aunque sigan jugando
            # (p.ej. un equipo que picó ganador en octavos y sigue vivo pero que en
            # su cuadro ya fue reemplazado por otro cruce).
            equipos_vivos_jug = set()
            for g in pendientes:
                for slot in ("eq1", "eq2"):
                    t = _disp_team(g, slot, by_jgo, by_ronda, _pp)
                    if t and not _is_placeholder(t) and t in vivos:
                        equipos_vivos_jug.add(t)

            # "Por cobrar" = PARTIDOS pendientes donde el jugador AÚN puede sumar
            # (al menos uno de sus dos equipos predichos sigue vivo). Es más útil que
            # contar equipos: dice cuántos juegos le quedan por rendir. El max realista
            # por partido solo suma los conceptos de equipos vivos (igual que Mi cuadro).
            _RLBL = {"R32": "16avos", "R16": "Octavos", "QF": "Cuartos",
                     "SF": "Semis", "3ER": "3er puesto", "FINAL": "Final"}
            max_add = 0
            por_cobrar = 0
            por_cobrar_lista = []
            for g in pendientes:
                js = str(g["jgo"]); pk = _pp.get(js) or {}
                e1 = _disp_team(g, "eq1", by_jgo, by_ronda, _pp)
                e2 = _disp_team(g, "eq2", by_jgo, by_ronda, _pp)
                a1 = bool(e1) and not _is_placeholder(e1) and e1 in vivos
                a2 = bool(e2) and not _is_placeholder(e2) and e2 in vivos
                if not (a1 or a2):
                    continue                       # ambos equipos muertos → no suma nada
                por_cobrar += 1
                r = (g.get("grupo") or g.get("ronda") or "").upper()
                gr = _gan_real(pk, _pp)
                etq = _RLBL.get(r, r)
                if gr and not _is_placeholder(gr) and gr in vivos:
                    etq += f" ({gr})"
                por_cobrar_lista.append(etq)
                max_add += vL                        # no-empate alcanzable
                if gr and not _is_placeholder(gr) and gr in vivos:
                    max_add += vG                    # ganador
                if a1: max_add += v1                 # gol equipo 1
                if a2: max_add += v2                 # gol equipo 2
            if final_pend and vC:
                pkf = _pp.get(str(final_pend[0]["jgo"])) or {}
                camp = _gan_real(pkf, _pp)
                if camp and not _is_placeholder(camp) and camp in vivos:
                    max_add += vC

            out.append({
                "jugador_id":      j["id"],
                "nombre":          j["nombre"],
                "pts":             pts,
                "pts_breakdown":   breakdown,
                "max_realista":    pts + max_add,
                "equipos_vivos":   len(equipos_vivos_jug),
                "equipos_lista":   sorted(equipos_vivos_jug),
                "por_cobrar":      por_cobrar,
                "por_cobrar_lista": por_cobrar_lista,
            })

        out.sort(key=lambda x: (-x["pts"], -x["max_realista"], x["nombre"]))
        return out
    finally:
        conn.close()


def db_compute_probabilities_mc(cfg: dict = None, n_sims: int = 1500) -> dict:
    """Probabilidades por simulación de Monte Carlo, ADAPTADO al bracket de F2.

    A diferencia de la fase de grupos (partidos independientes), aquí cada
    partido pendiente de ronda superior NO tiene equipos fijos: dependen de
    quién avance. Por eso cada simulación recorre el cuadro REAL hacia adelante
    (R32 → R16 → … → FINAL) propagando ganadores simulados, y luego puntúa los
    picks de cada jugador (logro/ganador/goles/campeón, con inferencia de
    bracket) contra ese universo simulado.

    Devuelve por jugador: rank, current_pts, techo, dist_1, prob_1st, prob_top2,
    trend, estado/chance (opcion_1|solo_2|eliminado), univ_win/univ_top2/
    univ_dif2/univ_total y pts_breakdown.
    """
    import random as _rnd, math as _math
    cfg = cfg or {}
    vL = int(cfg.get("PTS_LOGRO",   1) or 1)
    vG = int(cfg.get("PTS_GAN",     2) or 2)
    v1 = int(cfg.get("PTS_GOL1",    1) or 1)
    v2 = int(cfg.get("PTS_GOL2",    1) or 1)
    vC = int(cfg.get("PTS_CAMPEON", 0) or 0)
    MAXG = vL + vG + v1 + v2

    conn = get_conn()
    try:
        games = [dict(r) for r in conn.execute(
            "SELECT jgo,grupo,eq1,eq2,gol1,gol2,ganador,estado,espn_id FROM horarios").fetchall()]
        jus = conn.execute(
            "SELECT id,nombre FROM jugadores WHERE COALESCE(excluido,0)=0 ORDER BY num,id").fetchall()
        jugadores = [dict(j) for j in jus if (j["nombre"] or "").strip()]
        picks_by_player = {}
        for j in jugadores:
            rows = conn.execute(
                "SELECT jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick "
                "FROM picks WHERE jugador_id=?", (j["id"],)).fetchall()
            picks_by_player[j["id"]] = {
                str(r["jgo"]): {"g1": r["g1_pick"], "g2": r["g2_pick"],
                                "gan": r["gan_pick"], "eq1": r["eq1_pick"] or "",
                                "eq2": r["eq2_pick"] or ""} for r in rows}
    finally:
        conn.close()

    by_jgo, by_ronda = build_bracket_index(games)
    games_sorted = sorted(games, key=lambda g: int(g["jgo"]) if str(g["jgo"]).isdigit() else 0)
    games_map    = {str(g["jgo"]): g for g in games_sorted}

    def _isfin(g): return bool(g.get("estado")) and g["estado"] != "PROG"
    def _num(x):
        try: return int(str(x).strip())
        except (ValueError, TypeError): return None

    fixed     = [g for g in games_sorted if _isfin(g)]
    pending   = [g for g in games_sorted if not _isfin(g)]
    pend_jgos = [str(g["jgo"]) for g in pending]

    base = {"players": [], "fixed_games": len(fixed), "pending_games": len(pending),
            "max_pts": MAXG, "last_game": ""}
    if not jugadores:
        return base

    names = {j["id"]: j["nombre"] for j in jugadores}
    ids   = list(names.keys())

    # ── Fuerza de cada equipo a partir de resultados (ataque/defensa, Poisson) ──
    gf, ga, pj, nfin = {}, {}, {}, 0
    for g in fixed:
        ra, rb = _num(g.get("gol1")), _num(g.get("gol2"))
        e1 = (g.get("eq1") or "").strip(); e2 = (g.get("eq2") or "").strip()
        if ra is None or rb is None or not e1 or not e2:
            continue
        nfin += 1
        for t in (e1, e2):
            gf.setdefault(t, 0); ga.setdefault(t, 0); pj.setdefault(t, 0)
        gf[e1] += ra; ga[e1] += rb; pj[e1] += 1
        gf[e2] += rb; ga[e2] += ra; pj[e2] += 1
    mu = (sum(gf.values()) / (2 * nfin)) if nfin else 1.3
    Kf = 4.0
    att  = {t: ((gf[t] + Kf * mu) / (pj[t] + Kf)) / mu for t in pj}
    deff = {t: ((ga[t] + Kf * mu) / (pj[t] + Kf)) / mu for t in pj}

    def _lam(e1, e2):
        l1 = mu * att.get(e1, 1.0) * deff.get(e2, 1.0)
        l2 = mu * att.get(e2, 1.0) * deff.get(e1, 1.0)
        return max(0.15, l1), max(0.15, l2)

    def _pois(rng, lam):
        L = _math.exp(-lam); k, p = 0, 1.0
        while True:
            k += 1; p *= rng.random()
            if p <= L:
                return min(k - 1, 9)

    # ── Predicción resuelta de cada jugador por partido (precálculo) ───────────
    # pred[id][jgo] = (eq1_pred, eq2_pred, gan_pred, g1_pred, g2_pred)
    pred = {}
    for pid in ids:
        pp = picks_by_player[pid]; d = {}
        for g in games_sorted:
            js = str(g["jgo"]); pk = pp.get(js)
            if not pk:
                continue
            e1p = _disp_team(g, "eq1", by_jgo, by_ronda, pp)
            e2p = _disp_team(g, "eq2", by_jgo, by_ronda, pp)
            raw = (pk.get("gan") or "").strip()
            ganp = _resolve_team_name(raw, by_jgo, by_ronda, pp) or raw
            d[js] = (e1p, e2p, ganp, _num(pk.get("g1")), _num(pk.get("g2")))
        pred[pid] = d

    # ── Puntos actuales (partidos jugados) + desglose ──────────────────────────
    cur, brk = {}, {}
    for pid in ids:
        tot = 0; b = {}
        for g in fixed:
            pk = picks_by_player[pid].get(str(g["jgo"]))
            if not pk:
                continue
            _, _, _, _, pt = calc_pts_inferred(g, pk, by_jgo, by_ronda,
                                               picks_by_player[pid], vL, vG, v1, v2, vC)
            tot += pt; b[pt] = b.get(pt, 0) + 1
        cur[pid] = tot; brk[pid] = b

    FINAL_JS = {str(g["jgo"]) for g in games_sorted
                if (g.get("ronda") or g.get("grupo") or "").upper() == "FINAL"}

    def _pts_pending(pid, real, jgos):
        """Puntos del jugador en los partidos `jgos` contra el universo `real`
        (mismas reglas que _calc_pts, con equipos predichos ya resueltos)."""
        s = 0; predp = pred[pid]
        for js in jgos:
            pr = predp.get(js)
            if not pr:
                continue
            rs = real.get(js)
            if not rs:
                continue
            e1p, e2p, ganp, g1p, g2p = pr
            e1r = rs["eq1"]; e2r = rs["eq2"]; ar = rs["g1"]; br = rs["g2"]; ganr = rs["gan"]
            # teamAlive: al menos un equipo predicho debe jugar el partido real
            pt = {t for t in (e1p, e2p, ganp) if t and not t.startswith("Gan. ")}
            if e1r and e2r and pt and not (pt & {e1r, e2r}):
                continue
            # logro: empate vs no-empate a los 90'
            if None not in (g1p, g2p, ar, br) and (ar == br) == (g1p == g2p):
                s += vL
            # ganador (equipo que avanza)
            hit_gan = bool(ganp and ganr and ganp == ganr)
            if hit_gan:
                s += vG
            # goles por NOMBRE de equipo
            gp1 = g1p if e1p == e1r else (g2p if e2p == e1r else None)
            gp2 = g1p if e1p == e2r else (g2p if e2p == e2r else None)
            if gp1 is not None and ar is not None and gp1 == ar:
                s += v1
            if gp2 is not None and br is not None and gp2 == br:
                s += v2
            # campeón
            if vC and hit_gan and js in FINAL_JS:
                s += vC
        return s

    def _simulate(rng, pend_set):
        """Recorre el cuadro real propagando ganadores. Los partidos FINAL que no
        estén en pend_set usan su resultado real; el resto se simula."""
        real = {}
        for g in games_sorted:
            js = str(g["jgo"])
            if _isfin(g) and js not in pend_set:
                real[js] = {"eq1": (g.get("eq1") or "").strip(),
                            "eq2": (g.get("eq2") or "").strip(),
                            "g1": _num(g.get("gol1")), "g2": _num(g.get("gol2")),
                            "gan": (g.get("ganador") or "").strip()}
                continue
            e1 = _disp_team(g, "eq1", by_jgo, by_ronda, real)
            e2 = _disp_team(g, "eq2", by_jgo, by_ronda, real)
            l1, l2 = _lam(e1, e2)
            a, b = _pois(rng, l1), _pois(rng, l2)
            if a > b:   gan = e1
            elif b > a: gan = e2
            else:       gan = e1 if rng.random() < l1 / (l1 + l2) else e2
            real[js] = {"eq1": e1, "eq2": e2, "g1": a, "g2": b, "gan": gan}
        return real

    def _run_mc(cur_map, pend_set, n):
        """Devuelve (prob_1st, prob_top2) en % por jugador."""
        p1 = {pid: 0.0 for pid in ids}; p2 = {pid: 0.0 for pid in ids}
        plist = [str(g["jgo"]) for g in games_sorted if str(g["jgo"]) in pend_set]
        if not plist:
            order2 = sorted(ids, key=lambda pid: -cur_map[pid])
            top = cur_map[order2[0]]
            second = sorted({cur_map[pid] for pid in ids}, reverse=True)
            mx2 = second[1] if len(second) > 1 else second[0]
            champs = [pid for pid in ids if cur_map[pid] == top]
            for pid in champs: p1[pid] = 100.0 / len(champs)
            for pid in ids:
                if cur_map[pid] >= mx2: p2[pid] = 100.0
            return p1, p2
        rng = _rnd.Random(20260628)
        for _ in range(n):
            real = _simulate(rng, pend_set)
            scores = [(cur_map[pid] + _pts_pending(pid, real, plist), pid) for pid in ids]
            vals = sorted({s for s, _ in scores}, reverse=True)
            mx1 = vals[0]; mx2 = vals[1] if len(vals) > 1 else vals[0]
            champs = [pid for s, pid in scores if s == mx1]
            for pid in champs: p1[pid] += 1.0 / len(champs)
            for s, pid in scores:
                if s >= mx2: p2[pid] += 1.0
        for pid in ids:
            p1[pid] = 100.0 * p1[pid] / n
            p2[pid] = 100.0 * p2[pid] / n
        return p1, p2

    pend_set = set(pend_jgos)
    if pending:
        now_p1, now_p2 = _run_mc(cur, pend_set, n_sims)
    else:
        now_p1, now_p2 = _run_mc(cur, pend_set, 1)  # determinista

    # ── Ranking por puntos actuales (empates = misma posición) ─────────────────
    order = sorted(ids, key=lambda pid: (-cur[pid], names[pid].lower()))
    rank = {}; prev = None; r = 0
    for i, pid in enumerate(order):
        if cur[pid] != prev:
            r = i + 1; prev = cur[pid]
        rank[pid] = r
    lider = cur[order[0]] if order else 0

    # ── Proyección por "universo perfecto" de cada jugador → estado + universos ─
    # Un equipo está MUERTO si PERDIÓ un partido ya jugado: un equipo eliminado NO
    # puede reaparecer en cruces futuros. El "techo" (máximo posible) DEBE respetar
    # esto — antes asumía que todos los equipos predichos seguían vivos e inflaba el
    # tope (p.ej. daba 125 cuando el real era 110). Se usa la MISMA lógica de "Mi
    # cuadro": por partido pendiente, solo suman los conceptos de equipos vivos.
    eliminados = set()
    for g in fixed:
        e1 = (g.get("eq1") or "").strip(); e2 = (g.get("eq2") or "").strip()
        gn = (g.get("ganador") or "").strip()
        if e1 and e2 and gn:
            loser = e2 if gn == e1 else (e1 if gn == e2 else "")
            if loser:
                eliminados.add(loser)

    def _status2(name):
        n = (name or "").strip()
        if (not n) or _is_placeholder(n) or n.startswith("Gan. ") or n.startswith("Perdedor "):
            return "unknown"
        return "dead" if n in eliminados else "alive"

    def _alive_here(name, re1, re2, real_known):
        """¿El equipo predicho puede puntuar en ESTE partido? Si los equipos reales
        del cruce ya se conocen, debe ser participante real (un finalista NO puede
        puntuar en el 3er puesto; un perdedor de semis SÍ, aunque esté 'eliminado'
        del torneo). Si el cruce aún no está definido, se usa el criterio global."""
        n = (name or "").strip()
        if (not n) or _is_placeholder(n) or n.startswith("Gan. ") or n.startswith("Perdedor "):
            return False
        if real_known:
            return n in (re1, re2)
        return n not in eliminados

    def _max_disp(pid, js):
        """Máximo de puntos que pid aún puede sacar en el pendiente js, según qué
        equipos predichos pueden puntuar en ese cruce real (idéntico a Mi cuadro)."""
        pr = pred[pid].get(js)
        if not pr:
            return 0
        e1p, e2p, ganp, g1p, g2p = pr
        g = games_map.get(js) or by_jgo.get(js) or {}
        re1 = (g.get("eq1") or "").strip(); re2 = (g.get("eq2") or "").strip()
        def _ph(n):
            n = (n or "").strip()
            return ((not n) or _is_placeholder(n)
                    or n.startswith("Gan. ") or n.startswith("Perdedor "))
        real_known = bool(re1 and re2 and not _ph(re1) and not _ph(re2))
        a1 = _alive_here(e1p, re1, re2, real_known)
        a2 = _alive_here(e2p, re1, re2, real_known)
        aw = _alive_here(ganp, re1, re2, real_known)
        if not a1 and not a2:
            return 0                       # ningún equipo puede puntuar aquí
        m = vL                             # logro (no-empate) sigue disponible
        if aw: m += vG                     # ganador
        if a1: m += v1                     # gol equipo 1
        if a2: m += v2                     # gol equipo 2
        if js in FINAL_JS and aw: m += vC  # campeón
        return m

    techo = {}
    for pid in ids:
        techo[pid] = cur[pid] + sum(_max_disp(pid, js) for js in pend_jgos)

    # ── Estado por ENUMERACIÓN EXACTA del universo de cada jugador ──────────────
    # "¿Sigue con vida / solo 2° / eliminado?" es una pregunta de POSIBILIDAD, no de
    # probabilidad: se resuelve mirando TODOS los desenlaces posibles de los partidos
    # que faltan (no una muestra al azar). Como al final del torneo quedan muy pocos
    # partidos, se enumeran todos los cruces posibles (2^pendientes) y se revisa si
    # existe ALGÚN desenlace donde el jugador termina 1° (o top-2). Los goles se fijan
    # en el pronóstico del propio jugador (su universo); solo varía quién gana cada
    # partido. El Monte Carlo (prob_1st/prob_top2) queda SOLO para el % de arriba.
    univ_win  = {pid: None for pid in ids}
    univ_top2 = {pid: None for pid in ids}
    univ_dif2 = {pid: None for pid in ids}
    chance = {}
    univ_total_n = 0

    pend_seq = [g for g in games_sorted if str(g["jgo"]) in pend_set]

    def _score_for(pr, e1, e2, w):
        """Marcador P-favorable: los goles que predijo el jugador si son coherentes
        con que gane `w` (o empate a penales); si no, `w` gana 1-0."""
        if pr:
            e1p, e2p, ganp, g1p, g2p = pr
            a = g1p if e1p == e1 else (g2p if e2p == e1 else None)
            b = g1p if e1p == e2 else (g2p if e2p == e2 else None)
            if a is not None and b is not None:
                if a == b or (a > b and w == e1) or (b > a and w == e2):
                    return a, b
        return (1, 0) if w == e1 else (0, 1)

    ENUM_CAP = 512                      # 2^9: enumera exacto solo si hay pocos pendientes
    n_universos = 1 << len(pend_seq)

    if not pending:
        # Torneo terminado: el estado sale del ranking final.
        for P in ids:
            chance[P] = ("opcion_1" if rank[P] == 1
                         else ("solo_2" if rank[P] == 2 else "eliminado"))
    elif n_universos <= ENUM_CAP:
        from itertools import product as _product
        # Genera TODOS los cruces posibles (equipos + ganador) una sola vez; los
        # equipos de cada partido se propagan según los ganadores ya elegidos.
        combos = [{}]
        for g in pend_seq:
            js = str(g["jgo"]); nuevos = []
            for parcial in combos:
                e1 = _disp_team(g, "eq1", by_jgo, by_ronda, parcial)
                e2 = _disp_team(g, "eq2", by_jgo, by_ronda, parcial)
                opciones = [w for w in (e1, e2) if w]
                opciones = list(dict.fromkeys(opciones)) or [e1]
                for w in opciones:
                    nd = dict(parcial)
                    nd[js] = {"eq1": e1, "eq2": e2, "gan": w}
                    nuevos.append(nd)
            combos = nuevos
        univ_total_n = len(combos)

        # Para cada cruce, además de quién gana, se prueban unos pocos MARCADORES por
        # partido: el que predijo P (le da sus goles/logro) y marcadores "estériles"
        # que hunden a los rivales (nadie acierta goles; se prueba con y sin empate).
        # Así el estado es una verdadera prueba de posibilidad: existe ALGÚN desenlace
        # (cruces + marcadores) donde P termina 1° / top-2. Si el volumen es muy alto
        # se cae a un solo marcador (el de P) para no penalizar el tiempo de cómputo.
        SCORE_CAP = 50000
        multi = (univ_total_n * (3 ** len(pend_seq))) <= SCORE_CAP

        def _cands(predP, js, e1, e2, w):
            a, b = _score_for(predP.get(js), e1, e2, w)
            if not multi:
                return [(a, b)]
            ster_nd = (9, 0) if w == e1 else (0, 9)     # gana sin empate, nadie acierta goles
            ster_dr = (8, 8)                            # empate (gana por penales), nadie acierta
            out = []
            for c in ((a, b), ster_nd, ster_dr):
                if c not in out:
                    out.append(c)
            return out

        for P in ids:
            predP = pred[P]
            can1 = False; wins = 0; tops = 0; best = None
            for combo in combos:
                jss = list(combo.keys())
                cand_lists = [_cands(predP, js, combo[js]["eq1"], combo[js]["eq2"],
                                     combo[js]["gan"]) for js in jss]
                combo_1 = False; combo_top2 = False
                for choice in _product(*cand_lists):
                    real = {}
                    for js, (a, b) in zip(jss, choice):
                        info = combo[js]
                        real[js] = {"eq1": info["eq1"], "eq2": info["eq2"],
                                    "g1": a, "g2": b, "gan": info["gan"]}
                    sc = {X: cur[X] + _pts_pending(X, real, pend_jgos) for X in ids}
                    sp = sc[P]; mx = max(sc.values())
                    if sp >= mx:                       # 1° (empate cuenta como 1°)
                        combo_1 = True
                    otros = sorted((sc[X] for X in ids if X != P), reverse=True)
                    corte2 = otros[1] if len(otros) >= 2 else None
                    if corte2 is None or sp >= corte2:
                        combo_top2 = True
                    if corte2 is not None:
                        m = sp - corte2
                        if best is None or m > best:
                            best = m
                    if combo_1 and combo_top2:
                        break
                if combo_1:
                    wins += 1; can1 = True
                if combo_top2:
                    tops += 1
            univ_win[P]  = wins
            univ_top2[P] = tops
            univ_dif2[P] = best
            chance[P] = ("opcion_1" if can1
                         else ("solo_2" if tops > 0 else "eliminado"))
    else:
        # Demasiados pendientes para enumerar (inicio del torneo): cota SEGURA que
        # nunca elimina de más — solo descarta a quien ya tiene rivales por encima de
        # su techo (imposible alcanzarlos aun acertando todo).
        for P in ids:
            n_above = sum(1 for R in ids if R != P and cur[R] > techo[P])
            chance[P] = ("opcion_1" if n_above == 0
                         else ("solo_2" if n_above == 1 else "eliminado"))

    # ── Tendencia ▲▼: prob_1st antes vs después del último partido finalizado ──
    before_p1 = dict(now_p1)
    if fixed and pending:
        last_final = max((str(g["jgo"]) for g in fixed),
                         key=lambda x: int(x) if str(x).isdigit() else -1)
        lg = games_map[last_final]
        before_cur = dict(cur)
        for pid in ids:
            pk = picks_by_player[pid].get(last_final)
            if pk:
                _, _, _, _, earned = calc_pts_inferred(lg, pk, by_jgo, by_ronda,
                                                       picks_by_player[pid], vL, vG, v1, v2, vC)
                before_cur[pid] = cur[pid] - earned
        before_p1, _ = _run_mc(before_cur, pend_set | {last_final}, n_sims)

    last_game = ""
    if fixed:
        lg = games_map[max((str(g["jgo"]) for g in fixed),
                           key=lambda x: int(x) if str(x).isdigit() else -1)]
        last_game = f"{lg.get('eq1','')} vs {lg.get('eq2','')}"

    players = []
    for pid in order:
        # El ESTADO exacto (posibilidad real) manda sobre el % de Monte Carlo: si es
        # imposible ser 1° / top-2, su probabilidad se fija en 0 (el MC puede dar un
        # residuo por muestreo, pero no puede haber % de algo matemáticamente
        # imposible). Así el badge y el % nunca se contradicen.
        _est   = chance[pid]
        _can1  = _est == "opcion_1"
        _cant2 = _est in ("opcion_1", "solo_2")
        prob1 = round(now_p1.get(pid, 0.0), 5) if _can1  else 0.0
        ptop  = round(now_p2.get(pid, 0.0), 5) if _cant2 else 0.0
        players.append({
            "name": names[pid], "rank": rank[pid], "current_pts": cur[pid],
            "max_possible": techo[pid], "techo": techo[pid],
            "dist_1": max(0, lider - cur[pid]),
            "prob_1st": prob1, "prob_top2": ptop,
            "univ_1st": prob1, "univ_2nd": round(max(0.0, ptop - prob1), 5),
            "trend": round(prob1 - before_p1.get(pid, prob1), 1),
            "estado": chance[pid], "chance": chance[pid],
            "univ_win": univ_win[pid], "univ_top2": univ_top2[pid],
            "univ_dif2": univ_dif2[pid], "univ_total": univ_total_n or None,
            "pts_breakdown": brk[pid],
        })
    return {"players": players, "fixed_games": len(fixed), "pending_games": len(pending),
            "max_pts": MAXG, "last_game": last_game}

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

# Catálogo estándar de ligas con su league-id real de ESPN (uid s:600~l:<id>).
# El Mundial (fifa.world=606) es el que usa F2; las demás quedan disponibles para
# seleccionar en el admin. Verificados contra el API de ESPN (oct-2025).
_LIGAS_DEFAULT = [
    ("La Liga",          "spa.1",          "740"),
    ("Premier League",   "eng.1",          "700"),
    ("Champions League", "uefa.champions",  "775"),
    ("Serie A",          "ita.1",          "730"),
    ("Bundesliga",       "ger.1",          "720"),
    ("Ligue 1",          "fra.1",          "710"),
    ("MLS",              "usa.1",          "770"),
    ("Liga MX",          "mex.1",          "760"),
    ("Mundial",          "fifa.world",     "606"),
]

def db_seed_ligas_default() -> int:
    """Inserta las ligas del catálogo que falten (por código), SIN tocar las que ya
    existan (preserva su espn_id). Idempotente. Retorna cuántas insertó."""
    conn = get_conn()
    try:
        existentes = {r["codigo"] for r in conn.execute("SELECT codigo FROM ligas").fetchall()}
        faltan = [(n, c, e) for (n, c, e) in _LIGAS_DEFAULT if c not in existentes]
        if faltan:
            with conn:
                conn.executemany(
                    "INSERT INTO ligas(nombre,codigo,espn_id) VALUES(?,?,?)", faltan)
        return len(faltan)
    finally:
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
