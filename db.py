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
    """Calcula posiciones completas desde picks + horarios. Retorna lista ordenada."""
    cfg = cfg or {}
    pts_logro_val   = int(cfg.get("PTS_LOGRO",   1) or 1)
    pts_gan_val     = int(cfg.get("PTS_GAN",     2) or 2)
    pts_g1_val      = int(cfg.get("PTS_GOL1",    1) or 1)
    pts_g2_val      = int(cfg.get("PTS_GOL2",    1) or 1)
    pts_campeon_val = int(cfg.get("PTS_CAMPEON", 0) or 0)

    conn = get_conn()
    try:
        games_rows = conn.execute(
            "SELECT jgo,grupo,eq1,eq2,gol1,gol2,ganador,estado FROM horarios WHERE estado!='PROG'"
        ).fetchall()
        games = {r["jgo"]: dict(r) for r in games_rows}

        jugadores = conn.execute("SELECT id, nombre FROM jugadores ORDER BY num, id").fetchall()

        standings = []
        for j in jugadores:
            pk_rows = conn.execute(
                "SELECT jgo,g1_pick,g2_pick,gan_pick,eq1_pick,eq2_pick FROM picks WHERE jugador_id=?",
                (j["id"],)
            ).fetchall()

            pts_total = gan_acert = g1_acert = g2_acert = jugados = 0
            for pk in pk_rows:
                game = games.get(pk["jgo"])
                if not game:
                    continue
                jugados += 1
                pl, pg, pg1, pg2, ptot = _calc_pts(
                    pk["g1_pick"], pk["g2_pick"], pk["gan_pick"],
                    game["gol1"], game["gol2"], game["ganador"], game["estado"],
                    pts_logro_val, pts_gan_val, pts_g1_val, pts_g2_val,
                    pts_campeon_val, game.get("grupo", ""),
                    eq1_pick=pk["eq1_pick"] or "", eq2_pick=pk["eq2_pick"] or "",
                    eq1_real=game.get("eq1", ""), eq2_real=game.get("eq2", "")
                )
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
