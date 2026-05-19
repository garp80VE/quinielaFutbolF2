"""
validar_jugador.py — Valida los puntos de un jugador comparando su pestaña vs HORARIOS.
Uso:  python validar_jugador.py GGG
"""
import sys
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = "16K7YJeSZfChlDkoLOAYdNlQSmTs1cRH5ZsJKtewPXc4"
CREDS    = "credentials.json"
FILA_INI = 4   # primera fila de datos (después del header)

def _num(v):
    try: return int(float(str(v).strip()))
    except: return None

def _res(g1, g2):
    if g1 is None or g2 is None: return None
    return "1" if g1 > g2 else "2" if g1 < g2 else "X"

def main():
    nombre = sys.argv[1] if len(sys.argv) > 1 else "GGG"

    creds = Credentials.from_service_account_file(
        CREDS, scopes=["https://spreadsheets.google.com/feeds",
                       "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    # Leer CONFIG para obtener valores de puntos
    cfg = {}
    try:
        cfg_rows = sh.worksheet("CONFIG").get_all_values()
        cfg = {r[0].strip(): r[1].strip() for r in cfg_rows if len(r) >= 2 and r[0].strip()}
    except Exception as e:
        print(f"[WARN] No se pudo leer CONFIG: {e}")

    v_logro   = int(cfg.get("PTS_LOGRO",   "1") or 1)
    v_gan     = int(cfg.get("PTS_GAN",     "2") or 2)
    v_gol1    = int(cfg.get("PTS_GOL1",    "1") or 1)
    v_gol2    = int(cfg.get("PTS_GOL2",    "1") or 1)
    v_campeon = int(cfg.get("PTS_CAMPEON", "0") or 0)
    print(f"Config puntos → Logro:{v_logro}  Gan:{v_gan}  Gol1:{v_gol1}  Gol2:{v_gol2}  Campeón:{v_campeon}")
    print()

    # Leer pestaña del jugador
    ws = sh.worksheet(nombre)
    data = ws.get_all_values()

    print(f"{'JGO':>3} {'RONDA':<6} {'PICK_EQ1':<18} {'P-G1':>4} {'P-G2':>4} {'P-GAN':<18} {'R-G1':>4} {'R-G2':>4} {'R-GAN':<18} {'EST':<7} {'O':>2} {'P':>2} {'Q':>2} {'R':>2} {'S(C)':>4} {'T_HOJA':>6} {'T_CALC':>6} {'DIFF':>5}")
    print("-" * 158)

    total_hoja = 0
    total_calc = 0
    diffs = []

    for i, row in enumerate(data[FILA_INI - 1:], start=FILA_INI):
        if not row or not row[0].strip(): continue
        def c(idx): return row[idx].strip() if idx < len(row) else ""

        jgo     = c(0)   # A
        ronda   = c(1)   # B
        pick_eq1= c(4)   # E
        pick_g1 = _num(c(6))  # G
        pick_g2 = _num(c(7))  # H
        pick_gan= c(9)   # J
        real_g1 = _num(c(10)) # K
        real_g2 = _num(c(11)) # L
        real_gan= c(12)  # M
        estado  = c(13)  # N
        pts_O   = c(14)  # O — PTS_LOGRO en hoja
        pts_P   = c(15)  # P — PTS_GAN en hoja
        pts_Q   = c(16)  # Q — PTS_GOL1 en hoja
        pts_R   = c(17)  # R — PTS_GOL2 en hoja
        pts_S   = c(18)  # S — PTS_CAMPEON en hoja
        pts_T   = c(19)  # T — PTS_TOTAL en hoja

        if estado not in ("FINAL", "PRORROGA", "PENALES"):
            continue  # solo partidos terminados

        # Calcular puntos manualmente
        pick_res = _res(pick_g1, pick_g2)
        real_res = _res(real_g1, real_g2)

        calc_logro  = v_logro  if (real_g1 is not None and real_g2 is not None and pick_res == real_res) else 0
        calc_gan    = v_gan    if (real_gan and pick_gan == real_gan) else 0
        calc_gol1   = v_gol1   if (real_g1 is not None and pick_g1 == real_g1) else 0
        calc_gol2   = v_gol2   if (real_g2 is not None and pick_g2 == real_g2) else 0
        calc_camp   = v_campeon if (v_campeon and ronda == "FINAL" and real_gan and pick_gan == real_gan) else 0
        calc_total  = calc_logro + calc_gan + calc_gol1 + calc_gol2 + calc_camp

        s_hoja = _num(pts_T) if pts_T else None
        s_camp = _num(pts_S) if pts_S else 0
        diff = (s_hoja - calc_total) if s_hoja is not None else None

        flag = ""
        if diff is not None and diff != 0:
            flag = " ◄ DIFF"
            diffs.append((jgo, ronda, s_hoja, calc_total, diff))

        if s_hoja is not None:
            total_hoja += s_hoja
        total_calc += calc_total

        camp_str = f"+{s_camp}" if s_camp else "  0"
        print(f"{jgo:>3} {ronda:<6} {pick_eq1:<18} {str(pick_g1):>4} {str(pick_g2):>4} {pick_gan:<18} "
              f"{str(real_g1):>4} {str(real_g2):>4} {real_gan:<18} {estado:<7} "
              f"{pts_O:>2} {pts_P:>2} {pts_Q:>2} {pts_R:>2} {camp_str:>4} {str(s_hoja):>6} {calc_total:>6}{flag}")

    print("-" * 158)
    print(f"{'TOTAL':>3}{'':>133} {total_hoja:>6} {total_calc:>6}")
    print()

    if diffs:
        print("⚠️  DIFERENCIAS ENCONTRADAS:")
        for jgo, ronda, hoja, calc, d in diffs:
            print(f"  JGO {jgo} ({ronda}): hoja={hoja}  calculado={calc}  diff={d:+d}")
    else:
        print("✅ Todos los puntos coinciden entre la hoja y el cálculo manual.")

    print()
    print(f"Total en hoja (col S):    {total_hoja} pts")
    print(f"Total calculado manualm.: {total_calc} pts")
    if total_hoja != total_calc:
        print(f"\u26a0\ufe0f  Diferencia total: {total_hoja - total_calc:+d} pts")
    else:
        print("\u2705 Totales coinciden.")

if __name__ == "__main__":
    main()
