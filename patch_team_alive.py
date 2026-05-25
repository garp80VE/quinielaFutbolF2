"""
patch_team_alive.py — Aplica la regla "equipo vivo" en el cálculo de puntos F2.

Desde R16 en adelante, si ninguno de los equipos predichos por el jugador
(PICK_EQ1, PICK_EQ2 o PICK_GANADOR) está jugando en el partido real,
el jugador obtiene 0 puntos en todos los conceptos de ese partido.

Modifica:
  1. webapp.py — fórmulas de Sheets en _init_player_tab (para nuevas tabs)
  2. webapp.py — cálculo Python en _compute_compare_picks (para Comparar)
  3. webapp.py — nuevo endpoint /api/admin/update-scoring-f2 para actualizar tabs existentes
  4. index.html — visualización de puntos en tarjetas de picks

Ejecutar una sola vez:
    python patch_team_alive.py
"""
import os, sys

base = os.path.dirname(__file__)

# ──────────────────────────────────────────────────────────────────────────────
# 1 + 2 + 3. webapp.py
# ──────────────────────────────────────────────────────────────────────────────
wa_path = os.path.join(base, "webapp.py")
with open(wa_path, "r", encoding="utf-8") as f:
    wa = f.read()

wa_changes = 0

# 1. Fórmulas Sheets en _init_player_tab
WA_OLD1 = (
    '            # O: PTS_LOGRO — pts si el resultado a 90min (1/X/2) predicho coincide con real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X"));{_VL};0);"")\'  ,\n'
    '            # P: PTS_GAN — pts si el ganador predicho coincide con el ganador real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(J{r}=M{r};{_VG};0);"")\'  ,\n'
    '            # Q: PTS_GOL1 — pts si el gol del equipo 1 predicho coincide con el real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(G{r}&""=K{r}&"";{_V1};0);"")\'  ,\n'
    '            # R: PTS_GOL2 — pts si el gol del equipo 2 predicho coincide con el real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(H{r}&""=L{r}&"";{_V2};0);"")\'  ,\n'
    '            # S: PTS_CAMPEON — bono solo en fila FINAL si acertó al campeón\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(B{r}="FINAL";IF(J{r}=M{r};{_VC};0);0);"")\'  ,'
)

# Busqueda flexible (sin espacios extra al final de línea)
WA_OLD1b = (
    '            # O: PTS_LOGRO — pts si el resultado a 90min (1/X/2) predicho coincide con real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X"));{_VL};0);"")' "' ,\n"
    '            # P: PTS_GAN — pts si el ganador predicho coincide con el ganador real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(J{r}=M{r};{_VG};0);"")' "' ,\n"
    '            # Q: PTS_GOL1 — pts si el gol del equipo 1 predicho coincide con el real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(G{r}&""=K{r}&"";{_V1};0);"")' "' ,\n"
    '            # R: PTS_GOL2 — pts si el gol del equipo 2 predicho coincide con el real\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(H{r}&""=L{r}&"";{_V2};0);"")' "' ,\n"
    '            # S: PTS_CAMPEON — bono solo en fila FINAL si acertó al campeón\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(B{r}="FINAL";IF(J{r}=M{r};{_VC};0);0);"")' "' ,"
)

WA_NEW1 = (
    '            # O: PTS_LOGRO — pts si resultado coincide Y al menos 1 equipo pick sigue vivo\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X")));{_VL};0);"")' "' ,\n"
    '            # P: PTS_GAN — pts si ganador coincide Y al menos 1 equipo pick sigue vivo\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};J{r}=M{r});{_VG};0);"")' "' ,\n"
    '            # Q: PTS_GOL1 — pts si gol EQ1 coincide Y al menos 1 equipo pick sigue vivo\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};G{r}&""=K{r}&"");{_V1};0);"")' "' ,\n"
    '            # R: PTS_GOL2 — pts si gol EQ2 coincide Y al menos 1 equipo pick sigue vivo\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};H{r}&""=L{r}&"");{_V2};0);"")' "' ,\n"
    '            # S: PTS_CAMPEON — bono FINAL si ganador coincide Y al menos 1 equipo pick sigue vivo\n'
    '            f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(B{r}="FINAL";IF(AND({lib};J{r}=M{r});{_VC};0);0);"")' "' ,"
)

if "AND({lib};" in wa:
    print("✓ webapp.py — fórmulas Sheets ya tenían lib aplicado")
elif WA_OLD1b in wa:
    wa = wa.replace(WA_OLD1b, WA_NEW1, 1)
    wa_changes += 1
    print("✓ webapp.py — fórmulas Sheets actualizadas (lib aplicado)")
else:
    # Buscar sin depender de espacios exactos
    import re
    pat = re.compile(
        r'# O: PTS_LOGRO.*?# T: PTS_TOTAL',
        re.DOTALL
    )
    m = pat.search(wa)
    if m:
        snippet = m.group(0)[:120]
        print(f"✗ No se encontró bloque exacto de fórmulas. Fragmento:\n{snippet}")
    else:
        print("✗ webapp.py — bloque de fórmulas no encontrado")
    sys.exit(1)

# 2. Cálculo Python _compute_compare_picks
WA_OLD2 = (
    '            real_eq1 = game.get("eq1", "")\n'
    '            real_eq2 = game.get("eq2", "")\n'
    '            # Pick incompleto: requiere marcador (gol1+gol2) Y ganador\n'
    '            if not pick_gol1 or not pick_gol2 or not pick_gan:'
)
WA_NEW2 = (
    '            real_eq1 = game.get("eq1", "")\n'
    '            real_eq2 = game.get("eq2", "")\n'
    '            # Regla F2: al menos 1 equipo del pick debe estar jugando en el partido real\n'
    '            team_alive = (\n'
    '                not real_eq1 or not real_eq2 or\n'
    '                (pick_eq1 and (pick_eq1 == real_eq1 or pick_eq1 == real_eq2)) or\n'
    '                (pick_eq2 and (pick_eq2 == real_eq1 or pick_eq2 == real_eq2)) or\n'
    '                (pick_gan and (pick_gan == real_eq1 or pick_gan == real_eq2))\n'
    '            )\n'
    '            # Pick incompleto o ningún equipo con vida: sin puntos\n'
    '            if not pick_gol1 or not pick_gol2 or not pick_gan or not team_alive:'
)

if "team_alive" in wa:
    print("✓ webapp.py — team_alive Python ya aplicado")
elif WA_OLD2 in wa:
    wa = wa.replace(WA_OLD2, WA_NEW2, 1)
    wa_changes += 1
    print("✓ webapp.py — cálculo Python team_alive añadido")
else:
    print("✗ webapp.py — bloque _compute_compare_picks no encontrado")
    sys.exit(1)

# 3. Nuevo endpoint /api/admin/update-scoring-f2
ENDPOINT_MARKER = '@app.post("/api/admin/reinit-formulas")'
NEW_ENDPOINT = '''@app.post("/api/admin/update-scoring-f2")
async def admin_update_scoring_f2(ql_admin: str = Cookie(default="")):
    """Actualiza SOLO las fórmulas de puntos (O-S) en todas las tabs de jugadores,
    sin tocar los picks (F-J). Aplica la regla equipo-vivo para R16+."""
    if not _admin_check(ql_admin):
        raise HTTPException(403, "No autorizado")
    sh  = state.get("sh")
    cfg = state.get("cfg", {})
    if not sh:
        raise HTTPException(500, "Sheet no conectado")

    total    = int(cfg.get("TOTAL_JUEGOS_F2", 32))
    last_row = 3 + total
    _VL = 'IFERROR(VLOOKUP("PTS_LOGRO";CONFIG!$A:$B;2;0)*1;1)'
    _VG = 'IFERROR(VLOOKUP("PTS_GAN";CONFIG!$A:$B;2;0)*1;2)'
    _V1 = 'IFERROR(VLOOKUP("PTS_GOL1";CONFIG!$A:$B;2;0)*1;1)'
    _V2 = 'IFERROR(VLOOKUP("PTS_GOL2";CONFIG!$A:$B;2;0)*1;1)'
    _VC = 'IFERROR(VLOOKUP("PTS_CAMPEON";CONFIG!$A:$B;2;0)*1;0)'

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
                lib = f"OR(F{r}=D{r};F{r}=E{r};I{r}=D{r};I{r}=E{r};J{r}=D{r};J{r}=E{r})"
                formula_rows.append([
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};IF(G{r}*1>H{r}*1;"1";IF(G{r}*1<H{r}*1;"2";"X"))=IF(K{r}*1>L{r}*1;"1";IF(K{r}*1<L{r}*1;"2";"X")));{_VL};0);"")\',
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};J{r}=M{r});{_VG};0);"")\',
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};G{r}&""=K{r}&"");{_V1};0);"")\',
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(AND({lib};H{r}&""=L{r}&"");{_V2};0);"")\',
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IF(B{r}="FINAL";IF(AND({lib};J{r}=M{r});{_VC};0);0);"")\',
                    f\'=IF(AND(N{r}<>"";N{r}<>"PROG");IFERROR(SUM(O{r}:S{r});0);"")\'
                ])
            with _sheets_lock:
                ws_p = sh.worksheet(p["TAB_NOMBRE"])
                ws_p.update(formula_rows, f"O4:T{last_row}", value_input_option="USER_ENTERED")
            updated += 1
            time.sleep(0.4)
        except Exception as e:
            print(f"[update-scoring-f2] Error en {p.get(\'TAB_NOMBRE\',\'?\')}: {e}")

    return {"ok": True, "msg": f"Fórmulas O-S actualizadas en {updated} tab(s)"}


'''

if "/api/admin/update-scoring-f2" in wa:
    print("✓ webapp.py — endpoint update-scoring-f2 ya existe")
elif ENDPOINT_MARKER in wa:
    wa = wa.replace(ENDPOINT_MARKER, NEW_ENDPOINT + ENDPOINT_MARKER, 1)
    wa_changes += 1
    print("✓ webapp.py — endpoint /api/admin/update-scoring-f2 añadido")
else:
    print("⚠ webapp.py — no se encontró marcador para insertar endpoint (no crítico)")

if wa_changes > 0:
    with open(wa_path, "w", encoding="utf-8") as f:
        f.write(wa)
    print(f"   webapp.py guardado. Líneas aprox: {wa.count(chr(10))}")

# ──────────────────────────────────────────────────────────────────────────────
# 4. index.html — visualización de puntos en tarjetas
# ──────────────────────────────────────────────────────────────────────────────
idx_path = os.path.join(base, "index.html")
with open(idx_path, "r", encoding="utf-8") as f:
    idx = f.read()

IDX_OLD = (
    "      // PTS_LOGRO: pts si resultado 90min (1/X/2) predicho coincide con real\n"
    "      const pts_logro = (g.gol1 !== '' && g.gol2 !== '' && pickResult === realResult) ? S.pts.logro : 0;\n"
    "      // PTS_GAN: pts si ganador predicho coincide con ganador real\n"
    "      const pts_gan = (!!g.ganador && pickGan === g.ganador) ? S.pts.gan : 0;\n"
    "      // PTS_GOL1: pts si cantidad de goles EQ1 coincide\n"
    "      const pts_gol1 = (g.gol1 !== '' && String(p.gol1) === String(g.gol1)) ? S.pts.gol1 : 0;\n"
    "      // PTS_GOL2: pts si cantidad de goles EQ2 coincide\n"
    "      const pts_gol2 = (g.gol2 !== '' && String(p.gol2) === String(g.gol2)) ? S.pts.gol2 : 0;\n"
    "      // PTS_CAMPEON: bono si es la FINAL y acertó al ganador\n"
    "      const pts_camp = (S.pts.campeon && g.ronda === 'FINAL' && !!g.ganador && pickGan === g.ganador) ? S.pts.campeon : 0;"
)
IDX_NEW = (
    "      // Regla F2: al menos 1 equipo del pick debe estar jugando en el partido real\n"
    "      const _realTeams = [g.eq1, g.eq2].filter(Boolean);\n"
    "      const teamAlive = _realTeams.length === 0 ||\n"
    "        (p.eq1 && _realTeams.includes(p.eq1)) ||\n"
    "        (p.eq2 && _realTeams.includes(p.eq2)) ||\n"
    "        (pickGan && _realTeams.includes(pickGan));\n"
    "      // PTS_LOGRO: pts si resultado 90min (1/X/2) predicho coincide con real\n"
    "      const pts_logro = teamAlive && (g.gol1 !== '' && g.gol2 !== '' && pickResult === realResult) ? S.pts.logro : 0;\n"
    "      // PTS_GAN: pts si ganador predicho coincide con ganador real\n"
    "      const pts_gan = teamAlive && (!!g.ganador && pickGan === g.ganador) ? S.pts.gan : 0;\n"
    "      // PTS_GOL1: pts si cantidad de goles EQ1 coincide\n"
    "      const pts_gol1 = teamAlive && (g.gol1 !== '' && String(p.gol1) === String(g.gol1)) ? S.pts.gol1 : 0;\n"
    "      // PTS_GOL2: pts si cantidad de goles EQ2 coincide\n"
    "      const pts_gol2 = teamAlive && (g.gol2 !== '' && String(p.gol2) === String(g.gol2)) ? S.pts.gol2 : 0;\n"
    "      // PTS_CAMPEON: bono si es la FINAL y acertó al ganador\n"
    "      const pts_camp = teamAlive && (S.pts.campeon && g.ronda === 'FINAL' && !!g.ganador && pickGan === g.ganador) ? S.pts.campeon : 0;"
)

if "teamAlive" in idx:
    print("✓ index.html — teamAlive ya aplicado")
elif IDX_OLD in idx:
    idx = idx.replace(IDX_OLD, IDX_NEW, 1)
    with open(idx_path, "w", encoding="utf-8") as f:
        f.write(idx)
    print(f"✓ index.html — teamAlive añadido en visualización de puntos")
    print(f"   index.html guardado. Líneas aprox: {idx.count(chr(10))}")
else:
    print("✗ index.html — bloque pts no encontrado")
    sys.exit(1)

print("\n✅ Patch completo.")
print("Después del deploy:")
print("  - Para pruebas desde cero: reinicializa las tabs de jugadores (picks se recrean con nuevas fórmulas)")
print("  - Para producción (tabs existentes): POST /api/admin/update-scoring-f2")
