"""
patch_bracket_fix.py — Paso 3 de propagate_bracket: no tocar picks de partidos ya FINAL.
Ejecutar una sola vez:
    python patch_bracket_fix.py
"""
import os, sys

path = os.path.join(os.path.dirname(__file__), "webapp.py")
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

OLD = (
    "            fila_fin_p = fila_inicio + total_juegos - 1\n"
    "            # Columnas de picks: F=PICK_EQ1(5), I=PICK_EQ2(8), J=PICK_GANADOR(9) — 0-indexed\n"
    "            PICK_COLS = [(\"F\", 5), (\"I\", 8), (\"J\", 9)]\n"
    "\n"
    "            for tab_name in tab_names:\n"
    "                try:\n"
    "                    with _sheets_lock:\n"
    "                        ws_p  = sh.worksheet(tab_name)\n"
    "                        rows  = ws_p.get(f\"A{fila_inicio}:J{fila_fin_p}\")\n"
    "                    pick_batch = []\n"
    "                    for i, row in enumerate(rows):\n"
    "                        def _c(idx, r=row): return r[idx].strip() if len(r) > idx else \"\"\n"
    "                        row_num = fila_inicio + i\n"
    "                        jgo_val = _c(0)\n"
    "                        for col_letter, col_idx in PICK_COLS:\n"
    "                            val = _c(col_idx)\n"
    "                            if val in placeholder_map:\n"
    "                                new_val = placeholder_map[val]\n"
    "                                pick_batch.append({\n"
    "                                    \"range\":  f\"{col_letter}{row_num}\",\n"
    "                                    \"values\": [[new_val]],\n"
    "                                })\n"
    "                                changes.append(\n"
    "                                    f\"[{tab_name}] JGO {jgo_val} {col_letter}: {val!r} → {new_val!r}\"\n"
    "                                )"
)

NEW = (
    "            fila_fin_p = fila_inicio + total_juegos - 1\n"
    "            # Columnas de picks: F=PICK_EQ1(5), I=PICK_EQ2(8), J=PICK_GANADOR(9) — 0-indexed\n"
    "            # Leemos hasta col N (estado, idx 13) para no tocar picks de partidos ya FINAL\n"
    "            PICK_COLS = [(\"F\", 5), (\"I\", 8), (\"J\", 9)]\n"
    "            ESTADOS_CERRADOS = (\"FINAL\", \"PRORROGA\", \"PENALES\")\n"
    "\n"
    "            for tab_name in tab_names:\n"
    "                try:\n"
    "                    with _sheets_lock:\n"
    "                        ws_p  = sh.worksheet(tab_name)\n"
    "                        rows  = ws_p.get(f\"A{fila_inicio}:N{fila_fin_p}\")\n"
    "                    pick_batch = []\n"
    "                    for i, row in enumerate(rows):\n"
    "                        def _c(idx, r=row): return r[idx].strip() if len(r) > idx else \"\"\n"
    "                        row_num = fila_inicio + i\n"
    "                        jgo_val = _c(0)\n"
    "                        estado_p = _c(13)  # col N — ESTADO del partido\n"
    "                        # No modificar picks de partidos ya jugados\n"
    "                        if estado_p in ESTADOS_CERRADOS:\n"
    "                            continue\n"
    "                        for col_letter, col_idx in PICK_COLS:\n"
    "                            val = _c(col_idx)\n"
    "                            if val in placeholder_map:\n"
    "                                new_val = placeholder_map[val]\n"
    "                                pick_batch.append({\n"
    "                                    \"range\":  f\"{col_letter}{row_num}\",\n"
    "                                    \"values\": [[new_val]],\n"
    "                                })\n"
    "                                changes.append(\n"
    "                                    f\"[{tab_name}] JGO {jgo_val} {col_letter}: {val!r} → {new_val!r}\"\n"
    "                                )"
)

if OLD not in content:
    print("ERROR: bloque no encontrado — quizas ya fue aplicado o el archivo cambio.")
    sys.exit(1)

original_len = len(content)
content = content.replace(OLD, NEW, 1)

if len(content) < original_len * 0.95:
    print(f"ERROR: archivo reducido demasiado ({len(content)} vs {original_len}). Abortando.")
    sys.exit(1)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"OK. Lineas aprox: {content.count(chr(10))}")
print("Paso 3 de propagate_bracket ahora omite partidos ya FINAL/PRORROGA/PENALES.")
