"""
patch_bracket_fix2.py — Paso 3 de propagate_bracket:
- Nunca reemplazar PICK_GANADOR (col J) — es la prediccion del jugador
- Solo actualizar PICK_EQ1 y PICK_EQ2 (para mostrar equipos reales en pantalla)
- Tampoco tocar picks de partidos ya FINAL/PRORROGA/PENALES

Ejecutar una sola vez:
    python patch_bracket_fix2.py
"""
import os, sys

path = os.path.join(os.path.dirname(__file__), "webapp.py")
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# ── Fix 1: quitar PICK_GANADOR de PICK_COLS ───────────────────────────────────
OLD1 = (
    "            # Columnas de picks: F=PICK_EQ1(5), I=PICK_EQ2(8), J=PICK_GANADOR(9) — 0-indexed\n"
    "            # Leemos hasta col N (estado, idx 13) para no tocar picks de partidos ya FINAL\n"
    "            PICK_COLS = [(\"F\", 5), (\"I\", 8), (\"J\", 9)]\n"
    "            ESTADOS_CERRADOS = (\"FINAL\", \"PRORROGA\", \"PENALES\")"
)
NEW1 = (
    "            # Solo actualizar nombres de equipos (PICK_EQ1/EQ2) para display.\n"
    "            # NUNCA tocar PICK_GANADOR (J) — es la prediccion del jugador.\n"
    "            # Tampoco tocar picks de partidos ya jugados.\n"
    "            PICK_COLS = [(\"F\", 5), (\"I\", 8)]  # EQ1 y EQ2 solo, NO ganador\n"
    "            ESTADOS_CERRADOS = (\"FINAL\", \"PRORROGA\", \"PENALES\")"
)

if OLD1 in content:
    content = content.replace(OLD1, NEW1, 1)
    print("✓ PICK_GANADOR removido de PICK_COLS")
else:
    # Puede que el patch anterior no haya incluido el comentario exacto
    OLD1b = (
        "            # Columnas de picks: F=PICK_EQ1(5), I=PICK_EQ2(8), J=PICK_GANADOR(9) — 0-indexed\n"
        "            PICK_COLS = [(\"F\", 5), (\"I\", 8), (\"J\", 9)]\n"
    )
    NEW1b = (
        "            # Solo actualizar nombres de equipos (PICK_EQ1/EQ2) para display.\n"
        "            # NUNCA tocar PICK_GANADOR (J) — es la prediccion del jugador.\n"
        "            PICK_COLS = [(\"F\", 5), (\"I\", 8)]  # EQ1 y EQ2 solo, NO ganador\n"
    )
    if OLD1b in content:
        content = content.replace(OLD1b, NEW1b, 1)
        print("✓ PICK_GANADOR removido de PICK_COLS (variante b)")
    else:
        # Check current state
        if 'PICK_COLS = [("F", 5), ("I", 8)]' in content:
            print("✓ PICK_GANADOR ya estaba removido")
        else:
            print("✗ No se encontro PICK_COLS — revisar manualmente")
            sys.exit(1)

original_len = len(content)
if len(content) < original_len * 0.95:
    print(f"ERROR: archivo reducido demasiado. Abortando.")
    sys.exit(1)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

lines = content.count('\n')
print(f"OK. Lineas aprox: {lines}")
print("propagate_bracket ya NO modifica PICK_GANADOR de ningun jugador.")
