"""
patch_bracket_fix3.py — Deshabilita completamente el Paso 3 de propagate_bracket.
Los picks de los jugadores (EQ1, EQ2, GANADOR) no deben ser modificados nunca
por el sistema — solo el jugador puede cambiar sus propios picks.

Ejecutar una sola vez:
    python patch_bracket_fix3.py
"""
import os, sys

path = os.path.join(os.path.dirname(__file__), "webapp.py")
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

OLD = '            PICK_COLS = [("F", 5), ("I", 8)]  # EQ1 y EQ2 solo, NO ganador'
NEW = '            PICK_COLS = []  # Paso 3 deshabilitado: los picks del jugador son intocables'

if OLD not in content:
    if 'PICK_COLS = []' in content:
        print("✓ Paso 3 ya estaba deshabilitado")
        sys.exit(0)
    print("✗ No se encontro PICK_COLS — revisar manualmente")
    sys.exit(1)

content = content.replace(OLD, NEW, 1)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"OK. Lineas aprox: {content.count(chr(10))}")
print("Paso 3 deshabilitado — picks de jugadores completamente intocables.")
