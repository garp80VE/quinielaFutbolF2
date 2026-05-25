"""
patch_index_pick_names.py — En rondas R16+, mostrar siempre los equipos
que el JUGADOR predijo (pick_eq1/pick_eq2), no los equipos reales de HORARIOS.

Ejecutar una sola vez:
    python patch_index_pick_names.py
"""
import os, sys

path = os.path.join(os.path.dirname(__file__), "index.html")
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

OLD = '    if (locked && isFinal && g.ronda && g.ronda !== \'R32\') {\n      if (_pickIsRealName(p.eq1)) rEq1 = p.eq1;\n      if (_pickIsRealName(p.eq2)) rEq2 = p.eq2;\n    }'

NEW = '    if (g.ronda && g.ronda !== \'R32\') {\n      if (_pickIsRealName(p.eq1)) rEq1 = p.eq1;\n      if (_pickIsRealName(p.eq2)) rEq2 = p.eq2;\n    }'

if OLD not in content:
    if "if (g.ronda && g.ronda !== 'R32') {" in content:
        print("✓ Patch ya estaba aplicado")
        sys.exit(0)
    print("✗ No se encontro el bloque — revisar manualmente")
    sys.exit(1)

original_len = len(content)
content = content.replace(OLD, NEW, 1)

if len(content) < original_len * 0.95:
    print(f"ERROR: archivo reducido demasiado. Abortando.")
    sys.exit(1)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"OK. Lineas aprox: {content.count(chr(10))}")
print("Fix aplicado: equipos del jugador siempre visibles en R16+.")
