"""
patch_performance.py — Pre-calienta cachés de prob y comparar en el background.
Ejecutar UNA sola vez desde la carpeta del proyecto:
    python patch_performance.py
"""
import re, sys, os

path = os.path.join(os.path.dirname(__file__), "webapp.py")

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

original_len = len(content)
changes = 0

# ── 1. _standings_async: también refresca prob y compare ─────────────────────
OLD1 = (
    "                def _standings_async():\n"
    "                    if not _standings_lock.acquire(blocking=False):\n"
    "                        return  # Ya hay un standings corriendo, saltarlo\n"
    "                    try:\n"
    "                        _update_standings()\n"
    "                        global _standings_last_update\n"
    "                        _standings_last_update = time.time()\n"
    "                    except Exception as e:\n"
    "                        print(f\"[standings-async] ERROR: {e}\")\n"
    "                    finally:\n"
    "                        _standings_lock.release()"
)
NEW1 = (
    "                def _standings_async():\n"
    "                    if not _standings_lock.acquire(blocking=False):\n"
    "                        return  # Ya hay un standings corriendo, saltarlo\n"
    "                    try:\n"
    "                        _update_standings()\n"
    "                        global _standings_last_update\n"
    "                        _standings_last_update = time.time()\n"
    "                        # Pre-calentar cachés de probabilidades y comparar\n"
    "                        try:\n"
    "                            result = _compute_probabilities()\n"
    "                            _cache[\"prob\"]    = result\n"
    "                            _cache[\"prob_ts\"] = time.time()\n"
    "                        except Exception as ep:\n"
    "                            print(f\"[standings-async] prob ERROR: {ep}\")\n"
    "                        try:\n"
    "                            _compute_compare_picks()\n"
    "                        except Exception as ec:\n"
    "                            print(f\"[standings-async] compare ERROR: {ec}\")\n"
    "                    except Exception as e:\n"
    "                        print(f\"[standings-async] ERROR: {e}\")\n"
    "                    finally:\n"
    "                        _standings_lock.release()"
)

if OLD1 in content:
    content = content.replace(OLD1, NEW1, 1)
    changes += 1
    print("✓ _standings_async actualizado")
else:
    print("✗ _standings_async — no encontrado (quizás ya aplicado)")

# ── 2. Reemplazar _standings_on_start por _warmup_caches ─────────────────────
OLD2 = (
    "    # Calcular standings al arrancar para poblar caché desde el inicio\n"
    "    def _standings_on_start():\n"
    "        time.sleep(3)   # esperar a que el updater loop arranque\n"
    "        try:\n"
    "            _update_standings()\n"
    "            global _standings_last_update\n"
    "            _standings_last_update = time.time()\n"
    "            print(\"[webapp] standings iniciales calculados\")\n"
    "        except Exception as e:\n"
    "            print(f\"[webapp] standings startup error: {e}\")\n"
    "    threading.Thread(target=_standings_on_start, daemon=True, name=\"standings-init\").start()"
)
NEW2 = (
    "    # Pre-calentar todas las cachés al arrancar (standings, prob, comparar)\n"
    "    def _warmup_caches():\n"
    "        time.sleep(3)\n"
    "        try:\n"
    "            _update_standings()\n"
    "            global _standings_last_update\n"
    "            _standings_last_update = time.time()\n"
    "            print(\"[webapp] standings iniciales calculados\")\n"
    "        except Exception as e:\n"
    "            print(f\"[webapp] standings startup error: {e}\")\n"
    "        try:\n"
    "            result = _compute_probabilities()\n"
    "            _cache[\"prob\"]    = result\n"
    "            _cache[\"prob_ts\"] = time.time()\n"
    "            print(\"[webapp] probabilidades iniciales calculadas\")\n"
    "        except Exception as e:\n"
    "            print(f\"[webapp] prob startup error: {e}\")\n"
    "        try:\n"
    "            _compute_compare_picks()\n"
    "            print(\"[webapp] comparar iniciales calculado\")\n"
    "        except Exception as e:\n"
    "            print(f\"[webapp] compare startup error: {e}\")\n"
    "    threading.Thread(target=_warmup_caches, daemon=True, name=\"warmup\").start()"
)

if OLD2 in content:
    content = content.replace(OLD2, NEW2, 1)
    changes += 1
    print("✓ _warmup_caches reemplaza _standings_on_start")
elif "_warmup_caches" in content:
    print("✓ _warmup_caches ya aplicado anteriormente")
else:
    print("✗ _standings_on_start — no encontrado")

# ── Guardar ───────────────────────────────────────────────────────────────────
if changes > 0:
    if len(content) < original_len * 0.9:
        print(f"ERROR: el contenido se redujo demasiado ({len(content)} vs {original_len}). Abortando.")
        sys.exit(1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n✅ {changes} cambio(s) aplicado(s). Líneas aprox: {content.count(chr(10))}")
else:
    print("\nℹ️  Sin cambios nuevos que aplicar.")
