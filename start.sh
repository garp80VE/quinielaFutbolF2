#!/bin/bash
# ── Quiniela WFC 2026 — Script de arranque ────────────────────────────────────
# Inicia el servidor Baileys (Node) en background y luego la webapp (Python).
# Compatible con Railway, Fly.io y ejecución local.
#
# Variables de entorno esperadas en producción (Fly.io secrets):
#   GOOGLE_CREDENTIALS  — contenido del credentials.json (JSON como texto)
#   SHEET_ID            — ID del Google Sheet
#   PORT                — puerto HTTP (Fly.io lo inyecta automáticamente)

set -e

# ── Restaurar credentials.json desde variable de entorno ─────────────────────
if [ -n "$GOOGLE_CREDENTIALS" ]; then
  echo "$GOOGLE_CREDENTIALS" > /app/credentials.json
  echo "[start] credentials.json restaurado desde GOOGLE_CREDENTIALS"
fi

# ── Ruta de sesión: /data si existe (Fly.io / Railway Volume), si no local ────
if [ -d "/data" ]; then
  export SESSION_PATH="/data/baileys_session"
  echo "[start] Volumen detectado — sesión en $SESSION_PATH"
else
  export SESSION_PATH="$(pwd)/baileys_session"
  echo "[start] Local — sesión en $SESSION_PATH"
fi

export WA_PORT="${WA_PORT:-3001}"
WEBAPP_PORT="${PORT:-8000}"

# ── Construir argumentos para webapp.py ───────────────────────────────────────
EXTRA_ARGS="--port $WEBAPP_PORT"
if [ -n "$SHEET_ID" ]; then
  EXTRA_ARGS="$EXTRA_ARGS --sheet $SHEET_ID"
fi

echo "[start] Iniciando servidor Baileys (Node) en puerto $WA_PORT..."
node baileys-server.js &
BAILEYS_PID=$!
echo "[start] Baileys PID: $BAILEYS_PID"

# Esperar que Baileys esté listo
sleep 3

echo "[start] Iniciando webapp Python en puerto $WEBAPP_PORT..."
python webapp.py $EXTRA_ARGS

# Si Python termina, matar Baileys también
echo "[start] Python terminó, cerrando Baileys..."
kill $BAILEYS_PID 2>/dev/null || true
