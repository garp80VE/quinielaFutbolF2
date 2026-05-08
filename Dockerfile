# ── Quiniela WFC 2026 — Dockerfile para Railway ──────────────────────────────
# Python 3.12 + Node.js 20 en un solo contenedor.
# Railway monta un Volume en /data para persistir la sesión de Baileys.

FROM python:3.12-slim

# ── Sistema base ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gnupg \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── Directorio de trabajo ─────────────────────────────────────────────────────
WORKDIR /app

# ── Dependencias Python ───────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Dependencias Node ─────────────────────────────────────────────────────────
COPY package.json package-lock.json* ./
RUN npm install --omit=dev

# ── Código de la aplicación ───────────────────────────────────────────────────
COPY . .

# ── Script de arranque ────────────────────────────────────────────────────────
RUN chmod +x start.sh

# Railway inyecta PORT automáticamente (default 8000 si no viene)
EXPOSE 8000

CMD ["./start.sh"]
