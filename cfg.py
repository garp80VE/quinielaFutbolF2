"""
cfg.py — Carga configuración desde quiniela.cfg
Todos los scripts del proyecto importan esto.

Prioridad: argumento CLI > quiniela.cfg > valor por defecto
"""

import argparse
from pathlib import Path

_DEFAULTS = {
    "SHEET_ID": "",
    "CREDS":    "credentials.json",
    "INTERVAL": "60",
    "TZ":       "-6",
    "PORT":     "8000",
}

def _load_cfg_file() -> dict:
    """Lee quiniela.cfg del mismo directorio que este archivo."""
    cfg_path = Path(__file__).parent / "quiniela.cfg"
    result = {}
    if not cfg_path.exists():
        return result
    for line in cfg_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip()
    return result


def load(description: str = "Quiniela WFC 2026", extra_args: list = None) -> argparse.Namespace:
    """
    Parsea argumentos CLI con fallback a quiniela.cfg.

    extra_args: lista de dicts con kwargs para parser.add_argument()
    Retorna Namespace con: sheet, creds, interval, tz, port + cualquier extra.
    """
    file_cfg = _load_cfg_file()

    def get(key: str) -> str:
        return file_cfg.get(key, _DEFAULTS.get(key, ""))

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--sheet",    default=get("SHEET_ID"),
                        help=f"ID del Google Sheet (default: quiniela.cfg)")
    parser.add_argument("--creds",    default=get("CREDS"),
                        help=f"Ruta a credentials.json (default: {get('CREDS')})")
    parser.add_argument("--interval", type=int, default=int(get("INTERVAL")),
                        help=f"Intervalo en segundos (default: {get('INTERVAL')})")
    parser.add_argument("--tz",       type=float, default=float(get("TZ")),
                        help=f"UTC offset (default: {get('TZ')})")
    parser.add_argument("--port",     type=int, default=int(get("PORT")),
                        help=f"Puerto del webapp (default: {get('PORT')})")

    if extra_args:
        for kwargs in extra_args:
            parser.add_argument(**kwargs)

    args = parser.parse_args()

    if not args.sheet:
        parser.error(
            "No se encontró SHEET_ID. Agrégalo en quiniela.cfg o pásalo con --sheet ID"
        )

    return args
