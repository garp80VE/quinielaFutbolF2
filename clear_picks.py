"""
Borra todas las pestañas de jugadores del Sheet F2.
Conserva: HORARIOS, JUGADORES, POSICIONES, CONFIG
"""
import gspread
from google.oauth2.service_account import Credentials

SCOPES    = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SHEET_ID  = "16K7YJeSZfChlDkoLOAYdNlQSmTs1cRH5ZsJKtewPXc4"
CREDS_PATH = "credentials.json"
BASE_SHEETS = {"HORARIOS", "JUGADORES", "POSICIONES", "CONFIG"}

creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
gc    = gspread.authorize(creds)
sh    = gc.open_by_key(SHEET_ID)

worksheets = sh.worksheets()
print(f"Pestañas encontradas: {[ws.title for ws in worksheets]}")

to_delete = [ws for ws in worksheets if ws.title not in BASE_SHEETS]
if not to_delete:
    print("✅ No hay pestañas de jugadores — ya está limpio")
else:
    for ws in to_delete:
        sh.del_worksheet(ws)
        print(f"  ❌ Borrada: {ws.title}")
    print("✅ Listo — solo quedan pestañas base")
