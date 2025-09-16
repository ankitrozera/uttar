import requests, json, time
from datetime import datetime

# 🔐 OAuth credentials
CLIENT_ID = "737936576743-5dq4nrm7gemrhcks9k4rj5jb0i1futqh.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REFRESH_TOKEN = "1//04SZPj0Na1xgFCgYIARAAGAQSNwF-L9IrvlAvyrcEU5z2rVto6skNdq9MgFjQUAIPA7zfdJ6yhnT3zz77EpVEmXPCU7gWnSviCzo"
ACCESS_TOKEN = "ya29.a0AS3H6Nxk4L6qLjkxisU4QEfSvFFU3PyzNL5XFNRL1ZYhp1OLa4yVTavEeNTfjytwMJ4njQSVugnW-5sOV-araEOTpvUwxMDwXcuYc81YYoDVMXCchNMi2r98q-ztaCU4lnmvy4Ml1clfVqciuZY1KylSHTKkVlYEDVLrZXToexW4w4i97eElucopHsJ5GtdbVRtX34waCgYKAcUSARMSFQHGX2MimISWb5_lv3XIgKWZcpPozg0206"

# 📄 Sheet setup
SHEET_PREFIX = "UHBVN_Test_"
MAX_ROWS_PER_SHEET = 100000
current_sheet_index = 1
rows_written = 0
spreadsheet_ids = []

# 🔄 Refresh access token
def refresh_access_token():
    global ACCESS_TOKEN
    token_url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }
    res = requests.post(token_url, data=payload)
    if res.status_code == 200:
        ACCESS_TOKEN = res.json()["access_token"]
        print("🔄 Token refreshed.")
    else:
        print("❌ Token refresh failed:", res.text)

# ✅ Create new sheet
def create_new_sheet():
    global current_sheet_index
    url = "https://sheets.googleapis.com/v4/spreadsheets"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    body = {
        "properties": {
            "title": f"{SHEET_PREFIX}{current_sheet_index}"
        }
    }
    res = requests.post(url, headers=headers, data=json.dumps(body))
    if res.status_code == 200:
        sheet_id = res.json()["spreadsheetId"]
        spreadsheet_ids.append(sheet_id)
        print(f"📄 Sheet created: {sheet_id}")
        write_headers(sheet_id)
        current_sheet_index += 1
        return sheet_id
    else:
        print("❌ Sheet creation failed:", res.text)
        return None

# 📝 Write headers
def write_headers(sheet_id):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:append"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {"valueInputOption": "RAW"}
    data = {"values": [["timestamp"]]}
    requests.post(url, headers=headers, params=params, data=json.dumps(data))

# 📤 Write timestamp row
def write_timestamp():
    global rows_written
    if rows_written >= MAX_ROWS_PER_SHEET or not spreadsheet_ids:
        sheet_id = create_new_sheet()
    else:
        sheet_id = spreadsheet_ids[-1]

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:append"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {"valueInputOption": "RAW"}
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {"values": [[now]]}
    res = requests.post(url, headers=headers, params=params, data=json.dumps(data))
    if res.status_code == 200:
        rows_written += 1
        print(f"🕒 Logged: {now}")
    elif res.status_code == 401:
        refresh_access_token()
        write_timestamp()
    else:
        print("❌ Write failed:", res.text)

# 🚀 Main loop
def main():
    while True:
        write_timestamp()
        time.sleep(60)  # Wait 1 minute

main()
