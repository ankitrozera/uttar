import requests, json, random, time, re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# 🔐 OAuth credentials aaa
# =======================================================================
CLIENT_ID = ""
CLIENT_SECRET = ""
REFRESH_TOKEN = ""
ACCESS_TOKEN = ""

# 🌐 Target URL
url = "https://epayment.uhbvn.org.in/b2cpaybilladvance.aspx"

DATA_SHEET_ID = ""
DATA_RANGE = "sheet1!A1:B4"

SHEET_PREFIX = "HBVN_Accounts_"    # 📄 Base name for generated sheets
current_sheet_index = 1
MAX_SHEETS = 99                    # 📚 Total sheet limit
MAX_ACCOUNTS_PER_SHEET = 1001   # 💾 Max rows per sheet
accounts_written = 0
spreadsheet_ids = []

BATCH_SIZE = 100                   # 📦 Accounts per batch
THREAD_WORKERS = 50                # 🧵 Parallel workers per batch
RETRY_DELAY = 300                  # ⏱ Retry delay in seconds (5 min)

# These are serial no not account no be carefull
base_start_serial = 999799                    #9999999750     # 🔢 Starting serial
end_serial        = 999999     # 🔢 Ending serial no 

# 📋 Sheet headers
sheet_headers = ["account", "name", "address", "load", "error"]

# 🧠 Hidden form fields
# ====================================================================
hidden_fields = {
    "__EVENTTARGET": "",
    "__EVENTARGUMENT": "",
    "__VIEWSTATE": "/wEPDwULLTExOTgxNTI1NjIPZBYCAgIPZBYEAgUPZBYCAgsPDxYCHgRUZXh0ZRYEHgpvbmtleXByZXNzBUtOdW1lcmljQm94X05TX0FkZE51bWVyaWNJdGVtKGV2ZW50LCAndHh0bW9iaWxlJywgdHJ1ZSwgZmFsc2UsIC0xLCAnLicsIC0xKTseCG9uY2hhbmdlBR5OdW1lcmljQm94X05TX3R4dG1vYmlsZShldmVudClkAgcPZBYCAgkPDxYCHwBlFgQfAQVLTnVtZXJpY0JveF9OU19BZGROdW1lcmljSXRlbShldmVudCwgJ3R4dEFtb3VudCcsIHRydWUsIGZhbHNlLCAtMSwgJy4nLCAtMSk7HwIFHk51bWVyaWNCb3hfTlNfdHh0QW1vdW50KGV2ZW50KWQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgUFCEJJTExERVNLBQNVQk8FB05lZnRZZXMFCU5lZnRJQ0lDSQUHTmVmdEJPQqb7AjaVUj4qg1vdjSgYCrIbcdprNCQUX1asAMIP7ATV",
    "__VIEWSTATEGENERATOR": "3D155CCF",
    "__EVENTVALIDATION": "/wEdABgH1QjLfaObk6/QiX81+tYxwqX48iQ/BOEYRUcG2kRxl8H3fvDr2EvDlueXpV8GeO0xJq7FPlKOHFBhS9vJjXHQwsQHZSgtak0GckVf30r3B3rOuTNWtuI4oTnTGbHrm6J9eoWt1VYsoV8N3B0z4auFH3nra93OB915opICXNmZSM34O/GfAV4V4n0wgFZHr3cJCINU3Zf7QS6Q8BtN2AgBS0yGv7RCxzINwXnW/9iE4uPrUuXgyiivfy73LCwMG2kzSJ/Ho+75/W5JrDcM9jIOH3EXzc6QJnG5nW8rKBPubzKclef9U7xGFg9Mgl55r8gcNlpvAow/ZI3vZE6GyfxIohlx63WKm+RO6PiKFDpe77v0X2PzWW66XPpYQtFb5Ou5ztp/aHnGzXf5V3ZlVWBGh1LSWKtqdpJdGpg/AJzErSM8mLwg8DTtPr4t4Wlfofe0Gg0NN/WnbR+w/IO0ihXTCneGDFU2v/lin3Lo4qVUHG8198tKhPvb8nM/HnIE5KYms/0ydnbCi6r2DdV4DamqM8PX/d7kFXH0kuxAL59G/w=="
}    
# ===============================================================

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
        print("🔄 Access token refreshed.")
    else:
        print("❌ Token refresh failed:", res.text)

def safe_request(method, url, headers=None, **kwargs):
    if headers is None:
        headers = {}
    res = requests.request(method, url, headers=headers, **kwargs)
    if res.status_code == 401:  # Token expired
        print("⚠️ Access token expired. Refreshing...")
        refresh_access_token()
        headers["Authorization"] = f"Bearer {ACCESS_TOKEN}"
        res = requests.request(method, url, headers=headers, **kwargs)
    
    return res

# DATA_SHEET_ID = ""
# DATA_RANGE = "sheet1!A1:B4"  # A1=last_serial, B1=sheet_id

def read_data():
    """Read last_serial, sheet_id, accounts_written, sheet_index from data sheet safely."""
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{DATA_SHEET_ID}/values/{DATA_RANGE}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    res = safe_request("GET", url, headers=headers)

    # Default safe values
    last_serial = 0
    sheet_id = None
    accounts_written = 0
    sheet_index = 1

    if res.status_code == 200:
        values = res.json().get("values", [])
        if len(values) > 0 and len(values[0]) > 1:
            try:
                last_serial = int(values[0][1])
            except:
                last_serial = 0
        if len(values) > 1 and len(values[1]) > 1:
            sheet_id = values[1][1]
        if len(values) > 2 and len(values[2]) > 1:
            try:
                accounts_written = int(values[2][1])
            except:
                accounts_written = 0
        if len(values) > 3 and len(values[3]) > 1:
            try:
                sheet_index = int(values[3][1])
            except:
                sheet_index = 1
    else:
        update_data(0, None, 0, 1)

    return last_serial, sheet_id, accounts_written, sheet_index

def update_data(last_serial, sheet_id, accounts_written, sheet_index):
    """Update last_serial and current sheet_id in data sheet."""
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{DATA_SHEET_ID}/values/{DATA_RANGE}?valueInputOption=RAW"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    data = {
        "values": [
            ["last_serial", str(last_serial)],
            ["sheet_id", sheet_id or ""],
            ["accounts_written", str(accounts_written)],
            ["current_sheet_index", str(sheet_index)]
        ]
    }
    res =safe_request("PUT", url, headers=headers, data=json.dumps(data))
    if res.status_code == 200:
        print(f"✅ Metadata updated → last_serial={last_serial}, sheet_id={sheet_id}")
    else:
        print("❌ Metadata update failed:", res.text)

# ✅ Create new Google Sheet
def create_new_sheet():
    global current_sheet_index, start_serial, accounts_written
    if current_sheet_index < 1:
        current_sheet_index = 1
    current_sheet_index += 1  # ✅ always increment before creating new sheet
    accounts_written = 0
    url = "https://sheets.googleapis.com/v4/spreadsheets"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    body = {"properties": {"title": f"{SHEET_PREFIX}{current_sheet_index}"}}
    res = safe_request("POST", url, headers=headers, data=json.dumps(body))
    if res.status_code == 200:
        sheet_id = res.json()["spreadsheetId"]
        spreadsheet_ids.append(sheet_id)
        print(f"📄 Sheet created: {sheet_id}")
        write_headers(sheet_id)
        update_data(last_serial=start_serial - 1, sheet_id=sheet_id, accounts_written=0, sheet_index=current_sheet_index)
        # update_data(start_serial - 1, sheet_id, 0, current_sheet_index)
        return sheet_id
    else:
        print("❌ Sheet creation failed even after token refresh:", res.text)
        return None

# 📝 Write headers
def write_headers(sheet_id):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:append"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {"valueInputOption": "RAW"}
    data = {"values": [sheet_headers]}
    requests.post(url, headers=headers, params=params, data=json.dumps(data))

# 📤 Write batch to sheet
def write_batch_to_sheet(rows):
    global accounts_written, current_sheet_index
    remaining_rows = rows[:]
    while remaining_rows:
        # If no sheet or current sheet is full → create new
        if not spreadsheet_ids or accounts_written >= MAX_ACCOUNTS_PER_SHEET:
            sheet_id = create_new_sheet()
            accounts_written = 0  # reset after new sheet
        else:
            sheet_id = spreadsheet_ids[-1]
        if not sheet_id:
            print("❌ Sheet creation failed, stopping batch.")
            break    

        # how many rows can fit in current sheet
        can_fit = MAX_ACCOUNTS_PER_SHEET - accounts_written
        to_write = remaining_rows[:can_fit]
        remaining_rows = remaining_rows[can_fit:]

        url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:append"
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        params = {"valueInputOption": "RAW"}
        data = {"values": to_write}

        res = safe_request("POST", url, headers=headers, params=params, data=json.dumps(data))
        if res.status_code == 200:
            accounts_written += len(to_write)
            print(f"📊 {len(to_write)} rows written to sheet {sheet_id}.")
            last_serial = start_serial + len(rows) - len(remaining_rows) - 1
            update_data(last_serial, sheet_id, accounts_written, current_sheet_index)
        else:
            print(f"❌ Sheet write failed: {res.text}")
            break

# 🧵 Process one account
def process_account(account):
    captcha = str(random.randint(1000, 9999))
    payload = hidden_fields.copy()
    payload.update({
        "txtacntnumber": account,
        "txtPPPNo": "",
        "txtmobile": "",
        "txtemail": "",
        "txtcaptcha": captcha,
        "txtAdhaarNo": "",
        "btnsubmit": "Proceed",
        "lblAcNo": "",
        "lblConsumerName": "",
        "lblAddress": "",
        "lblLoad": "",
        "txtAmount": "",
        "txtTransactionID": "",
        "txtIsConfirmed": "",
        "txtRegisteredMob": "",
        "txtIsMsgSend": ""
    })

    try:
        res = requests.post(url, headers={"Origin": "", "Referer": ""}, data=payload, timeout=15)

        # Step 1️⃣ – Quick server check
        if res.status_code != 200:
            return [account, "", "", "", f"SERVER_DOWN_{res.status_code}"]
        html = res.text

        # Step 2️⃣ – Directly identify alert vs data using <div id="part1">
        if '<div id="part1" style="display:none">' in html:
            def extract(tag):
                start = html.find(f'id="{tag}"')
                if start == -1:
                    return ""
                start = html.find('value="', start)
                if start == -1:
                    return ""
                end = html.find('"', start + 7)
                return html[start + 7:end].strip()

            soup = BeautifulSoup(html, "html.parser")
            consumer_name = soup.find(id="lblConsumerName").get("value", "").strip() if soup.find(id="lblConsumerName") else ""
            address = soup.find(id="lblAddress").text.strip() if soup.find(id="lblAddress") else ""
            load = soup.find(id="lblLoad").get("value", "").strip() if soup.find(id="lblLoad") else ""
            return [account, consumer_name, address, load, ""]
        
        # ⚡ 3️⃣ If data not found, check for alert section (direct alert read)
        elif '<div id="part1">' in html:
            # Extract exact alert message from CDATA
            start = html.find("alert('")
            if start != -1:
                end = html.find("');", start)
                alert_msg = html[start + 7:end].strip()
            else:
                alert_msg = "Unknown alert"

            return [account, "", "", "", alert_msg]

        else:
            # ❓ Unexpected HTML (neither alert nor data)
            return [account, "", "", "", "UNRECOGNIZED_PAGE"]

    except Exception as e:
        return [account, "", "", "", f"EXCEPTION: {e}"]

# base_start_serial = 9999999750    
# end_serial   = 9999999999 

def generate_next_batch(start_serial, count=BATCH_SIZE):
    batch = []
    for i in range(count):
        next_num = start_serial + i
        if next_num > end_serial:
            break
        # batch.append(f"{str(next_num).zfill(10)}")
        batch.append(f"{str(next_num).zfill(6)}2000")  # for 2000 series
    return batch

# 🔁 Retry loop
def retry_loop(start_serial):
    delay = RETRY_DELAY  # 5 minutes
    while True:
        batch = generate_next_batch(start_serial)
        with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
            results = list(executor.map(process_account, batch))
        # for r in results[:3]:
        #     print("DEBUG →", r)    
        valid = []
        for r in results:
            msg = str(r[4]).lower()
            if "server_error" not in msg and "exception" not in msg:
                valid.append(r)
            elif "no bill" in msg or "unable to fetch" in msg:
                valid.append(r)
            elif msg == "":
                valid.append(r)
        
        if valid:
            write_batch_to_sheet(results)
            # ✅ Batch successful hone ke baad last serial update karo
            last_serial = start_serial + len(results) - 1
            update_data(last_serial, spreadsheet_ids[-1], accounts_written, current_sheet_index)

            print(f"✅ Batch written & last_serial updated to {last_serial}")
            return last_serial + 1
        else:
            print(f"⚠️ Server down or all failed. Retrying in {delay//60} min...")
            time.sleep(delay)
            delay = min(delay * 2, 3600)  # Max 1 hour
            
# 🚀 Main flow
def main():
    global start_serial, current_sheet_index, accounts_written
    last_serial, sheet_id, accounts_written, current_sheet_index = read_data()
    if accounts_written < 0:
        accounts_written = 0
    if current_sheet_index < 1:
        current_sheet_index = 1

    if sheet_id:
        spreadsheet_ids.append(sheet_id)
    else:
        current_sheet_index = 1    

    start_serial = last_serial + 1 if last_serial else base_start_serial

    print(f"🚀 Starting main loop from serial: {start_serial}")
    while current_sheet_index <= MAX_SHEETS and start_serial <= end_serial:
        start_serial = retry_loop(start_serial)

main()

