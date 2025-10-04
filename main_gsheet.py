import os, json, requests
from datetime import datetime, timezone, timedelta
import gspread

JST = timezone(timedelta(hours=9))
HEADERS = ["text","image_url","alt_text","link_attachment",
           "reply_control","topic_tag","location_id",
           "status","posted_at","error"]

def open_ws(sheet_url: str, sheet_tab: str):
    gc = gspread.service_account()  # ~/.config/gspread/service_account.json を自動参照
    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(sheet_tab) if sheet_tab else sh.sheet1
    first = ws.row_values(1)
    if [c.strip().lower() for c in first] != HEADERS:
        ws.update('A1', [HEADERS])
    return ws

def find_next_row(ws):
    records = ws.get_all_records(default_blank='')
    for idx, row in enumerate(records, start=2):
        text = (row.get("text") or "").strip()
        status = (row.get("status") or "").strip().lower()
        if text and status not in ("posted","done","済","posted✅"):
            return idx, row
    return None, None

def post_text(token: str, user_id: str, text: str):
    url = f"https://graph.threads.net/v1.0/{user_id}/threads"
    payload = {"media_type": "TEXT", "text": text, "auto_publish_text": True}
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}",
                                       "Content-Type": "application/json"},
                         data=json.dumps(payload), timeout=30)
    if resp.status_code >= 400:
        raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json()

def main():
    token = os.environ.get("THREADS_ACCESS_TOKEN")
    user_id = os.environ.get("THREADS_USER_ID")
    sheet_url = os.environ.get("SHEET_URL")
    sheet_tab = os.environ.get("SHEET_TAB","")

    assert token and user_id and sheet_url, "Missing env: THREADS_ACCESS_TOKEN / THREADS_USER_ID / SHEET_URL"

    ws = open_ws(sheet_url, sheet_tab)
    row_idx, row = find_next_row(ws)
    if not row_idx:
        print(json.dumps({"ok": True, "skipped": "no-row"}))
        return

    text = row.get("text","").strip()
    try:
        _ = post_text(token, user_id, text)
        now = datetime.now(JST).isoformat(timespec="seconds")
        ws.update_cell(row_idx, HEADERS.index("status")+1, "posted")
        ws.update_cell(row_idx, HEADERS.index("posted_at")+1, now)
        ws.update_cell(row_idx, HEADERS.index("error")+1, "")
        print(json.dumps({"ok": True, "row_idx": row_idx, "text": text}))
    except Exception as e:
        ws.update_cell(row_idx, HEADERS.index("status")+1, "failed")
        ws.update_cell(row_idx, HEADERS.index("error")+1, str(e)[:500])
        print(json.dumps({"ok": False, "row_idx": row_idx, "err": str(e)[:500]}))

if __name__ == "__main__":
    main()
