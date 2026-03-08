import os
import time
import sqlite3
import requests
from flask import Flask

PORT = int(os.environ.get("PORT", 8080))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

KEYWORDS = [
"production assistant",
"production coordinator",
"graduate producer",
"studio runner",
"studio assistant",
"production trainee"
]

URLS = [
"https://boards-api.greenhouse.io/v1/boards/framestore/jobs",
"https://api.lever.co/v0/postings/nexusstudios"
]

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    requests.post(url, json=data)

def check_jobs():
    for url in URLS:
        try:
            r = requests.get(url).json()
            text = str(r).lower()
            for k in KEYWORDS:
                if k in text:
                    send_telegram(f"New possible job found:\n{url}")
        except:
            pass

def monitor():
    while True:
        check_jobs()
        time.sleep(21600)

app = Flask(__name__)

@app.route("/")
def home():
    return "Job monitor running"

if __name__ == "__main__":
    import threading
    threading.Thread(target=monitor).start()
    app.run(host="0.0.0.0", port=PORT)
