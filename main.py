import os
import time
import json
import sqlite3
import threading
from datetime import datetime

import requests
from flask import Flask

PORT = int(os.environ.get("PORT", 8080))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "600"))

app = Flask(__name__)

KEYWORDS = [
    "production assistant",
    "production coordinator",
    "junior production coordinator",
    "graduate producer",
    "junior producer",
    "assistant producer",
    "studio runner",
    "runner",
    "studio assistant",
    "production trainee",
    "production intern",
]

URLS = [
    "https://boards-api.greenhouse.io/v1/boards/framestore/jobs",
    "https://api.lever.co/v0/postings/nexusstudios",
]

DB_PATH = "jobs.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            company TEXT,
            url TEXT UNIQUE,
            source TEXT,
            found_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()


def db_execute(query, params=(), fetch=False, many=False):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if many:
        cur.executemany(query, params)
    else:
        cur.execute(query, params)
    rows = cur.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return rows


def set_state(key, value):
    db_execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
        (key, value)
    )


def get_state(key, default=""):
    rows = db_execute(
        "SELECT value FROM state WHERE key = ?",
        (key,),
        fetch=True
    )
    return rows[0][0] if rows else default


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception:
        pass


def telegram_api(method, payload=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    response = requests.post(url, json=payload or {}, timeout=30)
    response.raise_for_status()
    return response.json()


def get_updates(offset=None):
    payload = {"timeout": 20}
    if offset is not None:
        payload["offset"] = offset
    try:
        return telegram_api("getUpdates", payload).get("result", [])
    except Exception:
        return []


def parse_greenhouse_jobs(url):
    jobs = []
    try:
        data = requests.get(url, timeout=30).json()
        for job in data.get("jobs", []):
            jobs.append({
                "title": job.get("title", ""),
                "company": "Framestore",
                "url": job.get("absolute_url", ""),
                "source": url,
            })
    except Exception:
        pass
    return jobs


def parse_lever_jobs(url):
    jobs = []
    try:
        data = requests.get(url, timeout=30).json()
        for job in data:
            jobs.append({
                "title": job.get("text", ""),
                "company": "Nexus Studios",
                "url": job.get("hostedUrl", ""),
                "source": url,
            })
    except Exception:
        pass
    return jobs


def fetch_jobs():
    all_jobs = []
    for url in URLS:
        if "greenhouse" in url:
            all_jobs.extend(parse_greenhouse_jobs(url))
        elif "lever" in url:
            all_jobs.extend(parse_lever_jobs(url))
    return all_jobs


def matches_keywords(title):
    title_lower = (title or "").lower()
    return any(keyword in title_lower for keyword in KEYWORDS)


def save_job(job):
    now = datetime.utcnow().isoformat()
    try:
        db_execute(
            "INSERT INTO jobs (title, company, url, source, found_at) VALUES (?, ?, ?, ?, ?)",
            (job["title"], job["company"], job["url"], job["source"], now)
        )
        return True
    except Exception:
        return False


def check_jobs():
    jobs = fetch_jobs()
    new_jobs = []

    for job in jobs:
        if not job.get("title") or not job.get("url"):
            continue
        if not matches_keywords(job["title"]):
            continue
        inserted = save_job(job)
        if inserted:
            new_jobs.append(job)

    if new_jobs:
        lines = ["New matching jobs found:\n"]
        for job in new_jobs[:5]:
            lines.append(f"{job['title']} — {job['company']}\n{job['url']}\n")
        send_telegram_message("\n".join(lines))

    set_state("last_checked", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    set_state("last_found_count", str(len(new_jobs)))


def format_jobs(rows):
    if not rows:
        return "No matching jobs found yet."

    lines = []
    for i, row in enumerate(rows, start=1):
        title, company, url, found_at = row
        lines.append(f"{i}. {title} — {company}\n{url}\nFound: {found_at}\n")
    return "\n".join(lines[:10])


def handle_command(text):
    text = (text or "").strip()

    if text == "/help":
        return (
            "Commands:\n"
            "/help - show commands\n"
            "/jobs - latest matching jobs\n"
            "/search <term> - search stored jobs\n"
            "/status - bot status\n"
            "/keywords - show tracked keywords"
        )

    if text == "/jobs":
        rows = db_execute(
            "SELECT title, company, url, found_at FROM jobs ORDER BY id DESC LIMIT 10",
            fetch=True
        )
        return format_jobs(rows)

    if text.startswith("/search "):
        term = text.replace("/search ", "", 1).strip()
        rows = db_execute(
            """
            SELECT title, company, url, found_at
            FROM jobs
            WHERE lower(title) LIKE ? OR lower(company) LIKE ?
            ORDER BY id DESC
            LIMIT 10
            """,
            (f"%{term.lower()}%", f"%{term.lower()}%"),
            fetch=True
        )
        if not rows:
            return f'No saved jobs found for "{term}".'
        return format_jobs(rows)

    if text == "/status":
        last_checked = get_state("last_checked", "Not checked yet")
        last_found_count = get_state("last_found_count", "0")
        count_rows = db_execute("SELECT COUNT(*) FROM jobs", fetch=True)
        total_jobs = count_rows[0][0] if count_rows else 0
        return (
            f"Bot status: running\n"
            f"Last checked: {last_checked}\n"
            f"New matches on last check: {last_found_count}\n"
            f"Saved jobs: {total_jobs}\n"
            f"Check interval: {CHECK_INTERVAL_SECONDS} seconds"
        )

    if text == "/keywords":
        return "Tracked keywords:\n" + "\n".join(f"- {k}" for k in KEYWORDS)

    return 'Unknown command. Send /help'


def command_loop():
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message", {})
                chat = message.get("chat", {})
                chat_id = str(chat.get("id", ""))
                text = message.get("text", "")

                if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                    continue

                if text.startswith("/"):
                    reply = handle_command(text)
                    if reply:
                        send_telegram_message(reply)
        except Exception:
            pass
        time.sleep(3)


def monitor_loop():
    while True:
        try:
            check_jobs()
        except Exception:
            pass
        time.sleep(CHECK_INTERVAL_SECONDS)


@app.route("/")
def home():
    return "Job monitor running", 200


@app.route("/health")
def health():
    return {"status": "ok"}, 200


init_db()

_started = False

def start_background_threads():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=command_loop, daemon=True).start()
    send_telegram_message("Bot started successfully.")

start_background_threads()
