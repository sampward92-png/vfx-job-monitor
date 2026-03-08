import os
import re
import time
import json
import sqlite3
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from flask import Flask

PORT = int(os.environ.get("PORT", "8080"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "600"))
DB_PATH = "jobs.db"

APP_TZ = timezone.utc
app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VFXJobMonitor/2.0; +https://railway.app)"
}

DEFAULT_KEYWORDS = [
    "production assistant",
    "production coordinator",
    "junior production coordinator",
    "graduate producer",
    "assistant producer",
    "junior producer",
    "production trainee",
    "studio runner",
    "runner",
    "studio assistant",
    "production intern",
    "project coordinator",
    "project assistant",
]

DEFAULT_EXCLUDES = [
    "senior",
    "supervisor",
    "head of",
    "director",
    "lead ",
    "principal",
    "executive producer",
]

UK_STUDIO_COMPANIES = {
    "Framestore",
    "Nexus Studios",
    "DNEG",
    "Cinesite",
    "Blue Zoo",
    "Jellyfish Pictures",
}

SOURCES = [
    {
        "name": "Framestore Recruitee",
        "company": "Framestore",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://framestore.recruitee.com/",
    },
    {
        "name": "Framestore Careers",
        "company": "Framestore",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://www.framestore.com/careers",
    },
    {
        "name": "Nexus Studios Workable",
        "company": "Nexus Studios",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://apply.workable.com/nexusstudios/",
    },
    {
        "name": "Nexus Studios Teamtailor",
        "company": "Nexus Studios",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://nexusstudios.teamtailor.com/",
    },
    {
        "name": "DNEG Jobvite",
        "company": "DNEG",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://jobs.jobvite.com/double-negative-visual-effects/jobs",
    },
    {
        "name": "DNEG Careers",
        "company": "DNEG",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://www.dneg.com/careers/open-positions",
    },
    {
        "name": "Cinesite Job Vacancies",
        "company": "Cinesite",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://cinesite.com/job-vacancies/",
    },
    {
        "name": "Blue Zoo Careers",
        "company": "Blue Zoo",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://careers.blue-zoo.co.uk/vacancies/vacancy-search-results.aspx?view=grid",
    },
    {
        "name": "Jellyfish Pictures Workable",
        "company": "Jellyfish Pictures",
        "kind": "studio",
        "priority": 1,
        "type": "html",
        "url": "https://apply.workable.com/jellyfish-pictures-ltd/?lng=en",
    },
    {
        "name": "ScreenSkills Jobs",
        "company": "ScreenSkills",
        "kind": "industry_board",
        "priority": 2,
        "type": "html",
        "url": "https://www.screenskills.com/jobs/",
    },

    # Adapters below are ready for future source additions:
    # {"name": "Example Greenhouse", "company": "Example", "kind": "studio", "priority": 1, "type": "greenhouse", "slug": "example"},
    # {"name": "Example Lever", "company": "Example", "kind": "studio", "priority": 1, "type": "lever", "company_slug": "example"},
    # {"name": "Example SmartRecruiters", "company": "Example", "kind": "studio", "priority": 1, "type": "smartrecruiters", "company_id": "Example"},
    # {"name": "Example Workday", "company": "Example", "kind": "studio", "priority": 1, "type": "workday", "api_url": "https://example.wd1.myworkdayjobs.com/wday/cxs/example/careers/jobs"},
]

_started = False


def utc_now():
    return datetime.now(APP_TZ)


def now_str():
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    cleaned = parsed._replace(query=urlencode(query), fragment="")
    return urlunparse(cleaned)


def clean_text(text: str) -> str:
    text = text or ""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_text(text: str) -> str:
    return clean_text(text).lower()


def short_hash(*parts: str) -> str:
    joined = "|".join(normalize_text(p) for p in parts if p)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:24]


def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def db_execute(query, params=(), fetch=False):
    conn = db()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return rows


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key TEXT UNIQUE,
            canonical_url TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            url TEXT,
            source_name TEXT,
            source_kind TEXT,
            source_priority INTEGER,
            source_type TEXT,
            first_seen TEXT,
            last_seen TEXT,
            matched_keyword TEXT,
            raw_blob TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key TEXT,
            source_name TEXT,
            source_type TEXT,
            url TEXT,
            seen_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            keyword TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS excludes (
            phrase TEXT PRIMARY KEY
        )
    """)

    conn.commit()
    conn.close()

    seed_defaults()


def seed_defaults():
    existing_keywords = db_execute("SELECT keyword FROM keywords", fetch=True)
    if not existing_keywords:
        for kw in DEFAULT_KEYWORDS:
            db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))

    existing_excludes = db_execute("SELECT phrase FROM excludes", fetch=True)
    if not existing_excludes:
        for phrase in DEFAULT_EXCLUDES:
            db_execute("INSERT OR IGNORE INTO excludes (phrase) VALUES (?)", (phrase,))

    if not get_state("location_mode"):
        set_state("location_mode", "uk")

    if not get_state("paused"):
        set_state("paused", "0")


def set_state(key, value):
    db_execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
        (key, str(value)),
    )


def get_state(key, default=""):
    rows = db_execute("SELECT value FROM state WHERE key = ?", (key,), fetch=True)
    return rows[0][0] if rows else default


def get_keywords():
    rows = db_execute("SELECT keyword FROM keywords ORDER BY keyword ASC", fetch=True)
    return [row[0] for row in rows]


def get_excludes():
    rows = db_execute("SELECT phrase FROM excludes ORDER BY phrase ASC", fetch=True)
    return [row[0] for row in rows]


def add_keyword(keyword):
    keyword = normalize_text(keyword)
    if not keyword:
        return False
    db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (keyword,))
    return True


def remove_keyword(keyword):
    keyword = normalize_text(keyword)
    if not keyword:
        return False
    db_execute("DELETE FROM keywords WHERE keyword = ?", (keyword,))
    return True


def send_telegram_message(text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return

    target_chat_id = str(chat_id or TELEGRAM_CHAT_ID).strip()
    if not target_chat_id:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target_chat_id,
        "text": text[:4000],
        "disable_web_page_preview": True,
    }

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


def fetch_text(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def fetch_json(url, method="GET", payload=None):
    if method.upper() == "POST":
        response = requests.post(url, headers=HEADERS, json=payload or {}, timeout=30)
    else:
        response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def detect_location(text, company=""):
    hay = normalize_text(text)
    london_terms = ["london", "greater london", "shoreditch", "soho", "london/hybrid", "london / hybrid"]
    uk_terms = ["uk", "united kingdom", "england", "remote uk", "hybrid remote", "remote, uk", "london"]
    us_terms = ["new york", "los angeles", "california", "united states", "usa", "chicago", "canada", "montreal", "vancouver", "sydney", "mumbai", "melbourne"]

    if any(term in hay for term in london_terms):
        return "London"

    if any(term in hay for term in uk_terms):
        return "UK"

    if any(term in hay for term in us_terms):
        return "Non-UK"

    if company in UK_STUDIO_COMPANIES:
        return "Unknown-UK-Studio"

    return ""


def location_allowed(job):
    mode = get_state("location_mode", "uk").lower()
    if mode == "off":
        return True

    text_blob = " ".join([
        job.get("title", ""),
        job.get("location", ""),
        job.get("body", ""),
        job.get("company", ""),
        job.get("source_name", ""),
    ])

    loc = detect_location(text_blob, company=job.get("company", ""))

    if mode == "london":
        return loc in {"London", "Unknown-UK-Studio"}

    if mode == "uk":
        return loc in {"London", "UK", "Unknown-UK-Studio"}

    return True


def title_keyword_match(title, body):
    keywords = get_keywords()
    excludes = get_excludes()
    hay = normalize_text(f"{title} {body}")

    matched_keyword = None
    for kw in keywords:
        if kw in hay:
            matched_keyword = kw
            break

    if not matched_keyword:
        return False, None

    for phrase in excludes:
        if phrase in hay:
            return False, None

    return True, matched_keyword


def build_unique_key(job):
    canonical_url = canonicalize_url(job.get("url", ""))
    if canonical_url:
        return f"url::{canonical_url}"

    return "fp::" + short_hash(
        job.get("title", ""),
        job.get("company", ""),
        job.get("location", ""),
    )


def upsert_job(job):
    unique_key = build_unique_key(job)
    canonical_url = canonicalize_url(job.get("url", ""))
    now = now_str()

    rows = db_execute(
        "SELECT id, source_priority, source_name FROM jobs WHERE unique_key = ?",
        (unique_key,),
        fetch=True,
    )

    if not rows:
        db_execute("""
            INSERT INTO jobs (
                unique_key, canonical_url, title, company, location, url,
                source_name, source_kind, source_priority, source_type,
                first_seen, last_seen, matched_keyword, raw_blob
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            unique_key,
            canonical_url,
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("url", ""),
            job.get("source_name", ""),
            job.get("source_kind", ""),
            int(job.get("source_priority", 9)),
            job.get("source_type", ""),
            now,
            now,
            job.get("matched_keyword", ""),
            json.dumps(job, ensure_ascii=False)[:2000],
        ))
        created = True
    else:
        job_id, current_priority, current_source = rows[0]
        created = False

        replace_primary = int(job.get("source_priority", 9)) < int(current_priority)

        if replace_primary:
            db_execute("""
                UPDATE jobs
                SET canonical_url = ?, title = ?, company = ?, location = ?, url = ?,
                    source_name = ?, source_kind = ?, source_priority = ?, source_type = ?,
                    last_seen = ?, matched_keyword = ?, raw_blob = ?
                WHERE unique_key = ?
            """, (
                canonical_url,
                job.get("title", ""),
                job.get("company", ""),
                job.get("location", ""),
                job.get("url", ""),
                job.get("source_name", ""),
                job.get("source_kind", ""),
                int(job.get("source_priority", 9)),
                job.get("source_type", ""),
                now,
                job.get("matched_keyword", ""),
                json.dumps(job, ensure_ascii=False)[:2000],
                unique_key,
            ))
        else:
            db_execute("""
                UPDATE jobs
                SET last_seen = ?
                WHERE unique_key = ?
            """, (now, unique_key))

    db_execute("""
        INSERT INTO job_sources (unique_key, source_name, source_type, url, seen_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        unique_key,
        job.get("source_name", ""),
        job.get("source_type", ""),
        job.get("url", ""),
        now,
    ))

    return created


def extract_jobs_from_html(source):
    jobs = []
    try:
        html = fetch_text(source["url"])
        soup = BeautifulSoup(html, "html.parser")
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            title = clean_text(a.get_text(" ", strip=True))
            if not href or not title:
                continue

            full_url = urljoin(source["url"], href)
            context = clean_text(a.parent.get_text(" ", strip=True)) if a.parent else title
            context_blob = f"{title} {context} {full_url}"

            looks_job_like = any(term in normalize_text(context_blob) for term in [
                "job", "career", "vacancy", "opening", "role",
                "producer", "production", "runner", "assistant", "coordinator", "intern"
            ])

            if not looks_job_like:
                continue

            dedupe_key = short_hash(full_url, title)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            jobs.append({
                "title": title,
                "company": source["company"],
                "location": detect_location(context_blob, company=source["company"]),
                "url": full_url,
                "body": context,
                "source_name": source["name"],
                "source_kind": source["kind"],
                "source_priority": source["priority"],
                "source_type": source["type"],
            })
    except Exception:
        return []
    return jobs


def parse_greenhouse(source):
    jobs = []
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{source['slug']}/jobs"
        data = fetch_json(url)
        for item in data.get("jobs", []):
            location = (item.get("location") or {}).get("name", "")
            jobs.append({
                "title": clean_text(item.get("title", "")),
                "company": source["company"],
                "location": location,
                "url": item.get("absolute_url", ""),
                "body": json.dumps(item, ensure_ascii=False),
                "source_name": source["name"],
                "source_kind": source["kind"],
                "source_priority": source["priority"],
                "source_type": source["type"],
            })
    except Exception:
        return []
    return jobs


def parse_lever(source):
    jobs = []
    try:
        url = f"https://api.lever.co/v0/postings/{source['company_slug']}?mode=json"
        data = fetch_json(url)
        for item in data:
            categories = item.get("categories") or {}
            location = categories.get("location", "")
            jobs.append({
                "title": clean_text(item.get("text", "")),
                "company": source["company"],
                "location": location,
                "url": item.get("hostedUrl", ""),
                "body": json.dumps(item, ensure_ascii=False),
                "source_name": source["name"],
                "source_kind": source["kind"],
                "source_priority": source["priority"],
                "source_type": source["type"],
            })
    except Exception:
        return []
    return jobs


def parse_smartrecruiters(source):
    jobs = []
    try:
        url = f"https://api.smartrecruiters.com/v1/companies/{source['company_id']}/postings"
        data = fetch_json(url)
        for item in data.get("content", []):
            jobs.append({
                "title": clean_text(item.get("name", "")),
                "company": source["company"],
                "location": clean_text((item.get("location") or {}).get("city", "")),
                "url": item.get("ref", ""),
                "body": json.dumps(item, ensure_ascii=False),
                "source_name": source["name"],
                "source_kind": source["kind"],
                "source_priority": source["priority"],
                "source_type": source["type"],
            })
    except Exception:
        return []
    return jobs


def parse_workday(source):
    jobs = []
    try:
        data = fetch_json(source["api_url"], method="POST", payload={"limit": 20, "offset": 0, "searchText": ""})
        for item in data.get("jobPostings", []) or data.get("jobs", []):
            title = clean_text(item.get("title", "") or item.get("bulletFields", [""])[0])
            location = clean_text(item.get("locationsText", "") or item.get("location", ""))
            external_path = item.get("externalPath", "")
            url = source.get("base_url", "").rstrip("/") + external_path if external_path and source.get("base_url") else external_path
            jobs.append({
                "title": title,
                "company": source["company"],
                "location": location,
                "url": url,
                "body": json.dumps(item, ensure_ascii=False),
                "source_name": source["name"],
                "source_kind": source["kind"],
                "source_priority": source["priority"],
                "source_type": source["type"],
            })
    except Exception:
        return []
    return jobs


def fetch_source_jobs(source):
    source_type = source["type"]
    if source_type == "greenhouse":
        return parse_greenhouse(source)
    if source_type == "lever":
        return parse_lever(source)
    if source_type == "smartrecruiters":
        return parse_smartrecruiters(source)
    if source_type == "workday":
        return parse_workday(source)
    return extract_jobs_from_html(source)


def collect_jobs():
    jobs = []
    for source in SOURCES:
        jobs.extend(fetch_source_jobs(source))
    return jobs


def filter_and_store_jobs():
    raw_jobs = collect_jobs()
    new_jobs = []

    for job in raw_jobs:
        title = job.get("title", "")
        body = job.get("body", "")
        ok, matched_keyword = title_keyword_match(title, body)
        if not ok:
            continue

        job["matched_keyword"] = matched_keyword

        if not location_allowed(job):
            continue

        created = upsert_job(job)
        if created:
            new_jobs.append(job)

    return new_jobs


def format_job_rows(rows):
    if not rows:
        return "No matching jobs saved yet."

    lines = []
    for idx, row in enumerate(rows, start=1):
        title, company, location, url, first_seen = row
        loc_part = f" | {location}" if location else ""
        lines.append(f"{idx}. {title} — {company}{loc_part}\n{url}\nFound: {first_seen}\n")
    return "\n".join(lines[:10])


def latest_rows(hours=24, limit=10):
    cutoff = utc_now() - timedelta(hours=hours)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S UTC")
    return db_execute("""
        SELECT title, company, location, url, first_seen
        FROM jobs
        WHERE first_seen >= ?
        ORDER BY id DESC
        LIMIT ?
    """, (cutoff_str, limit), fetch=True)


def handle_command(text):
    text = clean_text(text)
    lower = text.lower()

    if lower == "/help":
        return (
            "Commands:\n"
            "/help\n"
            "/status\n"
            "/jobs\n"
            "/latest\n"
            "/search <term>\n"
            "/keywords\n"
            "/addkeyword <term>\n"
            "/removekeyword <term>\n"
            "/companies\n"
            "/sources\n"
            "/setlocation london|uk|off\n"
            "/pause\n"
            "/resume"
        )

    if lower == "/status":
        total_rows = db_execute("SELECT COUNT(*) FROM jobs", fetch=True)
        total = total_rows[0][0] if total_rows else 0
        return (
            f"Bot status: {'paused' if get_state('paused', '0') == '1' else 'running'}\n"
            f"Last checked: {get_state('last_checked', 'Not checked yet')}\n"
            f"New matches on last check: {get_state('last_match_count', '0')}\n"
            f"Saved jobs: {total}\n"
            f"Location mode: {get_state('location_mode', 'uk')}\n"
            f"Check interval: {CHECK_INTERVAL_SECONDS} seconds"
        )

    if lower == "/jobs":
        rows = db_execute("""
            SELECT title, company, location, url, first_seen
            FROM jobs
            ORDER BY id DESC
            LIMIT 10
        """, fetch=True)
        return format_job_rows(rows)

    if lower == "/latest":
        return format_job_rows(latest_rows(hours=24, limit=10))

    if lower.startswith("/search "):
        term = lower.replace("/search ", "", 1).strip()
        rows = db_execute("""
            SELECT title, company, location, url, first_seen
            FROM jobs
            WHERE lower(title) LIKE ? OR lower(company) LIKE ? OR lower(location) LIKE ?
            ORDER BY id DESC
            LIMIT 10
        """, (f"%{term}%", f"%{term}%", f"%{term}%"), fetch=True)
        if not rows:
            return f'No saved jobs found for "{term}".'
        return format_job_rows(rows)

    if lower == "/keywords":
        return "Tracked keywords:\n" + "\n".join(f"- {kw}" for kw in get_keywords())

    if lower.startswith("/addkeyword "):
        kw = text[len("/addkeyword "):].strip()
        if not kw:
            return "Please provide a keyword."
        add_keyword(kw)
        return f'Added keyword: "{normalize_text(kw)}"'

    if lower.startswith("/removekeyword "):
        kw = text[len("/removekeyword "):].strip()
        if not kw:
            return "Please provide a keyword."
        remove_keyword(kw)
        return f'Removed keyword: "{normalize_text(kw)}"'

    if lower == "/companies":
        companies = sorted({src["company"] for src in SOURCES})
        return "Studios / sources monitored:\n" + "\n".join(f"- {c}" for c in companies)

    if lower == "/sources":
        lines = []
        for src in SOURCES:
            lines.append(f"- {src['company']} | {src['type']} | {src['kind']}")
        return "Source list:\n" + "\n".join(lines)

    if lower.startswith("/setlocation "):
        mode = lower.replace("/setlocation ", "", 1).strip()
        if mode not in {"london", "uk", "off"}:
            return "Use /setlocation london or /setlocation uk or /setlocation off"
        set_state("location_mode", mode)
        return f"Location filter set to: {mode}"

    if lower == "/pause":
        set_state("paused", "1")
        return "Bot paused."

    if lower == "/resume":
        set_state("paused", "0")
        return "Bot resumed."

    return "Unknown command. Send /help"


def send_new_job_alerts(jobs):
    if not jobs:
        return
    lines = ["New matching jobs found:\n"]
    for job in jobs[:8]:
        loc = f" | {job.get('location', '')}" if job.get("location") else ""
        lines.append(f"{job['title']} — {job['company']}{loc}\n{job['url']}\n")
    send_telegram_message("\n".join(lines))


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
                        send_telegram_message(reply, chat_id=chat_id)
        except Exception:
            pass
        time.sleep(3)


def monitor_loop():
    while True:
        try:
            if get_state("paused", "0") != "1":
                new_jobs = filter_and_store_jobs()
                set_state("last_match_count", str(len(new_jobs)))
                set_state("last_checked", now_str())
                if new_jobs:
                    send_new_job_alerts(new_jobs)
            else:
                set_state("last_checked", now_str())
                set_state("last_match_count", "0")
        except Exception:
            set_state("last_checked", now_str())
        time.sleep(CHECK_INTERVAL_SECONDS)


@app.route("/")
def home():
    return "VFX studio monitor running", 200


@app.route("/health")
def health():
    return {"status": "ok"}, 200


init_db()


def start_background_threads():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=command_loop, daemon=True).start()
    send_telegram_message("Studio monitor started successfully.")


start_background_threads()
