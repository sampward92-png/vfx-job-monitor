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
    "User-Agent": "Mozilla/5.0 (compatible; VFXJobMonitor/5.0; +https://railway.app)"
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
    "ILM",
    "Milk",
    "BlueBolt",
    "Outpost",
    "MPC",
    "The Mill",
    "Absolute",
    "Coffee & TV",
    "Envy",
    "Lola",
    "ScreenSkills",
}

LONDON_TERMS = [
    "london", "greater london", "central london", "east london", "west london",
    "shoreditch", "soho", "camden", "king's cross", "kings cross",
    "london, uk", "london uk", "london / hybrid", "london/hybrid",
    "clerkenwell", "london bridge",
]

UK_TERMS = [
    "uk", "united kingdom", "england", "remote uk", "uk remote",
    "hybrid uk", "remote, uk", "bournemouth", "manchester", "bristol",
]

NON_UK_TERMS = [
    "usa", "united states", "new york", "los angeles", "california",
    "canada", "montreal", "vancouver", "sydney", "australia", "melbourne",
    "mumbai", "india", "singapore", "berlin", "munich", "france", "paris",
    "barcelona", "toronto",
]

# ── PHASE 1: Default sources seeded into the DB on first run ──────────────────
DEFAULT_SOURCES = [
    {"name": "Framestore Careers",     "company": "Framestore",         "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.framestore.com/careers"},
    {"name": "Framestore Recruitee",   "company": "Framestore",         "kind": "studio",         "priority": 1, "type": "html",       "url": "https://framestore.recruitee.com/"},
    {"name": "DNEG Open Positions",    "company": "DNEG",               "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.dneg.com/join-us/open-positions"},
    {"name": "DNEG Jobvite",           "company": "DNEG",               "kind": "studio",         "priority": 1, "type": "jobvite",    "url": "https://jobs.jobvite.com/double-negative-visual-effects/jobs"},
    {"name": "Cinesite Job Vacancies", "company": "Cinesite",           "kind": "studio",         "priority": 1, "type": "html",       "url": "https://cinesite.com/job-vacancies/"},
    {"name": "Blue Zoo Careers",       "company": "Blue Zoo",           "kind": "studio",         "priority": 1, "type": "html",       "url": "https://careers.blue-zoo.co.uk/vacancies/vacancy-search-results.aspx?view=grid"},
    {"name": "Jellyfish Workable",     "company": "Jellyfish Pictures", "kind": "studio",         "priority": 1, "type": "workable",   "url": "https://apply.workable.com/jellyfish-pictures-ltd/"},
    {"name": "Nexus Studios Workable", "company": "Nexus Studios",      "kind": "studio",         "priority": 1, "type": "workable",   "url": "https://apply.workable.com/nexusstudios/"},
    {"name": "Nexus Teamtailor",       "company": "Nexus Studios",      "kind": "studio",         "priority": 1, "type": "teamtailor", "url": "https://nexusstudios.teamtailor.com/jobs"},
    {"name": "ILM Careers",            "company": "ILM",                "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.ilm.com/careers/"},
    {"name": "Milk Careers",           "company": "Milk",               "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.milk-vfx.com/careers/"},
    {"name": "BlueBolt Hiring",        "company": "BlueBolt",           "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.blue-bolt.com/hiring"},
    {"name": "Outpost Careers",        "company": "Outpost",            "kind": "studio",         "priority": 1, "type": "html",       "url": "https://careers.outpost-vfx.com/"},
    {"name": "MPC Careers",            "company": "MPC",                "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.moving-picture.com/careers"},
    {"name": "The Mill Careers",       "company": "The Mill",           "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.themill.com/careers"},
    {"name": "Absolute Careers",       "company": "Absolute",           "kind": "studio",         "priority": 2, "type": "html",       "url": "https://absolute.tv/careers"},
    {"name": "Coffee & TV Careers",    "company": "Coffee & TV",        "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.coffeeand.tv/careers"},
    {"name": "Envy Careers",           "company": "Envy",               "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.envypost.co.uk/careers"},
    {"name": "Lola Post Careers",      "company": "Lola",               "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.lola-post.com/careers"},
    {"name": "ScreenSkills Jobs",      "company": "ScreenSkills",       "kind": "industry_board", "priority": 3, "type": "html",       "url": "https://www.screenskills.com/jobs/"},
]

ATS_PATTERNS = {
    "greenhouse":      ["boards.greenhouse.io", "job-boards.greenhouse.io"],
    "lever":           ["jobs.lever.co", "api.lever.co"],
    "workable":        ["apply.workable.com"],
    "ashby":           ["jobs.ashbyhq.com"],
    "jobvite":         ["jobs.jobvite.com"],
    "teamtailor":      ["teamtailor.com"],
    "smartrecruiters": ["smartrecruiters.com"],
    "workday":         ["myworkdayjobs.com", ".wd1.myworkdayjobs.com", ".wd3.myworkdayjobs.com"],
}

_started = False


# ── Utilities ─────────────────────────────────────────────────────────────────

def utc_now():
    return datetime.now(APP_TZ)

def now_str():
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")

def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
             if not k.lower().startswith("utm_")]
    cleaned = parsed._replace(query=urlencode(query), fragment="")
    return urlunparse(cleaned)

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def normalize_text(text: str) -> str:
    return clean_text(text).lower()

def short_hash(*parts: str) -> str:
    joined = "|".join(normalize_text(p) for p in parts if p)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:24]


# ── Database ──────────────────────────────────────────────────────────────────

def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    # PHASE 1: WAL mode for better concurrent read/write performance
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

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

    # Original jobs table — preserved exactly, with two new Phase 1 columns
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
            score INTEGER DEFAULT 0,
            raw_blob TEXT,
            miss_count INTEGER DEFAULT 0,
            job_status TEXT DEFAULT 'active'
        )
    """)

    # Add Phase 1 columns to existing deployments that don't have them yet
    for col, definition in [
        ("miss_count", "INTEGER DEFAULT 0"),
        ("job_status", "TEXT DEFAULT 'active'"),
    ]:
        try:
            cur.execute(f"ALTER TABLE jobs ADD COLUMN {col} {definition}")
        except sqlite3.OperationalError:
            pass  # Column already exists — safe to ignore

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

    # PHASE 1: sources table — replaces the hardcoded SOURCES list
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company TEXT NOT NULL,
            kind TEXT NOT NULL,
            priority INTEGER DEFAULT 3,
            type TEXT NOT NULL,
            url TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            added_at TEXT
        )
    """)

    # PHASE 1: source_health table — tracks per-source scrape outcomes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            source_name TEXT PRIMARY KEY,
            last_run_at TEXT,
            last_success_at TEXT,
            last_failure_at TEXT,
            last_error TEXT,
            consecutive_fails INTEGER DEFAULT 0,
            total_runs INTEGER DEFAULT 0,
            jobs_found_last INTEGER DEFAULT 0,
            jobs_found_total INTEGER DEFAULT 0,
            status TEXT DEFAULT 'unknown'
        )
    """)

    conn.commit()
    conn.close()

    seed_defaults()


def seed_defaults():
    if not db_execute("SELECT keyword FROM keywords", fetch=True):
        for kw in DEFAULT_KEYWORDS:
            db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))

    if not db_execute("SELECT phrase FROM excludes", fetch=True):
        for phrase in DEFAULT_EXCLUDES:
            db_execute("INSERT OR IGNORE INTO excludes (phrase) VALUES (?)", (phrase,))

    if not get_state("location_mode"):
        set_state("location_mode", "london")

    if not get_state("paused"):
        set_state("paused", "0")

    if not get_state("quality_mode"):
        set_state("quality_mode", "normal")

    # PHASE 1: Seed sources table from DEFAULT_SOURCES if empty
    existing = db_execute("SELECT COUNT(*) FROM sources", fetch=True)
    if not existing or existing[0][0] == 0:
        for s in DEFAULT_SOURCES:
            db_execute(
                """INSERT OR IGNORE INTO sources (name, company, kind, priority, type, url, active, added_at)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                (s["name"], s["company"], s["kind"], s["priority"], s["type"], s["url"], now_str())
            )


# ── PHASE 1: Load active sources from DB (replaces hardcoded SOURCES list) ───

def get_active_sources():
    rows = db_execute(
        "SELECT name, company, kind, priority, type, url FROM sources WHERE active = 1 ORDER BY priority ASC",
        fetch=True
    )
    return [
        {"name": r[0], "company": r[1], "kind": r[2], "priority": r[3], "type": r[4], "url": r[5]}
        for r in (rows or [])
    ]


# ── PHASE 1: Source health recording ─────────────────────────────────────────

def record_source_success(source_name: str, jobs_found: int):
    now = now_str()
    existing = db_execute(
        "SELECT total_runs, jobs_found_total FROM source_health WHERE source_name = ?",
        (source_name,), fetch=True
    )
    if existing:
        total_runs = existing[0][0] + 1
        jobs_total = existing[0][1] + jobs_found
        db_execute("""
            UPDATE source_health
            SET last_run_at = ?, last_success_at = ?, consecutive_fails = 0,
                total_runs = ?, jobs_found_last = ?, jobs_found_total = ?,
                status = 'healthy', last_error = NULL
            WHERE source_name = ?
        """, (now, now, total_runs, jobs_found, jobs_total, source_name))
    else:
        db_execute("""
            INSERT INTO source_health
                (source_name, last_run_at, last_success_at, consecutive_fails,
                 total_runs, jobs_found_last, jobs_found_total, status)
            VALUES (?, ?, ?, 0, 1, ?, ?, 'healthy')
        """, (source_name, now, now, jobs_found, jobs_found))


def record_source_failure(source_name: str, error: str):
    now = now_str()
    existing = db_execute(
        "SELECT consecutive_fails, total_runs FROM source_health WHERE source_name = ?",
        (source_name,), fetch=True
    )
    error_short = str(error)[:300]
    if existing:
        fails = existing[0][0] + 1
        total_runs = existing[0][1] + 1
        status = "dead" if fails >= 7 else "degraded" if fails >= 3 else "healthy"
        db_execute("""
            UPDATE source_health
            SET last_run_at = ?, last_failure_at = ?, last_error = ?,
                consecutive_fails = ?, total_runs = ?, jobs_found_last = 0, status = ?
            WHERE source_name = ?
        """, (now, now, error_short, fails, total_runs, status, source_name))
        return fails, status
    else:
        db_execute("""
            INSERT INTO source_health
                (source_name, last_run_at, last_failure_at, last_error,
                 consecutive_fails, total_runs, jobs_found_last, jobs_found_total, status)
            VALUES (?, ?, ?, ?, 1, 1, 0, 0, 'degraded')
        """, (source_name, now, now, error_short, 1))
        return 1, "degraded"


# ── PHASE 1: Job expiry — mark jobs not seen in 3+ consecutive runs ───────────

def expire_stale_jobs(seen_keys: set):
    """
    Called after each full monitoring run.
    Jobs not present in seen_keys have their miss_count incremented.
    After 3 consecutive misses, job_status is set to 'expired'.
    """
    # Increment miss_count for all active jobs not seen this run
    active_jobs = db_execute(
        "SELECT unique_key FROM jobs WHERE job_status = 'active'",
        fetch=True
    )
    for (key,) in (active_jobs or []):
        if key not in seen_keys:
            db_execute("""
                UPDATE jobs
                SET miss_count = miss_count + 1,
                    job_status = CASE WHEN miss_count + 1 >= 3 THEN 'expired' ELSE job_status END
                WHERE unique_key = ?
            """, (key,))
        else:
            # Reset miss_count for jobs that are still live
            db_execute(
                "UPDATE jobs SET miss_count = 0 WHERE unique_key = ?",
                (key,)
            )


# ── State / keywords / excludes ───────────────────────────────────────────────

def set_state(key, value):
    db_execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, str(value)))

def get_state(key, default=""):
    rows = db_execute("SELECT value FROM state WHERE key = ?", (key,), fetch=True)
    return rows[0][0] if rows else default

def get_keywords():
    return [row[0] for row in db_execute("SELECT keyword FROM keywords ORDER BY keyword ASC", fetch=True)]

def get_excludes():
    return [row[0] for row in db_execute("SELECT phrase FROM excludes ORDER BY phrase ASC", fetch=True)]

def add_keyword(keyword):
    keyword = normalize_text(keyword)
    if keyword:
        db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (keyword,))
        return True
    return False

def remove_keyword(keyword):
    keyword = normalize_text(keyword)
    if keyword:
        db_execute("DELETE FROM keywords WHERE keyword = ?", (keyword,))
        return True
    return False

def quality_threshold():
    mode = get_state("quality_mode", "normal").lower()
    if mode == "strict": return 75
    if mode == "off":    return 0
    return 45


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram_message(text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return
    target_chat_id = str(chat_id or TELEGRAM_CHAT_ID).strip()
    if not target_chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": target_chat_id, "text": text[:4000], "disable_web_page_preview": True},
            timeout=20,
        )
    except Exception:
        pass

def telegram_api(method, payload=None):
    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
        json=payload or {},
        timeout=30,
    )
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


# ── Location / scoring ────────────────────────────────────────────────────────

def detect_location(text, company=""):
    hay = normalize_text(text)
    if any(term in hay for term in NON_UK_TERMS): return "Non-UK"
    if any(term in hay for term in LONDON_TERMS):  return "London"
    if any(term in hay for term in UK_TERMS):      return "UK"
    if company in UK_STUDIO_COMPANIES:             return "Unknown-UK-Studio"
    return ""

def location_allowed(job):
    mode = get_state("location_mode", "london").lower()
    if mode == "off":
        return True
    blob = " ".join([
        job.get("title", ""), job.get("location", ""),
        job.get("body", ""), job.get("company", ""), job.get("source_name", ""),
    ])
    loc = detect_location(blob, company=job.get("company", ""))
    if loc == "Non-UK":
        return False
    strong_title = any(term in normalize_text(job.get("title", "")) for term in [
        "production assistant", "production coordinator", "junior production coordinator",
        "graduate producer", "assistant producer", "production trainee",
        "production intern", "studio assistant",
    ])
    if mode == "london":
        return loc == "London" or (loc == "Unknown-UK-Studio" and strong_title)
    if mode == "uk":
        return loc in {"London", "UK"} or (loc == "Unknown-UK-Studio" and strong_title)
    return True

def title_keyword_match(title, body):
    hay = normalize_text(f"{title} {body}")
    matched_keyword = None
    for kw in get_keywords():
        if kw in hay:
            matched_keyword = kw
            break
    if not matched_keyword:
        return False, None
    for phrase in get_excludes():
        if phrase in hay:
            return False, None
    return True, matched_keyword

def score_job(job):
    score = 0
    title   = normalize_text(job.get("title", ""))
    body    = normalize_text(job.get("body", ""))
    company = normalize_text(job.get("company", ""))
    blob    = f"{title} {body}"

    exact_title_boosts = {
        "production assistant": 35, "production coordinator": 32,
        "junior production coordinator": 38, "graduate producer": 40,
        "assistant producer": 30, "production trainee": 34,
        "production intern": 28, "studio assistant": 24,
        "studio runner": 22, "runner": 14,
        "project coordinator": 20, "project assistant": 18,
    }
    for phrase, points in exact_title_boosts.items():
        if phrase in title:
            score += points

    for term in ["junior", "graduate", "assistant", "trainee", "intern", "entry"]:
        if term in blob:
            score += 6

    loc = detect_location(
        " ".join([job.get("title", ""), job.get("location", ""), job.get("body", "")]),
        company=job.get("company", ""),
    )
    if loc == "London":             score += 30
    elif loc == "UK":               score += 18
    elif loc == "Unknown-UK-Studio":score += 10
    elif loc == "Non-UK":           score -= 100

    if job.get("source_kind") == "studio": score += 12
    priority = int(job.get("source_priority", 9))
    if priority == 1:   score += 10
    elif priority == 2: score += 6
    elif priority == 3: score += 2

    preferred = ["framestore", "nexus studios", "dneg", "cinesite", "blue zoo",
                 "jellyfish pictures", "ilm", "milk", "bluebolt", "outpost"]
    if company in preferred:
        score += 8

    for term in ["senior", "lead", "director", "supervisor", "executive producer", "principal"]:
        if term in blob:
            score -= 40

    return max(score, 0)


# ── Scraping ──────────────────────────────────────────────────────────────────

def fetch_text(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text

def fetch_json(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()

def detect_location_from_blob(blob, company):
    return detect_location(blob, company)

def identify_ats_type(url):
    low = url.lower()
    for ats_type, patterns in ATS_PATTERNS.items():
        if any(pattern in low for pattern in patterns):
            return ats_type
    return None

def discover_ats_sources_from_html(source, html):
    soup = BeautifulSoup(html, "html.parser")
    discovered = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(source["url"], a["href"].strip())
        ats_type = identify_ats_type(href)
        if not ats_type:
            continue
        key = f"{ats_type}|{canonicalize_url(href)}"
        if key in seen:
            continue
        seen.add(key)
        discovered.append({
            "name": f"{source['company']} discovered {ats_type}",
            "company": source["company"],
            "kind": source["kind"],
            "priority": source["priority"],
            "type": ats_type,
            "url": href,
        })
    return discovered

def generic_extract_jobs_from_soup(source, soup):
    """
    PHASE 1: Tightened candidate selection vs original.
    - Minimum title length of 8 characters
    - href must point to a different path than the source URL
    - Skip common navigation link texts
    """
    jobs = []
    seen = set()
    source_path = urlparse(source["url"]).path.rstrip("/")

    NAV_PATTERNS = {
        "home", "about", "contact", "menu", "login", "sign in", "register",
        "privacy", "terms", "cookie", "back", "next", "previous", "more",
        "read more", "view all", "see all", "apply now", "apply here",
    }

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        title = clean_text(a.get_text(" ", strip=True))

        # PHASE 1: Skip short, empty, or nav-pattern titles
        if not href or not title or len(title) < 8:
            continue
        if normalize_text(title) in NAV_PATTERNS:
            continue

        full_url = urljoin(source["url"], href)

        # PHASE 1: Skip anchors pointing back to the same path
        link_path = urlparse(full_url).path.rstrip("/")
        if link_path == source_path or not link_path:
            continue

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

    return jobs

def parse_html(source):
    try:
        html = fetch_text(source["url"])
        soup = BeautifulSoup(html, "html.parser")
        jobs = generic_extract_jobs_from_soup(source, soup)
        discovered = discover_ats_sources_from_html(source, html)
        return jobs, discovered
    except Exception:
        return [], []

def parse_greenhouse(source):
    try:
        url = source["url"]
        slug = None
        m = re.search(r"boards\.greenhouse\.io/([^/?#]+)", url)
        if m:
            slug = m.group(1)
        else:
            m = re.search(r"job-boards\.greenhouse\.io/([^/?#]+)", url)
            if m:
                slug = m.group(1)
        if slug:
            data = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            jobs = []
            for item in data.get("jobs", []):
                jobs.append({
                    "title": clean_text(item.get("title", "")),
                    "company": source["company"],
                    "location": clean_text((item.get("location") or {}).get("name", "")),
                    "url": item.get("absolute_url", ""),
                    "body": json.dumps(item, ensure_ascii=False),
                    "source_name": source["name"],
                    "source_kind": source["kind"],
                    "source_priority": source["priority"],
                    "source_type": source["type"],
                })
            return jobs, []
    except Exception:
        pass
    return parse_html(source)

def parse_lever(source):
    try:
        url = source["url"]
        company_slug = None
        m = re.search(r"jobs\.lever\.co/([^/?#]+)", url)
        if m:
            company_slug = m.group(1)
        else:
            m = re.search(r"api\.lever\.co/v0/postings/([^/?#]+)", url)
            if m:
                company_slug = m.group(1)
        if company_slug:
            data = fetch_json(f"https://api.lever.co/v0/postings/{company_slug}?mode=json")
            jobs = []
            for item in data:
                categories = item.get("categories") or {}
                jobs.append({
                    "title": clean_text(item.get("text", "")),
                    "company": source["company"],
                    "location": clean_text(categories.get("location", "")),
                    "url": item.get("hostedUrl", ""),
                    "body": json.dumps(item, ensure_ascii=False),
                    "source_name": source["name"],
                    "source_kind": source["kind"],
                    "source_priority": source["priority"],
                    "source_type": source["type"],
                })
            return jobs, []
    except Exception:
        pass
    return parse_html(source)

def parse_workable(source):  return parse_html(source)
def parse_ashby(source):     return parse_html(source)
def parse_jobvite(source):   return parse_html(source)
def parse_teamtailor(source):return parse_html(source)
def parse_smartrecruiters(source): return parse_html(source)
def parse_workday(source):   return parse_html(source)

def fetch_source_jobs(source):
    t = source["type"]
    if t == "greenhouse":     return parse_greenhouse(source)
    if t == "lever":          return parse_lever(source)
    if t == "workable":       return parse_workable(source)
    if t == "ashby":          return parse_ashby(source)
    if t == "jobvite":        return parse_jobvite(source)
    if t == "teamtailor":     return parse_teamtailor(source)
    if t == "smartrecruiters":return parse_smartrecruiters(source)
    if t == "workday":        return parse_workday(source)
    return parse_html(source)


# ── Job storage ───────────────────────────────────────────────────────────────

def build_unique_key(job):
    canonical_url = canonicalize_url(job.get("url", ""))
    if canonical_url:
        return f"url::{canonical_url}"
    return "fp::" + short_hash(job.get("title", ""), job.get("company", ""), job.get("location", ""))

def upsert_job(job):
    unique_key    = build_unique_key(job)
    canonical_url = canonicalize_url(job.get("url", ""))
    now = now_str()

    existing = db_execute(
        "SELECT id, source_priority FROM jobs WHERE unique_key = ?",
        (unique_key,), fetch=True,
    )

    if not existing:
        db_execute("""
            INSERT INTO jobs (
                unique_key, canonical_url, title, company, location, url,
                source_name, source_kind, source_priority, source_type,
                first_seen, last_seen, matched_keyword, score, raw_blob,
                miss_count, job_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'active')
        """, (
            unique_key, canonical_url, job.get("title", ""), job.get("company", ""),
            job.get("location", ""), job.get("url", ""), job.get("source_name", ""),
            job.get("source_kind", ""), int(job.get("source_priority", 9)),
            job.get("source_type", ""), now, now, job.get("matched_keyword", ""),
            int(job.get("score", 0)), json.dumps(job, ensure_ascii=False)[:3000],
        ))
        return True, unique_key
    else:
        _, current_priority = existing[0]
        replace_primary = int(job.get("source_priority", 9)) < int(current_priority)
        if replace_primary:
            db_execute("""
                UPDATE jobs
                SET canonical_url=?, title=?, company=?, location=?, url=?,
                    source_name=?, source_kind=?, source_priority=?, source_type=?,
                    last_seen=?, matched_keyword=?, score=?, raw_blob=?,
                    miss_count=0, job_status='active'
                WHERE unique_key=?
            """, (
                canonical_url, job.get("title", ""), job.get("company", ""),
                job.get("location", ""), job.get("url", ""), job.get("source_name", ""),
                job.get("source_kind", ""), int(job.get("source_priority", 9)),
                job.get("source_type", ""), now, job.get("matched_keyword", ""),
                int(job.get("score", 0)), json.dumps(job, ensure_ascii=False)[:3000],
                unique_key,
            ))
        else:
            db_execute(
                "UPDATE jobs SET last_seen=?, score=?, miss_count=0, job_status='active' WHERE unique_key=?",
                (now, int(job.get("score", 0)), unique_key),
            )
        db_execute("""
            INSERT INTO job_sources (unique_key, source_name, source_type, url, seen_at)
            VALUES (?, ?, ?, ?, ?)
        """, (unique_key, job.get("source_name", ""), job.get("source_type", ""),
              job.get("url", ""), now))
        return False, unique_key


# ── Core monitoring run ───────────────────────────────────────────────────────

def collect_and_store_jobs():
    """
    PHASE 1 version of filter_and_store_jobs:
    - Loads sources from DB instead of hardcoded list
    - Records source health after each source
    - Passes seen_keys to expire_stale_jobs at the end
    - Sends Telegram alert when a source goes degraded or dead
    """
    queued      = get_active_sources()
    seen_sources = set()
    all_raw_jobs = []

    while queued:
        source = queued.pop(0)
        source_key = f"{source.get('type')}|{canonicalize_url(source.get('url',''))}|{source.get('company')}"
        if source_key in seen_sources:
            continue
        seen_sources.add(source_key)

        try:
            jobs, discovered = fetch_source_jobs(source)
            record_source_success(source["name"], len(jobs))
            all_raw_jobs.extend(jobs)
            for ds in discovered:
                dk = f"{ds.get('type')}|{canonicalize_url(ds.get('url',''))}|{ds.get('company')}"
                if dk not in seen_sources:
                    queued.append(ds)
        except Exception as e:
            fails, status = record_source_failure(source["name"], str(e))
            # Alert on first degraded and first dead transitions
            if fails == 3:
                send_telegram_message(f"⚠️ Source degraded: {source['name']}\nError: {str(e)[:200]}")
            elif fails == 7:
                send_telegram_message(f"🔴 Source dead: {source['name']}\nError: {str(e)[:200]}")

        time.sleep(1.5)

    # Filter and store
    new_jobs   = []
    seen_keys  = set()
    threshold  = quality_threshold()

    for job in all_raw_jobs:
        ok, matched_keyword = title_keyword_match(job.get("title", ""), job.get("body", ""))
        if not ok:
            continue
        job["matched_keyword"] = matched_keyword
        if not location_allowed(job):
            continue
        job["score"] = score_job(job)
        if job["score"] < threshold:
            continue

        created, unique_key = upsert_job(job)
        seen_keys.add(unique_key)
        if created:
            new_jobs.append(job)

    # PHASE 1: Expire stale jobs
    expire_stale_jobs(seen_keys)

    return sorted(new_jobs, key=lambda x: x.get("score", 0), reverse=True)


# ── Telegram alerts ───────────────────────────────────────────────────────────

def format_job_rows(rows):
    if not rows:
        return "No matching jobs saved yet."
    lines = []
    for idx, row in enumerate(rows, start=1):
        title, company, location, url, first_seen, score = row
        loc_part = f" | {location}" if location else ""
        lines.append(
            f"{idx}. {title} — {company}{loc_part}\n"
            f"Score: {score}\n"
            f"{url}\n"
            f"Found: {first_seen}\n"
        )
    return "\n".join(lines[:10])

def latest_rows(hours=24, limit=10):
    cutoff = (utc_now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S UTC")
    return db_execute("""
        SELECT title, company, location, url, first_seen, score
        FROM jobs
        WHERE first_seen >= ? AND job_status = 'active'
        ORDER BY score DESC, id DESC
        LIMIT ?
    """, (cutoff, limit), fetch=True)

def send_new_job_alerts(jobs):
    if not jobs:
        return
    high_priority  = [j for j in jobs if j.get("score", 0) >= 75]
    normal_priority = [j for j in jobs if quality_threshold() <= j.get("score", 0) < 75]

    if high_priority:
        lines = ["🎬 HIGH PRIORITY JOBS FOUND:\n"]
        for job in high_priority[:6]:
            loc = f" | {job.get('location', '')}" if job.get("location") else ""
            lines.append(
                f"{job['title']} — {job['company']}{loc}\n"
                f"Score: {job['score']}\n"
                f"{job['url']}\n"
            )
        send_telegram_message("\n".join(lines))

    if normal_priority and get_state("quality_mode", "normal").lower() != "strict":
        lines = ["📋 New matching jobs found:\n"]
        for job in normal_priority[:6]:
            loc = f" | {job.get('location', '')}" if job.get("location") else ""
            lines.append(
                f"{job['title']} — {job['company']}{loc}\n"
                f"Score: {job['score']}\n"
                f"{job['url']}\n"
            )
        send_telegram_message("\n".join(lines))


# ── Telegram commands ─────────────────────────────────────────────────────────

def handle_command(text):
    text  = clean_text(text)
    lower = text.lower()

    if lower == "/help":
        return (
            "Commands:\n"
            "/help\n/status\n/jobs\n/latest\n/highpriority\n"
            "/search <term>\n/keywords\n/addkeyword <term>\n/removekeyword <term>\n"
            "/companies\n/sources\n/health\n/dead\n"
            "/setlocation london|uk|off\n/quality strict|normal|off\n"
            "/pause\n/resume"
        )

    if lower == "/status":
        total_rows = db_execute("SELECT COUNT(*) FROM jobs WHERE job_status = 'active'", fetch=True)
        total = total_rows[0][0] if total_rows else 0
        return (
            f"Bot status: {'paused' if get_state('paused', '0') == '1' else 'running'}\n"
            f"Last checked: {get_state('last_checked', 'Not checked yet')}\n"
            f"New matches on last check: {get_state('last_match_count', '0')}\n"
            f"Active jobs: {total}\n"
            f"Location mode: {get_state('location_mode', 'london')}\n"
            f"Quality mode: {get_state('quality_mode', 'normal')}\n"
            f"Score threshold: {quality_threshold()}\n"
            f"Check interval: {CHECK_INTERVAL_SECONDS} seconds"
        )

    if lower == "/jobs":
        rows = db_execute("""
            SELECT title, company, location, url, first_seen, score
            FROM jobs WHERE job_status = 'active'
            ORDER BY score DESC, id DESC LIMIT 10
        """, fetch=True)
        return format_job_rows(rows)

    if lower == "/latest":
        return format_job_rows(latest_rows(hours=24, limit=10))

    if lower == "/highpriority":
        rows = db_execute("""
            SELECT title, company, location, url, first_seen, score
            FROM jobs WHERE score >= 75 AND job_status = 'active'
            ORDER BY score DESC, id DESC LIMIT 10
        """, fetch=True)
        return format_job_rows(rows)

    if lower.startswith("/search "):
        term = lower.replace("/search ", "", 1).strip()
        rows = db_execute("""
            SELECT title, company, location, url, first_seen, score
            FROM jobs
            WHERE (lower(title) LIKE ? OR lower(company) LIKE ? OR lower(location) LIKE ?)
              AND job_status = 'active'
            ORDER BY score DESC, id DESC LIMIT 10
        """, (f"%{term}%", f"%{term}%", f"%{term}%"), fetch=True)
        return format_job_rows(rows) if rows else f'No active jobs found for "{term}".'

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
        rows = db_execute("""
            SELECT company, COUNT(*) FROM jobs
            WHERE job_status = 'active'
            GROUP BY company ORDER BY COUNT(*) DESC, company ASC
        """, fetch=True)
        if not rows:
            return "No active jobs yet."
        return "Companies with active jobs:\n" + "\n".join(f"- {c} — {n}" for c, n in rows)

    if lower == "/sources":
        rows = db_execute(
            "SELECT name, company, type, kind, active FROM sources ORDER BY priority ASC",
            fetch=True
        )
        if not rows:
            return "No sources configured."
        lines = [f"- {'✅' if r[4] else '❌'} {r[1]} | {r[2]} | {r[3]}" for r in rows]
        return "Sources:\n" + "\n".join(lines)

    # PHASE 1: New health commands
    if lower == "/health":
        rows = db_execute(
            "SELECT status, COUNT(*) FROM source_health GROUP BY status",
            fetch=True
        )
        if not rows:
            return "No source health data yet. Run a check first."
        summary = "\n".join(f"  {status}: {count}" for status, count in rows)
        total = db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True)
        total_count = total[0][0] if total else 0
        return f"Source health ({total_count} active sources):\n{summary}"

    if lower == "/dead":
        rows = db_execute("""
            SELECT source_name, consecutive_fails, last_error, last_success_at
            FROM source_health WHERE status IN ('dead', 'degraded')
            ORDER BY consecutive_fails DESC
        """, fetch=True)
        if not rows:
            return "No degraded or dead sources."
        lines = []
        for name, fails, error, last_ok in rows:
            lines.append(
                f"{'🔴' if fails >= 7 else '⚠️'} {name}\n"
                f"  Fails: {fails} | Last OK: {last_ok or 'never'}\n"
                f"  Error: {(error or '')[:120]}"
            )
        return "\n\n".join(lines)

    if lower.startswith("/setlocation "):
        mode = lower.replace("/setlocation ", "", 1).strip()
        if mode not in {"london", "uk", "off"}:
            return "Use /setlocation london or /setlocation uk or /setlocation off"
        set_state("location_mode", mode)
        return f"Location filter set to: {mode}"

    if lower.startswith("/quality "):
        mode = lower.replace("/quality ", "", 1).strip()
        if mode not in {"strict", "normal", "off"}:
            return "Use /quality strict or /quality normal or /quality off"
        set_state("quality_mode", mode)
        return f"Quality mode set to: {mode} (threshold {quality_threshold()})"

    if lower == "/pause":
        set_state("paused", "1")
        return "Bot paused."

    if lower == "/resume":
        set_state("paused", "0")
        return "Bot resumed."

    return "Unknown command. Send /help"


# ── Background threads ────────────────────────────────────────────────────────

def command_loop():
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                message  = update.get("message", {})
                chat     = message.get("chat", {})
                chat_id  = str(chat.get("id", ""))
                text     = message.get("text", "")
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
                new_jobs = collect_and_store_jobs()
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


# ── Flask ─────────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    total = db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True)
    sources = db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True)
    healthy = db_execute("SELECT COUNT(*) FROM source_health WHERE status='healthy'", fetch=True)
    return (
        f"VFX Job Monitor — Phase 1\n"
        f"Active jobs: {total[0][0] if total else 0}\n"
        f"Active sources: {sources[0][0] if sources else 0}\n"
        f"Healthy sources: {healthy[0][0] if healthy else 0}\n"
        f"Last checked: {get_state('last_checked', 'Never')}"
    ), 200

@app.route("/health")
def health():
    return {"status": "ok"}, 200


# ── Startup ───────────────────────────────────────────────────────────────────

init_db()

_started = False

def start_background_threads():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=command_loop, daemon=True).start()
    send_telegram_message(
        "✅ VFX Job Monitor — Phase 1 deployed.\n"
        "WAL mode ✓  Source health tracking ✓  Job expiry ✓  Sources in DB ✓"
    )

start_background_threads()
