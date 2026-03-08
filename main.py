import os
import re
import time
import json
import sqlite3
import hashlib
import threading
import concurrent.futures
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from flask import Flask

PORT = int(os.environ.get("PORT", "8080"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "600"))
DB_PATH = "jobs.db"

APP_TZ = timezone.utc
app    = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VFXJobMonitor/5.0; +https://railway.app)"
}

DEFAULT_KEYWORDS = [
    # Core production titles
    "production assistant",
    "production coordinator",
    "junior production coordinator",
    "graduate producer",
    "assistant producer",
    "junior producer",
    "production trainee",
    "production intern",
    "production runner",
    "studio runner",
    "runner",
    "studio assistant",
    "project coordinator",
    "project assistant",
    "production manager",      # junior production manager
    "production secretary",
    "production administrator",
    "production admin",
    "post production assistant",
    "post production coordinator",
    "post coordinator",
    "post assistant",
    "ep assistant",
    "executive assistant",     # exec assistant to EP etc
    # Junior / entry signals used as standalone titles
    "junior coordinator",
    "junior assistant",
    "graduate scheme",
    "graduate programme",
    "graduate program",
    "trainee",
    "apprentice",
    "internship",
    "intern",
    "work experience",
    "entry level",
    "launchpad",               # Framestore Launchpad etc
    "kickstart",
    "talent scheme",
    "emerging talent",
]

DEFAULT_EXCLUDES = [
    # Seniority — wrong level
    "senior",
    "supervisor",
    "head of",
    "director",
    "lead ",
    "principal",
    "executive producer",
    "vp ",
    "vice president",
    "manager",

    # Artist / technical roles — not production track
    "animator",
    "animation artist",
    "matte paint",
    "compositor",
    "compositing",
    "modell",          # modeller, modelling
    "model artist",    # gen ai model artist etc
    "ai artist",
    "media operator",
    "operator",
    "rigger",
    "rigging",
    "lighter",
    "lighting artist",
    "texture artist",
    "concept artist",
    "storyboard artist",
    "vfx artist",
    "cg artist",
    "3d artist",
    "2d artist",
    "motion graphic",
    "motion design",
    "editor",          # vfx editor, offline editor etc
    "colourist",
    "colorist",
    "sound design",
    "music supervisor",
    "technical director",
    " td ",
    "pipeline",
    "software engineer",
    "developer",
    "programmer",
    "it support",
    "systems admin",
    "data scientist",
    "machine learning",
    "recruiter",
    "talent acquisition",
    "hr ",
    "human resources",
    "accountant",
    "finance manager",
    "legal ",
    "lawyer",
    "solicitor",
    "sales ",
    "business development",
    "marketing manager",

    # Specific known non-production programmes
    "jedi academy",            # ILM US programme
    "animation launchpad",     # animation-specific, not production (e.g. "Animation - Launchpad")
    "animation - launchpad",   # dash variant
    "animation intern",        # animation-specific internship
]

UK_STUDIO_COMPANIES = {
    "Framestore", "Nexus Studios", "DNEG", "Cinesite", "Blue Zoo",
    "Jellyfish Pictures", "ILM", "Milk", "BlueBolt", "Outpost",
    "MPC", "The Mill", "Absolute", "Coffee & TV", "Envy", "Lola", "ScreenSkills",
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
    "mumbai", "india", "bengaluru", "bangalore", "hyderabad", "chennai", "pune",
    "singapore", "berlin", "munich", "germany", "france", "paris",
    "barcelona", "spain", "toronto", "chicago", "atlanta", "seattle",
    "san francisco", "new zealand", "auckland", "dubai", "uae",
    "amsterdam", "netherlands", "sweden", "stockholm",
]

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
    "greenhouse":       ["boards.greenhouse.io", "job-boards.greenhouse.io"],
    "lever":            ["jobs.lever.co", "api.lever.co"],
    "workable":         ["apply.workable.com"],
    "ashby":            ["jobs.ashbyhq.com"],
    "jobvite":          ["jobs.jobvite.com"],
    "teamtailor":       ["teamtailor.com"],
    "smartrecruiters":  ["smartrecruiters.com"],
    "workday":          ["myworkdayjobs.com", ".wd1.myworkdayjobs.com", ".wd3.myworkdayjobs.com"],
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
    query  = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
              if not k.lower().startswith("utm_")]
    return urlunparse(parsed._replace(query=urlencode(query), fragment=""))

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def normalize_text(text: str) -> str:
    return clean_text(text).lower()

def short_hash(*parts: str) -> str:
    joined = "|".join(normalize_text(p) for p in parts if p)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:24]


# ── CanonicalJob dataclass ────────────────────────────────────────────────────
# Every adapter must return a list of these. No raw dicts in the pipeline.

@dataclass
class CanonicalJob:
    # Identity
    title:               str
    company:             str
    apply_url:           str
    canonical_url:       str
    fingerprint:         str = ""

    # Location
    location_raw:        Optional[str] = None
    location_normalized: Optional[str] = None

    # Detail
    description_text:    Optional[str] = None
    department:          Optional[str] = None
    employment_type:     Optional[str] = None

    # ATS metadata
    ats_type:            Optional[str] = None
    external_job_id:     Optional[str] = None
    posted_at:           Optional[str] = None

    # Source metadata
    source_name:         str = ""
    source_kind:         str = ""
    source_priority:     int = 9
    source_type:         str = "html"

    # Scoring (populated by scoring engine)
    score:               Optional[float] = None
    score_breakdown:     dict = field(default_factory=dict)

    # Pipeline state
    matched_keyword:     Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["score_breakdown_json"] = json.dumps(self.score_breakdown)
        return d

    @staticmethod
    def build_fingerprint(company: str, title: str, location: str = "") -> str:
        parts = "|".join(normalize_text(p) for p in [company, title, location] if p)
        return hashlib.sha1(parts.encode()).hexdigest()


def normalise_to_canonical(raw: dict, source: dict) -> CanonicalJob:
    """
    Convert a raw dict from any adapter into a CanonicalJob.
    This is the single normalisation point — adapters return dicts,
    this function owns the shape contract.
    """
    title    = clean_text(raw.get("title", ""))
    company  = raw.get("company", source.get("company", ""))
    url      = raw.get("url", "")
    location = clean_text(raw.get("location", "") or "")
    body     = clean_text(raw.get("body", "") or "")

    can_url = canonicalize_url(url)
    # Use normalised location for fingerprint so "London", "London UK", "London / Hybrid"
    # all resolve to the same bucket and don't create duplicate entries.
    norm_loc    = detect_location(f"{location} {body}", company=company)
    fingerprint = CanonicalJob.build_fingerprint(company, title, norm_loc)

    return CanonicalJob(
        title=title,
        company=company,
        apply_url=url,
        canonical_url=can_url,
        fingerprint=fingerprint,
        location_raw=location or None,
        description_text=body or None,
        department=raw.get("department") or None,
        employment_type=raw.get("employment_type") or None,
        ats_type=raw.get("ats_type") or source.get("type") or None,
        external_job_id=raw.get("external_id") or None,
        posted_at=raw.get("posted_at") or None,
        source_name=source.get("name", ""),
        source_kind=source.get("kind", ""),
        source_priority=int(source.get("priority", 9)),
        source_type=source.get("type", "html"),
        matched_keyword=raw.get("matched_keyword") or None,
    )


# ── Database ──────────────────────────────────────────────────────────────────

def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")   # Phase 1b: prevents locking errors
    return conn

def db_execute(query, params=(), fetch=False):
    conn = db()
    cur  = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return rows

def init_db():
    conn = db()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key          TEXT UNIQUE,
            canonical_url       TEXT,
            fingerprint         TEXT,
            title               TEXT,
            company             TEXT,
            location_raw        TEXT,
            location_normalized TEXT,
            description_text    TEXT,
            department          TEXT,
            employment_type     TEXT,
            apply_url           TEXT,
            ats_type            TEXT,
            external_job_id     TEXT,
            posted_at           TEXT,
            source_name         TEXT,
            source_kind         TEXT,
            source_priority     INTEGER,
            source_type         TEXT,
            first_seen          TEXT,
            last_seen           TEXT,
            matched_keyword     TEXT,
            score               REAL,
            score_breakdown_json TEXT,
            miss_count          INTEGER DEFAULT 0,
            job_status          TEXT DEFAULT 'active',
            raw_blob            TEXT
        )
    """)

    # Safe migrations — add any missing columns to existing deployments
    existing_cols_rows = conn.execute("PRAGMA table_info(jobs)").fetchall()
    existing_cols = {r[1] for r in existing_cols_rows}
    migrations = [
        ("miss_count",           "INTEGER DEFAULT 0"),
        ("job_status",           "TEXT DEFAULT 'active'"),
        ("fingerprint",          "TEXT"),
        ("location_raw",         "TEXT"),
        ("location_normalized",  "TEXT"),
        ("description_text",     "TEXT"),
        ("department",           "TEXT"),
        ("employment_type",      "TEXT"),
        ("apply_url",            "TEXT"),
        ("ats_type",             "TEXT"),
        ("external_job_id",      "TEXT"),
        ("posted_at",            "TEXT"),
        ("score_breakdown_json", "TEXT"),
    ]
    for col, definition in migrations:
        if col not in existing_cols:
            try:
                cur.execute(f"ALTER TABLE jobs ADD COLUMN {col} {definition}")
            except sqlite3.OperationalError:
                pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_sources (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key  TEXT,
            source_name TEXT,
            source_type TEXT,
            url         TEXT,
            seen_at     TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key   TEXT PRIMARY KEY,
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            company  TEXT NOT NULL,
            kind     TEXT NOT NULL,
            priority INTEGER DEFAULT 3,
            type     TEXT NOT NULL,
            url      TEXT NOT NULL,
            active   INTEGER DEFAULT 1,
            added_at TEXT
        )
    """)

    # Phase 1b: extended source_health with event_type column
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            source_name          TEXT PRIMARY KEY,
            last_run_at          TEXT,
            last_success_at      TEXT,
            last_failure_at      TEXT,
            last_error           TEXT,
            last_event_type      TEXT,
            consecutive_fails    INTEGER DEFAULT 0,
            consecutive_successes INTEGER DEFAULT 0,
            total_runs           INTEGER DEFAULT 0,
            jobs_found_last      INTEGER DEFAULT 0,
            jobs_found_total     INTEGER DEFAULT 0,
            status               TEXT DEFAULT 'unknown'
        )
    """)

    # Safe migration for source_health new columns
    sh_cols_rows = conn.execute("PRAGMA table_info(source_health)").fetchall()
    sh_cols = {r[1] for r in sh_cols_rows}
    for col, definition in [
        ("last_event_type",       "TEXT"),
        ("consecutive_successes", "INTEGER DEFAULT 0"),
    ]:
        if col not in sh_cols:
            try:
                cur.execute(f"ALTER TABLE source_health ADD COLUMN {col} {definition}")
            except sqlite3.OperationalError:
                pass

    # Indexes — safe to run repeatedly, match current query patterns
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_unique_key ON jobs(unique_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(job_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_score      ON jobs(score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company    ON jobs(company)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen)")

    conn.commit()
    conn.close()
    seed_defaults()


def seed_defaults():
    # Always ensure all default keywords are present — handles new additions on redeploy
    for kw in DEFAULT_KEYWORDS:
        db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))

    # Always ensure all default excludes are present — handles new additions
    # on redeploy without wiping any custom excludes the user has added
    for phrase in DEFAULT_EXCLUDES:
        db_execute("INSERT OR IGNORE INTO excludes (phrase) VALUES (?)", (phrase,))

    if not get_state("location_mode"): set_state("location_mode", "london")
    if not get_state("paused"):        set_state("paused", "0")
    if not get_state("quality_mode"):  set_state("quality_mode", "off")

    existing = db_execute("SELECT COUNT(*) FROM sources", fetch=True)
    if not existing or existing[0][0] == 0:
        for s in DEFAULT_SOURCES:
            db_execute(
                """INSERT OR IGNORE INTO sources (name, company, kind, priority, type, url, active, added_at)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                (s["name"], s["company"], s["kind"], s["priority"], s["type"], s["url"], now_str())
            )


# ── Source registry ───────────────────────────────────────────────────────────

def get_active_sources():
    rows = db_execute(
        "SELECT name, company, kind, priority, type, url FROM sources WHERE active=1 ORDER BY priority ASC",
        fetch=True
    )
    return [
        {"name": r[0], "company": r[1], "kind": r[2], "priority": r[3], "type": r[4], "url": r[5]}
        for r in (rows or [])
    ]


# ── Source health ─────────────────────────────────────────────────────────────
#
# Phase 1b: distinguishes between event types:
#   success_nonzero  — fetched OK, jobs found
#   success_zero     — fetched OK, but zero jobs returned (may be parser break)
#   http_error       — HTTP request failed or non-200 status
#   parse_error      — request OK but parsing raised an exception
#   timeout          — request timed out

def _upsert_health(source_name: str, now: str, success: bool, event_type: str,
                   jobs_found: int = 0, error: str = ""):
    existing = db_execute(
        "SELECT consecutive_fails, consecutive_successes, total_runs, jobs_found_total FROM source_health WHERE source_name=?",
        (source_name,), fetch=True
    )

    error_short = error[:300] if error else None

    if existing:
        c_fails, c_succ, total_runs, jobs_total = existing[0]
        total_runs  += 1
        jobs_total  += jobs_found

        if success:
            c_succ  += 1
            c_fails  = 0
        else:
            c_fails += 1
            c_succ   = 0

        status = _compute_status(c_fails, c_succ, event_type)

        db_execute("""
            UPDATE source_health SET
                last_run_at=?, last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END,
                last_failure_at=CASE WHEN NOT ? THEN ? ELSE last_failure_at END,
                last_error=CASE WHEN NOT ? THEN ? ELSE last_error END,
                last_event_type=?,
                consecutive_fails=?, consecutive_successes=?,
                total_runs=?, jobs_found_last=?, jobs_found_total=?, status=?
            WHERE source_name=?
        """, (
            now,
            success, now,
            success, now,
            success, error_short,
            event_type,
            c_fails, c_succ,
            total_runs, jobs_found, jobs_total, status,
            source_name,
        ))
        return c_fails, status
    else:
        status = _compute_status(0 if success else 1, 1 if success else 0, event_type)
        db_execute("""
            INSERT INTO source_health
                (source_name, last_run_at, last_success_at, last_failure_at, last_error,
                 last_event_type, consecutive_fails, consecutive_successes,
                 total_runs, jobs_found_last, jobs_found_total, status)
            VALUES (?,?,?,?,?,?,?,?,1,?,?,?)
        """, (
            source_name, now,
            now if success else None,
            now if not success else None,
            error_short,
            event_type,
            0 if success else 1,
            1 if success else 0,
            jobs_found, jobs_found, status,
        ))
        return (0 if success else 1), status


def _compute_status(c_fails: int, c_succ: int, event_type: str) -> str:
    if c_fails >= 7:
        return "dead"
    if c_fails >= 3:
        return "degraded"
    # success_zero is suspicious but not immediately degraded
    if event_type == "success_zero" and c_succ == 0:
        return "suspect"
    return "healthy"


def record_source_success(source_name: str, jobs_found: int):
    event_type = "success_nonzero" if jobs_found > 0 else "success_zero"
    return _upsert_health(source_name, now_str(), True, event_type, jobs_found=jobs_found)


def record_source_failure(source_name: str, error: str, event_type: str = "http_error"):
    fails, status = _upsert_health(source_name, now_str(), False, event_type, error=error)
    return fails, status


# ── Job expiry ────────────────────────────────────────────────────────────────

def expire_stale_jobs(seen_keys: set):
    active = db_execute("SELECT unique_key FROM jobs WHERE job_status='active'", fetch=True)
    for (key,) in (active or []):
        if key not in seen_keys:
            db_execute("""
                UPDATE jobs
                SET miss_count = miss_count + 1,
                    job_status = CASE WHEN miss_count + 1 >= 3 THEN 'expired' ELSE job_status END
                WHERE unique_key = ?
            """, (key,))
        else:
            db_execute("UPDATE jobs SET miss_count=0 WHERE unique_key=?", (key,))


# ── State / settings ──────────────────────────────────────────────────────────

def set_state(key, value):
    db_execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, str(value)))

def get_state(key, default=""):
    rows = db_execute("SELECT value FROM state WHERE key=?", (key,), fetch=True)
    return rows[0][0] if rows else default

def get_keywords():
    return [r[0] for r in db_execute("SELECT keyword FROM keywords ORDER BY keyword", fetch=True)]

def get_excludes():
    return [r[0] for r in db_execute("SELECT phrase FROM excludes ORDER BY phrase", fetch=True)]

def add_keyword(keyword):
    kw = normalize_text(keyword)
    if kw: db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))

def remove_keyword(keyword):
    kw = normalize_text(keyword)
    if kw: db_execute("DELETE FROM keywords WHERE keyword=?", (kw,))

def quality_threshold():
    mode = get_state("quality_mode", "off").lower()
    return 75 if mode == "strict" else (45 if mode == "normal" else 0)


# ── Scoring engine ────────────────────────────────────────────────────────────
# Phase 1b: returns (total_score, breakdown_dict) instead of just an int.

TITLE_BOOSTS = {
    "junior production coordinator": 38,
    "graduate producer":             40,
    "production assistant":          35,
    "production coordinator":        32,
    "production trainee":            34,
    "assistant producer":            30,
    "production intern":             28,
    "studio assistant":              24,
    "studio runner":                 22,
    "project coordinator":           20,
    "project assistant":             18,
    "runner":                        14,
}

PREFERRED_COMPANIES = {
    "framestore", "nexus studios", "dneg", "cinesite", "blue zoo",
    "jellyfish pictures", "ilm", "milk", "bluebolt", "outpost",
}

def score_job(job: CanonicalJob) -> tuple[float, dict]:
    """
    Returns (total_score, breakdown_dict).
    Breakdown is stored as score_breakdown_json for explainability.
    """
    title   = normalize_text(job.title)
    body    = normalize_text(job.description_text or "")
    company = normalize_text(job.company)
    blob    = f"{title} {body}"
    breakdown = {}

    # Title strength
    title_pts = 0
    for phrase, pts in TITLE_BOOSTS.items():
        if phrase in title:
            title_pts = max(title_pts, pts)
    breakdown["title_strength"] = title_pts

    # Juniority signals (in body)
    juniority_terms = ["junior", "graduate", "assistant", "trainee", "intern", "entry level",
                       "no experience", "recent graduate", "entry-level", "school leaver"]
    juniority_pts = min(sum(6 for t in juniority_terms if t in blob), 30)
    breakdown["juniority"] = juniority_pts

    # Location confidence
    loc_raw = normalize_text((job.location_raw or "") + " " + (job.description_text or ""))
    if any(t in loc_raw for t in NON_UK_TERMS):
        loc_pts = -100
    elif any(t in loc_raw for t in LONDON_TERMS):
        loc_pts = 30
    elif any(t in loc_raw for t in UK_TERMS):
        loc_pts = 18
    elif job.company in UK_STUDIO_COMPANIES:
        loc_pts = 10
    else:
        loc_pts = 0
    breakdown["location_confidence"] = loc_pts

    # Source quality
    source_pts = 0
    if job.source_kind == "studio":
        p = job.source_priority
        source_pts = 20 if p == 1 else (14 if p == 2 else 8)
    elif job.source_kind == "industry_board":
        source_pts = 8
    breakdown["source_quality"] = source_pts

    # ATS type bonus
    ats_pts = 8 if job.ats_type in {"greenhouse", "lever", "ashby", "workable"} else \
              5 if job.ats_type in {"jobvite", "teamtailor"} else 0
    breakdown["ats_type"] = ats_pts

    # Company tier
    company_pts = 10 if company in PREFERRED_COMPANIES else 0
    breakdown["company_tier"] = company_pts

    # Negative indicators
    neg_terms = ["senior", "lead", "director", "supervisor", "executive producer",
                 "principal", "recruiter", "software engineer", "technical director", " td "]
    neg_pts = sum(-40 for t in neg_terms if t in blob)
    neg_pts = max(neg_pts, -100)
    breakdown["negative_indicators"] = neg_pts

    total = max(
        title_pts + juniority_pts + loc_pts + source_pts + ats_pts + company_pts + neg_pts,
        0
    )
    breakdown["total"] = total
    return float(total), breakdown


# ── Location / filtering ──────────────────────────────────────────────────────

def detect_location(text: str, company: str = "") -> str:
    hay = normalize_text(text)
    if any(t in hay for t in NON_UK_TERMS): return "Non-UK"
    if any(t in hay for t in LONDON_TERMS):  return "London"
    if any(t in hay for t in UK_TERMS):      return "UK"
    if company in UK_STUDIO_COMPANIES:       return "Unknown-UK-Studio"
    return ""

def location_allowed(job: CanonicalJob) -> bool:
    mode = get_state("location_mode", "off").lower()
    if mode == "off":
        return True

    # For the Non-UK hard-reject, only check the location field itself —
    # NOT the full description. Studio careers pages often mention global
    # offices in body text which was incorrectly triggering Non-UK rejection.
    loc_signal = detect_location(job.location_raw or "", company=job.company)
    if loc_signal == "Non-UK":
        return False

    # For positive location detection, use title + location + company
    blob = " ".join(filter(None, [job.title, job.location_raw, job.company]))
    loc  = detect_location(blob, company=job.company)

    # Clean London signal
    if loc == "London":
        return True

    # UK mode — also accept explicit UK signal
    if mode == "uk" and loc == "UK":
        return True

    # Known London VFX studio source — trust it
    if job.source_kind == "studio" and job.company in UK_STUDIO_COMPANIES:
        return True

    return False

def title_keyword_match(job: CanonicalJob):
    """
    Require a keyword match for all sources.
    Always apply excludes to drop non-production-track roles.
    """
    hay = normalize_text(f"{job.title} {job.description_text or ''}")

    # Always apply excludes first
    if any(ex in hay for ex in get_excludes()):
        return False, None

    # Require keyword match for all sources
    matched = next((kw for kw in get_keywords() if kw in hay), None)
    if not matched:
        return False, None
    return True, matched


# ── Scraping ──────────────────────────────────────────────────────────────────

def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.text

def fetch_json(url: str):
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

def identify_ats_type(url: str) -> Optional[str]:
    low = url.lower()
    for ats_type, patterns in ATS_PATTERNS.items():
        if any(p in low for p in patterns):
            return ats_type
    return None

def discover_ats_sources_from_html(source: dict, html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    discovered, seen = [], set()
    for a in soup.find_all("a", href=True):
        href     = urljoin(source["url"], a["href"].strip())
        ats_type = identify_ats_type(href)
        if not ats_type:
            continue
        key = f"{ats_type}|{canonicalize_url(href)}"
        if key in seen:
            continue
        seen.add(key)
        discovered.append({
            "name": f"{source['company']} {ats_type}", "company": source["company"],
            "kind": source["kind"], "priority": source["priority"],
            "type": ats_type, "url": href,
        })
    return discovered

def generic_extract_jobs_from_soup(source: dict, soup) -> list:
    jobs, seen = [], set()
    source_path = urlparse(source["url"]).path.rstrip("/")
    NAV_PATTERNS = {
        "home", "about", "contact", "menu", "login", "sign in", "register",
        "privacy", "terms", "cookie", "back", "next", "previous", "more",
        "read more", "view all", "see all", "apply now", "apply here",
    }
    for a in soup.find_all("a", href=True):
        href  = a.get("href", "").strip()
        title = clean_text(a.get_text(" ", strip=True))
        if not href or not title or len(title) < 8:
            continue
        # Reject titles that look like URL paths — e.g. "/resource/launchpad-internship"
        if title.startswith("/") or title.startswith("http"):
            continue
        if normalize_text(title) in NAV_PATTERNS:
            continue
        full_url  = urljoin(source["url"], href)
        link_path = urlparse(full_url).path.rstrip("/")
        if link_path == source_path or not link_path:
            continue
        context      = clean_text(a.parent.get_text(" ", strip=True)) if a.parent else title
        context_blob = f"{title} {context} {full_url}"
        if not any(t in normalize_text(context_blob) for t in [
            "job", "career", "vacancy", "opening", "role",
            "producer", "production", "runner", "assistant", "coordinator", "intern"
        ]):
            continue
        key = short_hash(full_url, title)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({
            "title": title, "company": source["company"],
            "location": detect_location(context_blob, company=source["company"]),
            "url": full_url, "body": context,
        })
    return jobs

def parse_html(source: dict):
    try:
        html = fetch_text(source["url"])
        soup = BeautifulSoup(html, "html.parser")
        return generic_extract_jobs_from_soup(source, soup), discover_ats_sources_from_html(source, html)
    except requests.exceptions.Timeout:
        raise RuntimeError("timeout")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"http_error:{e.response.status_code if e.response else '?'}")

def parse_greenhouse(source: dict):
    try:
        url = source["url"]
        m   = re.search(r"(?:boards|job-boards)\.greenhouse\.io/([^/?#]+)", url)
        if m:
            data = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{m.group(1)}/jobs")
            jobs = []
            for item in data.get("jobs", []):
                jobs.append({
                    "title":       clean_text(item.get("title", "")),
                    "location":    clean_text((item.get("location") or {}).get("name", "")),
                    "url":         item.get("absolute_url", ""),
                    "body":        json.dumps(item),
                    "external_id": str(item.get("id", "")),
                    "ats_type":    "greenhouse",
                })
            return jobs, []
    except Exception:
        pass
    return parse_html(source)

def parse_lever(source: dict):
    try:
        m = re.search(r"(?:jobs|api)\.lever\.co/(?:v0/postings/)?([^/?#]+)", source["url"])
        if m:
            data = fetch_json(f"https://api.lever.co/v0/postings/{m.group(1)}?mode=json")
            jobs = []
            for item in data:
                cats = item.get("categories") or {}
                jobs.append({
                    "title":       clean_text(item.get("text", "")),
                    "location":    clean_text(cats.get("location", "")),
                    "url":         item.get("hostedUrl", ""),
                    "body":        json.dumps(item),
                    "external_id": item.get("id", ""),
                    "ats_type":    "lever",
                    "posted_at":   str(item.get("createdAt", "")),
                })
            return jobs, []
    except Exception:
        pass
    return parse_html(source)

def parse_workable(source: dict):  return parse_html(source)
def parse_ashby(source: dict):     return parse_html(source)
def parse_jobvite(source: dict):   return parse_html(source)
def parse_teamtailor(source: dict):return parse_html(source)
def parse_smartrecruiters(source: dict): return parse_html(source)
def parse_workday(source: dict):   return parse_html(source)

def fetch_source_jobs(source: dict):
    t = source["type"]
    if t == "greenhouse":      return parse_greenhouse(source)
    if t == "lever":           return parse_lever(source)
    if t == "workable":        return parse_workable(source)
    if t == "ashby":           return parse_ashby(source)
    if t == "jobvite":         return parse_jobvite(source)
    if t == "teamtailor":      return parse_teamtailor(source)
    if t == "smartrecruiters": return parse_smartrecruiters(source)
    if t == "workday":         return parse_workday(source)
    return parse_html(source)


# ── Job storage ───────────────────────────────────────────────────────────────

def build_unique_key(job: CanonicalJob) -> str:
    if job.canonical_url:
        return f"url::{job.canonical_url}"
    return f"fp::{job.fingerprint}"

def upsert_job(job: CanonicalJob) -> tuple[bool, str]:
    unique_key = build_unique_key(job)
    now        = now_str()
    existing   = db_execute(
        "SELECT id, source_priority FROM jobs WHERE unique_key=?", (unique_key,), fetch=True
    )
    bd_json = json.dumps(job.score_breakdown)

    if not existing:
        db_execute("""
            INSERT INTO jobs (
                unique_key, canonical_url, fingerprint, title, company,
                location_raw, location_normalized, description_text, department, employment_type,
                apply_url, ats_type, external_job_id, posted_at,
                source_name, source_kind, source_priority, source_type,
                first_seen, last_seen, matched_keyword,
                score, score_breakdown_json, miss_count, job_status, raw_blob
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'active',?)
        """, (
            unique_key, job.canonical_url, job.fingerprint, job.title, job.company,
            job.location_raw, job.location_normalized, job.description_text,
            job.department, job.employment_type,
            job.apply_url, job.ats_type, job.external_job_id, job.posted_at,
            job.source_name, job.source_kind, job.source_priority, job.source_type,
            now, now, job.matched_keyword,
            job.score, bd_json,
            json.dumps(job.to_dict(), ensure_ascii=False, default=str)[:3000],
        ))
        return True, unique_key
    else:
        _, current_priority = existing[0]
        if int(job.source_priority) < int(current_priority):
            db_execute("""
                UPDATE jobs SET
                    canonical_url=?, fingerprint=?, title=?, company=?,
                    location_raw=?, description_text=?, apply_url=?,
                    ats_type=?, external_job_id=?, posted_at=?,
                    source_name=?, source_kind=?, source_priority=?, source_type=?,
                    last_seen=?, matched_keyword=?, score=?, score_breakdown_json=?,
                    miss_count=0, job_status='active'
                WHERE unique_key=?
            """, (
                job.canonical_url, job.fingerprint, job.title, job.company,
                job.location_raw, job.description_text, job.apply_url,
                job.ats_type, job.external_job_id, job.posted_at,
                job.source_name, job.source_kind, job.source_priority, job.source_type,
                now, job.matched_keyword, job.score, bd_json,
                unique_key,
            ))
        else:
            db_execute(
                "UPDATE jobs SET last_seen=?, score=?, score_breakdown_json=?, miss_count=0, job_status='active' WHERE unique_key=?",
                (now, job.score, bd_json, unique_key),
            )
        return False, unique_key


# ── Core monitoring run ───────────────────────────────────────────────────────

def collect_and_store_jobs(force: bool = False) -> list:
    """
    Run a full scrape across all active sources in parallel.

    force=False (default, scheduler): only return genuinely new jobs.
    force=True (/scan): return all jobs passing filters, including existing ones.
    """
    sources   = get_active_sources()
    threshold = quality_threshold()
    all_matched = []
    seen_keys   = set()
    lock        = threading.Lock()

    def _scrape_one(source):
        try:
            raw_jobs, discovered = fetch_source_jobs(source)
            event_type = "success_nonzero" if raw_jobs else "success_zero"
            record_source_success(source["name"], len(raw_jobs))
            return source, raw_jobs, None
        except Exception as e:
            err = str(e)
            event_type = "timeout" if "timeout" in err.lower() else "parse_error"
            fails, _ = record_source_failure(source["name"], err[:200], event_type)
            if fails == 3:
                send_telegram_message(f"⚠️ Source degraded: {source['name']}")
            elif fails == 7:
                send_telegram_message(f"🔴 Source dead: {source['name']}")
            return source, [], err

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_scrape_one, s): s for s in sources}
        for future in concurrent.futures.as_completed(futures, timeout=60):
            try:
                source, raw_jobs, err = future.result()
            except Exception:
                continue
            if err or not raw_jobs:
                continue
            for raw in raw_jobs:
                job = normalise_to_canonical(raw, source)
                ok, matched_keyword = title_keyword_match(job)
                if not ok:
                    continue
                job.matched_keyword = matched_keyword
                if not location_allowed(job):
                    continue
                total_score, breakdown = score_job(job)
                job.score           = total_score
                job.score_breakdown = breakdown
                blob = " ".join(filter(None, [job.title, job.location_raw, job.description_text]))
                job.location_normalized = detect_location(blob, company=job.company) or None

                # Hard drop anything explicitly Non-UK — score 0 means location penalty fired
                if job.location_normalized == "Non-UK" or total_score <= 0:
                    continue

                if total_score < threshold:
                    continue
                with lock:
                    created, unique_key = upsert_job(job)
                    seen_keys.add(unique_key)
                    if created or force:
                        all_matched.append(job)

    expire_stale_jobs(seen_keys)
    return sorted(all_matched, key=lambda j: j.score or 0, reverse=True)


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram_message(text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return
    target = str(chat_id or TELEGRAM_CHAT_ID).strip()
    if not target:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": target, "text": text[:4000], "disable_web_page_preview": True},
            timeout=20,
        )
    except Exception:
        pass

def telegram_api(method, payload=None):
    r = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
        json=payload or {}, timeout=30,
    )
    r.raise_for_status()
    return r.json()

def get_updates(offset=None):
    payload = {"timeout": 20}
    if offset is not None:
        payload["offset"] = offset
    try:
        return telegram_api("getUpdates", payload).get("result", [])
    except Exception:
        return []


# ── Alert formatting ──────────────────────────────────────────────────────────

def format_job_alert(job: CanonicalJob) -> str:
    """Phase 1b: rich alert format with score breakdown."""
    bd   = job.score_breakdown or {}
    loc  = job.location_raw or job.location_normalized or "Unknown"
    lines = [
        f"🎯 {'HIGH PRIORITY' if (job.score or 0) >= 75 else 'New role'}",
        f"{job.company} — {job.title}",
        f"📍 {loc}",
        f"⭐ Score: {int(job.score or 0)}",
        "",
        "Why it matched:",
    ]
    reasons = [
        (bd.get("title_strength", 0),    "strong title match"),
        (bd.get("juniority", 0),         "junior/entry-level signal"),
        (bd.get("location_confidence", 0),"London/UK location"),
        (bd.get("source_quality", 0),    "direct studio source"),
        (bd.get("ats_type", 0),          "ATS-native listing"),
        (bd.get("company_tier", 0),      "preferred studio"),
    ]
    for pts, label in reasons:
        if pts > 0:
            lines.append(f"  + {label} (+{pts})")
    negs = bd.get("negative_indicators", 0)
    if negs < 0:
        lines.append(f"  − negative signals ({negs})")

    if job.source_type:
        lines.append(f"\n📡 Source: {job.source_name} ({job.ats_type or job.source_type})")
    if job.apply_url:
        lines.append(f"🔗 {job.apply_url}")
    return "\n".join(lines)

def format_job_rows(rows) -> str:
    if not rows:
        return "No matching jobs saved yet."
    lines = []
    for idx, row in enumerate(rows, 1):
        title, company, loc, url, first_seen, score = row
        loc_part = f" | {loc}" if loc else ""
        lines.append(f"{idx}. {title} — {company}{loc_part}\nScore: {int(score)}\n{url}\nFound: {first_seen}\n")
    return "\n".join(lines[:10])

def latest_rows(hours=24, limit=10):
    cutoff = (utc_now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S UTC")
    return db_execute("""
        SELECT title, company, location_raw, apply_url, first_seen, score
        FROM jobs WHERE first_seen >= ? AND job_status='active'
        ORDER BY score DESC, id DESC LIMIT ?
    """, (cutoff, limit), fetch=True)

def send_new_job_alerts(jobs: list):
    if not jobs:
        return
    high   = [j for j in jobs if (j.score or 0) >= 75]
    normal = [j for j in jobs if quality_threshold() <= (j.score or 0) < 75]

    for job in high[:6]:
        send_telegram_message(format_job_alert(job))
        time.sleep(0.5)

    if normal and get_state("quality_mode", "normal").lower() != "strict":
        lines = [f"📋 {len(normal)} new matching role{'s' if len(normal) != 1 else ''}:\n"]
        for job in normal[:6]:
            loc = job.location_raw or job.location_normalized or ""
            loc_part = f" | {loc}" if loc else ""
            lines.append(f"• {job.title} — {job.company}{loc_part} (score: {int(job.score or 0)})\n{job.apply_url}\n")
        send_telegram_message("\n".join(lines))


# ── Telegram commands ─────────────────────────────────────────────────────────

def handle_command(text: str) -> str:
    text  = clean_text(text)
    lower = text.lower()

    if lower == "/help":
        return (
            "Commands:\n"
            "/help /status\n"
            "/showall — instantly show everything in DB matching your filters\n"
            "/scan — fresh scrape NOW + show all matching roles\n"
            "/scandebug — same as /scan but shows per-source detail\n"
            "/jobs /latest /highpriority\n"
            "/search <term>\n"
            "/keywords /addkeyword <x> /removekeyword <x>\n"
            "/companies /sources\n"
            "/health /dead\n"
            "/setlocation london|uk|off\n"
            "/quality strict|normal|off\n"
            "/pause /resume"
        )

    if lower == "/showall":
        rows = db_execute("""
            SELECT title, company, location_raw, apply_url, first_seen, score,
                   score_breakdown_json, matched_keyword, source_name, ats_type
            FROM jobs
            WHERE job_status = 'active'
            ORDER BY score DESC, id DESC
            LIMIT 30
        """, fetch=True)
        if not rows:
            return (
                "No active jobs in the database yet.\n"
                "Use /scan to trigger a fresh scrape, or check /status to see if the bot has run."
            )
        total = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
        # Send header
        send_telegram_message(
            f"📋 All stored matching roles ({total} total, showing top {min(len(rows), 30)} by score):"
        )
        time.sleep(0.3)
        # Send each job as a rich card
        for row in rows[:30]:
            title, company, loc, url, first_seen, score, bd_json, keyword, source_name, ats_type = row
            bd = {}
            try:
                bd = json.loads(bd_json or "{}")
            except Exception:
                pass
            loc_line  = f"📍 {loc}" if loc else ""
            src_line  = f"📡 {source_name}" + (f" ({ats_type})" if ats_type else "")
            kw_line   = f"🔑 Matched: {keyword}" if keyword else ""
            date_line = f"👁 First seen: {first_seen}"

            reasons = []
            for key, label in [
                ("title_strength",    "title match"),
                ("juniority",         "junior signal"),
                ("location_confidence","location"),
                ("source_quality",    "studio source"),
            ]:
                v = bd.get(key, 0)
                if v and v > 0:
                    reasons.append(f"+{v} {label}")

            lines = [
                f"⭐ Score: {int(score or 0)} — {title}",
                f"🏢 {company}",
            ]
            if loc_line:   lines.append(loc_line)
            if kw_line:    lines.append(kw_line)
            if reasons:    lines.append("Why: " + " | ".join(reasons))
            lines.append(date_line)
            lines.append(src_line)
            if url:        lines.append(f"🔗 {url}")

            send_telegram_message("\n".join(lines))
            time.sleep(0.4)

        if total > 30:
            send_telegram_message(f"...and {total - 30} more. Use /search <company> to filter.")
        return ""   # already sent everything directly

    if lower == "/scan" or lower == "/scandebug":
        debug = (lower == "/scandebug")

        def _run_scan():
            try:
                sources   = get_active_sources()
                threshold = quality_threshold()
                send_telegram_message(
                    f"🔍 Scanning {len(sources)} sources...\n"
                    f"Location: {get_state('location_mode','london')} | "
                    f"Score threshold: {threshold}"
                    + ("\n[debug mode — full per-source detail]" if debug else "")
                )

                all_matched   = []
                source_log    = []
                seen_keys     = set()
                lock          = threading.Lock()

                def _scrape_source(source):
                    """Scrape one source and return (source, raw_jobs, discovered, error)."""
                    try:
                        raw_jobs, discovered = fetch_source_jobs(source)
                        record_source_success(source["name"], len(raw_jobs))
                        return source, raw_jobs, discovered, None
                    except Exception as e:
                        err = str(e)[:120]
                        record_source_failure(source["name"], err, "parse_error")
                        return source, [], [], err

                sources_to_run = list(get_active_sources())

                # Run all sources in parallel — max 10 workers, 12s timeout each
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {executor.submit(_scrape_source, s): s for s in sources_to_run}
                    for future in concurrent.futures.as_completed(futures, timeout=30):
                        try:
                            source, raw_jobs, discovered, err = future.result()
                        except concurrent.futures.TimeoutError:
                            source = futures[future]
                            record_source_failure(source["name"], "timeout", "timeout")
                            source_log.append((source["name"], 0, 0, "timeout"))
                            continue
                        except Exception as e:
                            source = futures[future]
                            source_log.append((source["name"], 0, 0, str(e)[:120]))
                            continue

                        if err:
                            source_log.append((source["name"], 0, 0, err))
                            continue

                        matched_this_source = 0
                        for raw in raw_jobs:
                            job = normalise_to_canonical(raw, source)
                            ok, matched_keyword = title_keyword_match(job)
                            if not ok:
                                continue
                            job.matched_keyword = matched_keyword
                            if not location_allowed(job):
                                continue
                            total_score, breakdown = score_job(job)
                            job.score           = total_score
                            job.score_breakdown = breakdown
                            blob = " ".join(filter(None, [job.title, job.location_raw, job.description_text]))
                            job.location_normalized = detect_location(blob, company=job.company) or None
                            if job.location_normalized == "Non-UK" or total_score <= 0:
                                continue
                            if total_score < threshold:
                                continue
                            with lock:
                                created, unique_key = upsert_job(job)
                                seen_keys.add(unique_key)
                                all_matched.append(job)
                            matched_this_source += 1

                        source_log.append((source["name"], len(raw_jobs), matched_this_source, None))

                expire_stale_jobs(seen_keys)
                all_matched.sort(key=lambda j: j.score or 0, reverse=True)

                # ── Debug: per-source breakdown ──
                if debug:
                    lines = ["📊 Per-source results:"]
                    for name, raw_c, match_c, err in source_log:
                        if err:
                            lines.append(f"  ❌ {name}: ERROR — {err}")
                        elif raw_c == 0:
                            lines.append(f"  🟡 {name}: 0 jobs found")
                        elif match_c == 0:
                            lines.append(f"  ⚪ {name}: {raw_c} found, 0 passed filters")
                        else:
                            lines.append(f"  ✅ {name}: {raw_c} found, {match_c} matched")
                    # Split into chunks to avoid Telegram 4096 char limit
                    chunk, chunks = [], []
                    for line in lines:
                        chunk.append(line)
                        if len("\n".join(chunk)) > 3500:
                            chunks.append("\n".join(chunk))
                            chunk = []
                    if chunk:
                        chunks.append("\n".join(chunk))
                    for c in chunks:
                        send_telegram_message(c)
                        time.sleep(0.3)

                # ── Summary ──
                errors   = sum(1 for _, _, _, e in source_log if e)
                zero_src = sum(1 for _, r, _, e in source_log if r == 0 and not e)
                send_telegram_message(
                    f"✅ Scan complete\n"
                    f"Sources: {len(source_log)} checked, {errors} errors, {zero_src} returned zero jobs\n"
                    f"Matched: {len(all_matched)} role{'s' if len(all_matched) != 1 else ''} passed all filters\n"
                    + (f"Threshold was {threshold} — try /quality off then /scan to see everything"
                       if len(all_matched) == 0 and errors < len(source_log) else "")
                )

                if not all_matched:
                    return

                send_telegram_message(f"Sending top {min(len(all_matched), 10)}:")
                for job in all_matched[:10]:
                    send_telegram_message(format_job_alert(job))
                    time.sleep(0.5)
                if len(all_matched) > 10:
                    send_telegram_message(f"...and {len(all_matched) - 10} more. Use /jobs to see all.")

            except Exception as e:
                import traceback
                send_telegram_message(f"❌ Scan crashed: {str(e)[:300]}\n{traceback.format_exc()[:500]}")

        threading.Thread(target=_run_scan, daemon=True).start()
        return "⏳ Scan started — results incoming shortly."

    if lower == "/status":
        total = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
        return (
            f"{'⏸ Paused' if get_state('paused','0')=='1' else '▶️ Running'}\n"
            f"Last checked: {get_state('last_checked','Never')}\n"
            f"New last run: {get_state('last_match_count','0')}\n"
            f"Active jobs: {total}\n"
            f"Location: {get_state('location_mode','london')} | "
            f"Quality: {get_state('quality_mode','normal')} (≥{quality_threshold()})\n"
            f"Interval: {CHECK_INTERVAL_SECONDS}s"
        )

    if lower == "/jobs":
        rows = db_execute("""
            SELECT title, company, location_raw, apply_url, first_seen, score
            FROM jobs WHERE job_status='active' ORDER BY score DESC, id DESC LIMIT 10
        """, fetch=True)
        return format_job_rows(rows)

    if lower == "/latest":
        return format_job_rows(latest_rows(hours=24))

    if lower == "/highpriority":
        rows = db_execute("""
            SELECT title, company, location_raw, apply_url, first_seen, score
            FROM jobs WHERE score >= 75 AND job_status='active'
            ORDER BY score DESC LIMIT 10
        """, fetch=True)
        return format_job_rows(rows)

    if lower.startswith("/search "):
        term = lower.replace("/search ", "", 1).strip()
        rows = db_execute("""
            SELECT title, company, location_raw, apply_url, first_seen, score
            FROM jobs WHERE (lower(title) LIKE ? OR lower(company) LIKE ?) AND job_status='active'
            ORDER BY score DESC LIMIT 10
        """, (f"%{term}%", f"%{term}%"), fetch=True)
        return format_job_rows(rows) if rows else f'No active jobs for "{term}".'

    if lower == "/keywords":
        return "Keywords:\n" + "\n".join(f"- {k}" for k in get_keywords())

    if lower.startswith("/addkeyword "):
        kw = text[len("/addkeyword "):].strip()
        add_keyword(kw); return f'Added: "{normalize_text(kw)}"'

    if lower.startswith("/removekeyword "):
        kw = text[len("/removekeyword "):].strip()
        remove_keyword(kw); return f'Removed: "{normalize_text(kw)}"'

    if lower == "/companies":
        rows = db_execute("""
            SELECT company, COUNT(*) FROM jobs WHERE job_status='active'
            GROUP BY company ORDER BY COUNT(*) DESC
        """, fetch=True)
        return ("Companies:\n" + "\n".join(f"- {c} — {n}" for c, n in rows)) if rows else "No active jobs yet."

    if lower == "/sources":
        rows = db_execute("SELECT name, company, type, kind, active FROM sources ORDER BY priority", fetch=True)
        lines = [f"{'✅' if r[4] else '❌'} {r[1]} | {r[2]} | {r[3]}" for r in (rows or [])]
        return "Sources:\n" + "\n".join(lines) if lines else "No sources."

    if lower == "/health":
        rows = db_execute("SELECT status, COUNT(*) FROM source_health GROUP BY status", fetch=True)
        if not rows:
            return "No health data yet. Run a check first."
        total = (db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]])[0][0]
        summary = "\n".join(f"  {s}: {n}" for s, n in rows)
        return f"Source health ({total} active):\n{summary}"

    if lower == "/dead":
        rows = db_execute("""
            SELECT source_name, consecutive_fails, last_event_type, last_error, last_success_at
            FROM source_health WHERE status IN ('dead','degraded','suspect')
            ORDER BY consecutive_fails DESC
        """, fetch=True)
        if not rows:
            return "No degraded or dead sources. ✅"
        lines = []
        for name, fails, evt, error, last_ok in rows:
            icon = "🔴" if fails >= 7 else ("🟡" if evt == "success_zero" else "⚠️")
            lines.append(
                f"{icon} {name}\n"
                f"  Fails: {fails} | Type: {evt or '?'} | Last OK: {last_ok or 'never'}\n"
                f"  {(error or '')[:100]}"
            )
        return "\n\n".join(lines)

    if lower.startswith("/setlocation "):
        mode = lower.replace("/setlocation ", "", 1).strip()
        if mode not in {"london", "uk", "off"}: return "Use: london, uk, or off"
        set_state("location_mode", mode); return f"Location → {mode}"

    if lower.startswith("/quality "):
        mode = lower.replace("/quality ", "", 1).strip()
        if mode not in {"strict", "normal", "off"}: return "Use: strict, normal, or off"
        set_state("quality_mode", mode); return f"Quality → {mode} (threshold: {quality_threshold()})"

    if lower == "/pause":
        set_state("paused", "1"); return "⏸ Paused."

    if lower == "/resume":
        set_state("paused", "0"); return "▶️ Resumed."

    return "Unknown command. Send /help"


# ── Background threads ────────────────────────────────────────────────────────

def command_loop():
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text    = msg.get("text", "")
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
    total   = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
    sources = (db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]])[0][0]
    healthy = (db_execute("SELECT COUNT(*) FROM source_health WHERE status='healthy'", fetch=True) or [[0]])[0][0]
    return (
        f"VFX Job Monitor — Phase 1b\n"
        f"Active jobs: {total} | Sources: {sources} | Healthy: {healthy}\n"
        f"Last checked: {get_state('last_checked', 'Never')}"
    ), 200

@app.route("/health")
def health_check():
    return {"status": "ok"}, 200


# ── Startup ───────────────────────────────────────────────────────────────────

init_db()

def start_background_threads():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=command_loop, daemon=True).start()
    send_telegram_message(
        "✅ VFX Monitor — Phase 1c live\n"
        "WAL ✓  CanonicalJob ✓  Score breakdown ✓  Health events ✓\n"
        "New: /scan to trigger an immediate full scrape"
    )

start_background_threads()
