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
    "production assistant", "production coordinator", "junior production coordinator",
    "graduate producer", "assistant producer", "junior producer",
    "production trainee", "production intern", "production runner",
    "studio runner", "runner", "studio assistant",
    "project coordinator", "project assistant",
    "production manager", "production secretary",
    "production administrator", "production admin",
    "post production assistant", "post production coordinator",
    "post coordinator", "post assistant",
    "ep assistant", "executive assistant",
    "junior coordinator", "junior assistant",
    "graduate scheme", "graduate programme", "graduate program",
    "trainee", "apprentice", "internship", "intern",
    "work experience", "entry level",
    "launchpad", "kickstart", "talent scheme", "emerging talent",
]

DEFAULT_EXCLUDES = [
    # Seniority
    "senior", "supervisor", "head of", "director", "lead ",
    "principal", "executive producer", "vp ", "vice president", "manager",
    # Artist / technical
    "animator", "animation artist", "matte paint", "compositor", "compositing",
    "modell", "model artist", "ai artist", "media operator", "operator",
    "rigger", "rigging", "lighter", "lighting artist", "texture artist",
    "concept artist", "storyboard artist", "vfx artist", "cg artist",
    "3d artist", "2d artist", "motion graphic", "motion design",
    "editor", "colourist", "colorist", "sound design", "music supervisor",
    "technical director", " td ", "pipeline",
    "software engineer", "developer", "programmer",
    "it support", "systems admin", "data scientist", "machine learning",
    "recruiter", "talent acquisition", "hr ", "human resources",
    "accountant", "finance manager", "legal ", "lawyer", "solicitor",
    "sales ", "business development", "marketing manager",
    # Generic / non-specific postings
    "general application", "general enquiry", "speculative application",
    "internal mobility", "internal transfer", "expression of interest",
    # Specific non-production programmes
    "jedi academy", "animation launchpad", "animation-launchpad", "animation - launchpad", "animation intern",
]

PROGRAMME_TERMS = [
    "internship", "internships", "trainee", "traineeship", "academy",
    "work experience", "launchpad", "placement", "placements",
    "graduate programme", "graduate program", "graduate scheme",
]

DIRECT_ROLE_TERMS = [
    "production assistant", "production coordinator", "junior production coordinator",
    "assistant producer", "studio runner", "production runner", "runner",
    "studio assistant", "project assistant", "project coordinator",
    "post production assistant", "post-production assistant",
    "post production runner", "post-production runner", "team assistant",
]

UK_STUDIO_COMPANIES = {
    "Framestore", "Nexus Studios", "DNEG", "Cinesite", "Blue Zoo",
    "Jellyfish Pictures", "ILM", "Milk", "BlueBolt", "Outpost",
    "MPC", "The Mill", "Absolute", "Coffee & TV", "Envy", "Lola", "ScreenSkills",
    "Untold Studios", "Electric Theatre Collective",
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

# Domains that should never be auto-added as discovered sources
# (job boards, aggregators, non-studio sites)
DISCOVERY_BLOCKED_DOMAINS = {
    "entertainmentcareers.net", "indeed.com", "linkedin.com", "glassdoor.com",
    "reed.co.uk", "totaljobs.com", "cv-library.co.uk", "monster.co.uk",
    "mandy.com", "productionbase.co.uk", "theguardian.com", "broadcastnow.co.uk",
    "animationcareerreview.com", "creativeskillset.org",
}

DEFAULT_SOURCES = [
    {"name": "Framestore Careers",       "company": "Framestore",                  "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.framestore.com/careers"},
    {"name": "Framestore Recruitee",     "company": "Framestore",                  "kind": "studio",         "priority": 1, "type": "html",       "url": "https://framestore.recruitee.com/"},
    {"name": "DNEG Open Positions",      "company": "DNEG",                        "kind": "studio",         "priority": 1, "type": "jobvite",    "url": "https://jobs.jobvite.com/dneg"},
    {"name": "DNEG Jobvite",             "company": "DNEG",                        "kind": "studio",         "priority": 1, "type": "jobvite",    "url": "https://jobs.jobvite.com/double-negative-visual-effects/jobs"},
    {"name": "Cinesite Job Vacancies",   "company": "Cinesite",                    "kind": "studio",         "priority": 1, "type": "html",       "url": "https://cinesite.com/job-vacancies/"},
    {"name": "Blue Zoo Careers",         "company": "Blue Zoo",                    "kind": "studio",         "priority": 1, "type": "html",       "url": "https://careers.blue-zoo.co.uk/vacancies/vacancy-search-results.aspx?view=grid"},
    {"name": "Jellyfish Workable",       "company": "Jellyfish Pictures",          "kind": "studio",         "priority": 1, "type": "workable",   "url": "https://apply.workable.com/jellyfish-pictures-ltd/"},
    {"name": "Nexus Studios Workable",   "company": "Nexus Studios",               "kind": "studio",         "priority": 1, "type": "workable",   "url": "https://apply.workable.com/nexusstudios/"},
    {"name": "Nexus Teamtailor",         "company": "Nexus Studios",               "kind": "studio",         "priority": 1, "type": "teamtailor", "url": "https://nexusstudios.teamtailor.com/jobs"},
    {"name": "ILM Careers",              "company": "ILM",                         "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.ilm.com/careers/"},
    {"name": "Milk Careers",             "company": "Milk",                        "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.milk-vfx.com/careers/"},
    {"name": "BlueBolt Hiring",          "company": "BlueBolt",                    "kind": "studio",         "priority": 1, "type": "html",       "url": "https://www.blue-bolt.com/hiring"},
    {"name": "Outpost Careers",          "company": "Outpost",                     "kind": "studio",         "priority": 1, "type": "html",       "url": "https://careers.outpost-vfx.com/"},
    {"name": "Coffee & TV Careers",      "company": "Coffee & TV",                 "kind": "studio",         "priority": 2, "type": "html",       "url": "https://coffeeand.tv/about/join-us/"},
    {"name": "Envy Careers",             "company": "Envy",                        "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.envypost.co.uk/careers"},
    {"name": "Lola Post Careers",        "company": "Lola",                        "kind": "studio",         "priority": 2, "type": "html",       "url": "https://www.lola-post.com/careers"},
    {"name": "ScreenSkills Jobs",        "company": "ScreenSkills",                "kind": "industry_board", "priority": 3, "type": "html",       "url": "https://www.screenskills.com/jobs/"},
    {"name": "Realtime Careers",           "company": "Realtime",                    "kind": "studio",         "priority": 2, "type": "teamtailor", "url": "https://careers.realtimeuk.com/jobs"},
    {"name": "Untold Studios Teamtailor","company": "Untold Studios",              "kind": "studio",         "priority": 2, "type": "teamtailor", "url": "https://careers.untoldstudios.tv/jobs"},
    {"name": "Untold Studios Careers",   "company": "Untold Studios",              "kind": "studio",         "priority": 2, "type": "html",       "url": "https://untoldstudios.tv/careers/"},
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

# ── Utilities ──────────────────────────────────────────────────────────────────

def utc_now():
    return datetime.now(APP_TZ)

def now_str():
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")

def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    while url.startswith("https://https://") or url.startswith("http://http://"):
        url = url.split("://", 1)[1]
    if url.startswith("https://http://"):
        url = "http://" + url[len("https://http://"):]
    if url.startswith("http://https://"):
        url = "https://" + url[len("http://https://"):]
    parsed = urlparse(url)
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

def is_malformed_url(url: str) -> bool:
    low = (url or "").strip().lower()
    return low.startswith("https://https://") or low.startswith("http://http://")

def same_or_subdomain(candidate_host: str, source_host: str) -> bool:
    candidate_host = (candidate_host or "").lower()
    source_host    = (source_host or "").lower()
    return (candidate_host == source_host
            or candidate_host.endswith("." + source_host)
            or source_host.endswith("." + candidate_host))

def identify_ats_type(url: str) -> Optional[str]:
    low = url.lower()
    for ats_type, patterns in ATS_PATTERNS.items():
        if any(p in low for p in patterns):
            return ats_type
    return None

def is_allowed_html_link(source: dict, full_url: str) -> bool:
    if not full_url or is_malformed_url(full_url):
        return False
    parsed = urlparse(full_url)
    path   = normalize_text(parsed.path or "")

    # Paths that are never job listings
    blocked_exact = {
        "/contact", "/contacts", "/about", "/team", "/news", "/blog",
        "/our-work", "/work", "/services", "/projects", "/portfolio",
        "/case-study", "/case-studies", "/studio", "/capabilities",
        "/subscribe", "/membership", "/members", "/join", "/donate",
        "/privacy", "/terms", "/cookies", "/accessibility",
        "/training", "/skills-checklists",
        "/careers", "/jobs", "/vacancies",           # root index pages
        "/subpages/job-vacancies",                   # index pages
    }
    blocked_contains = [
        "/subscribe", "/membership", "/information-and-resources",
        "/skills-checklists", "/training/screenskills",
        "/applying-uk-film", "/tax-incentive", "/bfi-network",
        "/job-vacancies",                            # generic job board index paths
    ]
    if any(path == b or path.startswith(b + "/") for b in blocked_exact):
        return False
    if any(b in path for b in blocked_contains):
        return False

    if identify_ats_type(full_url):
        return True
    source_host = urlparse(source["url"]).netloc.lower()
    return same_or_subdomain(parsed.netloc.lower(), source_host)

# ── CanonicalJob ───────────────────────────────────────────────────────────────

@dataclass
class CanonicalJob:
    title:               str
    company:             str
    apply_url:           str
    canonical_url:       str
    fingerprint:         str = ""
    location_raw:        Optional[str] = None
    location_normalized: Optional[str] = None
    description_text:    Optional[str] = None
    department:          Optional[str] = None
    employment_type:     Optional[str] = None
    ats_type:            Optional[str] = None
    external_job_id:     Optional[str] = None
    posted_at:           Optional[str] = None
    source_name:         str = ""
    source_kind:         str = ""
    source_priority:     int = 9
    source_type:         str = "html"
    score:               Optional[float] = None
    score_breakdown:     dict = field(default_factory=dict)
    matched_keyword:     Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["score_breakdown_json"] = json.dumps(self.score_breakdown)
        return d

    @staticmethod
    def build_fingerprint(company: str, title: str, location: str = "") -> str:
        parts = "|".join(normalize_text(p) for p in [company, title, location] if p)
        return hashlib.sha1(parts.encode()).hexdigest()

# Signals that indicate a page is a real job listing (not a directory/index page)
_JOB_PAGE_SIGNALS = {
    "apply", "apply now", "job description", "responsibilities", "requirements",
    "location", "salary", "contract", "full time", "part time", "full-time",
    "part-time", "about the role", "what you'll do", "what you will do",
    "qualifications", "experience required", "the role", "we are looking for",
    "you will", "you'll be", "closing date", "start date", "benefits",
}

# Titles that are clearly navigation/index pages, not real job titles
_GENERIC_TITLES = {
    "careers", "jobs", "opportunities", "skills", "main navigation", "navigation",
    "careers, skills and jobs", "learn more", "browse roles", "see vacancies",
    "vacancies", "openings", "roles", "current vacancies", "current openings",
    "job listings", "job board", "all jobs", "view all jobs", "see all roles",
    "find a job", "find jobs", "search jobs", "work with us", "join our team",
    "join us", "hiring", "explore careers", "explore roles",
}

def is_real_job_page(title: str, body: str) -> bool:
    """
    Returns True only if the page looks like an actual job/opportunity listing.
    Rejects navigation pages, careers landing pages, and job board index pages.
    JSON-LD JobPosting pages bypass this check entirely (self-validating).
    """
    low_title = title.strip().lower()
    if low_title in _GENERIC_TITLES:
        return False
    if len(low_title) <= 15 and low_title in {
        "jobs", "careers", "skills", "roles", "vacancies", "opportunities", "hiring"
    }:
        return False
    # Require at least one real job-page signal in the body
    low_body = body.lower()
    return any(sig in low_body for sig in _JOB_PAGE_SIGNALS)


def normalise_to_canonical(raw: dict, source: dict) -> CanonicalJob:
    title    = clean_text(raw.get("title", ""))
    # Reject generic navigation/index titles before they enter the pipeline
    if title.strip().lower() in _GENERIC_TITLES:
        title = ""  # Will fail keyword match downstream -- safe rejection path
    # Reject titles that are clearly page body text, not a job title (>80 chars)
    elif len(title) > 80:
        title = ""  # Too long to be a real job title
    # Reject titles that are clearly scraped body text, not a job title
    # (real job titles are short; >120 chars is almost always a page description)
    if len(title) > 120:
        title = ""
    company  = raw.get("company", source.get("company", ""))
    url      = raw.get("url", "")
    location = clean_text(raw.get("location", "") or "")
    body     = clean_text(raw.get("body", "") or "")
    can_url  = canonicalize_url(url)
    norm_loc = detect_location(f"{location} {body}", company=company)
    fingerprint = CanonicalJob.build_fingerprint(company, title, norm_loc)
    return CanonicalJob(
        title=title, company=company, apply_url=url,
        canonical_url=can_url, fingerprint=fingerprint,
        location_raw=location or None, description_text=body or None,
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

# ── Database ───────────────────────────────────────────────────────────────────

_db_lock  = threading.Lock()
_scan_lock = threading.Lock()  # prevents concurrent scans

def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def db_execute(query, params=(), fetch=False):
    with _db_lock:
        conn = db()
        try:
            cur  = conn.cursor()
            cur.execute(query, params)
            rows = cur.fetchall() if fetch else None
            conn.commit()
            return rows
        finally:
            conn.close()

def init_db():
    conn = db()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key           TEXT UNIQUE,
            canonical_url        TEXT,
            fingerprint          TEXT,
            title                TEXT,
            company              TEXT,
            location_raw         TEXT,
            location_normalized  TEXT,
            description_text     TEXT,
            department           TEXT,
            employment_type      TEXT,
            apply_url            TEXT,
            ats_type             TEXT,
            external_job_id      TEXT,
            posted_at            TEXT,
            source_name          TEXT,
            source_kind          TEXT,
            source_priority      INTEGER,
            source_type          TEXT,
            first_seen           TEXT,
            last_seen            TEXT,
            matched_keyword      TEXT,
            score                REAL,
            opportunity_type     TEXT,
            score_breakdown_json TEXT,
            miss_count           INTEGER DEFAULT 0,
            job_status           TEXT DEFAULT 'active',
            raw_blob             TEXT
        )
    """)

    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    for col, defn in [
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
        ("opportunity_type",     "TEXT"),
    ]:
        if col not in existing_cols:
            try:
                cur.execute(f"ALTER TABLE jobs ADD COLUMN {col} {defn}")
            except sqlite3.OperationalError:
                pass

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS discovered_sources (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            company         TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            discovered_from TEXT,
            ats_type        TEXT NOT NULL,
            candidate_url   TEXT NOT NULL,
            status          TEXT DEFAULT 'pending',
            notes           TEXT,
            created_at      TEXT NOT NULL,
            reviewed_at     TEXT,
            UNIQUE(candidate_url)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            source_name           TEXT PRIMARY KEY,
            last_run_at           TEXT,
            last_success_at       TEXT,
            last_failure_at       TEXT,
            last_error            TEXT,
            last_event_type       TEXT,
            consecutive_fails     INTEGER DEFAULT 0,
            consecutive_successes INTEGER DEFAULT 0,
            total_runs            INTEGER DEFAULT 0,
            jobs_found_last       INTEGER DEFAULT 0,
            jobs_found_total      INTEGER DEFAULT 0,
            status                TEXT DEFAULT 'unknown'
        )
    """)
    sh_cols = {r[1] for r in conn.execute("PRAGMA table_info(source_health)").fetchall()}
    for col, defn in [("last_event_type", "TEXT"), ("consecutive_successes", "INTEGER DEFAULT 0")]:
        if col not in sh_cols:
            try:
                cur.execute(f"ALTER TABLE source_health ADD COLUMN {col} {defn}")
            except sqlite3.OperationalError:
                pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_key     TEXT NOT NULL,
            event_type     TEXT NOT NULL,
            event_at       TEXT NOT NULL,
            old_value_json TEXT,
            new_value_json TEXT,
            source_name    TEXT,
            notes          TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_interactions (
            unique_key   TEXT NOT NULL,
            action       TEXT NOT NULL,
            actioned_at  TEXT NOT NULL,
            PRIMARY KEY (unique_key, action)
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_jobs_unique_key  ON jobs(unique_key)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs(job_status)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_score       ON jobs(score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_company     ON jobs(company)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_first_seen  ON jobs(first_seen)",
        "CREATE INDEX IF NOT EXISTS idx_job_events_key   ON job_events(unique_key)",
        "CREATE INDEX IF NOT EXISTS idx_job_events_at    ON job_events(event_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_disc_status      ON discovered_sources(status)",
        "CREATE INDEX IF NOT EXISTS idx_disc_company     ON discovered_sources(company)",
    ]:
        cur.execute(idx)

    conn.commit()
    conn.close()
    seed_defaults()

def seed_defaults():
    for kw in DEFAULT_KEYWORDS:
        db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))
    for phrase in DEFAULT_EXCLUDES:
        db_execute("INSERT OR IGNORE INTO excludes (phrase) VALUES (?)", (phrase,))

    if not get_state("location_mode"): set_state("location_mode", "london")
    if not get_state("paused"):        set_state("paused", "0")
    if not get_state("quality_mode"):  set_state("quality_mode", "off")

    existing_sources = {
        (r[0], r[1]) for r in db_execute("SELECT name, url FROM sources", fetch=True) or []
    }
    for s in DEFAULT_SOURCES:
        if (s["name"], s["url"]) not in existing_sources:
            db_execute(
                "INSERT INTO sources (name, company, kind, priority, type, url, active, added_at) VALUES (?,?,?,?,?,?,1,?)",
                (s["name"], s["company"], s["kind"], s["priority"], s["type"], s["url"], now_str())
            )

# ── Source registry ────────────────────────────────────────────────────────────

def get_active_sources():
    rows = db_execute(
        "SELECT name, company, kind, priority, type, url FROM sources WHERE active=1 ORDER BY priority ASC",
        fetch=True
    )
    return [{"name": r[0], "company": r[1], "kind": r[2], "priority": r[3], "type": r[4], "url": r[5]}
            for r in (rows or [])]

def save_discovered_sources(parent_source: dict, discovered: list) -> int:
    if not discovered:
        return 0
    new_count = 0
    for ds in discovered:
        candidate_url = canonicalize_url(ds.get("url", ""))
        if not candidate_url:
            continue
        # Block job boards, aggregators, and non-studio sites from being auto-added
        from urllib.parse import urlparse as _up
        _domain = _up(candidate_url).netloc.lstrip("www.")
        if any(_domain == bd or _domain.endswith("." + bd) for bd in DISCOVERY_BLOCKED_DOMAINS):
            continue
        if db_execute("SELECT 1 FROM sources WHERE url=? LIMIT 1", (candidate_url,), fetch=True):
            continue
        if db_execute("SELECT 1 FROM discovered_sources WHERE candidate_url=? LIMIT 1", (candidate_url,), fetch=True):
            continue
        db_execute(
            """INSERT INTO discovered_sources
               (company, source_name, discovered_from, ats_type, candidate_url, status, created_at)
               VALUES (?,?,?,?,?,'pending',?)""",
            (
                ds.get("company", parent_source.get("company", "Unknown")),
                ds.get("name", f"{parent_source.get('company','Unknown')} {ds.get('type','ats')}"),
                parent_source.get("name", ""),
                ds.get("type", "html"),
                candidate_url,
                now_str(),
            ),
        )
        new_count += 1
    return new_count

def pending_discoveries(limit: int = 20):
    return db_execute(
        """SELECT id, company, source_name, ats_type, candidate_url, discovered_from, created_at
           FROM discovered_sources WHERE status='pending' ORDER BY id DESC LIMIT ?""",
        (limit,), fetch=True,
    ) or []

def approve_discovery(discovery_id: int) -> tuple[bool, str]:
    row = db_execute(
        "SELECT id, company, source_name, ats_type, candidate_url, status FROM discovered_sources WHERE id=?",
        (discovery_id,), fetch=True,
    )
    if not row:
        return False, "Discovery not found."
    _, company, source_name, ats_type, candidate_url, status = row[0]
    if status == "approved": return False, "Already approved."
    if status == "rejected": return False, "Already rejected."
    if not db_execute("SELECT 1 FROM sources WHERE url=? LIMIT 1", (candidate_url,), fetch=True):
        db_execute(
            "INSERT INTO sources (name, company, kind, priority, type, url, active, added_at) VALUES (?,?,'studio',2,?,?,1,?)",
            (source_name, company, ats_type, candidate_url, now_str()),
        )
    db_execute("UPDATE discovered_sources SET status='approved', reviewed_at=? WHERE id=?", (now_str(), discovery_id))
    return True, f"Approved and added: {source_name}"

def reject_discovery(discovery_id: int) -> tuple[bool, str]:
    row = db_execute("SELECT id, status FROM discovered_sources WHERE id=?", (discovery_id,), fetch=True)
    if not row:               return False, "Discovery not found."
    if row[0][1] == "rejected": return False, "Already rejected."
    if row[0][1] == "approved": return False, "Already approved."
    db_execute("UPDATE discovered_sources SET status='rejected', reviewed_at=? WHERE id=?", (now_str(), discovery_id))
    return True, f"Rejected discovery #{discovery_id}."

def format_discoveries(limit: int = 12) -> str:
    rows = pending_discoveries(limit)
    if not rows:
        return "No pending ATS discoveries."
    lines = ["Pending ATS discoveries"]
    for d_id, company, source_name, ats_type, candidate_url, discovered_from, created_at in rows:
        lines += ["", f"{d_id}. {company} -- {ats_type}", f"   {source_name}"]
        if discovered_from: lines.append(f"   Found on: {discovered_from}")
        lines += [f"   {candidate_url}", f"   Added: {created_at}"]
    lines += ["", "Use /approve_source <id> or /reject_source <id>."]
    return "\n".join(lines)

# ── Source health ──────────────────────────────────────────────────────────────

def _upsert_health(source_name: str, now: str, success: bool, event_type: str,
                   jobs_found: int = 0, error: str = ""):
    existing = db_execute(
        "SELECT consecutive_fails, consecutive_successes, total_runs, jobs_found_total FROM source_health WHERE source_name=?",
        (source_name,), fetch=True,
    )
    error_short = error[:300] if error else None
    if existing:
        c_fails, c_succ, total_runs, jobs_total = existing[0]
        total_runs += 1; jobs_total += jobs_found
        if success: c_succ += 1; c_fails = 0
        else:       c_fails += 1; c_succ = 0
        status = _compute_status(c_fails, c_succ, event_type)
        db_execute("""
            UPDATE source_health SET
                last_run_at=?,
                last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END,
                last_failure_at=CASE WHEN NOT ? THEN ? ELSE last_failure_at END,
                last_error=CASE WHEN NOT ? THEN ? ELSE last_error END,
                last_event_type=?, consecutive_fails=?, consecutive_successes=?,
                total_runs=?, jobs_found_last=?, jobs_found_total=?, status=?
            WHERE source_name=?
        """, (now, success, now, success, now, success, error_short,
              event_type, c_fails, c_succ, total_runs, jobs_found, jobs_total, status, source_name))
        return c_fails, status
    else:
        status = _compute_status(0 if success else 1, 1 if success else 0, event_type)
        db_execute("""
            INSERT INTO source_health
            (source_name, last_run_at, last_success_at, last_failure_at, last_error,
             last_event_type, consecutive_fails, consecutive_successes,
             total_runs, jobs_found_last, jobs_found_total, status)
            VALUES (?,?,?,?,?,?,?,?,1,?,?,?)
        """, (source_name, now,
              now if success else None, now if not success else None,
              error_short, event_type,
              0 if success else 1, 1 if success else 0,
              jobs_found, jobs_found, status))
        return (0 if success else 1), status

def _compute_status(c_fails: int, c_succ: int, event_type: str) -> str:
    if c_fails >= 7:  return "dead"
    if c_fails >= 3:  return "degraded"
    if event_type == "success_zero" and c_succ == 0: return "suspect"
    return "healthy"

def record_source_success(source_name: str, jobs_found: int):
    event_type = "success_nonzero" if jobs_found > 0 else "success_zero"
    return _upsert_health(source_name, now_str(), True, event_type, jobs_found=jobs_found)

def record_source_failure(source_name: str, error: str, event_type: str = "http_error"):
    return _upsert_health(source_name, now_str(), False, event_type, error=error)

# ── Job events ─────────────────────────────────────────────────────────────────

def record_job_event(unique_key: str, event_type: str, source_name: str = "",
                     old_value: Optional[dict] = None, new_value: Optional[dict] = None, notes: str = ""):
    db_execute(
        """INSERT INTO job_events (unique_key, event_type, event_at, old_value_json, new_value_json, source_name, notes)
           VALUES (?,?,?,?,?,?,?)""",
        (unique_key, event_type, now_str(),
         json.dumps(old_value, ensure_ascii=False, default=str) if old_value else None,
         json.dumps(new_value, ensure_ascii=False, default=str) if new_value else None,
         source_name or "", notes or ""),
    )

def score_band(score: float) -> str:
    v = int(score or 0)
    return "high" if v >= 75 else ("normal" if v >= 45 else "low")

def job_event_snapshot_from_job(job: CanonicalJob, status: str = "active") -> dict:
    return {
        "title": job.title, "company": job.company,
        "location_raw": job.location_raw, "location_normalized": job.location_normalized,
        "apply_url": job.apply_url, "canonical_url": job.canonical_url,
        "source_name": job.source_name, "source_type": job.source_type,
        "ats_type": job.ats_type, "score": int(job.score or 0),
        "opportunity_type": classify_opportunity(job), "job_status": status,
    }

def job_event_snapshot_from_db_row(row) -> dict:
    return {
        "title": row[0], "company": row[1],
        "location_raw": row[2], "location_normalized": row[3],
        "apply_url": row[4], "canonical_url": row[5],
        "source_name": row[6], "source_type": row[7],
        "ats_type": row[8], "score": int(row[9] or 0),
        "opportunity_type": row[10] or "direct_role", "job_status": row[11] or "active",
    }

def detect_material_changes(old: dict, new: dict) -> dict:
    changes = {}
    for f in ["title", "location_raw", "location_normalized", "apply_url",
              "canonical_url", "source_name", "opportunity_type", "job_status"]:
        if (old.get(f) or "") != (new.get(f) or ""):
            changes[f] = {"old": old.get(f), "new": new.get(f)}
    if score_band(old.get("score", 0)) != score_band(new.get("score", 0)):
        changes["score_band"] = {"old": score_band(old.get("score", 0)), "new": score_band(new.get("score", 0))}
    return changes

def recent_event_exists(unique_key: str, event_type: str) -> bool:
    rows = db_execute(
        "SELECT event_type FROM job_events WHERE unique_key=? ORDER BY id DESC LIMIT 1",
        (unique_key,), fetch=True,
    )
    return bool(rows and rows[0][0] == event_type)

# ── Job expiry ─────────────────────────────────────────────────────────────────

def expire_stale_jobs(seen_keys: set):
    active_rows = db_execute(
        """SELECT unique_key, title, company, location_raw, location_normalized, apply_url,
                  canonical_url, source_name, source_type, ats_type, score, opportunity_type
           FROM jobs WHERE job_status='active'""",
        fetch=True,
    )
    for row in (active_rows or []):
        key = row[0]
        if key not in seen_keys:
            db_execute(
                """UPDATE jobs SET miss_count=miss_count+1,
                   job_status=CASE WHEN miss_count+1>=3 THEN 'expired' ELSE job_status END
                   WHERE unique_key=?""",
                (key,),
            )
            status_row = db_execute("SELECT job_status FROM jobs WHERE unique_key=?", (key,), fetch=True)
            if status_row and status_row[0][0] == "expired" and not recent_event_exists(key, "expired"):
                old = job_event_snapshot_from_db_row(row[1:] + ("active",))
                new = {**old, "job_status": "expired"}
                record_job_event(key, "expired", old.get("source_name", ""), old, new)
        else:
            db_execute("UPDATE jobs SET miss_count=0 WHERE unique_key=?", (key,))

# ── State / settings ───────────────────────────────────────────────────────────

def set_state(key, value):
    db_execute("INSERT OR REPLACE INTO state (key, value) VALUES (?,?)", (key, str(value)))

def get_state(key, default=""):
    rows = db_execute("SELECT value FROM state WHERE key=?", (key,), fetch=True)
    return rows[0][0] if rows else default

def get_keywords():
    return [r[0] for r in db_execute("SELECT keyword FROM keywords ORDER BY keyword", fetch=True)]

def get_excludes():
    return [r[0] for r in db_execute("SELECT phrase FROM excludes ORDER BY phrase", fetch=True)]

def add_keyword(kw):
    kw = normalize_text(kw)
    if kw: db_execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (kw,))

def remove_keyword(kw):
    kw = normalize_text(kw)
    if kw: db_execute("DELETE FROM keywords WHERE keyword=?", (kw,))

def quality_threshold():
    mode = get_state("quality_mode", "off").lower()
    return 75 if mode == "strict" else (45 if mode == "normal" else 0)

# ── Scoring ────────────────────────────────────────────────────────────────────

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
    title   = normalize_text(job.title)
    body    = normalize_text(job.description_text or "")
    company = normalize_text(job.company)
    blob    = f"{title} {body}"
    bd = {}

    title_pts = max((pts for phrase, pts in TITLE_BOOSTS.items() if phrase in title), default=0)
    bd["title_strength"] = title_pts

    juniority_terms = ["junior", "graduate", "assistant", "trainee", "intern", "entry level",
                       "no experience", "recent graduate", "entry-level", "school leaver"]
    bd["juniority"] = min(sum(6 for t in juniority_terms if t in blob), 30)

    loc_raw_hay = normalize_text(job.location_raw or "")
    if any(t in loc_raw_hay for t in NON_UK_TERMS):
        loc_pts = -100
    elif any(t in loc_raw_hay for t in LONDON_TERMS) or any(t in normalize_text(job.description_text or "") for t in LONDON_TERMS):
        loc_pts = 30
    elif any(t in loc_raw_hay for t in UK_TERMS):
        loc_pts = 18
    elif job.company in UK_STUDIO_COMPANIES:
        loc_pts = 10
    else:
        loc_pts = 0
    bd["location_confidence"] = loc_pts

    if job.source_kind == "studio":
        p = job.source_priority
        source_pts = 20 if p == 1 else (14 if p == 2 else 8)
    elif job.source_kind == "industry_board":
        source_pts = 8
    else:
        source_pts = 0
    bd["source_quality"] = source_pts

    bd["ats_type"] = (8 if job.ats_type in {"greenhouse", "lever", "ashby", "workable"} else
                      5 if job.ats_type in {"jobvite", "teamtailor"} else 0)

    bd["company_tier"] = 10 if company in PREFERRED_COMPANIES else 0

    neg_terms = ["senior", "lead", "director", "supervisor", "executive producer",
                 "principal", "recruiter", "software engineer", "technical director", " td "]
    neg = max(sum(-40 for t in neg_terms if t in blob), -100)
    # French-language title = almost certainly non-UK office
    french_title_signals = ["coordonnateur", "coordinateur", "chef de", "directeur", "responsable"]
    if any(t in title for t in french_title_signals):
        neg = min(neg - 80, -80)
    bd["negative_indicators"] = neg

    total = max(sum(bd.values()), 0)
    bd["total"] = total
    return float(total), bd

# ── Location / filtering ───────────────────────────────────────────────────────

def detect_location(text: str, company: str = "") -> str:
    hay = normalize_text(text)
    if any(t in hay for t in NON_UK_TERMS):  return "Non-UK"
    if any(t in hay for t in LONDON_TERMS):  return "London"
    if any(t in hay for t in UK_TERMS):      return "UK"
    if company in UK_STUDIO_COMPANIES:       return "Unknown-UK-Studio"
    return ""

def location_allowed(job: CanonicalJob) -> bool:
    mode = get_state("location_mode", "off").lower()
    if mode == "off":
        return True
    if detect_location(job.location_raw or "", company=job.company) == "Non-UK":
        return False
    blob = " ".join(filter(None, [job.title, job.location_raw, job.company]))
    loc  = detect_location(blob, company=job.company)
    if loc == "London":
        return True
    if mode == "uk" and loc == "UK":
        return True
    if job.source_kind == "studio" and job.company in UK_STUDIO_COMPANIES:
        return True
    # Industry boards (ScreenSkills) are UK-only by definition
    if job.source_kind == "industry_board":
        return True
    return False

def title_keyword_match(job: CanonicalJob):
    # Excludes: title + URL — catches cases where title is generic ("View job")
    # but the URL reveals it's an excluded programme (e.g. animation-launchpad-internship)
    title_hay = normalize_text(f"{job.title} {job.apply_url or ''}")
    if any(ex in title_hay for ex in get_excludes()):
        return False, None

    keywords = get_keywords()

    # Primary: title match (strong signal — title must contain a keyword)
    title_only = normalize_text(job.title)
    matched = next((kw for kw in keywords if kw in title_only), None)
    if matched:
        return True, matched

    # Secondary: body-only match, but ONLY for short specific keywords that are
    # meaningful on their own (trainee, apprentice, intern, runner, graduate scheme etc.)
    # This catches listings like "VFX Department Opportunity – we seek a trainee"
    # but avoids false positives from "assistant" buried in boilerplate.
    BODY_ALLOWED_KEYWORDS = {
        "trainee", "apprentice", "internship", "intern", "work experience",
        "graduate scheme", "graduate programme", "graduate program",
        "launchpad", "kickstart", "talent scheme", "emerging talent",
        "entry level", "entry-level",
    }
    body_hay = normalize_text(job.description_text or "")
    matched = next((kw for kw in keywords if kw in BODY_ALLOWED_KEYWORDS and kw in body_hay), None)
    return (True, matched) if matched else (False, None)

def classify_rejection(job: CanonicalJob, threshold: float) -> str:
    # Uses same logic as title_keyword_match so counts stay consistent
    ok, _ = title_keyword_match(job)
    if not ok:
        title_hay = normalize_text(f"{job.title} {job.apply_url or ''}")
        if any(ex in title_hay for ex in get_excludes()):
            return "excluded"
        return "no_keyword"
    if not location_allowed(job):
        return "location"
    total_score, _ = score_job(job)
    blob = " ".join(filter(None, [job.title, job.location_raw, job.description_text]))
    if detect_location(blob, company=job.company) == "Non-UK":
        return "location"
    if total_score <= 0 or total_score < threshold:
        return "score"
    return "passed"

def classify_opportunity(job: CanonicalJob) -> str:
    title = normalize_text(job.title)
    blob  = normalize_text(f"{job.title} {job.description_text or ''} {job.apply_url or ''}")
    if any(t in title for t in DIRECT_ROLE_TERMS):  return "direct_role"
    if any(t in blob  for t in PROGRAMME_TERMS):    return "programme"
    return "direct_role"

def opportunity_label(job: CanonicalJob) -> str:
    return "Programme / internship" if classify_opportunity(job) == "programme" else "Direct role"

def prettify_location(loc: Optional[str]) -> str:
    if not loc:                    return "Location not listed"
    if loc == "Unknown-UK-Studio": return "UK studio (location not explicit)"
    return loc

# ── Help text ──────────────────────────────────────────────────────────────────

def format_help_text() -> str:
    return (
        "🎬 VFX Job Monitor\n\n"
        "This bot watches 25+ VFX, animation and post-production studios and sends you alerts"
        " when entry-level production roles appear. It runs automatically every 10 minutes.\n\n"
        "── Start here ──\n\n"
        "🚀 /scan\n"
        "Run a fresh scan right now and send the best jobs found.\n\n"
        "🗂️ /jobs\n"
        "Browse the best jobs already saved by the bot.\n\n"
        "🧭 /status\n"
        "See how many sources are running, what was found last time, and your current settings.\n\n"
        "── Main commands ──\n\n"
        "✨ /latest\n"
        "Show jobs found in the last 24 hours.\n\n"
        "🔎 /search <term>\n"
        "Search saved jobs by title or company name.\n\n"
        "── Tuning ──\n\n"
        "📍 /setlocation london | uk | off\n"
        "Filter jobs by location. Use london to keep results tight, uk for wider coverage, off for everything.\n\n"
        "🎚️ /quality strict | normal | off\n"
        "Set how picky the score filter is. off shows everything that matches a keyword.\n\n"
        "🧩 /keywords\n"
        "See the list of job title keywords the bot looks for.\n\n"
        "➕ /addkeyword <phrase>\n"
        "Add a new keyword to watch for.\n\n"
        "➖ /removekeyword <phrase>\n"
        "Remove a keyword you no longer want.\n\n"
        "── Operations ──\n\n"
        "⏸️ /pause\n"
        "Stop automatic scanning (you can still run /scan manually).\n\n"
        "▶️ /resume\n"
        "Turn automatic scanning back on.\n\n"
        "📌 /applied\n"
        "See jobs you've marked as applied.\n\n"
        "📋 /menu\n"
        "Show the main menu buttons anytime.\n\n"
        "📡 /coverage\n"
        "See how many jobs each studio is currently listing.\n\n"
        "── Alert labels ──\n\n"
        "🎯 Direct role -- a real job vacancy at a studio\n"
        "🎓 Programme / internship -- a scheme, traineeship or work experience opportunity\n\n"
        "Tip: start with 🚀 /scan to see what's out there right now."
    )

# ── Scraping ───────────────────────────────────────────────────────────────────

def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.text

def fetch_json(url: str, method: str = "GET", payload: dict = None):
    if method == "POST":
        r = requests.post(url, headers={**HEADERS, "Content-Type": "application/json"},
                          json=payload, timeout=10)
    else:
        r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

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

def _best_job_container(a):
    for parent in a.parents:
        if not getattr(parent, "name", None):
            continue
        if parent.name in {"article", "li", "tr", "section"}:
            return parent
        if parent.name == "div":
            classes = " ".join(parent.get("class", []))
            ident   = f"{classes} {parent.get('id', '')}".lower()
            if any(tok in ident for tok in ["job", "role", "career", "vacancy", "opening",
                                            "position", "posting", "listing", "card"]):
                return parent
    return a.parent or a

def _extract_title_and_context(a):
    container   = _best_job_container(a)
    anchor_text = clean_text(a.get_text(" ", strip=True))
    candidates  = [anchor_text] if anchor_text else []

    seen_norm = set()
    for sel in ["h1", "h2", "h3", "h4",
                "[class*='title']", "[class*='job']", "[class*='role']", "[class*='position']",
                "[id*='title']", "[id*='job']"]:
        try:
            for el in container.select(sel):
                txt = clean_text(el.get_text(" ", strip=True))
                n   = normalize_text(txt)
                if txt and len(txt) >= 4 and n not in seen_norm:
                    seen_norm.add(n); candidates.append(txt)
        except Exception:
            pass

    for sib in list(a.previous_siblings)[-2:] + list(a.next_siblings)[:2]:
        txt = clean_text(sib.get_text(" ", strip=True) if getattr(sib, "get_text", None) else str(sib))
        n   = normalize_text(txt)
        if txt and 4 <= len(txt) <= 160 and n not in seen_norm:
            seen_norm.add(n); candidates.append(txt)

    NAV = {"home", "about", "contact", "menu", "login", "sign in", "register",
           "privacy", "terms", "cookie", "back", "next", "previous", "more",
           "read more", "view all", "see all", "apply now", "apply here",
           "learn more", "details", "view details", "find out more", "job details"}

    def _score(txt: str) -> int:
        low = normalize_text(txt)
        s   = 0
        if low in NAV or txt.startswith("/") or txt.startswith("http"): s -= 100
        if 8 <= len(txt) <= 120: s += 10
        if any(t in low for t in ["producer", "production", "coordinator", "assistant",
                                   "runner", "intern", "trainee", "job", "role", "vacancy"]): s += 30
        if any(ch.isalpha() for ch in txt): s += 3
        return s

    title   = max(candidates, key=_score) if candidates else anchor_text
    context = clean_text(container.get_text(" ", strip=True)) if container else anchor_text
    return title, context[:1200]

def generic_extract_jobs_from_soup(source: dict, soup) -> list:
    jobs, seen  = [], set()
    source_path = urlparse(source["url"]).path.rstrip("/")
    nav_set     = {"home", "about", "contact", "menu", "login", "sign in", "register",
                   "privacy", "terms", "cookie", "back", "next", "previous", "more",
                   "read more", "view all", "see all", "apply now", "apply here",
                   "learn more", "details", "view details", "find out more", "job details"}
    trigger     = {"job", "career", "vacancy", "opening", "role", "position",
                   "producer", "production", "runner", "assistant", "coordinator", "intern", "trainee"}

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue
        full_url = canonicalize_url(urljoin(source["url"], href))
        if not is_allowed_html_link(source, full_url):
            continue
        if urlparse(full_url).path.rstrip("/") in (source_path, ""):
            continue

        title, context = _extract_title_and_context(a)
        low_title = normalize_text(title)

        if not title or len(title) < 4:
            continue
        if low_title in nav_set or title.startswith("/") or title.startswith("http"):
            continue
        # Reject generic page headings that aren't actual job titles
        generic_headings = {
            "jobs", "careers", "vacancies", "openings", "opportunities",
            "post production", "production", "animation", "vfx", "roles",
            "current openings", "current vacancies", "view all jobs",
            "all jobs", "job listings", "job board",
        }
        if low_title in generic_headings:
            continue
        if not any(t in normalize_text(f"{title} {context} {full_url}") for t in trigger):
            continue

        key = short_hash(full_url, title)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({
            "title":    title,
            "company":  source["company"],
            "location": detect_location(f"{title} {context} {full_url}", company=source["company"]),
            "url":      full_url,
            "body":     context,
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

def enrich_from_detail_page(raw: dict, source: dict) -> dict:
    """
    Fetch the linked detail page and extract richer title/body text.
    Returns an enriched copy of raw, or the original if fetch fails.
    Only called for HTML sources on weak candidates (no_keyword).
    """
    url = raw.get("url", "")
    if not url or not is_allowed_html_link(source, url):
        return raw
    # Don't re-fetch ATS pages — they have their own parsers
    if identify_ats_type(url):
        return raw
    try:
        html = fetch_text(url)
        soup = BeautifulSoup(html, "html.parser")
        enriched = dict(raw)

        # 1. JSON-LD JobPosting — most reliable signal
        jsonld_title = jsonld_body = jsonld_loc = ""
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "")
                if isinstance(data, list):
                    data = next((d for d in data if d.get("@type") == "JobPosting"), {})
                if data.get("@type") == "JobPosting":
                    jsonld_title = clean_text(data.get("title", ""))
                    jsonld_body  = clean_text(data.get("description", ""))[:1500]
                    loc_data     = data.get("jobLocation", {})
                    if isinstance(loc_data, list): loc_data = loc_data[0] if loc_data else {}
                    addr         = loc_data.get("address", {})
                    jsonld_loc   = clean_text(
                        addr.get("addressLocality", "") + " " + addr.get("addressRegion", "") +
                        " " + addr.get("addressCountry", "")
                    )
            except Exception:
                pass

        # 2. <title> tag (often contains role name)
        page_title = ""
        if soup.title and soup.title.string:
            page_title = clean_text(soup.title.string)
            # Strip common site suffixes like " | Framestore Careers"
            page_title = re.sub(r"\s*[|\-–]\s*.{3,40}$", "", page_title).strip()

        # 3. First <h1>, then <h2>
        h1 = clean_text(soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""
        h2 = clean_text(soup.find("h2").get_text(" ", strip=True)) if soup.find("h2") else ""

        # 4. Meta description
        meta_desc = ""
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            meta_desc = clean_text(meta.get("content", ""))

        # 5. Top visible text block (first 1000 chars of body text)
        body_text = ""
        main = soup.find("main") or soup.find("article") or soup.find("body")
        if main:
            body_text = clean_text(main.get_text(" ", strip=True))[:1200]

        # Pick the best title: JSON-LD > h1 > page_title > h2 > original
        def _title_score(t):
            if not t: return -1
            low = t.lower()
            score = 0
            if 6 <= len(t) <= 100: score += 10
            if any(w in low for w in ["coordinator", "assistant", "producer", "runner",
                                       "trainee", "intern", "manager", "director"]): score += 20
            return score

        candidates = [(jsonld_title, "jsonld"), (h1, "h1"), (page_title, "title"),
                      (h2, "h2"), (raw.get("title", ""), "original")]
        best_title = max(candidates, key=lambda x: _title_score(x[0]))[0] or raw.get("title", "")

        # Compose enriched body — prioritise JSON-LD description, fallback to body text
        enriched_body = " ".join(filter(None, [
            jsonld_body or body_text,
            meta_desc,
            jsonld_loc,
            raw.get("body", ""),
        ]))[:2000]

        # Only accept enrichment if page looks like a real job listing.
        # JSON-LD JobPosting is self-validating -- only gate non-JSON-LD pages.
        if not jsonld_title and not is_real_job_page(best_title, enriched_body):
            return raw  # Index/nav page -- discard enrichment, keep original

        enriched["title"]    = best_title
        enriched["body"]     = enriched_body
        if jsonld_loc:
            enriched["location"] = jsonld_loc
        enriched["_enriched"] = True
        return enriched

    except Exception:
        return raw

def parse_greenhouse(source: dict):
    try:
        m = re.search(r"(?:boards|job-boards).greenhouse.io/([^/?#]+)", source["url"])
        if m:
            data = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{m.group(1)}/jobs")
            return [{"title": clean_text(i.get("title", "")),
                     "location": clean_text((i.get("location") or {}).get("name", "")),
                     "url": i.get("absolute_url", ""), "body": json.dumps(i),
                     "external_id": str(i.get("id", "")), "ats_type": "greenhouse"}
                    for i in data.get("jobs", [])], []
    except Exception as _e:
        import logging as _log; _log.warning(f"parse_greenhouse error: {type(_e).__name__}: {_e}")
    return parse_html(source)

def parse_lever(source: dict):
    try:
        m = re.search(r"(?:jobs|api).lever.co/(?:v0/postings/)?([^/?#]+)", source["url"])
        if m:
            data = fetch_json(f"https://api.lever.co/v0/postings/{m.group(1)}?mode=json")
            return [{"title": clean_text(i.get("text", "")),
                     "location": clean_text((i.get("categories") or {}).get("location", "")),
                     "url": i.get("hostedUrl", ""), "body": json.dumps(i),
                     "external_id": i.get("id", ""), "ats_type": "lever",
                     "posted_at": str(i.get("createdAt", ""))}
                    for i in data], []
    except Exception as _e:
        import logging as _log; _log.warning(f"parse_lever error: {type(_e).__name__}: {_e}")
    return parse_html(source)

def parse_workable(source: dict):
    """Workable public JSON API: /api/v3/accounts/{slug}/jobs"""
    try:
        m = re.search(r"apply\.workable\.com/([^/?#]+)", source["url"])
        if m:
            slug = m.group(1).rstrip("/")
            data = fetch_json(
                f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
                method="POST", payload={"query": "", "location": [], "department": [],
                                        "worktype": [], "remote": []}
            )
            results = data.get("results", [])
            return [{"title":    clean_text(i.get("title", "")),
                     "location": clean_text(i.get("location", {}).get("city", "") or
                                            i.get("location", {}).get("country", "") or ""),
                     "url":      f"https://apply.workable.com/{slug}/j/{i.get('shortcode','')}/",
                     "body":     json.dumps(i),
                     "external_id": i.get("shortcode", ""),
                     "ats_type": "workable"}
                    for i in results], []
    except Exception as _e:
        import logging as _log; _log.warning(f"parse_workable error: {type(_e).__name__}: {_e}")
    return parse_html(source)
def parse_ashby(source):           return parse_html(source)
def parse_jobvite(source: dict):
    """Jobvite RSS feed: jobs.jobvite.com/{slug}/feed"""
    import xml.etree.ElementTree as ET
    try:
        from urllib.parse import urlparse as _up
        parts = _up(source["url"])
        # Extract slug: first path segment after the host
        path_parts = [p for p in parts.path.split("/") if p]
        if not path_parts:
            raise ValueError("no slug")
        slug = path_parts[0]
        feed_url = f"https://jobs.jobvite.com/{slug}/feed"
        r = requests.get(feed_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {"jv": "http://www.jobvite.com/ns/feed/1.0"}
        out = []
        for item in root.iter("item"):
            title   = clean_text((item.findtext("title") or ""))
            link    = clean_text((item.findtext("link") or ""))
            loc_el  = item.find("jv:location", ns)
            loc     = clean_text(loc_el.text if loc_el is not None else "")
            dept_el = item.find("jv:jobType", ns) or item.find("jv:department", ns)
            dept    = clean_text(dept_el.text if dept_el is not None else "")
            body    = clean_text((item.findtext("description") or ""))[:1000]
            jid     = clean_text((item.findtext("guid") or link))
            if not title:
                continue
            out.append({
                "title":       title,
                "location":    loc,
                "url":         link or source["url"],
                "body":        f"{dept} {body}".strip(),
                "external_id": jid,
                "ats_type":    "jobvite",
            })
        if out:
            return out, []
    except Exception as _e:
        import logging as _log; _log.warning(f"parse_jobvite error: {type(_e).__name__}: {_e}")
    return parse_html(source)
def parse_teamtailor(source: dict):
    """Teamtailor public JSON endpoint: GET {subdomain}.teamtailor.com/jobs.json"""
    try:
        from urllib.parse import urlparse as _up
        parsed = _up(source["url"])
        base = f"{parsed.scheme}://{parsed.netloc}"
        data = fetch_json(f"{base}/jobs.json")
        jobs_raw = data if isinstance(data, list) else data.get("jobs", data.get("data", []))
        out = []
        for i in jobs_raw:
            # Teamtailor wraps fields under 'attributes' in JSON:API format, or flat
            attrs = i.get("attributes", i)
            title = clean_text(attrs.get("title", ""))
            if not title:
                continue
            loc_data = attrs.get("locations") or []
            loc_str = ", ".join(
                l.get("name", "") if isinstance(l, dict) else str(l)
                for l in (loc_data if isinstance(loc_data, list) else [loc_data])
            ) if loc_data else clean_text(attrs.get("location", "") or "")
            slug  = attrs.get("slug", "") or str(i.get("id", ""))
            apply = attrs.get("apply_url") or (f"{base}/jobs/{slug}" if slug else "")
            out.append({
                "title":       title,
                "location":    loc_str,
                "url":         apply or source["url"],
                "body":        json.dumps(attrs),
                "external_id": str(i.get("id", slug)),
                "ats_type":    "teamtailor",
            })
        if out:
            return out, []
    except Exception as _e:
        import logging as _log; _log.warning(f"parse_teamtailor error: {type(_e).__name__}: {_e}")
    return parse_html(source)
def parse_smartrecruiters(source): return parse_html(source)
def parse_workday(source):         return parse_html(source)

def fetch_source_jobs(source: dict):
    t = source["type"]
    dispatch = {
        "greenhouse": parse_greenhouse, "lever": parse_lever,
        "workable": parse_workable, "ashby": parse_ashby,
        "jobvite": parse_jobvite, "teamtailor": parse_teamtailor,
        "smartrecruiters": parse_smartrecruiters, "workday": parse_workday,
    }
    return dispatch.get(t, parse_html)(source)

# ── Job storage ────────────────────────────────────────────────────────────────

def build_unique_key(job: CanonicalJob) -> str:
    return f"url::{job.canonical_url}" if job.canonical_url else f"fp::{job.fingerprint}"

def upsert_job(job: CanonicalJob) -> tuple[bool, str]:
    unique_key = build_unique_key(job)
    now        = now_str()
    existing   = db_execute(
        """SELECT id, source_priority, title, company, location_raw, location_normalized,
                  apply_url, canonical_url, source_name, source_type, ats_type, score,
                  opportunity_type, job_status
           FROM jobs WHERE unique_key=?""",
        (unique_key,), fetch=True,
    )
    bd_json      = json.dumps(job.score_breakdown)
    opp_type     = classify_opportunity(job)
    new_snapshot = job_event_snapshot_from_job(job, status="active")

    if not existing:
        db_execute(
            """INSERT INTO jobs (
                unique_key, canonical_url, fingerprint, title, company,
                location_raw, location_normalized, description_text, department, employment_type,
                apply_url, ats_type, external_job_id, posted_at,
                source_name, source_kind, source_priority, source_type,
                first_seen, last_seen, matched_keyword,
                score, opportunity_type, score_breakdown_json, miss_count, job_status, raw_blob
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'active',?)""",
            (unique_key, job.canonical_url, job.fingerprint, job.title, job.company,
             job.location_raw, job.location_normalized, job.description_text,
             job.department, job.employment_type,
             job.apply_url, job.ats_type, job.external_job_id, job.posted_at,
             job.source_name, job.source_kind, job.source_priority, job.source_type,
             now, now, job.matched_keyword,
             job.score, opp_type, bd_json,
             json.dumps(job.to_dict(), ensure_ascii=False, default=str)[:3000]),
        )
        record_job_event(unique_key, "created", job.source_name, None, new_snapshot)
        return True, unique_key

    row              = existing[0]
    current_priority = row[1]
    old_snapshot     = job_event_snapshot_from_db_row(row[2:])
    was_expired      = (old_snapshot.get("job_status") == "expired")

    if int(job.source_priority) < int(current_priority):
        db_execute(
            """UPDATE jobs SET
                canonical_url=?, fingerprint=?, title=?, company=?,
                location_raw=?, location_normalized=?, description_text=?, apply_url=?,
                ats_type=?, external_job_id=?, posted_at=?,
                source_name=?, source_kind=?, source_priority=?, source_type=?,
                last_seen=?, matched_keyword=?, score=?, opportunity_type=?,
                score_breakdown_json=?, miss_count=0, job_status='active'
               WHERE unique_key=?""",
            (job.canonical_url, job.fingerprint, job.title, job.company,
             job.location_raw, job.location_normalized, job.description_text, job.apply_url,
             job.ats_type, job.external_job_id, job.posted_at,
             job.source_name, job.source_kind, job.source_priority, job.source_type,
             now, job.matched_keyword, job.score, opp_type, bd_json, unique_key),
        )
    else:
        db_execute(
            """UPDATE jobs SET location_normalized=?, last_seen=?, matched_keyword=?,
               score=?, opportunity_type=?, score_breakdown_json=?, miss_count=0, job_status='active'
               WHERE unique_key=?""",
            (job.location_normalized, now, job.matched_keyword, job.score, opp_type, bd_json, unique_key),
        )

    if was_expired:
        record_job_event(unique_key, "reopened", job.source_name, old_snapshot, new_snapshot)
    else:
        changes = detect_material_changes(old_snapshot, new_snapshot)
        if changes:
            record_job_event(unique_key, "updated", job.source_name, old_snapshot, new_snapshot,
                             notes=", ".join(sorted(changes.keys()))[:240])
    return False, unique_key

# ── Core monitoring run ────────────────────────────────────────────────────────

def collect_and_store_jobs(force: bool = False) -> list:
    sources      = get_active_sources()
    threshold    = quality_threshold()
    all_matched  = []
    emitted_keys = set()
    seen_keys    = set()
    lock         = threading.Lock()

    def _scrape_one(source):
        try:
            raw_jobs, discovered = fetch_source_jobs(source)
            save_discovered_sources(source, discovered)
            record_source_success(source["name"], len(raw_jobs))

            # Detail-page fallback: for HTML sources, attempt enrichment on weak candidates
            # (those failing no_keyword) up to MAX_ENRICHMENTS per source
            if source.get("type") == "html":
                MAX_ENRICHMENTS = 5
                enriched_count  = 0
                enriched_jobs   = []
                for raw in raw_jobs:
                    if enriched_count >= MAX_ENRICHMENTS:
                        enriched_jobs.append(raw)
                        continue
                    # Quick pre-check: only enrich if title looks weak/generic
                    title_hay = normalize_text(f"{raw.get('title','')} {raw.get('url','')}")
                    if any(ex in title_hay for ex in get_excludes()):
                        enriched_jobs.append(raw)
                        continue
                    full_hay = normalize_text(f"{raw.get('title','')} {raw.get('body','')}")
                    kw_match = next((kw for kw in get_keywords() if kw in full_hay), None)
                    if kw_match:
                        enriched_jobs.append(raw)  # already good, no need to enrich
                        continue
                    # Candidate failed keyword match — try fetching its detail page
                    enriched = enrich_from_detail_page(raw, source)
                    if enriched.get("_enriched"):
                        enriched_count += 1
                    enriched_jobs.append(enriched)
                raw_jobs = enriched_jobs

            return source, raw_jobs, None
        except Exception as e:
            err    = str(e)
            etype  = "timeout" if "timeout" in err.lower() else "parse_error"
            fails, _ = record_source_failure(source["name"], err[:200], etype)
            if fails == 3: send_telegram_message(f"Source degraded: {source['name']}")
            elif fails == 7: send_telegram_message(f"Source dead: {source['name']}")
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
                if not ok: continue
                job.matched_keyword = matched_keyword
                if not location_allowed(job): continue
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
                    if (created or force) and unique_key not in emitted_keys:
                        emitted_keys.add(unique_key)
                        all_matched.append(job)

    expire_stale_jobs(seen_keys)
    return sorted(all_matched, key=lambda j: j.score or 0, reverse=True)

# ── Telegram ───────────────────────────────────────────────────────────────────

def send_telegram_message(text, chat_id=None, buttons=None):
    if not TELEGRAM_BOT_TOKEN: return
    target = str(chat_id or TELEGRAM_CHAT_ID).strip()
    if not target: return
    try:
        payload = {
            "chat_id": target,
            "text": text[:4000],
            "disable_web_page_preview": True,
        }
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=20,
        )
        # Telegram returns 400 when inline button URLs are malformed.
        # Retry without buttons so the message still delivers.
        if not resp.ok and buttons:
            payload.pop("reply_markup", None)
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=20,
            )
    except Exception:
        pass

def answer_callback(callback_id: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_id},
            timeout=10,
        )
    except Exception:
        pass

def main_menu_buttons():
    return [
        [{"text": "🚀 Scan for jobs", "callback_data": "/scan"},
         {"text": "🗂 Saved jobs",    "callback_data": "/jobs"}],
        [{"text": "✨ Last 24h",       "callback_data": "/latest"},
         {"text": "🧭 Status",         "callback_data": "/status"}],
        [{"text": "❓ Help",           "callback_data": "/help"}],
    ]

def send_menu(chat_id=None):
    send_telegram_message(
        "📋 What would you like to do?",
        chat_id=chat_id,
        buttons=main_menu_buttons(),
    )

def telegram_api(method, payload=None):
    r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
                      json=payload or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def get_updates(offset=None):
    payload = {"timeout": 20}
    if offset is not None: payload["offset"] = offset
    try:
        return telegram_api("getUpdates", payload).get("result", [])
    except Exception:
        return []

# ── Alert formatting ───────────────────────────────────────────────────────────

def format_job_alert(job: CanonicalJob) -> str:
    bd         = job.score_breakdown or {}
    loc        = prettify_location(job.location_raw or job.location_normalized)
    kind       = classify_opportunity(job)
    kind_label = opportunity_label(job)
    header     = ("🎓 Programme" if kind == "programme" else
                  "🎯 HIGH PRIORITY" if (job.score or 0) >= 75 else "🎯 Role")

    reasons = []
    for pts, label in [
        (bd.get("title_strength", 0),     "strong title match"),
        (bd.get("juniority", 0),           "junior/entry-level signal"),
        (bd.get("location_confidence", 0), "London/UK location"),
        (bd.get("source_quality", 0),      "direct studio source"),
        (bd.get("ats_type", 0),            "ATS-native listing"),
        (bd.get("company_tier", 0),        "preferred studio"),
    ]:
        if pts > 0: reasons.append(f"+ {label} (+{pts})")
    negs = bd.get("negative_indicators", 0)
    if negs < 0: reasons.append(f"- negative signals ({negs})")

    lines = [header, f"{job.company} -- {job.title}",
             f"Type: {kind_label}", f"📍 {loc}", f"⭐ Score: {int(job.score or 0)}"]
    if reasons:
        lines += ["", "Why it matched:"] + [f"  {r}" for r in reasons[:6]]
    if job.source_type:
        lines += ["", f"📡 Source: {job.source_name} ({job.ats_type or job.source_type})"]
    if job.apply_url:
        lines.append(f"🔗 {job.apply_url}")
    return "\n".join(lines)

def send_job_alert(job: CanonicalJob, chat_id=None):
    text = format_job_alert(job)
    buttons = None
    url = (job.apply_url or "").strip()
    # Telegram URL buttons require absolute http/https -- skip if relative or missing
    if url.startswith(("http://", "https://")):
        unique_key = build_unique_key(job)
        buttons = [
            [{"text": "🔗 Open job", "url": url}],
            [{"text": "📌 Mark as applied", "callback_data": f"applied::{unique_key}"},
             {"text": "🚫 Ignore",          "callback_data": f"ignore::{unique_key}"}],
            [{"text": "🧠 Explain this role", "callback_data": f"explain::{normalize_text(job.title)[:60]}"}],
        ]
    send_telegram_message(text, chat_id=chat_id, buttons=buttons)

def format_job_rows(rows) -> str:
    if not rows:
        return (
            "📭 Nothing new right now.\n\n"
            "Production roles often appear mid-week, so more listings may show up soon.\n\n"
            "I'll keep monitoring studios and alert you when anything appears.\n\n"
            "Try 🚀 /scan to check live sources right now."
        )
    lines = ["🗂️ Saved jobs"]
    for idx, (title, company, loc, url, first_seen, score) in enumerate(rows[:10], 1):
        loc_text = prettify_location(loc) if loc else None
        kind_icon = "🎓" if any(t in title.lower() for t in ["intern", "trainee", "scheme", "programme", "program", "launchpad", "work experience"]) else "🎯"
        lines.append(f"\n{idx}. {kind_icon} {title}")
        lines.append(f"   🏢 {company}")
        if loc_text: lines.append(f"   📍 {loc_text}")
        lines.append(f"   ⭐ Score {int(score)}")
        lines.append(f"   🕘 First seen {first_seen}")
        lines.append(f"   🔗 {url}")
    return "\n".join(lines)

def latest_rows(hours=24, limit=10):
    cutoff = (utc_now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S UTC")
    return db_execute(
        """SELECT title, company, location_raw, apply_url, first_seen, score
           FROM jobs WHERE first_seen>=? AND job_status='active'
           ORDER BY score DESC, id DESC LIMIT ?""",
        (cutoff, limit), fetch=True,
    )

def send_new_job_alerts(jobs: list):
    if not jobs: return
    seen, deduped = set(), []
    for job in jobs:
        key = build_unique_key(job)
        if key not in seen: seen.add(key); deduped.append(job)

    deduped.sort(key=lambda j: (classify_opportunity(j) == "programme", -(j.score or 0), j.company))
    qt           = quality_threshold()
    high         = [j for j in deduped if classify_opportunity(j) != "programme" and (j.score or 0) >= 75]
    normal_roles = [j for j in deduped if classify_opportunity(j) != "programme" and qt <= (j.score or 0) < 75]
    programmes   = [j for j in deduped if classify_opportunity(j) == "programme" and (j.score or 0) >= qt]

    for job in high[:6]:
        send_job_alert(job); time.sleep(0.5)

    summary = []
    if normal_roles and get_state("quality_mode", "off").lower() != "strict":
        summary.append("Other possible matches:")
        for job in normal_roles[:6]:
            loc_part = f" | {prettify_location(job.location_raw or job.location_normalized)}"
            summary += [f"- {job.title} -- {job.company}{loc_part} (score: {int(job.score or 0)})", f"  {job.apply_url}"]

    if programmes and get_state("quality_mode", "off").lower() != "strict":
        if summary: summary.append("")
        summary.append(f"{len(programmes)} programme signal{'s' if len(programmes)!=1 else ''}:")
        for job in programmes[:4]:
            loc_part = f" | {prettify_location(job.location_raw or job.location_normalized)}"
            summary += [f"- {job.title} -- {job.company}{loc_part} (score: {int(job.score or 0)})", f"  {job.apply_url}"]

    if summary: send_telegram_message("\n".join(summary))

# ── Telegram commands ──────────────────────────────────────────────────────────

def handle_command(text: str) -> str:
    text  = clean_text(text)
    lower = text.lower()

    if lower in {"/help", "/howto", "/start"}:
        return format_help_text()

    if lower == "/discoveries":
        return format_discoveries()

    if lower.startswith("/approve_source "):
        raw_id = clean_text(text[len("/approve_source "):])
        if not raw_id.isdigit(): return "Use: /approve_source <id>"
        ok, msg = approve_discovery(int(raw_id))
        return ("OK: " + msg) if ok else ("Error: " + msg)

    if lower.startswith("/reject_source "):
        raw_id = clean_text(text[len("/reject_source "):])
        if not raw_id.isdigit(): return "Use: /reject_source <id>"
        ok, msg = reject_discovery(int(raw_id))
        return ("OK: " + msg) if ok else ("Error: " + msg)

    if lower == "/showall":
        rows = db_execute(
            """SELECT title, company, location_raw, apply_url, first_seen, score,
                      score_breakdown_json, matched_keyword, source_name, ats_type
               FROM jobs WHERE job_status='active' AND (location_normalized IS NULL OR location_normalized != 'Non-UK') ORDER BY score DESC, id DESC LIMIT 30""",
            fetch=True,
        )
        if not rows:
            return "No active jobs yet. Use /scan to trigger a fresh scrape."
        total = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
        send_telegram_message(f"Stored matches ({total} total, showing top {min(len(rows),30)} by score)")
        time.sleep(0.3)
        for row in rows[:30]:
            title, company, loc, url, first_seen, score, bd_json, keyword, source_name, ats_type = row
            bd = {}
            try: bd = json.loads(bd_json or "{}")
            except Exception: pass
            reasons = [f"+{bd.get(k,0)} {lbl}" for k, lbl in [
                ("title_strength", "title"), ("juniority", "junior signal"),
                ("location_confidence", "location"), ("source_quality", "studio source"),
            ] if bd.get(k, 0) > 0]
            lines = [f"Score: {int(score or 0)} -- {title}", f"{company}"]
            if loc:     lines.append(f"Location: {loc}")
            if keyword: lines.append(f"Matched: {keyword}")
            if reasons: lines.append("Why: " + " | ".join(reasons))
            lines += [f"First seen: {first_seen}", f"Source: {source_name}" + (f" ({ats_type})" if ats_type else "")]
            if url: lines.append(url)
            send_telegram_message("\n".join(lines))
            time.sleep(0.4)
        if total > 30:
            send_telegram_message(f"...and {total-30} more. Use /search <company> to filter.")
        return ""

    if lower == "/diag":
        def _diag():
            import traceback as _tb
            results = []
            # Test 1: basic outbound HTTP
            try:
                r = requests.get("https://httpbin.org/get", headers=HEADERS, timeout=10)
                results.append(f"httpbin: {r.status_code}, {len(r.text)} chars")
            except Exception as e:
                results.append(f"httpbin FAIL: {type(e).__name__}: {e}")
            # Test 2: fetch a known Workable API
            try:
                r = requests.post(
                    "https://apply.workable.com/api/v3/accounts/nexusstudios/jobs",
                    headers={**HEADERS, "Content-Type": "application/json"},
                    json={"query":"","location":[],"department":[],"worktype":[],"remote":[]},
                    timeout=10
                )
                data = r.json()
                count = len(data.get("results", []))
                results.append(f"Workable API: {r.status_code}, {count} jobs")
            except Exception as e:
                results.append(f"Workable FAIL: {type(e).__name__}: {str(e)[:80]}")
            # Test 3: fetch Framestore HTML, report first 200 chars
            try:
                r = requests.get("https://www.framestore.com/careers", headers=HEADERS, timeout=10)
                snippet = r.text[:200].replace("\n"," ")
                results.append(f"Framestore HTML: {r.status_code}, starts: {snippet!r:.120}")
            except Exception as e:
                results.append(f"Framestore FAIL: {type(e).__name__}: {str(e)[:80]}")
            send_telegram_message("🔬 Diagnostics:\n" + "\n".join(results))
        threading.Thread(target=_diag, daemon=True).start()
        return "Running diagnostics..."

    if lower in {"/scan", "/scandebug"}:
        debug = (lower == "/scandebug")

        def _run_scan():
            if not _scan_lock.acquire(blocking=False):
                send_telegram_message("⏳ A scan is already running — please wait.")
                return
            try:
                sources   = get_active_sources()
                threshold = quality_threshold()
                send_telegram_message(
                    f"Scanning {len(sources)} sources...\n"
                    f"Location: {get_state('location_mode','london')} | Score threshold: {threshold}"
                    + ("\n[debug mode]" if debug else "")
                )

                all_matched  = []
                source_log   = []
                seen_keys    = set()
                emitted_keys = set()
                lock         = threading.Lock()

                def _scrape_one(source):
                    try:
                        raw_jobs, discovered = fetch_source_jobs(source)
                        record_source_success(source["name"], len(raw_jobs))
                        return source, raw_jobs, discovered, None
                    except Exception as e:
                        import traceback as _tb
                        err = f"{type(e).__name__}: {str(e)[:100]}"
                        record_source_failure(source["name"], err, "parse_error")
                        return source, [], [], err

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {executor.submit(_scrape_one, s): s for s in sources}
                    try:
                        completed_iter = concurrent.futures.as_completed(futures, timeout=120)
                    except Exception:
                        completed_iter = iter([])
                    for future in completed_iter:
                        try:
                            source, raw_jobs, discovered, err = future.result(timeout=30)
                        except concurrent.futures.TimeoutError:
                            source = futures[future]
                            record_source_failure(source["name"], "timeout", "timeout")
                            source_log.append((source["name"], 0, 0, "timeout", {}))
                            continue
                        except Exception as e:
                            source = futures[future]
                            source_log.append((source["name"], 0, 0, str(e)[:120], {}))
                            continue
                    # Any futures not yet completed (outer timeout) → mark as timeout
                    for future, source in futures.items():
                        if not future.done():
                            record_source_failure(source["name"], "timeout", "timeout")
                            source_log.append((source["name"], 0, 0, "timeout", {}))

                        if err:
                            source_log.append((source["name"], 0, 0, err, {}))
                            continue

                        save_discovered_sources(source, discovered)
                        matched_this  = 0
                        reason_counts = {"excluded": 0, "no_keyword": 0, "location": 0, "score": 0}

                        for raw in raw_jobs:
                            job    = normalise_to_canonical(raw, source)
                            reason = classify_rejection(job, threshold)
                            if reason != "passed":
                                reason_counts[reason] += 1
                                continue
                            # classify_rejection already validated keyword match;
                            # call again to get the matched keyword string
                            _, matched_keyword = title_keyword_match(job)
                            job.matched_keyword = matched_keyword
                            total_score, breakdown = score_job(job)
                            job.score = total_score; job.score_breakdown = breakdown
                            blob = " ".join(filter(None, [job.title, job.location_raw, job.description_text]))
                            job.location_normalized = detect_location(blob, company=job.company) or None
                            with lock:
                                created, unique_key = upsert_job(job)
                                seen_keys.add(unique_key)
                                # Only notify if job is new OR this is a forced re-scan
                                # and the job hasn't been notified this session already
                                if unique_key not in emitted_keys:
                                    emitted_keys.add(unique_key)
                                    if created or debug:
                                        all_matched.append(job)
                            matched_this += 1
                        source_log.append((source["name"], len(raw_jobs), matched_this, None, reason_counts))

                expire_stale_jobs(seen_keys)
                all_matched.sort(key=lambda j: j.score or 0, reverse=True)

                if debug:
                    lines = ["📊 Per-source results:"]
                    for name, raw_c, match_c, err, reason_counts in source_log:
                        if err:
                            lines.append(f"  ❌ {name}: {err}")
                        elif raw_c == 0:
                            lines.append(f"  🟡 {name}: 0 jobs found")
                        elif match_c == 0:
                            rsummary = ", ".join(f"{k}={v}" for k, v in (reason_counts or {}).items() if v > 0)
                            lines.append(f"  ⚪ {name}: {raw_c} found, 0 passed" + (f" ({rsummary})" if rsummary else ""))
                        else:
                            lines.append(f"  ✅ {name}: {raw_c} found, {match_c} matched")
                    chunk, chunks = [], []
                    for line in lines:
                        chunk.append(line)
                        if len("\n".join(chunk)) > 3500:
                            chunks.append("\n".join(chunk)); chunk = []
                    if chunk: chunks.append("\n".join(chunk))
                    for c in chunks:
                        send_telegram_message(c); time.sleep(0.3)

                errors        = sum(1 for _, _, _, e, _ in source_log if e)
                zero_src      = sum(1 for _, r, _, e, _ in source_log if r == 0 and not e)
                direct_count  = sum(1 for j in all_matched if classify_opportunity(j) != "programme")
                prog_count    = sum(1 for j in all_matched if classify_opportunity(j) == "programme")
                matched_line  = (
                    f"Matched: {len(all_matched)} total"
                    + (f" ({direct_count} direct, {prog_count} programme)" if prog_count
                       else f" ({direct_count} direct role{'s' if direct_count!=1 else ''})")
                )
                hint = (
                    f"\nThreshold was {threshold} -- try /quality off then /scan to see everything"
                    if not all_matched and errors < len(source_log) else ""
                )
                send_telegram_message(
                    f"✅ Scan complete\n"
                    f"Sources: {len(source_log)} checked, {errors} errors, {zero_src} zero jobs\n"
                    f"{matched_line}{hint}"
                )

                if not all_matched: return

                seen_alert, deduped = set(), []
                for job in all_matched:
                    key = build_unique_key(job)
                    if key not in seen_alert: seen_alert.add(key); deduped.append(job)

                direct_roles    = [j for j in deduped if classify_opportunity(j) != "programme"]
                programme_roles = [j for j in deduped if classify_opportunity(j) == "programme"]

                if direct_roles:
                    send_telegram_message(f"Top direct roles ({min(len(direct_roles),10)} shown):")
                    for job in direct_roles[:10]:
                        send_job_alert(job); time.sleep(0.5)
                    if len(direct_roles) > 10:
                        send_telegram_message(f"...and {len(direct_roles)-10} more. Use /jobs to see all.")

                if programme_roles:
                    send_telegram_message(f"Programme signals ({min(len(programme_roles),5)} shown):")
                    for job in programme_roles[:5]:
                        send_job_alert(job); time.sleep(0.5)
                    if len(programme_roles) > 5:
                        send_telegram_message(f"...and {len(programme_roles)-5} more. Use /jobs to see all.")

            except Exception as e:
                import traceback
                send_telegram_message(f"Scan crashed: {str(e)[:300]}\n{traceback.format_exc()[:500]}")
            finally:
                _scan_lock.release()

        threading.Thread(target=_run_scan, daemon=True).start()
        return "Scan started -- results on the way."

    if lower == "/events":
        rows = db_execute(
            """SELECT event_type, event_at, source_name, old_value_json, new_value_json, notes
               FROM job_events ORDER BY id DESC LIMIT 12""",
            fetch=True,
        )
        if not rows: return "🕘 No job changes recorded yet."
        icon = {"created": "NEW", "updated": "UPD", "reopened": "REOPEN", "expired": "EXP"}
        lines = ["Recent activity"]
        for etype, eat, sname, old_json, new_json, notes in rows:
            snap    = (json.loads(new_json) if new_json else {}) or (json.loads(old_json) if old_json else {})
            title   = snap.get("title", "Unknown")
            company = snap.get("company", "Unknown")
            lines.append(f"{icon.get(etype,'?')} {etype.title()} -- {company} -- {title}")
            lines.append(f"  {eat} | {sname or snap.get('source_name','')}")
            if notes: lines.append(f"  {notes}")
        return "\n".join(lines[:40])

    if lower == "/status":
        total   = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
        healthy = (db_execute("SELECT COUNT(*) FROM source_health WHERE status='healthy'", fetch=True) or [[0]])[0][0]
        n_src   = (db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]])[0][0]
        paused  = get_state("paused", "0") == "1"
        mode    = get_state("location_mode", "london")
        quality = get_state("quality_mode", "off")
        return (
            f"🎬 VFX Job Monitor\n\n"
            f"{'⏸️ Paused' if paused else '▶️ Running'}\n\n"
            f"🛰️ Sources: {n_src} active, {healthy} healthy\n"
            f"🗂️ Active matches: {total}\n"
            f"✨ Matches found last run: {get_state('last_match_count', '0')}\n\n"
            f"📍 Location: {mode}\n"
            f"🎚️ Quality: {quality}\n"
            f"Score threshold: {quality_threshold()}\n"
            f"⏱️ Scan interval: {CHECK_INTERVAL_SECONDS}s\n"
            f"🕘 Last checked: {get_state('last_checked', 'Never')}\n\n"
            f"🚀 /scan -- run a fresh scan now\n"
            f"🗂️ /jobs -- browse saved jobs\n"
            f"🧪 /scandebug -- per-source detail (for testing)"
        )

    if lower == "/jobs":
        rows = db_execute(
            """SELECT title, company, location_raw, apply_url, first_seen, score
               FROM jobs WHERE job_status='active' ORDER BY score DESC, id DESC LIMIT 10""",
            fetch=True,
        )
        return format_job_rows(rows)

    if lower == "/latest":
        return format_job_rows(latest_rows(hours=24))

    if lower == "/highpriority":
        rows = db_execute(
            """SELECT title, company, location_raw, apply_url, first_seen, score
               FROM jobs WHERE score>=75 AND job_status='active' ORDER BY score DESC LIMIT 10""",
            fetch=True,
        )
        return format_job_rows(rows)

    if lower.startswith("/search "):
        term = lower[len("/search "):].strip()
        rows = db_execute(
            """SELECT title, company, location_raw, apply_url, first_seen, score
               FROM jobs WHERE (lower(title) LIKE ? OR lower(company) LIKE ?) AND job_status='active'
               ORDER BY score DESC LIMIT 10""",
            (f"%{term}%", f"%{term}%"), fetch=True,
        )
        return format_job_rows(rows) if rows else f'No active jobs for "{term}".'

    if lower == "/keywords":
        return "Keywords\n" + "\n".join(f"- {k}" for k in get_keywords())

    if lower.startswith("/addkeyword "):
        kw = text[len("/addkeyword "):].strip()
        add_keyword(kw); return f'Added: "{normalize_text(kw)}"'

    if lower.startswith("/removekeyword "):
        kw = text[len("/removekeyword "):].strip()
        remove_keyword(kw); return f'Removed: "{normalize_text(kw)}"'

    if lower == "/companies":
        rows = db_execute(
            "SELECT company, COUNT(*) FROM jobs WHERE job_status='active' GROUP BY company ORDER BY COUNT(*) DESC",
            fetch=True,
        )
        return ("Companies:\n" + "\n".join(f"- {c} ({n})" for c, n in rows)) if rows else "No active jobs yet."

    if lower == "/sources":
        rows = db_execute("SELECT name, company, type, kind, active FROM sources ORDER BY priority", fetch=True)
        return ("Sources:\n" + "\n".join(f"{'ON' if r[4] else 'OFF'} {r[1]} | {r[2]} | {r[3]}" for r in (rows or []))) or "No sources."

    if lower == "/health":
        rows = db_execute("SELECT status, COUNT(*) FROM source_health GROUP BY status", fetch=True)
        if not rows: return "No health data yet. Run /scan first."
        n = (db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]])[0][0]
        return f"Source health ({n} active):\n" + "\n".join(f"  {s}: {c}" for s, c in rows)

    if lower == "/dead":
        rows = db_execute(
            """SELECT source_name, consecutive_fails, last_event_type, last_error, last_success_at
               FROM source_health WHERE status IN ('dead','degraded','suspect')
               ORDER BY consecutive_fails DESC""",
            fetch=True,
        )
        if not rows: return "No degraded or dead sources."
        lines = []
        for name, fails, evt, error, last_ok in rows:
            status_lbl = "DEAD" if fails >= 7 else ("ZERO" if evt == "success_zero" else "WARN")
            lines.append(f"{status_lbl} {name}\n  Fails: {fails} | {evt or '?'} | Last OK: {last_ok or 'never'}\n  {(error or '')[:100]}")
        return "\n\n".join(lines)

    if lower == "/weekly":
        threading.Thread(target=send_weekly_digest, daemon=True).start()
        return "📊 Sending weekly digest..."

    if lower == "/coverage":
        rows = db_execute(
            """SELECT sh.source_name, sh.jobs_found_last, sh.jobs_found_total,
                      sh.last_success_at, sh.status, sh.last_event_type, s.company
               FROM source_health sh
               LEFT JOIN sources s ON s.name = sh.source_name
               ORDER BY sh.jobs_found_total DESC""",
            fetch=True,
        ) or []
        active       = db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]]
        total_active = active[0][0]

        def friendly_status(status, last_event, fails_implied=False):
            if status == "healthy" and last_event == "success_nonzero": return "Working normally"
            if status == "healthy" and last_event == "success_zero":    return "No jobs found recently"
            if status == "suspect":                                      return "No jobs found recently"
            if status == "degraded":                                     return "Needs attention"
            if status == "dead":                                         return "Not responding"
            if status == "unknown":                                      return "Not checked yet"
            return "No jobs found recently"

        producing, quiet, broken = [], [], []
        for r in rows:
            name, last, total, last_ok, status, evt, company = r
            label = company or name
            fs    = friendly_status(status, evt)
            if status in ("dead", "degraded"):
                broken.append((label, fs))
            elif (last or 0) > 0:
                producing.append((label, last or 0, total or 0, last_ok))
            else:
                quiet.append((label, fs))

        lines = [f"📡 Coverage report\n{total_active} studios monitored\n"]

        if producing:
            lines.append("Producing results:")
            for company, last, total, last_ok in producing[:12]:
                lines.append(f"  ✅ {company} -- {last} found last scan ({total} total)")

        if quiet:
            lines.append("\nNo listings found (may be JS-rendered or currently quiet):")
            for company, fs in quiet[:10]:
                lines.append(f"  🟡 {company} -- {fs}")

        if broken:
            lines.append("\nNot responding (URL may have changed):")
            for company, fs in broken:
                lines.append(f"  ❌ {company} -- {fs}")

        return "\n".join(lines)

    if lower.startswith("/setlocation "):
        mode = lower[len("/setlocation "):].strip()
        if mode not in {"london", "uk", "off"}: return "Use: london, uk, or off"
        set_state("location_mode", mode); return f"Location set to {mode}"

    if lower.startswith("/quality "):
        mode = lower[len("/quality "):].strip()
        if mode not in {"strict", "normal", "off"}: return "Use: strict, normal, or off"
        set_state("quality_mode", mode); return f"Quality set to {mode} (threshold: {quality_threshold()})"

    if lower == "/pause":
        set_state("paused", "1"); return "Monitoring paused."

    if lower == "/resume":
        set_state("paused", "0"); return "Monitoring resumed."

    if lower.startswith("/disablesource "):
        name = clean_text(text[len("/disablesource "):]).strip()
        rows = db_execute("SELECT id, name, company FROM sources WHERE active=1", fetch=True) or []
        matches = [r for r in rows if name.lower() in r[1].lower() or name.lower() in r[2].lower()]
        if not matches:
            return f"No active source matching '{name}'. Use /sources to list."
        for sid, sname, scompany in matches:
            db_execute("UPDATE sources SET active=0 WHERE id=?", (sid,))
        names = ", ".join(f"{r[1]} ({r[2]})" for r in matches)
        return f"Disabled: {names}"

    if lower.startswith("/enablesource "):
        name = clean_text(text[len("/enablesource "):]).strip()
        rows = db_execute("SELECT id, name, company FROM sources WHERE active=0", fetch=True) or []
        matches = [r for r in rows if name.lower() in r[1].lower() or name.lower() in r[2].lower()]
        if not matches:
            return f"No disabled source matching '{name}'."
        for sid, sname, scompany in matches:
            db_execute("UPDATE sources SET active=1 WHERE id=?", (sid,))
        names = ", ".join(f"{r[1]} ({r[2]})" for r in matches)
        return f"Enabled: {names}"

    return "Unknown command. Use /help."

# ── Background threads ─────────────────────────────────────────────────────────

# ── Job interactions ───────────────────────────────────────────────────────────

def mark_job_interaction(unique_key: str, action: str):
    db_execute(
        "INSERT OR REPLACE INTO job_interactions (unique_key, action, actioned_at) VALUES (?,?,?)",
        (unique_key, action, now_str()),
    )

def get_applied_jobs():
    return db_execute(
        """SELECT j.title, j.company, j.apply_url, ji.actioned_at
           FROM job_interactions ji
           JOIN jobs j ON j.unique_key = ji.unique_key
           WHERE ji.action='applied'
           ORDER BY ji.actioned_at DESC LIMIT 20""",
        fetch=True,
    ) or []

ROLE_EXPLANATIONS = {
    "production assistant": (
        "Production Assistant\n\n"
        "An entry-level role supporting the production team day-to-day.\n\n"
        "Typical responsibilities:\n"
        "  scheduling and diary management\n"
        "  coordinating between departments\n"
        "  tracking deliverables and deadlines\n"
        "  admin and paperwork\n\n"
        "A common first step into studio production."
    ),
    "production coordinator": (
        "Production Coordinator\n\n"
        "Keeps productions running smoothly between departments.\n\n"
        "Typical responsibilities:\n"
        "  managing schedules and shot tracking\n"
        "  coordinating artists, supervisors and producers\n"
        "  maintaining production databases\n"
        "  handling day-to-day logistics\n\n"
        "Often the step up from Production Assistant."
    ),
    "studio runner": (
        "Studio Runner\n\n"
        "The most entry-level studio role -- great for getting your foot in the door.\n\n"
        "Typical responsibilities:\n"
        "  general studio support and errands\n"
        "  helping with deliveries and equipment\n"
        "  supporting multiple departments\n\n"
        "Runners often move into PA or coordinator roles quickly."
    ),
    "production runner": (
        "Production Runner\n\n"
        "Similar to a Studio Runner but focused on a specific production.\n\n"
        "Typical responsibilities:\n"
        "  supporting the production office\n"
        "  running between departments\n"
        "  general admin and logistics support\n\n"
        "A great entry point into production."
    ),
    "assistant producer": (
        "Assistant Producer\n\n"
        "Supports producers in managing a production from creative through delivery.\n\n"
        "Typical responsibilities:\n"
        "  supporting the producer day-to-day\n"
        "  liaising with clients and internal teams\n"
        "  helping manage budgets and schedules\n\n"
        "Usually requires some prior production experience."
    ),
    "production trainee": (
        "Production Trainee\n\n"
        "A structured learning role designed for people new to the industry.\n\n"
        "Typical responsibilities:\n"
        "  rotating across production departments\n"
        "  learning studio workflows\n"
        "  supporting senior production staff\n\n"
        "Often part of a formal training scheme."
    ),
    "project coordinator": (
        "Project Coordinator\n\n"
        "Keeps a specific project or set of projects on track.\n\n"
        "Typical responsibilities:\n"
        "  tracking tasks, milestones and timelines\n"
        "  coordinating between teams\n"
        "  managing project documentation\n\n"
        "Transferable across many studio types."
    ),
    "post production assistant": (
        "Post Production Assistant\n\n"
        "Supports the post production team through editing and delivery.\n\n"
        "Typical responsibilities:\n"
        "  organising and managing media assets\n"
        "  assisting editors and post supervisors\n"
        "  coordinating review and feedback sessions\n\n"
        "A good entry point into post production."
    ),
}

def explain_role(title: str) -> str:
    low = normalize_text(title)
    for key, explanation in ROLE_EXPLANATIONS.items():
        if key in low:
            return explanation
    # Generic fallback
    return (
        f"{title.title()}\n\n"
        "This role is part of the production team at a VFX or animation studio.\n\n"
        "Entry-level production roles typically involve:\n"
        "  supporting producers and coordinators\n"
        "  tracking schedules and deliverables\n"
        "  coordinating between departments\n\n"
        "A good starting point for a career in studio production."
    )

def handle_callback(callback_query: dict, chat_id: str):
    data = callback_query.get("data", "")
    cb_id = callback_query.get("id", "")
    answer_callback(cb_id)

    # Inline menu buttons trigger commands
    if data.startswith("/"):
        reply = handle_command(data)
        if reply:
            send_telegram_message(reply, chat_id=chat_id, buttons=(
                main_menu_buttons() if data in {"/help", "/status"} else None
            ))
        return

    if data.startswith("applied::"):
        unique_key = data[len("applied::"):]
        mark_job_interaction(unique_key, "applied")
        send_telegram_message("📌 Marked as applied. Good luck!", chat_id=chat_id)
        return

    if data.startswith("ignore::"):
        unique_key = data[len("ignore::"):]
        mark_job_interaction(unique_key, "ignored")
        send_telegram_message("Got it. I'll note that.", chat_id=chat_id)
        return

    if data.startswith("explain::"):
        title = data[len("explain::"):]
        send_telegram_message(explain_role(title), chat_id=chat_id)
        return

    if data.startswith("location::"):
        mode = data[len("location::"):]
        set_state("location_mode", mode)
        send_telegram_message(
            f"📍 Location set to {mode}.\n\nWhat kinds of opportunities should I highlight?",
            chat_id=chat_id,
            buttons=[[
                {"text": "🎯 Direct roles only",          "callback_data": "opptype::direct"},
                {"text": "🎓 Programmes only",            "callback_data": "opptype::programme"},
                {"text": "Both",                          "callback_data": "opptype::both"},
            ]],
        )
        return

    if data.startswith("opptype::"):
        set_state("opportunity_type_pref", data[len("opptype::"):])
        send_telegram_message(
            "How often should scans run?",
            chat_id=chat_id,
            buttons=[[
                {"text": "Every 10 min",  "callback_data": "interval::600"},
                {"text": "Every 30 min",  "callback_data": "interval::1800"},
                {"text": "Every hour",    "callback_data": "interval::3600"},
            ]],
        )
        return

    if data.startswith("interval::"):
        # Can't change env var at runtime, but store preference
        set_state("scan_interval_pref", data[len("interval::"):])
        send_telegram_message(
            "You're all set.\n\nI'll monitor studios and send alerts when relevant opportunities appear.",
            chat_id=chat_id,
            buttons=[
                [{"text": "🚀 Scan now",      "callback_data": "/scan"},
                 {"text": "🗂 View saved jobs", "callback_data": "/jobs"}],
                [{"text": "⚙️ Settings",       "callback_data": "/status"}],
            ],
        )
        return

def command_loop():
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset  = update["update_id"] + 1

                # Handle inline button callbacks
                if "callback_query" in update:
                    cq      = update["callback_query"]
                    chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))
                    if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                        continue
                    handle_callback(cq, chat_id)
                    continue

                msg     = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text    = msg.get("text", "")
                if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                    continue
                if not text.startswith("/"):
                    continue

                lower = text.strip().lower()

                # Onboarding flow for /start
                if lower == "/start":
                    send_telegram_message(
                        "🎬 VFX Job Monitor\n\n"
                        "This bot watches VFX, animation and post-production studios and alerts you "
                        "when entry-level production roles appear.\n\n"
                        "It scans studios automatically and highlights the strongest opportunities.\n\n"
                        "Let's set it up quickly.\n\n"
                        "Where should I focus?",
                        chat_id=chat_id,
                        buttons=[[
                            {"text": "📍 London only",      "callback_data": "location::london"},
                            {"text": "🇬🇧 Anywhere in UK",  "callback_data": "location::uk"},
                            {"text": "🌍 Anywhere",         "callback_data": "location::off"},
                        ]],
                    )
                    continue

                # /menu always shows the main nav
                if lower == "/menu":
                    send_menu(chat_id=chat_id)
                    continue

                # /applied shows tracked applications
                if lower == "/applied":
                    rows = get_applied_jobs()
                    if not rows:
                        send_telegram_message(
                            "📌 No applications tracked yet.\n\nTap 'Mark as applied' on any job alert to track it here.",
                            chat_id=chat_id,
                        )
                    else:
                        lines = ["📌 Applied jobs\n"]
                        for i, (title, company, url, at) in enumerate(rows, 1):
                            lines.append(f"{i}. {title} -- {company}")
                            lines.append(f"   Applied: {at[:10]}")
                            if url: lines.append(f"   {url}")
                        send_telegram_message("\n".join(lines), chat_id=chat_id)
                    continue

                reply = handle_command(text)
                if reply:
                    # Add menu buttons after help and status
                    btns = main_menu_buttons() if lower in {"/help", "/howto", "/status"} else None
                    send_telegram_message(reply, chat_id=chat_id, buttons=btns)
        except Exception:
            pass
        time.sleep(3)

def send_weekly_digest():
    cutoff_week = (utc_now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Jobs found this week
    week_rows = db_execute(
        """SELECT title, company, opportunity_type, score
           FROM jobs WHERE first_seen >= ? ORDER BY score DESC""",
        (cutoff_week,), fetch=True,
    ) or []

    direct_jobs  = [r for r in week_rows if r[2] != "programme"]
    programme_jobs = [r for r in week_rows if r[2] == "programme"]

    if not week_rows:
        send_telegram_message(
            "📊 Weekly hiring signal\n\n"
            "No new production roles appeared this week across monitored studios.\n\n"
            "I'll keep watching and alert you when something comes up."
        )
        return

    # Most active studio
    from collections import Counter
    studio_counts = Counter(r[1] for r in week_rows)
    top_studio    = studio_counts.most_common(1)[0][0] if studio_counts else "Unknown"

    lines = [
        "📊 Weekly hiring signal\n",
        f"Across monitored studios this week:\n",
        f"  {len(direct_jobs)} production role{'s' if len(direct_jobs) != 1 else ''} appeared",
        f"  {len(programme_jobs)} internship / programme signal{'s' if len(programme_jobs) != 1 else ''}",
        f"  Most active studio: {top_studio}\n",
    ]

    if direct_jobs:
        lines.append("Top roles this week:")
        for title, company, _, score in direct_jobs[:5]:
            lines.append(f"  🎯 {title} -- {company}")

    if programme_jobs:
        lines.append("\nProgrammes:")
        for title, company, _, score in programme_jobs[:3]:
            lines.append(f"  🎓 {title} -- {company}")

    lines.append("\nRun 🚀 /scan to check for anything new right now.")
    send_telegram_message("\n".join(lines))

def monitor_loop():
    last_digest_day = None
    while True:
        try:
            if get_state("paused", "0") != "1":
                if _scan_lock.acquire(blocking=False):
                    try:
                        new_jobs = collect_and_store_jobs()
                        set_state("last_match_count", str(len(new_jobs)))
                        set_state("last_checked", now_str())
                        if new_jobs: send_new_job_alerts(new_jobs)
                    finally:
                        _scan_lock.release()
                else:
                    set_state("last_checked", now_str())  # manual scan in progress
            else:
                set_state("last_checked", now_str())
                set_state("last_match_count", "0")

            # Weekly digest — send on Monday mornings (weekday 0), once per day
            now = utc_now()
            today = now.date()
            if now.weekday() == 0 and 8 <= now.hour < 9 and last_digest_day != today:
                last_digest_day = today
                try:
                    send_weekly_digest()
                except Exception:
                    pass
        except Exception:
            set_state("last_checked", now_str())
        time.sleep(CHECK_INTERVAL_SECONDS)

# ── Flask ──────────────────────────────────────────────────────────────────────────────
# ── Web UI helpers ─────────────────────────────────────────────────────────────

_CSS = (
    "* { box-sizing: border-box; margin: 0; padding: 0; }\n"
    "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;\n"
    "       background: #0f0f0f; color: #e8e8e8; min-height: 100vh; }\n"
    "a { color: #a78bfa; text-decoration: none; }\n"
    "a:hover { text-decoration: underline; }\n"
    ".nav { background: #1a1a1a; border-bottom: 1px solid #2a2a2a; padding: 14px 24px;\n"
    "       display: flex; align-items: center; gap: 24px;\n"
    "       position: sticky; top: 0; z-index: 100; }\n"
    ".nav-title { font-size: 15px; font-weight: 600; color: #fff;\n"
    "             letter-spacing: 0.02em; margin-right: 4px; }\n"
    ".nav a { font-size: 13px; color: #9ca3af; }\n"
    ".nav a:hover { color: #e8e8e8; text-decoration: none; }\n"
    ".page { max-width: 880px; margin: 0 auto; padding: 28px 20px; }\n"
    ".overview { display: flex; gap: 12px; margin-bottom: 28px; flex-wrap: wrap; }\n"
    ".stat { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px;\n"
    "        padding: 16px 20px; flex: 1; min-width: 120px;\n"
    "        cursor: pointer; transition: border-color 0.15s; }\n"
    ".stat:hover { border-color: #a78bfa; }\n"
    ".stat-val { font-size: 28px; font-weight: 700; color: #fff; }\n"
    ".stat-label { font-size: 12px; color: #6b7280; margin-top: 2px; }\n"
    ".section-title { font-size: 12px; font-weight: 600; color: #6b7280;\n"
    "                 letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 12px; }\n"
    ".section-block { margin-bottom: 32px; }\n"
    ".job-card { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 10px;\n"
    "            padding: 16px 18px; margin-bottom: 8px; transition: border-color 0.15s; }\n"
    ".job-card:hover { border-color: #3a3a3a; }\n"
    ".card-badges { display: flex; gap: 6px; margin-bottom: 8px;\n"
    "               flex-wrap: wrap; align-items: center; }\n"
    ".badge { display: inline-block; font-size: 11px; font-weight: 600;\n"
    "         border-radius: 4px; padding: 2px 8px; }\n"
    ".badge-direct { background: #1e1b4b; color: #a78bfa; }\n"
    ".badge-prog   { background: #1a2e1a; color: #6ee7b7; }\n"
    ".badge-new    { background: #292010; color: #fbbf24; }\n"
    ".badge-score  { background: #1f1f1f; color: #9ca3af; border: 1px solid #2a2a2a; }\n"
    ".job-title { font-size: 16px; font-weight: 600; color: #fff;\n"
    "             margin-bottom: 5px; line-height: 1.3; }\n"
    ".job-meta { font-size: 13px; color: #6b7280; margin-bottom: 12px;\n"
    "            display: flex; flex-wrap: wrap; gap: 0 16px; }\n"
    ".actions { display: flex; gap: 8px; flex-wrap: wrap; }\n"
    ".btn { font-size: 12px; padding: 7px 13px; border-radius: 6px; border: none;\n"
    "       cursor: pointer; font-weight: 500; text-decoration: none;\n"
    "       display: inline-block; transition: opacity 0.12s; }\n"
    ".btn-primary   { background: #a78bfa; color: #0f0f0f; }\n"
    ".btn-secondary { background: #2a2a2a; color: #d1d5db; }\n"
    ".btn-ghost     { background: transparent; color: #6b7280; border: 1px solid #2a2a2a; }\n"
    ".btn:hover { opacity: 0.8; text-decoration: none; }\n"
    ".empty { color: #4b5563; font-size: 14px; padding: 40px 0; text-align: center; }\n"
    ".coverage-row { display: flex; justify-content: space-between; align-items: center;\n"
    "                padding: 10px 0; border-bottom: 1px solid #1f1f1f; font-size: 13px; }\n"
    ".coverage-row:last-child { border-bottom: none; }\n"
    ".coverage-ok   { color: #6ee7b7; }\n"
    ".coverage-warn { color: #fbbf24; }\n"
    ".coverage-dead { color: #f87171; }\n"
    ".last-checked  { font-size: 12px; color: #4b5563; margin-top: 4px; margin-bottom: 24px; }\n"
    ".filter-bar { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 10px;\n"
    "              padding: 14px 16px; margin-bottom: 20px;\n"
    "              display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }\n"
    ".filter-bar input, .filter-bar select {\n"
    "    background: #111; border: 1px solid #2a2a2a; color: #e8e8e8;\n"
    "    border-radius: 6px; padding: 6px 10px; font-size: 13px; outline: none; }\n"
    ".filter-bar input { flex: 1; min-width: 140px; }\n"
    ".filter-bar select { min-width: 110px; }\n"
    ".filter-bar input:focus, .filter-bar select:focus { border-color: #a78bfa; }\n"
    ".filter-chips { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 14px; }\n"
    ".chip { background: #1e1b4b; color: #a78bfa; border-radius: 20px; font-size: 12px;\n"
    "        padding: 3px 10px; }\n"
    ".result-count { font-size: 13px; color: #4b5563; margin-bottom: 14px; }\n"
    ".score-row { display: flex; justify-content: space-between; padding: 8px 0;\n"
    "             border-bottom: 1px solid #1f1f1f; font-size: 13px; }\n"
    ".score-row:last-child { border-bottom: none; }\n"
    ".score-pos { color: #6ee7b7; }\n"
    ".score-neg { color: #f87171; }\n"
    ".score-zero { color: #4b5563; }\n"
    "@media (max-width: 600px) {\n"
    "  .nav { gap: 16px; padding: 12px 16px; flex-wrap: wrap; }\n"
    "  .page { padding: 20px 14px; }\n"
    "  .filter-bar { flex-direction: column; align-items: stretch; }\n"
    "  .filter-bar input, .filter-bar select { min-width: unset; }\n"
    "  .actions { gap: 6px; }\n"
    "  .btn { padding: 8px 12px; }\n"
    "}\n"
    "\n"
)


def _nav(active=""):
    links = [("Home", "/"), ("Jobs", "/jobs"), ("Saved", "/saved"),
             ("Applied", "/applied"), ("Coverage", "/coverage")]
    items = "".join(
        f'<a href="{url}" style="color:{"#e8e8e8" if label==active else ""}">'
        f'{label}</a>'
        for label, url in links
    )
    return (f'<nav class="nav"><span class="nav-title">&#127916; VFX Monitor</span>'
            + items + '</nav>')

def _page(title, body, active=""):
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<title>{title} \u2013 VFX Job Monitor</title>'
        f'<style>{_CSS}</style></head><body>'
        + _nav(active)
        + f'<div class="page">{body}</div></body></html>'
    )

def _is_new(first_seen_str):
    if not first_seen_str:
        return False
    import datetime as _dt
    try:
        fs = _dt.datetime.fromisoformat(first_seen_str[:19])
        return (_dt.datetime.utcnow() - fs).total_seconds() < 86400
    except Exception:
        return False

def _job_card(row, actions=True, back_url="/jobs"):
    jid, title, company, loc, url, first_seen, score, opp_type, status = row[:9]
    loc_str   = prettify_location(loc) if loc else "Location unknown"
    is_prog   = (opp_type == "programme") or any(
        t in (title or "").lower() for t in ["intern","trainee","scheme","programme","launchpad","work experience"]
    )
    type_cls  = "badge-prog" if is_prog else "badge-direct"
    type_lbl  = "Programme" if is_prog else "Direct role"
    score_int = int(score or 0)
    date_str  = (first_seen or "")[:10]
    new_badge = '<span class="badge badge-new">New today</span>' if _is_new(first_seen or "") else ""
    score_badge = f'<span class="badge badge-score">Score {score_int}</span>'
    action_html = ""
    if actions:
        open_btn  = (f'<a class="btn btn-primary" href="{url}" target="_blank">Open job &#8599;</a>'
                     if url and url.startswith("http") else "")
        view_btn  = f'<a class="btn btn-secondary" href="/job/{jid}">Why matched</a>'
        save_btn  = f'<a class="btn btn-ghost" href="/save/{jid}">Save</a>'
        apply_btn = f'<a class="btn btn-ghost" href="/applied_mark/{jid}">Mark applied</a>'
        action_html = f'<div class="actions">{open_btn}{view_btn}{save_btn}{apply_btn}</div>'
    return (
        f'<div class="job-card">'
        f'<div class="card-badges">'
        f'<span class="badge {type_cls}">{type_lbl}</span>'
        + new_badge + score_badge +
        f'</div>'
        f'<div class="job-title">{title}</div>'
        f'<div class="job-meta"><span>{company}</span><span>&#128205; {loc_str}</span>'
        f'<span>First seen: {date_str}</span></div>'
        + action_html +
        f'</div>'
    )

def _score_explanation(bd_json):
    import json as _json
    try:
        bd = _json.loads(bd_json) if bd_json else {}
    except Exception:
        bd = {}
    if not bd:
        return '<p class="empty">No score data available.</p>'
    labels = {
        "title_strength":      ("Title match",         "How well the job title matches production keywords"),
        "juniority":           ("Entry-level signals",  "Words like junior, assistant, graduate, intern"),
        "location_confidence": ("Location match",       "London / UK location indicators"),
        "source_quality":      ("Source quality",       "Tier of the studio or board"),
        "ats_type":            ("ATS bonus",            "Trusted application system"),
        "company_tier":        ("Preferred studio",     "Top-tier studio bonus"),
        "negative_indicators": ("Senior role penalty",  "Words like senior, director, supervisor"),
    }
    rows_html = ""
    for key, (label, desc) in labels.items():
        pts = bd.get(key, 0)
        cls  = "score-pos" if pts > 0 else ("score-neg" if pts < 0 else "score-zero")
        sign = "+" if pts > 0 else ""
        rows_html += (
            f'<div class="score-row">'
            f'<span style="color:#e8e8e8">{label} '
            f'<span style="color:#4b5563;font-size:12px">({desc})</span></span>'
            f'<span class="{cls}">{sign}{int(pts)}</span></div>'
        )
    total = int(bd.get("total", 0))
    return (
        '<div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:4px 16px">'
        + rows_html
        + f'<div class="score-row"><span style="color:#fff;font-weight:600">Total score</span>'
          f'<span style="color:#fbbf24;font-weight:600">{total}</span></div>'
        + '</div>'
    )

# \u2500\u2500 Flask routes \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route("/")
def home():
    from datetime import datetime as _dt2, timedelta
    cutoff  = (_dt2.utcnow() - timedelta(hours=24)).isoformat()
    total    = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active'", fetch=True) or [[0]])[0][0]
    new_ct   = (db_execute("SELECT COUNT(*) FROM jobs WHERE job_status='active' AND first_seen > ?",
                           (cutoff,), fetch=True) or [[0]])[0][0]
    saved_ct = (db_execute(
        "SELECT COUNT(DISTINCT unique_key) FROM job_interactions WHERE action='saved'",
        fetch=True) or [[0]])[0][0]
    applied_ct = (db_execute(
        "SELECT COUNT(DISTINCT unique_key) FROM job_interactions WHERE action='applied'",
        fetch=True) or [[0]])[0][0]
    last = get_state("last_checked", "Never")

    new_rows = db_execute(
        """SELECT id, title, company, location_raw, apply_url, first_seen, score,
                  opportunity_type, job_status FROM jobs
           WHERE job_status='active' AND first_seen > ?
           ORDER BY score DESC, id DESC LIMIT 6""",
        (cutoff,), fetch=True) or []
    hi_rows = db_execute(
        """SELECT id, title, company, location_raw, apply_url, first_seen, score,
                  opportunity_type, job_status FROM jobs
           WHERE job_status='active' AND opportunity_type != 'programme'
           ORDER BY score DESC, id DESC LIMIT 5""",
        fetch=True) or []
    prog_rows = db_execute(
        """SELECT id, title, company, location_raw, apply_url, first_seen, score,
                  opportunity_type, job_status FROM jobs
           WHERE job_status='active' AND opportunity_type = 'programme'
           ORDER BY score DESC, id DESC LIMIT 4""",
        fetch=True) or []

    stats_html = (
        f'<div class="overview">'
        f'<a href="/jobs" class="stat" style="text-decoration:none">'
        f'<div class="stat-val">{total}</div><div class="stat-label">Active jobs</div></a>'
        f'<a href="/jobs?type=direct" class="stat" style="text-decoration:none">'
        f'<div class="stat-val">{new_ct}</div><div class="stat-label">New today</div></a>'
        f'<a href="/saved" class="stat" style="text-decoration:none">'
        f'<div class="stat-val">{saved_ct}</div><div class="stat-label">Saved</div></a>'
        f'<a href="/applied" class="stat" style="text-decoration:none">'
        f'<div class="stat-val">{applied_ct}</div><div class="stat-label">Applied</div></a>'
        f'</div>'
        f'<p class="last-checked">Last checked: {last}</p>'
    )

    def _section(heading, rows, empty_msg):
        if not rows:
            return (f'<div class="section-block"><div class="section-title">{heading}</div>'
                    f'<p class="empty" style="padding:16px 0">{empty_msg}</p></div>')
        cards = "".join(_job_card(r) for r in rows)
        return f'<div class="section-block"><div class="section-title">{heading}</div>{cards}</div>'

    body = (
        stats_html
        + _section("New today", new_rows, "Nothing new in the last 24 hours.")
        + _section("High priority direct roles", hi_rows, "No direct roles found yet.")
        + _section("Programmes &amp; internships", prog_rows, "No programmes found yet.")
    )
    return _page("Home", body, "Home")

@app.route("/jobs")
def jobs_page():
    from flask import request as _req
    q             = (_req.args.get("q", "") or "").strip()
    company       = (_req.args.get("company", "") or "").strip()
    min_score_raw = (_req.args.get("score", "0") or "0").strip()
    opp_type      = (_req.args.get("type", "all") or "all").strip()
    loc_mode      = (_req.args.get("loc", "all") or "all").strip()
    try:
        min_score_int = int(min_score_raw)
    except ValueError:
        min_score_int = 0

    conditions, params = ["j.job_status='active'"], []
    if q:
        conditions.append("(j.title LIKE ? OR j.company LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    if company:
        conditions.append("j.company = ?")
        params.append(company)
    if min_score_int > 0:
        conditions.append("j.score >= ?")
        params.append(min_score_int)
    if opp_type == "direct":
        conditions.append("j.opportunity_type != 'programme'")
    elif opp_type == "programme":
        conditions.append("j.opportunity_type = 'programme'")
    if loc_mode == "london":
        conditions.append("(LOWER(j.location_raw) LIKE '%london%')")
    elif loc_mode == "uk":
        conditions.append("j.location_raw NOT LIKE '%Non-UK%'")

    where = " AND ".join(conditions)
    rows  = db_execute(
        f"""SELECT j.id, j.title, j.company, j.location_raw, j.apply_url,
                   j.first_seen, j.score, j.opportunity_type, j.job_status
            FROM jobs j WHERE {where}
            ORDER BY j.score DESC, j.id DESC LIMIT 80""",
        tuple(params), fetch=True) or []

    all_cos = db_execute("SELECT DISTINCT company FROM jobs WHERE job_status='active' ORDER BY company",
                         fetch=True) or []
    co_opts = '<option value="">All companies</option>' + "".join(
        f'<option value="{r[0]}" {"selected" if r[0]==company else ""}>{r[0]}</option>'
        for r in all_cos)
    score_opts = "".join(
        f'<option value="{v}" {"selected" if str(v)==str(min_score_int) else ""}>{lbl}</option>'
        for v, lbl in [("0","Any score"),("30","30+"),("50","50+"),("70","70+"),("90","90+")])
    type_opts = "".join(
        f'<option value="{v}" {"selected" if v==opp_type else ""}>{lbl}</option>'
        for v, lbl in [("all","All types"),("direct","Direct roles"),("programme","Programmes")])
    loc_opts = "".join(
        f'<option value="{v}" {"selected" if v==loc_mode else ""}>{lbl}</option>'
        for v, lbl in [("all","All locations"),("london","London"),("uk","UK")])

    filter_bar = (
        f'<form method="get" action="/jobs" class="filter-bar">'
        f'<input name="q" placeholder="Search title or company..." value="{q}">'
        f'<select name="company">{co_opts}</select>'
        f'<select name="score">{score_opts}</select>'
        f'<select name="type">{type_opts}</select>'
        f'<select name="loc">{loc_opts}</select>'
        f'<button type="submit" class="btn btn-primary">Filter</button>'
        f'<a href="/jobs" class="btn btn-ghost">Clear</a>'
        f'</form>'
    )
    active_chips = []
    if q:          active_chips.append(f"Search: {q}")
    if company:    active_chips.append(company)
    if min_score_int > 0: active_chips.append(f"Score {min_score_int}+")
    if opp_type != "all": active_chips.append(opp_type.title())
    if loc_mode != "all": active_chips.append(loc_mode.title())
    chips_html = ('<div class="filter-chips">' +
                  "".join(f'<span class="chip">{c}</span>' for c in active_chips) +
                  '</div>') if active_chips else ""

    n = len(rows)
    count_html = f'<p class="result-count">{n} job{"s" if n != 1 else ""} found</p>'
    cards = "".join(_job_card(r) for r in rows) if rows else '<p class="empty">No jobs match these filters right now.</p>'
    body  = filter_bar + chips_html + count_html + cards
    return _page("Jobs", body, "Jobs")

@app.route("/saved")
def saved_page():
    rows = db_execute(
        """SELECT j.id, j.title, j.company, j.location_raw, j.apply_url,
                  j.first_seen, j.score, j.opportunity_type, j.job_status
           FROM job_interactions ji JOIN jobs j ON j.unique_key = ji.unique_key
           WHERE ji.action='saved' ORDER BY ji.actioned_at DESC LIMIT 30""",
        fetch=True) or []
    cards = "".join(_job_card(r) for r in rows) if rows else '<p class="empty">No saved jobs yet.</p>'
    return _page("Saved", f'<div class="section-title">Saved ({len(rows)})</div>' + cards, "Saved")

@app.route("/applied")
def applied_page():
    rows = db_execute(
        """SELECT j.id, j.title, j.company, j.location_raw, j.apply_url,
                  j.first_seen, j.score, j.opportunity_type, j.job_status
           FROM job_interactions ji JOIN jobs j ON j.unique_key = ji.unique_key
           WHERE ji.action='applied' ORDER BY ji.actioned_at DESC LIMIT 30""",
        fetch=True) or []
    cards = "".join(_job_card(r, actions=False) for r in rows) if rows else '<p class="empty">No applications tracked yet.</p>'
    return _page("Applied", f'<div class="section-title">Applied ({len(rows)})</div>' + cards, "Applied")

@app.route("/job/<int:jid>")
def job_detail(jid):
    row = db_execute(
        """SELECT id, title, company, location_raw, apply_url, first_seen, score,
                  opportunity_type, job_status, score_breakdown_json
           FROM jobs WHERE id=?""",
        (jid,), fetch=True)
    if not row:
        return _page("Not found", '<p class="empty">Job not found.</p>'), 404
    r    = row[0]
    card = _job_card(r[:9])
    why  = ('<div class="section-block"><div class="section-title">Why this matched</div>'
            + _score_explanation(r[9]) + '</div>')
    body = ('<a href="/jobs" style="font-size:13px;color:#6b7280">&#8592; Back to jobs</a>'
            + '<br><br>' + card + '<br>' + why)
    return _page(r[1], body, "Jobs")

@app.route("/save/<int:jid>")
def save_job(jid):
    row = db_execute("SELECT unique_key FROM jobs WHERE id=?", (jid,), fetch=True)
    if row:
        mark_job_interaction(row[0][0], "saved")
    from flask import redirect
    return redirect("/jobs")

@app.route("/applied_mark/<int:jid>")
def applied_mark(jid):
    row = db_execute("SELECT unique_key FROM jobs WHERE id=?", (jid,), fetch=True)
    if row:
        mark_job_interaction(row[0][0], "applied")
    from flask import redirect
    return redirect("/applied")

@app.route("/coverage")
def coverage_page():
    rows = db_execute(
        """SELECT sh.source_name, sh.jobs_found_last, sh.jobs_found_total,
                  sh.last_success_at, sh.status, sh.last_event_type, s.company
           FROM source_health sh LEFT JOIN sources s ON s.name = sh.source_name
           ORDER BY sh.jobs_found_total DESC""",
        fetch=True) or []

    def _friendly(status, evt):
        if status == "healthy" and evt == "success_nonzero": return ("Working normally", "coverage-ok")
        if status in ("healthy", "suspect") and evt == "success_zero": return ("No jobs found recently", "coverage-warn")
        if status == "degraded": return ("Needs attention", "coverage-warn")
        if status == "dead": return ("Not responding", "coverage-dead")
        return ("Not checked yet", "coverage-warn")

    items = ""
    for name, last, total, last_ok, status, evt, company in rows:
        label, cls = _friendly(status, evt)
        display    = company or name
        count_str  = (f"{last} last scan" if (last or 0) > 0 else "")
        items += (
            f'<div class="coverage-row"><span>{display}</span>'
            f'<span><span class="{cls}">{label}</span>'
            + (f' <span style="color:#4b5563;font-size:11px">{count_str}</span>' if count_str else "")
            + '</span></div>'
        )
    n_src = (db_execute("SELECT COUNT(*) FROM sources WHERE active=1", fetch=True) or [[0]])[0][0]
    body  = (
        f'<div class="section-title">{n_src} studios monitored</div>'
        + ('<div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:4px 16px">'
           + items + '</div>' if items else '<p class="empty">No coverage data yet.</p>')
    )
    return _page("Coverage", body, "Coverage")

@app.route("/health")
def health_check():
    return {"status": "ok"}, 200


# ── Startup ────────────────────────────────────────────────────────────────────

init_db()

def start_background_threads():
    global _started
    if _started: return
    _started = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=command_loop, daemon=True).start()
    send_telegram_message(
        "🎬 VFX Job Monitor is live\n\n"
        "Watching 25+ studios and industry boards for entry-level production roles.\n\n"
        "🚀 /scan -- run a fresh scan now\n"
        "🗂️ /jobs -- browse saved matches\n"
        "❓ /help -- full guide"
    )

start_background_threads()
