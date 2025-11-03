#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import httpx
from dateutil import parser as dtparse
from pydantic import BaseModel, Field, HttpUrl

from datetime import datetime, timezone
from dateutil import parser as dtparse
import hashlib
import json

from db_schema import ensure_common_schema, map_article_to_topic

import json, hashlib, sqlite3

#GUARDIAN_API_KEY = "e71d9136-0c51-4064-8240-bc2c860b9b3a" #os.getenv("GUARDIAN_API_KEY") or "test"  # "test" works but is heavily rate-limited
from config_keys import get_key
GUARDIAN_API_KEY = get_key("guardian", "")

def set_api_key(k: str):
    global GUARDIAN_API_KEY
    GUARDIAN_API_KEY = (k or "").strip()

import hashlib
import re

def _clean_for_hash(text: str) -> str:
    if not text:
        return ""
    # Lower, collapse whitespace, strip boilerplate-y leftovers
    t = re.sub(r"\s+", " ", text.lower()).strip()
    # Remove tracking junky tokens that often differ across mirrors
    t = re.sub(r"©|all rights reserved|subscribe now|sign up|advertisement", "", t)
    return t

def make_content_hash(title: str, url: str, body: str | None) -> str:
    """
    Prefer full body (best dedupe). Fallback to title+url if body is too short.
    """
    cleaned_body = _clean_for_hash(body or "")
    base = cleaned_body if len(cleaned_body) >= 500 else f"{_clean_for_hash(title)}||{(url or '').strip().lower()}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()

# --- add these near the top with your other imports ---
from typing import Optional, Iterable, Callable

# Optional logger callback type
LogFn = Optional[Callable[[str], None]]

def log(msg: str, log_fn: LogFn = None):
    if log_fn:
        log_fn(msg)
    else:
        print(msg)

# --- wrap your existing ingest loop into a callable ---
def run_ingest(
    topics: Iterable[str],
    pages: int = 1,
    page_size: int = 50,
    section: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    log_fn: LogFn = None
) -> int:
    client = GuardianClient(GUARDIAN_API_KEY)
    conn = get_db()

    total_new = 0
    for topic in topics:
        topic = (topic or "").strip()
        if not topic:
            continue
        log(f"[topic] {topic}", log_fn)

        for item in client.search(
            query=topic,
            pages=pages,
            page_size=page_size,
            section=section,
            from_date=from_date,
            to_date=to_date
        ):
            rec = to_record(item)
            inserted = upsert_article(conn, rec)

            # NEW: tag the article with this topic
            try:
                art_id = _get_article_id_by_url(conn, rec.get("canonical_url") or "")
                if art_id:
                    map_article_to_topic(conn, art_id, topic)
            except Exception as e:
                log(f"[guardian warn] topic map failed: {e}", log_fn)

            if inserted:
                total_new += 1
                log(f" + {rec['title'][:80]}  ({rec['canonical_url']})", log_fn)

    log(f"\nDone. Inserted {total_new} new articles into {DB_PATH}.", log_fn)
    return total_new


DB_PATH = "news.db"
USER_AGENT = "news-ingest/0.1 (contact: your-email@example.com)"

# ---------- DB LAYER ----------

DDL = """
CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_domain   TEXT NOT NULL,
    source_type     TEXT NOT NULL,               -- 'api'
    canonical_url   TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    section         TEXT,
    author          TEXT,
    published_at    TEXT,                        -- ISO 8601
    fetched_at      TEXT NOT NULL,               -- ISO 8601 (UTC)
    lang            TEXT,
    summary         TEXT,
    body            TEXT,
    tags_json       TEXT,                        -- JSON array of strings
    text_hash       TEXT                         -- md5 of body (lowercased, stripped)
);
CREATE INDEX IF NOT EXISTS ix_articles_published_desc ON articles(published_at DESC);
"""

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(DDL)
    ensure_common_schema(conn)  # makes sure article_topics exists, etc.
    return conn


def _get_article_id_by_url(conn: sqlite3.Connection, canonical_url: str) -> Optional[int]:
    if not canonical_url:
        return None
    row = conn.execute(
        "SELECT id FROM articles WHERE canonical_url = ? LIMIT 1;",
        (canonical_url,)
    ).fetchone()
    return row[0] if row else None

def _safe(s):
    return s if s is not None else ""

def _compute_hash(rec: dict) -> str | None:
    # Prefer body; fall back to summary; then title
    base = _safe(rec.get("body")).strip() or _safe(rec.get("summary")).strip() or _safe(rec.get("title")).strip()
    if not base:
        return None
    return hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()

def upsert_article(conn: sqlite3.Connection, rec: dict) -> bool:
    """
    Insert new article or update an existing one (matched on canonical_url).
    Returns True if row was inserted or updated with new material; False if it was a no-op/duplicate-content.
    """
    # Normalize & prepare
    rec = {**rec}  # shallow copy so we can modify
    rec["canonical_url"] = _safe(rec.get("canonical_url")).strip()
    rec["content_hash"] = rec.get("content_hash") or _compute_hash(rec)
    tags_json = json.dumps(rec.get("tags", []), ensure_ascii=False)

    # Build parameter tuple once
    params = (
        _safe(rec.get("source_domain")),
        _safe(rec.get("source_type")),
        rec["canonical_url"],
        _safe(rec.get("title")),
        rec.get("section"),
        rec.get("author"),
        rec.get("published_at"),
        _safe(rec.get("fetched_at")),   # REQUIRED by your schema
        rec.get("lang"),
        rec.get("summary"),
        rec.get("body"),
        tags_json,
        rec.get("content_hash"),
    )

    try:
        with conn:
            # Note: requires a UNIQUE index on canonical_url (typical) and your partial UNIQUE on content_hash
            cur = conn.execute(
                """
                INSERT INTO articles (
                    source_domain, source_type, canonical_url, title, section, author,
                    published_at, fetched_at, lang, summary, body, tags_json, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_url) DO UPDATE SET
                    -- prefer keeping existing non-empty fields; only fill gaps
                    title        = COALESCE(excluded.title, title),
                    section      = COALESCE(excluded.section, section),
                    author       = COALESCE(excluded.author, author),
                    published_at = COALESCE(excluded.published_at, published_at),
                    lang         = COALESCE(excluded.lang, lang),
                    -- if current summary/body are empty, accept incoming
                    summary      = CASE WHEN TRIM(COALESCE(summary,''))='' THEN excluded.summary ELSE summary END,
                    body         = CASE WHEN TRIM(COALESCE(body,''))='' THEN excluded.body ELSE body END,
                    -- keep first non-empty tags_json
                    tags_json    = CASE WHEN tags_json IS NULL OR TRIM(tags_json)='' THEN excluded.tags_json ELSE tags_json END,
                    -- backfill hash if we didn’t have one
                    content_hash = COALESCE(content_hash, excluded.content_hash),
                    -- always refresh fetched_at
                    fetched_at   = excluded.fetched_at
                """,
                params,
            )
        # If we reached here, we either inserted or updated.
        # Return True if we changed something (sqlite3 doesn’t tell us easily); treat as True.
        return True

    except sqlite3.IntegrityError as e:
        # Likely hit the partial UNIQUE on content_hash (i.e., same text already exists elsewhere)
        msg = str(e).lower()
        if "content_hash" in msg:
            # Duplicate content; you can optionally look up the canonical row and set is_duplicate_of.
            try:
                # Which existing row has that hash?
                row = conn.execute(
                    "SELECT id FROM articles WHERE content_hash = ? LIMIT 1",
                    (rec.get("content_hash"),)
                ).fetchone()
                if row and rec["canonical_url"]:
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO articles (
                                source_domain, source_type, canonical_url, title, section, author,
                                published_at, fetched_at, lang, summary, body, tags_json, content_hash, is_duplicate_of
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(canonical_url) DO UPDATE SET
                                is_duplicate_of = COALESCE(is_duplicate_of, excluded.is_duplicate_of),
                                fetched_at = excluded.fetched_at
                            """,
                            params + (row[0],)
                        )
                # We consider this a no-op for new material
                return False
            except Exception:
                # Fall back to “no-op”
                return False
        # Reraise unexpected constraint errors
        raise


# ---------- GUARDIAN CLIENT ----------

class GuardianContentFields(BaseModel):
    byline: Optional[str] = None
    trailText: Optional[str] = None
    bodyText: Optional[str] = None

class GuardianResult(BaseModel):
    id: str
    type: str
    sectionName: Optional[str] = None
    webPublicationDate: Optional[str] = None
    webTitle: str
    webUrl: HttpUrl
    fields: Optional[GuardianContentFields] = None
    tags: Optional[List[dict]] = None

class GuardianResponse(BaseModel):
    status: str
    userTier: Optional[str] = None
    total: Optional[int] = None
    currentPage: Optional[int] = None
    pageSize: Optional[int] = None
    results: List[GuardianResult] = Field(default_factory=list)

class GuardianClient:
    BASE = "https://content.guardianapis.com/search"

    def __init__(self, api_key: str, timeout: float = 20.0):
        import httpx
        self.api_key = api_key
        self.client = httpx.Client(timeout=timeout, headers={"User-Agent": "news-ingest/0.1 (contact: you@example.com)"})

    def search(
        self,
        query: str,
        pages: int = 1,
        page_size: int = 50,
        order_by: str = "newest",
        section: str | None = None,
        from_date: str | None = None,   # <-- add this
        to_date: str | None = None,     # <-- and this
        show_fields: str = "byline,trailText,bodyText",
        show_tags: str = "keyword,contributor",
    ):
        """
        Yield results across `pages`. `from_date` / `to_date` must be 'YYYY-MM-DD' if provided.
        """
        import time
        for page in range(1, pages + 1):
            params = {
                "api-key": self.api_key,
                "q": query,
                "order-by": order_by,
                "page": page,
                "page-size": page_size,
                "show-fields": show_fields,
                "show-tags": show_tags,
            }
            if section:
                params["section"] = section
            if from_date:
                params["from-date"] = from_date
            if to_date:
                params["to-date"] = to_date

            r = self.client.get(self.BASE, params=params)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "5"))
                time.sleep(max(5, retry_after))
                r = self.client.get(self.BASE, params=params)
            r.raise_for_status()

            data = r.json().get("response", {})
            # If you use Pydantic models, convert here; otherwise yield dicts:
            for item in data.get("results", []) or []:
                yield item  # or: yield GuardianResult(**item)


# ---------- NORMALIZATION ----------

def md5_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return hashlib.md5(s.strip().lower().encode("utf-8")).hexdigest()


def md5_text(s: str | None) -> str | None:
    if not s:
        return None
    return hashlib.md5(s.strip().lower().encode("utf-8")).hexdigest()

def to_record(g) -> dict:
    """
    Normalize a Guardian search result into our DB record.
    Works with either a raw dict or a Pydantic GuardianResult.
    """
    # helpers to read attributes or dict keys
    def get(o, name, default=None):
        return o.get(name, default) if isinstance(o, dict) else getattr(o, name, default)

    fields = get(g, "fields") or {}
    def fget(name, default=None):
        return fields.get(name, default) if isinstance(fields, dict) else getattr(fields, name, default)

    raw_tags = get(g, "tags") or []
    def tag_title(t):
        if isinstance(t, dict):
            return t.get("webTitle") or t.get("id")
        return getattr(t, "webTitle", None) or getattr(t, "id", None)

    now_iso = datetime.now(timezone.utc).isoformat()

    # core fields from result
    web_url   = get(g, "webUrl")
    title     = get(g, "webTitle")
    section   = get(g, "sectionName")
    byline    = fget("byline")
    trail     = fget("trailText")
    body_txt  = fget("bodyText")    # plain text if you requested bodyText
    body_html = fget("body")        # HTML if you requested body (optional)

    # pick a body: prefer plain text if present, else HTML string
    body = body_txt or body_html or None

    # parse publication time safely
    published_iso = None
    raw_pub = get(g, "webPublicationDate")
    if raw_pub:
        try:
            published_iso = dtparse.parse(raw_pub).astimezone(timezone.utc).isoformat()
        except Exception:
            published_iso = None

    # flatten tags to strings
    tags = [t for t in (tag_title(t) for t in raw_tags) if t]

    return {
        "source_domain": "theguardian.com",
        "source_type": "api",
        "canonical_url": str(web_url) if web_url else None,
        "title": title or "",
        "section": section,
        "author": byline,
        "published_at": published_iso,
        "fetched_at": now_iso,
        "lang": "en",
        "summary": trail,
        "body": body,
        "tags": tags,
        "text_hash": md5_text(body or ""),
    }


# ---------- CLI ----------
def main(argv=None):
    import argparse, sys
    ap = argparse.ArgumentParser(description="Ingest recent Guardian articles for given topics.")
    ap.add_argument("--topics", help='Semicolon-separated list, e.g. "defense;environment;technology"')
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--section", default=None)
    ap.add_argument("--from-date", dest="from_date", default=None)
    ap.add_argument("--to-date",   dest="to_date",   default=None)

    # Parse the provided argv (lets us control behavior on import/tests)
    args = ap.parse_args(argv)

    # If called with no args (e.g., `python WebFlooder.py`), just print help and RETURN
    if not args.topics:
        ap.print_help()
        print("\nExample:\n  python WebFlooder.py --topics '\"Donald Trump\";technology' --pages 2 --page-size 50")
        return 0  # <— important: do NOT sys.exit(2)

    topics = [t.strip() for t in args.topics.split(";") if t.strip()]
    run_ingest(
        topics=topics,
        pages=args.pages,
        page_size=args.page_size,
        section=args.section,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    return 0

if __name__ == "__main__":
    # Pass through only when run directly; import won't trigger this
    raise SystemExit(main())


