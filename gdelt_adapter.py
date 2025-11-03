# gdelt_adapter.py
import json, sqlite3
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional
import httpx
from dateutil import parser as dtparse

import time
import httpx
from datetime import datetime, timedelta
import random
import os, time, json, sqlite3, requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from json import JSONDecodeError
from db_schema import ensure_common_schema, map_article_to_topic

import hashlib
import re

from datetime import datetime, timedelta
import time, random

DB_PATH = "news.db"
UA = "webflooder-gdelt/0.2 (contact: you@example.com)"
GDELT_BASES = [
    "https://api.gdeltproject.org/api/v2/doc/doc",  # try HTTPS first
    "http://api.gdeltproject.org/api/v2/doc/doc",   # then fall back to HTTP
]

DDL = """
CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_domain   TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    external_id     TEXT,
    canonical_url   TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    section         TEXT,
    author          TEXT,
    published_at    TEXT,
    fetched_at      TEXT NOT NULL,
    lang            TEXT,
    summary         TEXT,
    body            TEXT,
    tags_json       TEXT,
    text_hash       TEXT
);
CREATE INDEX IF NOT EXISTS ix_articles_published_desc ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS ix_articles_type ON articles(source_type);
"""

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

def _requests_session():
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # Respect system proxies if set
    s.trust_env = True

    # TLS verification (use certifi, but allow override for diagnostics)
    no_verify = os.getenv("GDELT_NO_VERIFY") == "1"
    s.verify = False if no_verify else certifi.where()

    s.headers.update({"User-Agent": UA})
    return s

def _db():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(DDL)
    ensure_common_schema(con)  # <-- add this line
    return con

def _get_article_id_by_url(con: sqlite3.Connection, canonical_url: str) -> Optional[int]:
    if not canonical_url:
        return None
    row = con.execute(
        "SELECT id FROM articles WHERE canonical_url = ? LIMIT 1;",
        (canonical_url,)
    ).fetchone()
    return row[0] if row else None

def _domain(url: str) -> str:
    try:
        return httpx.URL(url).host or "unknown"
    except Exception:
        return "unknown"

def _normalize_date(s: Optional[str]) -> Optional[str]:
    """Accept 'YYYY-MM-DD' or 'YYYYMMDD' and return 'YYYYMMDD' or None."""
    if not s:
        return None
    s = s.strip()
    return s.replace("-", "") if len(s) in (8, 10) else s

def _http_client():
    # Beefy timeouts + HTTP/1.1 only
    timeout = httpx.Timeout(connect=20.0, read=60.0, write=20.0, pool=20.0)
    transport = httpx.HTTPTransport(retries=0, http2=False)  # force H1
    return httpx.Client(timeout=timeout, headers={"User-Agent": UA}, transport=transport)

def _try_parse_json(text: str) -> dict | None:
    if not text:
        return None
    t = text.lstrip("\ufeff \r\n\t")
    first_obj = t.find("{"); first_arr = t.find("[")
    cuts = [p for p in (first_obj, first_arr) if p != -1]
    if cuts and min(cuts) > 0:
        t = t[min(cuts):]
    try:
        return json.loads(t)
    except JSONDecodeError:
        return None

def _search(query: str, since: Optional[str], until: Optional[str], max_records: int, log_fn=None) -> Iterable[Dict]:
    def log(msg: str):
        if log_fn: log_fn(msg)

    params = {
        "query": query,
        "format": "json",
        "sort": "DateDesc",
        "maxrecords": str(max(1, min(int(max_records), 250))),
    }
    s, u = _normalize_date(since), _normalize_date(until)
    if s or u:
        params["daterange"] = f"{s or ''}-{u or ''}"

    attempts = 4
    backoff = 2.0
    last_err = None

    with _http_client() as cli:
        for base in GDELT_BASES:  # try HTTPS then HTTP
            for i in range(attempts):
                try:
                    r = cli.get(base, params=params)
                    if r.status_code in (429, 500, 502, 503, 504):
                        ra = r.headers.get("Retry-After")
                        delay = float(ra) if (ra and ra.isdigit()) else backoff * (2 ** i)
                        time.sleep(min(30.0, delay)); continue
                    r.raise_for_status()

                    ctype = (r.headers.get("Content-Type") or "").lower()
                    data = None
                    if "application/json" in ctype:
                        try:
                            data = r.json()
                        except JSONDecodeError:
                            pass
                    if data is None:
                        data = _try_parse_json(r.text)

                    if not isinstance(data, dict):
                        snippet = (r.text or "")[:220].replace("\n"," ").replace("\r"," ")
                        log(f"[gdelt] non-JSON reply (len={len(r.text)}): {snippet}")
                        return

                    arts = data.get("articles") or []
                    log(f"[gdelt] using {base.split(':',1)[0].upper()} | items={len(arts)}")
                    for it in arts:
                        yield it
                    return  # success

                except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout,
                        httpx.PoolTimeout, httpx.ProtocolError) as e:
                    last_err = e
                    time.sleep(min(30.0, backoff * (2 ** i)))
                except httpx.HTTPStatusError as e:
                    last_err = e; break
                except Exception as e:
                    last_err = e
                    try:
                        snippet = (r.text or "")[:220].replace("\n"," ").replace("\r"," ")
                        log(f"[gdelt] parse error: {e} | payload: {snippet}")
                    except Exception:
                        log(f"[gdelt] parse error: {e}")
                    break

    if last_err:
        raise last_err

def _to_record(it: Dict) -> Dict:
    url = it.get("url") or ""
    title = it.get("title") or ""
    # 'seendate' often like "2025-10-24 08:28:00"
    raw_dt = it.get("seendate")
    try:
        published_iso = dtparse.parse(raw_dt).astimezone(timezone.utc).isoformat() if raw_dt else None
    except Exception:
        published_iso = None

    return {
        "source_domain": _domain(url),
        "source_type": "gdelt",
        "external_id": it.get("sourceCommonName") or None,  # not unique; optional
        "canonical_url": url,
        "title": title,
        "section": None,
        "author": None,
        "published_at": published_iso,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "lang": it.get("language") or None,
        "summary": it.get("excerpt") or None,
        "body": None,  # (optional) add full-text fetch later
        "tags": [t for t in (it.get("themes") or "").split(";") if t],
        "text_hash": None,
    }

def _upsert(con: sqlite3.Connection, rec: Dict) -> bool:
    """
    Insert a row into `articles`, adapting to the table's actual columns.
    Works whether your schema has external_id or not, and tags_json vs tags.
    """
    cur = con.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(articles)")]  # actual columns

    # Normalize tags to whichever column exists
    tags_value = None
    if "tags_json" in cols:
        tags_value = json.dumps(rec.get("tags", []), ensure_ascii=False)
    elif "tags" in cols:
        # fall back to a comma-joined string if you have a legacy 'tags' TEXT column
        tags_value = ",".join(rec.get("tags", [])) if rec.get("tags") else None

    # Build a value map from rec -> DB columns
    value_map = {
        "source_domain": rec.get("source_domain"),
        "source_type":   rec.get("source_type"),
        "external_id":   rec.get("external_id"),       # may be absent in your DB; handled below
        "canonical_url": rec.get("canonical_url"),
        "title":         rec.get("title"),
        "section":       rec.get("section"),
        "author":        rec.get("author"),
        "published_at":  rec.get("published_at"),
        "fetched_at":    rec.get("fetched_at"),
        "lang":          rec.get("lang"),
        "summary":       rec.get("summary"),
        "body":          rec.get("body"),
        "tags_json":     tags_value if "tags_json" in cols else None,
        "tags":          tags_value if "tags" in cols else None,
        "text_hash":     rec.get("text_hash"),
    }

    # Preferred column order; we'll include only those that actually exist
    preferred_order = [
        "source_domain","source_type","external_id","canonical_url","title","section","author",
        "published_at","fetched_at","lang","summary","body","tags_json","tags","text_hash"
    ]
    insert_cols = [c for c in preferred_order if c in cols and value_map.get(c) is not None or c in cols]
    # For columns that exist but have no value, None is fine.
    params = [value_map.get(c) for c in insert_cols]

    placeholders = ",".join("?" for _ in insert_cols)
    sql = f"INSERT INTO articles ({', '.join(insert_cols)}) VALUES ({placeholders})"

    try:
        with con:
            cur.execute(sql, params)
        return True
    except sqlite3.IntegrityError:
        # likely UNIQUE(canonical_url) conflict
        return False

def _normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip()
    return s.replace("-", "") if len(s) in (8, 10) else s

def _parse_date_dash(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = s.replace("-", "")
    # Expect YYYYMMDD
    return datetime.strptime(s, "%Y%m%d")

def _daterange_slices(since: Optional[str], until: Optional[str], span_days: int = 3) -> list[tuple[str|None, str|None]]:
    """Return list of [ (YYYYMMDD, YYYYMMDD), ... ] covering [since, until]."""
    s = _normalize_date(since)
    u = _normalize_date(until)
    if not s and not u:
        # If no dates, just return a single “open” slice so caller can do a one-shot
        return [(None, None)]
    start = _parse_date_dash(s) if s else None
    end   = _parse_date_dash(u) if u else datetime.utcnow()
    if not start:
        # If only 'until' given, walk backwards span_days once
        start = end - timedelta(days=span_days-1)
    if start > end:
        start, end = end, start
    out = []
    cur = start
    delta = timedelta(days=span_days-1)
    while cur <= end:
        stop = min(cur + delta, end)
        out.append((cur.strftime("%Y%m%d"), stop.strftime("%Y%m%d")))
        cur = stop + timedelta(days=1)
    return out

def ingest_gdelt(
    query: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
    max_records: int = 200,
    slice_days: int = 3,
    per_slice_cap: int = 20,
    max_seconds: int = 90,              # bail after 3 minutes by default
    log_fn: Optional[callable] = None,   # e.g., GUI logger
    stop_cb: Optional[callable] = None,  # e.g., lambda: self._stop_flag
) -> Dict:
    """
    Robust ingest:
      - splits [since,until] into small slices (default 3 days)
      - fetches up to per_slice_cap per slice (default 50)
      - retries at _search level; continues on slice errors
      - obeys global time budget and optional stop callback
    """
    def log(msg: str):
        if log_fn:
            log_fn(msg)
        else:
            print(msg)

    start_ts = time.time()
    
    def time_exceeded() -> bool:
        return (time.time() - start_ts) > max_seconds

    # If no dates, you can default to last 7 days sliced:
    if not since and not until:
        until = datetime.utcnow().strftime("%Y-%m-%d")
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    con = _db()
    fetched = inserted = duplicates = 0
    remaining = max(1, int(max_records))
    slices = _daterange_slices(since, until, span_days=slice_days)

    log(f"[gdelt] slices={len(slices)} target={max_records} per_slice_cap={per_slice_cap}")

    for idx, (s, u) in enumerate(slices, 1):
        if remaining <= 0:
            log("[gdelt] reached requested max_records; stopping.")
            break
        if time_exceeded():
            log("[gdelt] global time budget exceeded; stopping.")
            break
        if stop_cb and stop_cb():
            log("[gdelt] stop requested; stopping.")
            break

        take = min(per_slice_cap, remaining)
        human_range = f"{(s or '…')}–{(u or '…')}"
        log(f"[gdelt] slice {idx}/{len(slices)} {human_range} → request up to {take}")

        slice_fetched = slice_inserted = slice_dupes = 0
        try:
            for it in _search(query=query, since=s, until=u, max_records=take, log_fn=log):
                slice_fetched += 1
                fetched += 1

                rec = None  # <-- make sure rec exists for this iteration
                try:
                    rec = _to_record(it)
                    url = rec.get("canonical_url") or ""
                    if not url:
                        continue

                    was_insert = _upsert(con, rec)
                    if was_insert:
                        slice_inserted += 1
                        inserted += 1
                    else:
                        slice_dupes += 1
                        duplicates += 1

                    # Topic map only if we have a real URL and id
                    art_id = _get_article_id_by_url(con, url)
                    if art_id:
                        map_article_to_topic(con, art_id, query)

                except Exception as e:
                    # DO NOT touch rec[...] unless rec is set
                    safe_url = (rec.get("canonical_url") if rec else "n/a")
                    log(f"[gdelt warn] iteration failed: {e} | url={safe_url}")
            remaining -= take
            log(f"[gdelt] slice {idx} done: fetched={slice_fetched}, inserted={slice_inserted}, duplicates={slice_dupes}")
        except Exception as e:
            log(f"[gdelt] slice {idx} error: {e}")

        # small jitter to avoid hammering/handshake issues
        if not (stop_cb and stop_cb()):
            time.sleep(0.6 + random.random() * 0.6)

    return {"fetched": fetched, "inserted": inserted, "duplicates": duplicates}

# Optional CLI smoke test:
if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else '"government shutdown"'
    stats = ingest_gdelt(query=q, since="2025-10-01", until="2025-10-26", max_records=200)
    print(stats)
