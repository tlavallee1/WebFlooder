# rss_adapter.py
import re
import ssl
import time
import hashlib
import sqlite3
import html
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from urllib.request import Request, urlopen

# ----------------------------
# Config / helpers
# ----------------------------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 15

# Some publishers require TLS without verification errors on older systems
_SSL_CTX = ssl.create_default_context()

def _domain(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat() if dt else None

def _content_hash(url: str, title: str) -> str | None:
    try:
        h = hashlib.sha1()
        h.update((url or "").encode("utf-8", "ignore"))
        h.update(b"|")
        h.update((title or "").encode("utf-8", "ignore"))
        return h.hexdigest()
    except Exception:
        return None

def _http_get(url: str, log) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(req, timeout=HTTP_TIMEOUT, context=_SSL_CTX) as resp:
        return resp.read()

def _strip_html_to_text(html_bytes: bytes) -> str:
    """Very small HTML→text converter (no external deps)."""
    try:
        s = html_bytes.decode("utf-8", "ignore")
    except Exception:
        s = html_bytes.decode("latin-1", "ignore")

    # drop scripts/styles
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    # tags → space
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    # entities
    s = html.unescape(s)
    # collapse whitespace
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n+", "\n", s)
    s = re.sub(r" +\n", "\n", s)
    s = re.sub(r"\n +", "\n", s)
    return s.strip()

def _parse_pubdate(text: str | None) -> datetime | None:
    if not text:
        return None
    # RSS 2.0: pubDate | Atom: updated/published in ISO
    try:
        # Try RFC822
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # Try ISO 8601-ish
    try:
        # Normalize a few common variants
        t = text.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(t)
    except Exception:
        return None

@dataclass
class FeedItem:
    url: str
    title: str
    published: datetime | None
    author: str | None
    summary: str | None
    lang: str | None
    section: str | None

def _parse_feed(xml_bytes: bytes, log) -> list[FeedItem]:
    items: list[FeedItem] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log(f"[rss][error] XML parse failed: {e}")
        return items

    # Namespaces we might encounter (Atom/RSS)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
        "media": "http://search.yahoo.com/mrss/",
    }

    def _text(node, tag, default=None):
        el = node.find(tag)
        return (el.text or "").strip() if el is not None and el.text else default

    def _first_text(node, tags, default=None):
        for t in tags:
            el = node.find(t, ns) if ":" in t else node.find(t)
            if el is not None and (el.text or "").strip():
                return el.text.strip()
        return default

    # RSS 2.0 style
    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            link = _first_text(it, ["link"]) or ""
            title = _first_text(it, ["title"]) or (link or "Untitled")
            pub = _first_text(it, ["pubDate"])
            author = _first_text(it, ["author", "dc:creator"], default=None)
            summary = _first_text(it, ["description", "content:encoded"], default=None)
            lang = None   # feed-level language could be read if needed
            section = _first_text(it, ["category"], default=None)

            items.append(
                FeedItem(
                    url=link,
                    title=title,
                    published=_parse_pubdate(pub),
                    author=author,
                    summary=summary,
                    lang=lang,
                    section=section,
                )
            )
        return items

    # Atom style
    for entry in root.findall("atom:entry", ns):
        link = ""
        for l in entry.findall("atom:link", ns):
            rel = (l.attrib.get("rel") or "alternate").lower()
            if rel in ("alternate", "") and l.attrib.get("href"):
                link = l.attrib["href"]
                break
        title = _text(entry, "{http://www.w3.org/2005/Atom}title") or (link or "Untitled")
        pub = _text(entry, "{http://www.w3.org/2005/Atom}published") or _text(entry, "{http://www.w3.org/2005/Atom}updated")
        author = None
        auth = entry.find("atom:author", ns)
        if auth is not None:
            author = _text(auth, "{http://www.w3.org/2005/Atom}name")

        summary = _text(entry, "{http://www.w3.org/2005/Atom}summary") or _text(entry, "{http://www.w3.org/2005/Atom}content")
        items.append(
            FeedItem(
                url=link,
                title=title,
                published=_parse_pubdate(pub),
                author=author,
                summary=summary,
                lang=None,
                section=None,
            )
        )

    return items

# ----------------------------
# DB: safe upsert
# ----------------------------
UPSERT_SQL = """
INSERT INTO articles
    (source_domain, source_type, canonical_url, title,
     section, author, published_at, fetched_at,
     lang, summary, body, content_hash)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(canonical_url) DO UPDATE SET
    title        = COALESCE(excluded.title, articles.title),
    section      = COALESCE(excluded.section, articles.section),
    author       = COALESCE(excluded.author, articles.author),
    published_at = COALESCE(articles.published_at, excluded.published_at),
    fetched_at   = excluded.fetched_at,
    lang         = COALESCE(articles.lang, excluded.lang),
    summary      = CASE
                     WHEN (articles.summary IS NULL OR TRIM(articles.summary) = '')
                          AND (excluded.summary IS NOT NULL AND TRIM(excluded.summary) <> '')
                     THEN excluded.summary
                     ELSE articles.summary
                   END,
    body         = CASE
                     WHEN (articles.body IS NULL OR TRIM(articles.body) = '')
                          AND (excluded.body IS NOT NULL AND TRIM(excluded.body) <> '')
                     THEN excluded.body
                     ELSE articles.body
                   END,
    content_hash = COALESCE(excluded.content_hash, articles.content_hash)
"""

def ingest_rss_feed(
    con: sqlite3.Connection,
    feed_url: str,
    *,
    max_items: int = 30,
    fetch_body: bool = False,
    per_host_delay: float = 0.0,
    log_fn = print,
) -> dict:
    """
    Fetch one RSS/Atom feed and upsert entries into `articles`.

    Returns stats dict: {'fetched': N, 'inserted': M, 'duplicates': D}
    """
    log = log_fn or (lambda *a, **k: None)
    stats = {"fetched": 0, "inserted": 0, "duplicates": 0}

    try:
        log(f"[rss] fetching {feed_url}")
        xml = _http_get(feed_url, log)
    except Exception as e:
        log(f"[rss error] {feed_url} → {e}")
        return stats

    items = _parse_feed(xml, log)
    if not items:
        return stats

    # Normalize and upsert
    now_iso = datetime.now(timezone.utc).isoformat()
    seen = 0
    with con:
        for it in items:
            if max_items and seen >= max_items:
                break
            seen += 1
            url = (it.url or "").strip()
            if not url:
                continue

            title = (it.title or "").strip() or url
            pub_iso = _iso(it.published)
            summary = (it.summary or "").strip() or None
            author = (it.author or "").strip() or None
            section = (it.section or "").strip() or None
            lang = (it.lang or "").strip() or None

            body_text = None
            if fetch_body:
                # Best-effort page fetch (may still be blocked by some sites)
                try:
                    if per_host_delay > 0:
                        time.sleep(per_host_delay)
                    page = _http_get(url, log)
                    txt = _strip_html_to_text(page)
                    # Skip super-thin pages
                    if txt and len(txt) >= 200:
                        body_text = txt
                except Exception as e:
                    log(f"[rss][body][warn] body fetch failed {url}: {e}")

            try:
                con.execute(
                    UPSERT_SQL,
                    (
                        _domain(url),
                        "rss",
                        url,
                        title,
                        section,
                        author,
                        pub_iso,
                        now_iso,
                        lang,
                        summary,
                        body_text,
                        _content_hash(url, title),
                    ),
                )
                # We cannot perfectly tell insert vs update here without extra probing
                stats["inserted"] += 1
                log(f"[rss] + {title} | {url}")
            except sqlite3.IntegrityError:
                stats["duplicates"] += 1
            except Exception as e:
                log(f"[rss][warn] upsert failed for {url}: {e}")

    stats["fetched"] = min(seen, max_items) if max_items else seen
    return stats

def ingest_rss_multi(
    con: sqlite3.Connection,
    feed_urls: list[str],
    *,
    max_items_per_feed: int = 30,
    fetch_body: bool = False,
    per_host_delay: float = 0.0,
    log_fn = print,
) -> dict:
    total = {"feeds": 0, "fetched": 0, "inserted": 0, "duplicates": 0}
    log = log_fn or (lambda *a, **k: None)
    for u in feed_urls:
        u = (u or "").strip()
        if not u:
            continue
        total["feeds"] += 1
        log(f"[rss] ingest: {u}")
        s = ingest_rss_feed(
            con,
            u,
            max_items=max_items_per_feed,
            fetch_body=fetch_body,
            per_host_delay=per_host_delay,
            log_fn=log,
        )
        total["fetched"] += s.get("fetched", 0)
        total["inserted"] += s.get("inserted", 0)
        total["duplicates"] += s.get("duplicates", 0)
    return total
