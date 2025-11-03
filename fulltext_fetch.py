# fulltext_fetch.py
# Drop-in helper to fetch and fill article bodies for recent GDELT rows.

from __future__ import annotations
import sqlite3, time, re, html
from datetime import datetime, timezone
from typing import Callable, Optional, List, Tuple
from contextlib import closing

# Optional deps
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

# --- BEGIN: smarter fetch helpers (add near top imports) ---------------------
import ssl
from urllib.parse import urlparse, urlunparse

# Optional: try to use trafilatura if present (it improves fetch+extraction)
try:
    import trafilatura
    _HAS_TRAFILATURA = True
except Exception:
    _HAS_TRAFILATURA = False

def _browser_headers():
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Referer": "https://www.google.com/",
    }

import re, html as _html
from urllib.parse import urlparse

# --------- HTML stripping primitives ----------
_SCRIPT_STYLE = re.compile(r'<(script|style)\b[^>]*>.*?</\1\s*>', re.I | re.S)
_TAGS = re.compile(r'<[^>]+>')
_BLOCK_BREAK_TAGS = re.compile(
    r'</?(?:p|div|section|article|header|footer|main|aside|nav|li|ul|ol|h[1-6]|br|figure|figcaption)\b[^>]*>',
    re.I
)
_WS_LINES = re.compile(r'[ \t]+\n')
_MULTI_BLANKS = re.compile(r'\n{3,}')
_MULTI_SPACE = re.compile(r'[ \t]{2,}')
_JSONLD_ARTICLE_BODY = re.compile(
    r'"articleBody"\s*:\s*"(?P<body>(?:\\.|[^"\\])*)"', re.I
)

def _word_count(s: str) -> int:
    return len(re.findall(r"[A-Za-z0-9’']+", s))

def _looks_like_non_article(url: str) -> bool:
    u = (url or '').lower()
    # hard/recurring problems: paywalls or non-article players
    if 'wsj.com/' in u:
        return True
    if 'reuters.com/' in u and '/world/' in u:
        return True
    if 'cbsnews.com/video/' in u:
        return True
    if 'yahoo.com/news/videos/' in u:
        return True
    return False

def _extract_articlebody_jsonld(html: str) -> str | None:
    """Pull JSON-LD articleBody if present; unescape \\n, \\" etc."""
    m = _JSONLD_ARTICLE_BODY.search(html or "")
    if not m:
        return None
    raw = m.group('body')
    # Unescape JSON string content
    raw = raw.encode('utf-8').decode('unicode_escape')
    raw = _html.unescape(raw)
    # Ensure paragraph breaks where JSON had \n
    raw = raw.replace('\r', '')
    raw = re.sub(r'\n{3,}', '\n\n', raw).strip()
    return raw

def _isolate_tag_block(html: str, tag: str) -> str | None:
    """Return inner HTML of the *first* matching block by tag name (regex, simple)."""
    if not html:
        return None
    # Greedy inner match; this is crude but works well for <article> / <main>
    pattern = re.compile(fr'<{tag}\b[^>]*>(?P<body>.*?)</{tag}\s*>', re.I | re.S)
    m = pattern.search(html)
    if not m:
        return None
    return m.group('body')

def _simple_html_to_text_brutal(html: str) -> str:
    """Aggressive HTML→text: drop script/style, convert block tags to newlines, strip tags, collapse whitespace."""
    if not html:
        return ""
    s = _SCRIPT_STYLE.sub('\n', html)
    s = _BLOCK_BREAK_TAGS.sub('\n', s)   # keep some paragraph boundaries
    s = _TAGS.sub('', s)                  # drop all remaining tags
    s = _html.unescape(s)
    s = s.replace('\r', '')
    s = _WS_LINES.sub('\n', s)
    s = _MULTI_BLANKS.sub('\n\n', s)     # collapse huge blank runs
    return s.strip()

def _compact_filter(text: str, min_len: int = 120, min_words: int = 25, remove_all_breaks: bool = True) -> str:
    """
    Keep only paragraphs >= min_len chars AND >= min_words words.
    Split on blank lines; also squash intraparagraph \n to space.
    Optionally remove all breaks at the end.
    """
    if not text:
        return ""
    # split on blank lines
    paras = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
    kept = []
    for p in paras:
        p1 = re.sub(r'\s*\n\s*', ' ', p).strip()  # collapse intra-paragraph newlines
        if len(p1) < min_len:
            continue
        if _word_count(p1) < min_words:
            continue
        kept.append(p1)

    if not kept:
        return ""

    out = '\n\n'.join(kept)
    out = _MULTI_SPACE.sub(' ', out).strip()

    if remove_all_breaks:
        out = re.sub(r'\s*\n\s*', ' ', out)
        out = _MULTI_SPACE.sub(' ', out).strip()

    return out

def _extract_main_text_brutal(html: str, url: str | None = None) -> str:
    """
    Best-effort main extraction with NO external deps:
      1) JSON-LD "articleBody"
      2) <article>…</article>
      3) <main>…</main>
      4) full-page fallback
    Then HTML→text and compact filter will run downstream.
    """
    # 1) JSON-LD articleBody (often cleanest)
    body = _extract_articlebody_jsonld(html)
    if body:
        return body

    # 2) Prefer article/main block slices if present
    for tag in ("article", "main"):
        block = _isolate_tag_block(html, tag)
        if block:
            return block

    # 3) Fallback to whole doc
    return html or ""


def _looks_like_non_article(url: str) -> bool:
    """Quick bailouts for known non-article/paywalled shells to save time/noise."""
    u = (url or '').lower()
    if 'cbsnews.com/video/' in u:
        return True
    if 'yahoo.com/news/videos/' in u:
        return True
    if 'wsj.com/' in u:         # hard paywall -> 401/403
        return True
    if 'reuters.com/' in u:     # often 401/forbidden via RSS path
        return True
    return False


def _http_fetch(url: str, timeout: int = 20) -> tuple[str, str]:
    """
    Try: normal URL → on 401/403 switch to AMP URL → if still failing, raise.
    Returns: (html_text, content_type)
    """
    def _try_url(u: str):
        req = Request(u, headers=_browser_headers())
        ctx = ssl.create_default_context()
        with urlopen(req, timeout=timeout, context=ctx) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            data = r.read()
        # decode bytes to str
        try:
            text = data.decode("utf-8", errors="ignore")
            if not text.strip():
                text = data.decode("latin-1", errors="ignore")
        except Exception:
            text = data.decode("latin-1", errors="ignore")
        return text, ctype

    # 1) normal URL first
    try:
        return _try_url(url)
    except Exception as e1:
        # 2) AMP fallback only for common domains that support it
        host = (urlparse(url).hostname or "").lower()
        wants_amp = any(k in host for k in [
            "thehill.com", "politico.com", "reuters.com", "bbc.com", "bbc.co.uk",
        ])
        if not wants_amp:
            raise

        try:
            parsed = urlparse(url)
            if not parsed.path.endswith("/amp"):
                amp_path = parsed.path.rstrip("/") + "/amp"
                amp_url = urlunparse(parsed._replace(path=amp_path))
            else:
                amp_url = url
            return _try_url(amp_url)
        except Exception:
            # If AMP also fails, raise the original error
            raise e1

def _fetch_and_extract(url: str, timeout: int = 20, log_fn=None) -> tuple[str, str]:
    """
    Returns (extracted_text, mime). Uses trafilatura when available; otherwise
    fetches raw HTML via _http_fetch() and runs _simple_html_to_text(...) on it.
    """
    def log(msg):
        if log_fn:
            try: log_fn(msg)
            except Exception: print(msg)
        else:
            print(msg)

    # 1) Prefer trafilatura (if installed)
    if _HAS_TRAFILATURA:
        try:
            log(f"[fulltext] trafilatura.fetch_url {url}")
            raw = trafilatura.fetch_url(
                url,
                no_ssl=False,
                timeout=timeout,
                user_agent=_browser_headers()["User-Agent"]
            )
            if raw:
                log("[fulltext] trafilatura.extract …")
                extracted = trafilatura.extract(
                    raw,
                    include_comments=False,
                    include_tables=False,
                    include_links=False,
                    favor_recall=True
                )
                if extracted and extracted.strip():
                    return extracted.strip(), "text/html"
        except Exception as te:
            log(f"[fulltext] trafilatura failed: {te}")

    # 2) Fallback: raw HTTP + your existing text extraction
    #html, ctype = _http_fetch(url, timeout=timeout)
    # assumes you already have _simple_html_to_text in this module
    #text = _simple_html_to_text(html)
    # NEW:
    text, ctype = _fetch_and_extract(url, timeout=timeout, log_fn=log)

    return text, ctype

DB_PATH_DEFAULT = "news.db"

def _default_logger(msg: str) -> None:
    print(msg)

def _coalesce_logger(log_fn: Optional[Callable[[str], None]] = None,
                     logger: Optional[Callable[[str], None]] = None) -> Callable[[str], None]:
    return log_fn or logger or _default_logger

def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat()

def _strip_tags_basic(html_text: str) -> str:
    # very basic fallback if bs4 is unavailable
    # remove scripts/styles
    html_text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html_text)
    # collapse tags to newlines
    text = re.sub(r"(?is)<[^>]+>", "\n", html_text)
    text = html.unescape(text)
    # normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def _extract_readable_text(html_text: str) -> str:
    if not html_text:
        return ""
    if BeautifulSoup is None:
        return _strip_tags_basic(html_text)

    soup = BeautifulSoup(html_text, "html.parser")

    # remove obvious non-content
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    # heuristics: prefer article/main if present
    candidates = []
    for sel in ["article", "main", "[role=main]", ".article-body", ".story-body", ".content__article-body"]:
        found = soup.select_one(sel)
        if found and found.get_text(strip=True):
            candidates.append(found.get_text("\n", strip=True))

    text = "\n\n".join([t for t in candidates if t]) if candidates else soup.get_text("\n", strip=True)
    # whitespace tidy
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text.strip()

def _http_get(url: str, timeout: float, headers: dict, retries: int, backoff: float,
              log: Callable[[str], None]) -> Tuple[Optional[int], Optional[str]]:
    if requests is None:
        log("[fulltext error] requests is not available in this environment")
        return None, None

    last_status = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
            last_status = resp.status_code
            if 200 <= resp.status_code < 300:
                return resp.status_code, resp.text
            # 429/5xx: backoff
            if resp.status_code in (429, 500, 502, 503, 504):
                log(f"[fulltext] retry {attempt}/{retries} status={resp.status_code} url={url}")
                time.sleep(backoff * attempt)
                continue
            # other non-success: give up
            log(f"[fulltext] non-OK status={resp.status_code} url={url}")
            return resp.status_code, None
        except Exception as e:
            log(f"[fulltext] request error on attempt {attempt}/{retries}: {e} url={url}")
            time.sleep(backoff * attempt)
    return last_status, None

def _fetch_and_extract(url: str, log: Callable[[str], None]) -> Optional[str]:
    # Reasonable desktop-ish UA
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
    }
    status, html_text = _http_get(url, timeout=15, headers=headers, retries=3, backoff=1.2, log=log)
    if status is None or html_text is None:
        return None
    text = _extract_readable_text(html_text)
    if not text:
        return None
    # reject super-short bodies
    if len(text) < 200:
        # sometimes pages are very short (video-only cards); still store if >= 80 chars
        if len(text) < 80:
            return None
    return text

def _select_targets(con: sqlite3.Connection, limit: int, log: Callable[[str], None]) -> List[Tuple[int, str]]:
    q = """
    SELECT id, canonical_url
    FROM articles
    WHERE source_type = 'gdelt'
      AND canonical_url IS NOT NULL
      AND TRIM(COALESCE(canonical_url,'')) <> ''
      AND (body IS NULL OR TRIM(body) = '')
    ORDER BY id DESC
    LIMIT ?
    """
    with closing(con.cursor()) as cur:
        cur.execute(q, (limit,))
        rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]

def _update_body(con: sqlite3.Connection, article_id: int, text: str) -> None:
    with closing(con.cursor()) as cur:
        cur.execute(
            """
            UPDATE articles
               SET body = ?, fetched_at = ?
             WHERE id = ?
            """,
            (text, _now_iso_z(), article_id),
        )
    con.commit()

# Compatibility alias that some callers used before
def fill_bodies(
    limit: int = 150,
    db_path: str = DB_PATH_DEFAULT,
    log_fn: Optional[Callable[[str], None]] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> int:
    return fetch_and_fill_recent_gdelt(
        db_path=db_path, limit=limit, log_fn=log_fn, logger=logger
    )
# --- Generic recent-body filler for any source_type ---------------------------
import sqlite3, time, re
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import re

def _word_count(s: str) -> int:
    return len(re.findall(r"\b\w+\b", s or ""))

def _normalize_paragraphs(text: str, min_words_per_paragraph: int = 8) -> str:
    if not text:
        return ""
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    parts = re.split(r"\n\s*\n+", t)          # split on blank lines
    keep = []
    for p in parts:
        p = re.sub(r"[ \t]*\n[ \t]*", " ", p) # collapse single line-breaks
        p = re.sub(r"[ \t]{2,}", " ", p).strip()
        if len(p.split()) >= min_words_per_paragraph:
            keep.append(p)
    return "\n\n".join(keep)


def _simple_html_to_text(html: str) -> str:
    # super-lightweight extractor: strip script/style + tags
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?is)<!--.*?-->", " ", html)
    text = re.sub(r"(?is)<[^>]+>", " ", html)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = _normalize_paragraphs(
        text,
        min_words_per_paragraph=12,
        collapse_internal_linebreaks=True
    )
    return text.strip()

def _http_fetch(url: str, timeout: int = 20) -> tuple[str, str]:
    """Return (text, mime). Raises on network errors."""
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    req = Request(url, headers={"User-Agent": ua, "Accept": "*/*"})
    with urlopen(req, timeout=timeout) as r:
        ctype = r.headers.get("Content-Type", "") or ""
        data = r.read()
    try:
        # try utf-8 first, fall back to latin-1
        text = data.decode("utf-8", errors="ignore")
        if not text.strip():
            text = data.decode("latin-1", errors="ignore")
    except Exception:
        text = data.decode("latin-1", errors="ignore")
    return text, ctype.lower()

def fetch_and_fill_recent(
    db_path: str = "news.db",
    source_type: str | None = None,    # e.g. "rss" or None for all
    limit: int = 200,
    log_fn = None,
    per_host_delay: float = 0.5,
    # final acceptance floors (after compaction)
    min_chars_to_write: int = 600,     # raise to 600 to nuke boilerplate-heavy pages
    min_words: int | None = 120,       # raise to 120 for better signal
    delete_short: bool = True,         # delete rows that fail floors
    # paragraph compactor thresholds (pre-acceptance)
    para_min_len: int = 140,
    para_min_words: int = 28,
) -> int:
    """
    Fetch bodies for the most recent articles with empty body, optionally
    restricted to a specific source_type (e.g., 'rss').
    Returns: number of rows whose body was filled.
    """
    import sqlite3, time

    def log(msg: str):
        if log_fn:
            try: log_fn(msg)
            except Exception: print(msg)
        else:
            print(msg)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    where = "WHERE (body IS NULL OR TRIM(body)='')"
    params: list[object] = []
    if source_type:
        where += " AND source_type = ?"
        params.append(source_type)

    cur.execute(f"""
        SELECT id, canonical_url
        FROM articles
        {where}
        ORDER BY id DESC
        LIMIT ?
    """, (*params, limit))
    rows = cur.fetchall()
    if not rows:
        log(f"[fulltext] nothing to fill (no empty article bodies{f' for {source_type}' if source_type else ''}).")
        con.close()
        return 0

    last_hit: dict[str, float] = {}
    filled = 0

    for row in rows:
        aid = row["id"]
        url = (row["canonical_url"] or "").strip()

        # quick bailouts for recurring paywalls/non-articles
        if _looks_like_non_article(url):
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} skip non-article/paywalled {url}")
            if delete_short:
                try:
                    cur.execute("DELETE FROM articles WHERE id = ?", (aid,))
                    con.commit()
                    log(f"[fulltext][clean]{'['+source_type+']' if source_type else ''} deleted article_id={aid}")
                except Exception as e:
                    log(f"[fulltext]{'['+source_type+']' if source_type else ''} delete failed id={aid}: {e}")
            continue

        host = urlparse(url).hostname or ""
        last = last_hit.get(host, 0.0)
        wait = per_host_delay - max(0.0, time.time() - last)
        if wait > 0:
            time.sleep(wait)

        log(f"[fulltext]{'['+source_type+']' if source_type else ''} GET {url}")

        try:
            html, ctype = _http_fetch(url, timeout=20)   # your existing fetcher
        except Exception as e:
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} fetch error: {e}")
            last_hit[host] = time.time()
            continue

        last_hit[host] = time.time()

        if "text/html" not in (ctype or "") and "application/xhtml+xml" not in (ctype or ""):
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} skip (non-HTML: {ctype})")
            continue

        # ---------  HTML → Main Slice (new)  ----------
        main_html = _extract_main_text_brutal(html, url)

        # ---------  Convert slice to text  ----------
        raw_text = _simple_html_to_text_brutal(main_html)

        # ---------  Brutal compaction / menu purge  ----------
        # Keep only chunky paragraphs; then remove ALL linebreaks to avoid “menu ladders”.
        text = _compact_filter(
            raw_text,
            min_len=para_min_len,
            min_words=para_min_words,
            remove_all_breaks=True
        )

        # ---------  Global acceptance floors  ----------
        wc = _word_count(text)
        too_few_chars = (min_chars_to_write is not None) and (len(text) < int(min_chars_to_write))
        too_few_words = (min_words is not None) and (wc < int(min_words))

        if not text or too_few_chars or too_few_words:
            why = []
            if not text:
                why.append("no text after compaction")
            if too_few_chars:
                why.append(f"{len(text)}<{min_chars_to_write} chars")
            if too_few_words:
                why.append(f"{wc}<{min_words} words")
            reason = "; ".join(why)
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} too short/noisy ({reason}); skipping id={aid}")

            if delete_short:
                try:
                    cur.execute("DELETE FROM articles WHERE id = ?", (aid,))
                    con.commit()
                    log(f"[fulltext][clean]{'['+source_type+']' if source_type else ''} deleted article_id={aid}")
                except Exception as e:
                    log(f"[fulltext]{'['+source_type+']' if source_type else ''} delete failed id={aid}: {e}")
            continue

        # ---------  Write to DB  ----------
        try:
            with con:
                cur.execute("UPDATE articles SET body = ? WHERE id = ?", (text, aid))
            filled += 1
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} OK → article_id={aid} ({len(text)} chars)")
        except Exception as e:
            log(f"[fulltext]{'['+source_type+']' if source_type else ''} DB update failed for id={aid}: {e}")

    con.close()
    return filled


# Convenience wrappers to mirror your earlier style
def fetch_and_fill_recent_rss(**kwargs) -> int:
    return fetch_and_fill_recent(source_type="rss", **kwargs)

def fetch_and_fill_recent_gdelt(**kwargs) -> int:
    return fetch_and_fill_recent(source_type="gdelt", **kwargs)
