# content_prep.py
# Minimal, dependency-free prep: cleans body → body_clean, summaries, quotes, facts, chunks.

from __future__ import annotations
import sqlite3, hashlib, json, re, time, datetime as _dt
from urllib.parse import urlparse

# ---------------------------
# Text utilities (simple, robust)
# ---------------------------

_WS_RE = re.compile(r"[ \t]+")
_BR_RE = re.compile(r"(?:\r\n|\r|\n)")
_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ENTITY_RE = re.compile(r"&(#\d+|#x[0-9A-Fa-f]+|\w+);")
_MULTIBR_RE = re.compile(r"(?:\n\s*){2,}")

def _unhtml(s: str) -> str:
    # extremely small entity subset to stay dependency-free
    entities = {
        "nbsp":" ", "amp":"&", "lt":"<", "gt":">", "quot":'"', "apos":"'",
    }
    def rep(m):
        ent = m.group(1)
        if ent.startswith("#x"):
            try: return chr(int(ent[2:], 16))
            except: return " "
        if ent.startswith("#"):
            try: return chr(int(ent[1:], 10))
            except: return " "
        return entities.get(ent, " ")
    return _HTML_ENTITY_RE.sub(rep, s)

def _simple_html_to_text(html: str) -> str:
    # strip tags, collapse whitespace; this is intentionally “dumb but stable”
    html = _unhtml(html)
    html = _TAG_RE.sub(" ", html)
    html = _BR_RE.sub("\n", html)
    html = _WS_RE.sub(" ", html)
    html = _MULTIBR_RE.sub("\n\n", html)
    return html.strip()

def _normalize_paragraphs(text: str, min_words_per_paragraph: int = 8) -> str:
    # Keep only paragraphs with >= N words; collapse extra blank lines.
    paras, keep = [], []
    for raw in re.split(r"\n{2,}", text):
        p = raw.strip()
        if not p: 
            continue
        wc = len(p.split())
        if wc >= max(1, int(min_words_per_paragraph)):
            keep.append(p)
    return "\n\n".join(keep)

def _compact_filter(text: str, min_len: int = 100, remove_all_breaks: bool = False) -> str:
    # Remove lines shorter than min_len (menu/crumb trash), then compact spacing.
    out = []
    for line in _BR_RE.split(text):
        L = line.strip()
        if len(L) >= min_len:
            out.append(L)
    joined = (" ".join(out) if remove_all_breaks else "\n".join(out)).strip()
    joined = _WS_RE.sub(" ", joined)
    if not remove_all_breaks:
        # shrink multi-blank lines produced by joins
        joined = _MULTIBR_RE.sub("\n\n", joined)
    return joined

def _word_count(s: str) -> int:
    return len([w for w in re.split(r"\s+", s.strip()) if w])

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

# ---------------------------
# Lightweight summarizers/extractors
# ---------------------------

_SENT_SPLIT_RE = re.compile(r"(?<=[\.!?])\s+")
_QUOTE_RE = re.compile(r"[“\"]([^”\"]{40,400})[”\"]")  # 40–400 chars inside quotes

def _lead_k(text: str, max_chars: int) -> str:
    if not text: return ""
    if len(text) <= max_chars: return text
    # sentence-aware trim
    acc, out = 0, []
    for sent in _SENT_SPLIT_RE.split(text):
        s = sent.strip()
        if not s: 
            continue
        if acc + len(s) + 1 > max_chars:
            break
        out.append(s)
        acc += len(s) + 1
        if acc >= max_chars:
            break
    if not out:
        return text[:max_chars].rsplit(" ", 1)[0] + "…"
    return " ".join(out)

def _bullets(text: str, k: int = 6) -> list[str]:
    # naive key points: pick the k most “dense” sentences (longer but not too long)
    cands = [s.strip() for s in _SENT_SPLIT_RE.split(text) if 50 <= len(s) <= 280]
    cands = sorted(cands, key=lambda s: (-len(s), s))[:k]
    return cands

def _quotes(text: str, k: int = 8) -> list[str]:
    qs = [m.group(1).strip() for m in _QUOTE_RE.finditer(text)]
    # also capture lines starting with em-dash quote style
    qs = qs[:k]
    return qs

def _facts(text: str, url: str, k: int = 8) -> list[dict]:
    # very light “fact” heuristic = declarative lines with numbers/percents/dates
    facts = []
    for sent in _SENT_SPLIT_RE.split(text):
        s = sent.strip()
        if not s: continue
        if re.search(r"\b\d{4}\b", s) or re.search(r"\b\d+(?:\.\d+)?%?\b", s):
            facts.append({"sentence": s, "cited_url": url})
        if len(facts) >= k: break
    return facts

# ---------------------------
# Schema helpers
# ---------------------------

DDL = [
    # Columns (added lazily if missing)
    ("ALTER TABLE articles ADD COLUMN body_clean TEXT", "body_clean"),
    ("ALTER TABLE articles ADD COLUMN body_clean_hash TEXT", "body_clean_hash"),
    ("ALTER TABLE articles ADD COLUMN word_count INTEGER", "word_count"),
    ("ALTER TABLE articles ADD COLUMN summary_256 TEXT", "summary_256"),
    ("ALTER TABLE articles ADD COLUMN summary_1k TEXT", "summary_1k"),
    ("ALTER TABLE articles ADD COLUMN key_points_json TEXT", "key_points_json"),
]

CREATE = [
    """
    CREATE TABLE IF NOT EXISTS chunks(
        article_id INTEGER NOT NULL,
        seq        INTEGER NOT NULL,
        text       TEXT    NOT NULL,
        text_hash  TEXT    NOT NULL,
        created_at TEXT    NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY(article_id, seq)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS facts(
        article_id INTEGER NOT NULL,
        sentence   TEXT    NOT NULL,
        cited_url  TEXT,
        created_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quotes(
        article_id INTEGER NOT NULL,
        quote      TEXT    NOT NULL,
        created_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )
    """,
    # convenience view of prepared rows
    """
    CREATE VIEW IF NOT EXISTS v_ready_articles AS
    SELECT id, source_type, source_domain, canonical_url, title, section, author,
           published_at, fetched_at, lang, body_clean, word_count,
           summary_256, summary_1k, key_points_json
    FROM articles
    WHERE body_clean IS NOT NULL AND TRIM(body_clean) <> ''
    """
]

def _column_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def _table_cols(cur: sqlite3.Cursor, table: str) -> list[str]:
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]
    except Exception:
        return []

def _recreate_chunks_if_needed(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    want_cols = ["article_id", "seq", "text", "text_hash", "created_at"]
    cols = _table_cols(cur, "chunks")
    if not cols:
        # Table doesn't exist; CREATE via normal path below.
        return
    # If any required column missing -> migrate
    if any(c not in cols for c in want_cols):
        cur.execute("ALTER TABLE chunks RENAME TO chunks_old")
        # Create fresh schema
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks(
                article_id INTEGER NOT NULL,
                seq        INTEGER NOT NULL,
                text       TEXT    NOT NULL,
                text_hash  TEXT    NOT NULL,
                created_at TEXT    NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY(article_id, seq)
            )
        """)
        # Try a best-effort copy if minimal columns exist
        old_cols = _table_cols(cur, "chunks_old")
        can_copy_min = ("article_id" in old_cols) and ("text" in old_cols)
        if can_copy_min:
            # Copy with seq=1, compute text_hash; created_at defaults
            cur.execute("SELECT article_id, text FROM chunks_old")
            rows = cur.fetchall()
            for (aid, txt) in rows:
                try:
                    cur.execute(
                        "INSERT OR IGNORE INTO chunks(article_id, seq, text, text_hash) VALUES (?, ?, ?, ?)",
                        (aid, 1, txt or "", _sha1(txt or "")),
                    )
                except Exception:
                    pass
        # Drop the old table
        try:
            cur.execute("DROP TABLE IF EXISTS chunks_old")
        except Exception:
            pass
        con.commit()

def ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    # 1) Columns for articles
    for sql, col in DDL:
        if not _column_exists(cur, "articles", col):
            try:
                cur.execute(sql)
            except Exception:
                pass

    # 2) Migrate chunks if an old/incompatible version exists
    _recreate_chunks_if_needed(con)

    # 3) Create helper tables / views if missing
    for sql in CREATE:
        cur.execute(sql)

    con.commit()


# ---------------------------
# Main prep runner
# ---------------------------

def run_content_prep(
    db_path: str = "news.db",
    *,
    per_host_delay: float = 0.0,        # not used here but kept for symmetry
    min_chars: int = 200,
    min_words: int = 100,
    min_words_per_paragraph: int = 8,
    chunk_chars: int = 1500,            # ~1k–2k char chunks are RAG-friendly
    max_quotes: int = 8,
    max_facts: int = 8,
    delete_short: bool = False,
    limit_rows: int | None = None,      # limit # of rows processed this pass
    log_fn = None,
    stop_cb = None
) -> dict:
    """
    Cleans and enriches articles in-place.
    - Fills: body_clean, body_clean_hash, word_count, summary_256, summary_1k, key_points_json
    - Populates: chunks, quotes, facts
    - Optionally deletes too-short rows.
    Returns stats.
    """
    def log(msg: str):
        if log_fn:
            try: log_fn(msg)
            except Exception: print(msg)
        else:
            print(msg)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_schema(con)
    cur = con.cursor()

    where = "WHERE (body IS NOT NULL AND TRIM(body) <> '')"
    # prefer rows that haven’t been cleaned yet
    order = "ORDER BY (body_clean IS NULL OR TRIM(body_clean)='') DESC, id DESC"
    limit_sql = f"LIMIT {int(limit_rows)}" if limit_rows else ""

    cur.execute(f"""
        SELECT id, canonical_url, body
        FROM articles
        {where}
        {order}
        {limit_sql}
    """)
    rows = cur.fetchall()
    if not rows:
        log("[prep] nothing to process.")
        con.close()
        return {"processed": 0, "updated": 0, "deleted": 0}

    processed = updated = deleted = 0

    for r in rows:
        if stop_cb and stop_cb():
            log("[prep] stop requested.")
            break

        aid = r["id"]
        url = r["canonical_url"]
        raw = r["body"] or ""
        processed += 1

        # 1) clean
        text = _simple_html_to_text(raw)
        text = _normalize_paragraphs(text, min_words_per_paragraph=min_words_per_paragraph)
        # remove short lines/menus; then collapse to paragraphs (but keep double breaks)
        text = _compact_filter(text, min_len=100, remove_all_breaks=False)

        wc = _word_count(text)
        too_short = (len(text) < max(0, int(min_chars))) or (wc < max(0, int(min_words)))

        if too_short:
            if delete_short:
                with con:
                    cur.execute("DELETE FROM articles WHERE id = ?", (aid,))
                    cur.execute("DELETE FROM chunks WHERE article_id = ?", (aid,))
                    cur.execute("DELETE FROM facts  WHERE article_id = ?", (aid,))
                    cur.execute("DELETE FROM quotes WHERE article_id = ?", (aid,))
                deleted += 1
                log(f"[prep] deleted short id={aid} ({len(text)} chars; {wc} words)")
            else:
                log(f"[prep] skip (short) id={aid} ({len(text)} chars; {wc} words)")
            continue

        # 2) summaries & points
        summary_256 = _lead_k(text, 256)
        summary_1k  = _lead_k(text, 1000)
        points      = _bullets(text, k=6)
        points_json = json.dumps(points, ensure_ascii=False)

        # 3) chunks
        # clear & re-write chunks for this article for determinism
        with con:
            cur.execute("DELETE FROM chunks WHERE article_id = ?", (aid,))
        seq = 0
        i = 0
        n = len(text)
        while i < n:
            # try to cut at a paragraph boundary near chunk_chars
            j = min(n, i + chunk_chars)
            if j < n:
                back = text.rfind("\n\n", i, j)
                if back != -1 and (j - back) < 400:
                    j = back
            piece = text[i:j].strip()
            i = j
            if not piece:
                continue
            seq += 1
            with con:
                cur.execute(
                    "INSERT OR REPLACE INTO chunks(article_id, seq, text, text_hash) VALUES (?, ?, ?, ?)",
                    (aid, seq, piece, _sha1(piece))
                )

        # 4) quotes/facts
        with con:
            cur.execute("DELETE FROM quotes WHERE article_id = ?", (aid,))
            cur.execute("DELETE FROM facts  WHERE article_id = ?", (aid,))
        qs = _quotes(text, k=max_quotes)
        fs = _facts(text, url, k=max_facts)
        if qs:
            with con:
                cur.executemany(
                    "INSERT INTO quotes(article_id, quote) VALUES (?, ?)",
                    [(aid, q) for q in qs]
                )
        if fs:
            with con:
                cur.executemany(
                    "INSERT INTO facts(article_id, sentence, cited_url) VALUES (?, ?, ?)",
                    [(aid, f["sentence"], f.get("cited_url")) for f in fs]
                )

        # 5) update article
        with con:
            cur.execute("""
                UPDATE articles
                SET body_clean = ?, body_clean_hash = ?, word_count = ?,
                    summary_256 = ?, summary_1k = ?, key_points_json = ?
                WHERE id = ?
            """, (text, _sha1(text), wc, summary_256, summary_1k, points_json, aid))
        updated += 1
        log(f"[prep] OK id={aid} wc={wc} chunks={seq} quotes={len(qs)} facts={len(fs)}")

    con.close()
    return {"processed": processed, "updated": updated, "deleted": deleted}


if __name__ == "__main__":
    # quick manual run
    stats = run_content_prep()
    print("[prep] done:", stats)
