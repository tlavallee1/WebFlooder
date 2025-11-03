# export_ready.py  — drop-in exporter for prepared + vectorized articles

import os
import sqlite3
import csv
import json
import re
from datetime import datetime

DB_PATH = "news.db"

# Output roots (a timestamped folder so each run is separate)
STAMP = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
EXPORT_ROOT = os.path.join("exports", STAMP)
CSV_PATH = os.path.join(EXPORT_ROOT, "ready_articles.csv")
TXT_PATH = os.path.join(EXPORT_ROOT, "ready_articles_readable.txt")
ART_DIR = os.path.join(EXPORT_ROOT, "articles")  # one clean .txt per article

# NEW: vectorization outputs
VEC_SUMMARY_CSV = os.path.join(EXPORT_ROOT, "vectors_summary.csv")
VEC_READABLE_TXT = os.path.join(EXPORT_ROOT, "vectors_readable.txt")

CHUNK_PREVIEW_LEN = 600   # chars from first chunk to preview in the TXT
VEC_PREVIEW_COUNT = 5     # how many recent vectorized articles to show, in detail
VEC_SAMPLE_PER_ART = 5    # how many chunks per article to preview in vectors_readable.txt
EMB_PREVIEW_ELEMS = 8     # how many embedding numbers to preview

os.makedirs(EXPORT_ROOT, exist_ok=True)
os.makedirs(ART_DIR, exist_ok=True)

def coalesce(x, fallback=""):
    return fallback if x is None else x

def pretty_json(s):
    if not s:
        return ""
    try:
        return json.dumps(json.loads(s), indent=2, ensure_ascii=False)
    except Exception:
        return str(s)

def slugify(s: str, max_len: int = 60) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:max_len] or "untitled"

def fetch_all(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except sqlite3.OperationalError:
        # table/view may not exist yet — return empty
        return []

def table_exists(cur, name: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1", (name,))
    return cur.fetchone() is not None

# Connect
con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

# -------- 1) CSV of ready articles (uses the view created by content_prep) --------
ready_cols = [
    "id", "source_type", "source_domain", "canonical_url", "title", "section", "author",
    "published_at", "fetched_at", "lang",
    "word_count", "summary_256", "summary_1k", "key_points_json"
]
ready_rows = fetch_all(cur, f"""
    SELECT {", ".join(ready_cols)}
    FROM v_ready_articles
    ORDER BY (published_at IS NULL), published_at DESC, id DESC
""")

with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(ready_cols)
    for r in ready_rows:
        w.writerow([coalesce(r[c]) for c in ready_cols])

# -------- 2) Rich readable TXT with details --------
sep = "\n" + ("-" * 100) + "\n\n"
with open(TXT_PATH, "w", encoding="utf-8") as f:
    f.write(f"# Prepared Articles (body_clean present)\nGenerated: {datetime.utcnow().isoformat()}Z\nTotal: {len(ready_rows)}\n")
    f.write(sep)

    for r in ready_rows:
        aid = r["id"]
        title = coalesce(r["title"])
        url = coalesce(r["canonical_url"])

        # Pull clean text and extras
        cur.execute("SELECT body_clean FROM articles WHERE id = ?", (aid,))
        bc_row = cur.fetchone()
        body_clean = (bc_row["body_clean"] if bc_row and bc_row["body_clean"] else "").strip()

        # Topics
        topics = [t["topic"] for t in fetch_all(cur,
                    "SELECT topic FROM article_topics WHERE article_id = ? ORDER BY topic", (aid,))]

        # YouTube mapping
        yt_ids = [v["video_id"] for v in fetch_all(cur,
                    "SELECT video_id FROM article_youtube_map WHERE article_id = ? ORDER BY video_id", (aid,))]

        # Chunks / first-chunk preview
        chunks = fetch_all(cur,
                    "SELECT seq, text FROM chunks WHERE article_id = ? ORDER BY seq", (aid,))
        chunk_count = len(chunks)
        first_chunk = (chunks[0]["text"].strip() if chunks else "")
        first_chunk_preview = (first_chunk[:CHUNK_PREVIEW_LEN] + ("…" if len(first_chunk) > CHUNK_PREVIEW_LEN else ""))

        # Quotes
        quotes = [q["quote"] for q in fetch_all(cur,
                    "SELECT quote FROM quotes WHERE article_id = ? LIMIT 8", (aid,))]
        # Facts
        facts = [{"sentence": x["sentence"], "cited_url": x["cited_url"]} for x in fetch_all(cur,
                    "SELECT sentence, cited_url FROM facts WHERE article_id = ? LIMIT 12", (aid,))]

        # Header
        f.write(f"[{aid}] {title}\n")
        f.write(f"URL: {url}\n")
        f.write(f"Source: {coalesce(r['source_type'])} | Domain: {coalesce(r['source_domain'])} | Lang: {coalesce(r['lang'])}\n")
        f.write(f"Published: {coalesce(r['published_at'])} | Fetched: {coalesce(r['fetched_at'])}\n")
        f.write(f"Section: {coalesce(r['section'])} | Author: {coalesce(r['author'])}\n")
        f.write(f"Word count: {coalesce(r['word_count'], 0)}\n")
        f.write(f"Topics: {', '.join(topics) if topics else '(none)'}\n")
        f.write(f"YouTube IDs: {', '.join(yt_ids) if yt_ids else '(none)'}\n")
        f.write(f"Chunks: {chunk_count}\n")

        # Summaries / key points
        f.write("\n-- Summaries --\n")
        f.write(coalesce(r["summary_256"]) + "\n\n")
        f.write(coalesce(r["summary_1k"]) + "\n\n")

        f.write("-- Key points --\n")
        kp = []
        try:
            kp = json.loads(r["key_points_json"]) if r["key_points_json"] else []
        except Exception:
            pass
        if kp:
            for i, p in enumerate(kp, 1):
                f.write(f"  {i}. {p}\n")
        else:
            f.write("  (none)\n")

        # Quotes
        f.write("\n-- Quotes (up to 8) --\n")
        if quotes:
            for q in quotes:
                f.write(f'  “{q}”\n')
        else:
            f.write("  (none)\n")

        # Facts
        f.write("\n-- Facts (up to 12) --\n")
        if facts:
            for i, d in enumerate(facts, 1):
                f.write(f"  {i}. {d['sentence']}  [src: {d.get('cited_url') or url}]\n")
        else:
            f.write("  (none)\n")

        # First chunk preview (what RAG will see)
        f.write("\n-- First chunk preview --\n")
        if first_chunk_preview:
            f.write(first_chunk_preview + "\n")
        else:
            f.write("(no chunks)\n")

        # Write the full clean body to its own file
        art_name = f"{aid}_{slugify(title)}.txt"
        art_path = os.path.join(ART_DIR, art_name)
        with open(art_path, "w", encoding="utf-8") as af:
            af.write(body_clean)

        f.write(f"\n[Saved clean body] {art_path}\n")
        f.write(sep)

# -------- 3) Vectorization exports (if present) --------
has_chunk_vectors = table_exists(cur, "chunk_vectors")
has_chunks = table_exists(cur, "chunks")
has_articles = table_exists(cur, "articles")

if has_chunk_vectors and has_articles:
    # 3a) CSV summary: per-article vector coverage + basics
    # n_vecs = number of chunk vectors for that article
    vec_rows = fetch_all(cur, """
        SELECT
            a.id AS article_id,
            a.title,
            a.source_domain,
            a.source_type,
            a.published_at,
            COUNT(v.seq) AS n_vecs
        FROM articles a
        JOIN chunk_vectors v ON v.article_id = a.id
        GROUP BY a.id, a.title, a.source_domain, a.source_type, a.published_at
        ORDER BY (a.published_at IS NULL), a.published_at DESC, a.id DESC
    """)
    with open(VEC_SUMMARY_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["article_id","title","source_domain","source_type","published_at","n_vectors"])
        for r in vec_rows:
            w.writerow([
                r["article_id"], coalesce(r["title"]), coalesce(r["source_domain"]),
                coalesce(r["source_type"]), coalesce(r["published_at"]), r["n_vecs"]
            ])

    # 3b) Readable TXT: recent vectorized articles, with per-chunk previews + embedding dims
    with open(VEC_READABLE_TXT, "w", encoding="utf-8") as f:
        f.write(f"# Vectorized Chunks (preview)\nGenerated: {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Total vectorized articles: {len(vec_rows)}\n")
        f.write(sep)

        # Pick the N most recent by published_at/id
        art_ids = [r["article_id"] for r in vec_rows[:VEC_PREVIEW_COUNT]]
        for i, aid in enumerate(art_ids, 1):
            # Article header
            cur.execute("""
                SELECT id, title, canonical_url, source_domain, source_type, published_at
                FROM articles WHERE id = ?
            """, (aid,))
            a = cur.fetchone()
            if not a:
                continue

            f.write(f"[{i}] Article {a['id']}: {coalesce(a['title'])}\n")
            f.write(f"URL: {coalesce(a['canonical_url'])}\n")
            f.write(f"Source: {coalesce(a['source_type'])} | Domain: {coalesce(a['source_domain'])}\n")
            f.write(f"Published: {coalesce(a['published_at'])}\n")

            # Pull up to VEC_SAMPLE_PER_ART vectors with chunk text (if chunks table exists)
            if has_chunks:
                rows = fetch_all(cur, """
                    SELECT v.article_id, v.seq, v.embedding, v.text_hash,
                           v.published_at, v.topics_json, v.source_domain,
                           c.text AS chunk_text
                    FROM chunk_vectors v
                    JOIN chunks c ON c.article_id = v.article_id AND c.seq = v.seq
                    WHERE v.article_id = ?
                    ORDER BY v.seq
                    LIMIT ?
                """, (aid, VEC_SAMPLE_PER_ART))
            else:
                rows = fetch_all(cur, """
                    SELECT v.article_id, v.seq, v.embedding, v.text_hash,
                           v.published_at, v.topics_json, v.source_domain
                    FROM chunk_vectors v
                    WHERE v.article_id = ?
                    ORDER BY v.seq
                    LIMIT ?
                """, (aid, VEC_SAMPLE_PER_ART))

            if not rows:
                f.write("  (no vector rows found)\n")
                f.write(sep)
                continue

            # Optional: topic list from chunk_vectors.topics_json (best-effort)
            topic_set = set()
            for r in rows:
                tj = r["topics_json"]
                if tj:
                    try:
                        for t in json.loads(tj):
                            topic_set.add(str(t))
                    except Exception:
                        # fallback heuristic
                        if isinstance(tj, str):
                            if "Donald Trump" in tj: topic_set.add('"Donald Trump"')
                            if "President Trump" in tj: topic_set.add('"President Trump"')

            if topic_set:
                f.write(f"Topics (sampled): {', '.join(sorted(topic_set))}\n")

            # Per-chunk preview with embedding dims
            for r in rows:
                f.write("\n-- Chunk #{:d} --\n".format(r["seq"]))
                # Embedding preview/dimension
                dim = 0
                preview_vals = ""
                emb_txt = r["embedding"]
                if emb_txt:
                    try:
                        arr = json.loads(emb_txt)
                        if isinstance(arr, list):
                            dim = len(arr)
                            preview_vals = ", ".join(f"{x:.4f}" for x in arr[:EMB_PREVIEW_ELEMS])
                    except Exception:
                        pass
                f.write(f"Embedding dim: {dim}  |  sample: [{preview_vals}]\n")

                # Chunk text preview
                chunk_text = (r["chunk_text"] if ("chunk_text" in r.keys() and r["chunk_text"] is not None) else "")
                if chunk_text:
                    preview = chunk_text.strip()[:CHUNK_PREVIEW_LEN]
                    if len(chunk_text.strip()) > CHUNK_PREVIEW_LEN:
                        preview += "…"
                    f.write(preview + "\n")
                else:
                    f.write("(chunk text unavailable; chunks table missing or join failed)\n")

            f.write(sep)

# Close DB
con.close()

print(f"Wrote: {CSV_PATH}")
print(f"Wrote: {TXT_PATH}")
print(f"Saved clean bodies in: {ART_DIR}")
if has_chunk_vectors and has_articles:
    print(f"Wrote: {VEC_SUMMARY_CSV}")
    print(f"Wrote: {VEC_READABLE_TXT}")
else:
    print("Vectorization tables not found — skipped vector exports.")
