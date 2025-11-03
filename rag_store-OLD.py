# rag_store.py
import os, sqlite3, json, math
from datetime import datetime, timezone
from typing import List, Dict, Any, Iterable, Tuple
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import textwrap, re

DB_PATH = "news.db"
EMB_PATH = "embeddings.faiss"
META_PATH = "embeddings_meta.json"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

def _ensure_chunks_table(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    cols = {r[1] for r in cur.fetchall()}
    if "article_id" not in cols: pass  # legacy guard; not needed here
    # Create chunks table if missing
    cur.execute("""
      CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id INTEGER NOT NULL,
        chunk_ix INTEGER NOT NULL,
        text TEXT NOT NULL,
        published_at TEXT,
        url TEXT,
        title TEXT,
        section TEXT,
        entities_json TEXT,
        keyphrases_json TEXT
      );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_chunks_pub ON chunks(published_at DESC)")
    conn.commit()

def _split_text(txt:str, target_tokens:int=180) -> List[str]:
    # crude splitter on paragraphs/sentences to ~200-token-ish chunks
    paras = re.split(r"\n\s*\n", txt.strip())
    chunks = []
    buf = ""
    for p in paras:
        if not p.strip(): continue
        if len((buf + " " + p).split()) > target_tokens:
            if buf: chunks.append(buf.strip()); buf = p
        else:
            buf = (buf + "\n\n" + p) if buf else p
    if buf: chunks.append(buf.strip())
    return chunks[:12]  # cap per-article to avoid bloat

def build_chunks(days:int=14) -> int:
    """Create/update chunks from recent articles."""
    con = sqlite3.connect(DB_PATH)
    _ensure_chunks_table(con)
    cur = con.cursor()
    cur.execute(f"""
      SELECT id, title, section, published_at, canonical_url,
             COALESCE(body,''), COALESCE(summary,''), 
             COALESCE(keyphrases_json,'[]'), COALESCE(entities_json,'[]')
      FROM articles
      WHERE published_at IS NOT NULL
        AND published_at >= datetime('now','-{days} day')
      ORDER BY published_at DESC
    """)
    rows = cur.fetchall()

    # wipe and rebuild recent window (keeps it simple)
    cur.execute("DELETE FROM chunks WHERE published_at >= datetime('now','-{} day')".format(days))

    added = 0
    for aid, title, section, pub, url, body, summary, kjs, ejs in rows:
        base = (body or summary).strip()
        if not base: continue
        parts = _split_text(base, target_tokens=180)
        for i, txt in enumerate(parts):
            cur.execute("""INSERT INTO chunks
               (article_id, chunk_ix, text, published_at, url, title, section, entities_json, keyphrases_json)
               VALUES (?,?,?,?,?,?,?,?,?)""",
               (aid, i, txt, pub, url, title or "", section or "", ejs, kjs))
            added += 1
    con.commit(); con.close()
    return added

def _load_corpus() -> Tuple[List[str], List[Dict[str,Any]]]:
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""SELECT id, text, url, title, published_at, section, entities_json, keyphrases_json
                   FROM chunks ORDER BY published_at DESC""")
    items = cur.fetchall(); con.close()
    texts, meta = [], []
    for cid, text, url, title, pub, section, ejs, kjs in items:
        texts.append(text)
        meta.append({
            "chunk_id": cid, "url": url, "title": title, "published_at": pub,
            "section": section, "entities": json.loads(ejs or "[]"), "keyphrases": json.loads(kjs or "[]"),
            "text_preview": textwrap.shorten(text, 180)
        })
    return texts, meta

def build_vector_index():
    texts, meta = _load_corpus()
    if not texts:
        raise RuntimeError("No chunks available. Run build_chunks() first.")
    model = SentenceTransformer(MODEL_NAME)
    emb = model.encode(texts, normalize_embeddings=True, convert_to_numpy=True)
    index = faiss.IndexFlatIP(emb.shape[1])  # cosine via dot with normalized vectors
    index.add(emb)
    faiss.write_index(index, EMB_PATH)
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "model": MODEL_NAME}, f, ensure_ascii=False, indent=2)
    return len(texts)

def search(query:str, k:int=12) -> List[Dict[str,Any]]:
    if not (os.path.exists(EMB_PATH) and os.path.exists(META_PATH)):
        raise RuntimeError("Vector index missing. Build it with build_vector_index().")
    index = faiss.read_index(EMB_PATH)
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)["meta"]
    model = SentenceTransformer(MODEL_NAME)
    qv = model.encode([query], normalize_embeddings=True, convert_to_numpy=True)
    D, I = index.search(qv, k)
    out = []
    for score, idx in zip(D[0], I[0]):
        if idx < 0: continue
        m = dict(meta[idx])
        m["score"] = float(score)
        out.append(m)
    return out
