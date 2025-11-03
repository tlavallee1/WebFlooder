# rag_prep.py
# Vectorize chunks -> chunk_vectors with article metadata for RAG filtering.

from __future__ import annotations
import os, sys, json, time, math, hashlib, sqlite3, configparser
from typing import Iterable, List, Tuple, Optional
from datetime import datetime
from openai import OpenAI
import time

def _log(msg, log_fn=None):
    if log_fn:
        try: log_fn(msg)
        except Exception: print(msg)
    else:
        print(msg)

def _embed_batch_openai(client, model: str, inputs: list[str], *, 
                        log_fn=None, log_payload: bool=False):
    """
    Calls OpenAI embeddings and returns (vectors, meta) where:
      - vectors is a list[list[float]]
      - meta is a dict with timing and optional header info
    Tries the new 'with_raw_response' path to capture headers; falls back if unavailable.
    """
    start = time.perf_counter()
    # Optional: show the size (and optionally a peek at inputs) but not the whole text
    _log(f"[rag/openai] request model={model} n_inputs={len(inputs)}", log_fn)
    if log_payload:
        preview = [s[:120].replace("\n", " ") for s in inputs[:3]]
        _log(f"[rag/openai] sample inputs: {preview} ...", log_fn)

    vectors = None
    meta = {"headers": None, "elapsed_s": None}

    try:
        # Preferred: capture raw response (headers may contain rate-limit info)
        with_raw = getattr(getattr(client, "embeddings", None), "with_raw_response", None)
        if callable(with_raw):
            resp = with_raw.create(model=model, input=inputs)
            meta["elapsed_s"] = time.perf_counter() - start
            try:
                meta["headers"] = dict(resp.http_response.headers)
            except Exception:
                pass
            data = resp.parse()
            vectors = [row.embedding for row in data.data]
        else:
            # Fallback: normal client (no headers)
            data = client.embeddings.create(model=model, input=inputs)
            meta["elapsed_s"] = time.perf_counter() - start
            vectors = [row.embedding for row in data.data]

        _log(f"[rag/openai] ok model={model} n={len(inputs)} in {meta['elapsed_s']:.3f}s", log_fn)
        # If headers available, log a few useful ones
        if meta["headers"]:
            rl = {k:v for k,v in meta["headers"].items() 
                  if k.lower() in ("x-ratelimit-limit-requests", "x-ratelimit-remaining-requests",
                                   "x-ratelimit-limit-tokens",   "x-ratelimit-remaining-tokens",
                                   "openai-model", "request-id")}
            if rl:
                _log(f"[rag/openai] headers: {rl}", log_fn)
        return vectors, meta

    except Exception as e:
        meta["elapsed_s"] = time.perf_counter() - start
        _log(f"[rag/openai] ERROR model={model} n={len(inputs)} after {meta['elapsed_s']:.3f}s → {e}", log_fn)
        raise

def load_chat_key() -> Optional[str]:
    """
    Tries to read an OpenAI-style key from:
      1) KEYS_INI / keys.ini -> [openai] api_key=
      2) ENV: OPENAI_API_KEY
    Returns None if not found.
    """
    # env override path
    ini_path = os.getenv("KEYS_INI", "keys.ini")
    if os.path.exists(ini_path):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(ini_path, encoding="utf-8")
            if "openai" in cfg and "api_key" in cfg["openai"]:
                k = cfg["openai"]["api_key"].strip()
                if k:
                    return k
        except Exception:
            pass
    # env
    k = os.getenv("OPENAI_API_KEY", "").strip()
    return k or None

# ---------------------------
# SQLite schema helpers
# ---------------------------

def ensure_vector_schema(db_path: str = "news.db") -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS chunk_vectors(
        article_id    INTEGER NOT NULL,
        seq           INTEGER NOT NULL,
        text_hash     TEXT    NOT NULL,
        embedding     TEXT    NOT NULL,   -- JSON string or base64
        published_at  TEXT,
        topics_json   TEXT,
        source_type   TEXT,
        source_domain TEXT,
        PRIMARY KEY(article_id, seq)
      )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunk_vectors_pub ON chunk_vectors(published_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunk_vectors_src ON chunk_vectors(source_type, source_domain)")
    con.commit()
    con.close()

# ---------------------------
# Embedding backends
# ---------------------------

def _hashing_embed(text: str, dim: int = 384) -> List[float]:
    """
    Deterministic, dependency-free fallback embedding.
    Not semantic, but stable. Produces a pseudo-vector in [-1,1].
    """
    b = text.encode("utf-8", errors="ignore")
    h = hashlib.sha256(b).digest()
    # repeat hash to fill dim
    out = []
    i = 0
    while len(out) < dim:
        if i + 32 > len(h):
            h = hashlib.sha256(h).digest()  # churn
            i = 0
        chunk = h[i:i+4]
        val = int.from_bytes(chunk, "little", signed=False)
        # map to [-1, 1]
        out.append((val % 1000) / 500.0 - 1.0)
        i += 4
    return out

class Embedder:
    """
    Thin wrapper that tries OpenAI embeddings; falls back to hashing.
    """
    def __init__(self, model: str, batch_size: int = 64):
        self.model = model or "text-embedding-3-small"
        self.batch_size = max(1, int(batch_size))

        # Try environment first
        api_key = os.getenv("OPENAI_API_KEY", "").strip()

        # If not found, try keys.ini
        if not api_key:
            try:
                import configparser
                cfg = configparser.ConfigParser()
                if cfg.read("keys.ini"):
                    api_key = (cfg.get("openai", "api_key", fallback="") or "").strip()
            except Exception:
                api_key = ""

        # Initialize client if we have a key
        self._client = None
        if api_key:
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=api_key)
            except Exception:
                self._client = None

    def embed_batch(self, texts: List[str], *, log_fn=None, log_payload: bool = False) -> List[List[float]]:
        """
        Embeds a batch. Uses OpenAI if possible; else hashing fallback.
        Adds lightweight logging around OpenAI calls.
        """
        t0 = time.perf_counter()
        if self._client:
            try:
                if log_fn:
                    try:
                        msg = f"[rag/openai] model={self.model} batch={len(texts)}"
                        if log_payload and texts:
                            preview = texts[0]
                            if len(preview) > 160:
                                preview = preview[:160] + "…"
                            msg += f" | first_snippet_len={len(texts[0])} | first_snippet_preview={preview!r}"
                        log_fn(msg)
                    except Exception:
                        pass

                resp = self._client.embeddings.create(model=self.model, input=texts)
                vecs = [d.embedding for d in resp.data]

                if log_fn:
                    try:
                        dt = (time.perf_counter() - t0) * 1000.0
                        log_fn(f"[rag/openai] ok batch={len(vecs)} elapsed={dt:.1f}ms")
                    except Exception:
                        pass

                return vecs
            except Exception as e:
                if log_fn:
                    try:
                        log_fn(f"[rag/openai] error → falling back to local hashing: {e}")
                    except Exception:
                        pass
                # fall through to hashing
        # fallback (or if no client)
        if log_fn:
            try:
                dt = (time.perf_counter() - t0) * 1000.0
                log_fn(f"[rag/hash] batch={len(texts)} elapsed={dt:.1f}ms")
            except Exception:
                pass
        return [_hashing_embed(t) for t in texts]

# ---------------------------
# Helpers
# ---------------------------

def _log(msg: str, log_fn=None):
    if log_fn:
        try:
            log_fn(msg)
            return
        except Exception:
            pass
    print(msg)

def _fetch_topics(cur: sqlite3.Cursor, article_id: int) -> List[str]:
    cur.execute("SELECT topic FROM article_topics WHERE article_id = ? ORDER BY topic", (article_id,))
    return [r[0] for r in cur.fetchall()]

def _topics_filter_sql(topics_any: Optional[List[str]]) -> Tuple[str, Tuple]:
    """
    Returns SQL fragment & params to restrict to articles having ANY of the given topics.
    Uses EXISTS against article_topics for speed & simplicity.
    """
    if not topics_any:
        return "", tuple()
    # e.g., EXISTS (SELECT 1 FROM article_topics t WHERE t.article_id=a.id AND t.topic IN (?,?,?))
    placeholders = ",".join(["?"] * len(topics_any))
    frag = f"""
        AND EXISTS (
          SELECT 1 FROM article_topics t
          WHERE t.article_id = a.id
            AND t.topic IN ({placeholders})
        )
    """
    return frag, tuple(topics_any)

def _date_filter_sql(date_from: Optional[str], date_to: Optional[str]) -> Tuple[str, Tuple]:
    frags = []
    params = []
    if date_from:
        frags.append("(a.published_at IS NOT NULL AND a.published_at >= ?)")
        params.append(date_from)
    if date_to:
        frags.append("(a.published_at IS NOT NULL AND a.published_at <= ?)")
        params.append(date_to)
    if not frags:
        return "", tuple()
    return "AND " + " AND ".join(frags), tuple(params)

# ---------------------------
# Main runner
# ---------------------------

def run_rag_prep(
    db_path: str = "news.db",
    *,
    model: str = "text-embedding-3-small",
    batch_size: int = 64,
    recompute_all: bool = False,
    # filters (applied at vectorization time)
    date_from: Optional[str] = None,   # "YYYY-MM-DD"
    date_to: Optional[str] = None,     # "YYYY-MM-DD"
    topics_any: Optional[List[str]] = None,
    # housekeeping
    limit_rows: Optional[int] = None,  # max chunks to process this call (post-filter)
    log_fn = None,
    stop_cb = None
) -> dict:
    """
    Vectorizes chunks into chunk_vectors with topic+date metadata.

    - If recompute_all=False: only missing (article_id, seq) are embedded.
    - If recompute_all=True: existing rows are replaced for the filtered set.
    - Filters:
        * date_from / date_to clamp by articles.published_at (inclusive)
        * topics_any requires ANY of listed topics in article_topics
    - Stores:
        * embedding as JSON text
        * metadata columns: published_at, topics_json, source_type, source_domain

    Returns stats: {"considered": X, "embedded": Y, "replaced": Z, "skipped": W}
    """
    ensure_vector_schema(db_path)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Build candidate set from chunks JOIN articles
    topic_sql, topic_params = _topics_filter_sql(topics_any)
    date_sql, date_params   = _date_filter_sql(date_from, date_to)

    # Select all candidate chunks with their article metadata & chunk text-hash
    cur.execute(f"""
        SELECT c.article_id, c.seq, c.text, c.text_hash,
               a.published_at, a.source_type, a.source_domain
        FROM chunks c
        JOIN articles a ON a.id = c.article_id
        WHERE 1=1
        {date_sql}
        {topic_sql}
        ORDER BY a.published_at DESC NULLS LAST, c.article_id DESC, c.seq ASC
        {"LIMIT " + str(int(limit_rows)) if limit_rows else ""}
    """, (*date_params, *topic_params))
    rows = cur.fetchall()

    if not rows:
        _log("[rag] no chunks match filters (nothing to vectorize).", log_fn)
        con.close()
        return {"considered": 0, "embedded": 0, "replaced": 0, "skipped": 0}

    # If not recomputing, fetch existing keys to skip
    existing = set()
    replaced = 0
    if not recompute_all:
        cur.execute("SELECT article_id, seq FROM chunk_vectors")
        existing = {(r[0], r[1]) for r in cur.fetchall()}

    embedder = Embedder(model=model, batch_size=batch_size)
    considered = embedded = skipped = 0

    # Process in batches
    batch: List[sqlite3.Row] = []
    def flush():
        nonlocal embedded, replaced, skipped
        if not batch:
            return
        texts = [r["text"] for r in batch]
        vecs  = embedder.embed_batch(texts, log_fn=log_fn, log_payload=False)

        now   = datetime.utcnow().isoformat() + "Z"  # not stored, but could be useful later

        # Insert/Replace rows
        for r, emb in zip(batch, vecs):
            aid, seq = int(r["article_id"]), int(r["seq"])
            if not recompute_all and (aid, seq) in existing:
                skipped += 1
                continue

            # topics for this article
            topics = _fetch_topics(cur, aid)
            topics_json = json.dumps(topics, ensure_ascii=False)

            # replace per PK (article_id, seq)
            cur.execute("""
                INSERT OR REPLACE INTO chunk_vectors
                  (article_id, seq, text_hash, embedding, published_at, topics_json, source_type, source_domain)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                aid, seq, r["text_hash"], json.dumps(emb),
                r["published_at"], topics_json, r["source_type"], r["source_domain"]
            ))
            embedded += 1
        con.commit()
        batch.clear()

    # If recomputing, delete first for the filtered set (faster than UPSERT churn)
    if recompute_all:
        # delete only the subset we are going to process
        # build temp table with keys to nuke
        cur.execute("DROP TABLE IF EXISTS _tmp_vec_keys")
        cur.execute("CREATE TEMP TABLE _tmp_vec_keys(article_id INTEGER, seq INTEGER, PRIMARY KEY(article_id, seq))")
        cur.executemany(
            "INSERT OR IGNORE INTO _tmp_vec_keys(article_id, seq) VALUES(?, ?)",
            [(int(r["article_id"]), int(r["seq"])) for r in rows]
        )
        con.commit()
        cur.execute("""
            DELETE FROM chunk_vectors
            WHERE (article_id, seq) IN (SELECT article_id, seq FROM _tmp_vec_keys)
        """)
        replaced = cur.rowcount if cur.rowcount is not None else 0
        con.commit()
        _log(f"[rag] cleared {replaced} existing vector rows for recompute.", log_fn)

    for r in rows:
        if stop_cb and stop_cb():
            _log("[rag] stop requested.", log_fn)
            break
        considered += 1
        # skip existing when not recomputing
        if not recompute_all and (int(r["article_id"]), int(r["seq"])) in existing:
            skipped += 1
            continue
        batch.append(r)
        if len(batch) >= embedder.batch_size:
            flush()

    flush()
    con.close()
    _log(f"[rag] done. considered={considered} embedded={embedded} skipped={skipped} replaced={replaced}", log_fn)
    return {"considered": considered, "embedded": embedded, "skipped": skipped, "replaced": replaced}

# ---------------------------
# Quick CLI for manual tests
# ---------------------------

if __name__ == "__main__":
    # Minimal smoke test; customize as you like.
    stats = run_rag_prep(
        db_path=os.getenv("DB_PATH", "news.db"),
        model=os.getenv("EMB_MODEL", "text-embedding-3-small"),
        batch_size=int(os.getenv("EMB_BATCH", "64")),
        recompute_all=bool(int(os.getenv("RECOMPUTE_ALL", "0"))),
        date_from=os.getenv("DATE_FROM") or None,
        date_to=os.getenv("DATE_TO") or None,
        topics_any=[t.strip() for t in os.getenv("TOPICS_ANY", "").split(";") if t.strip()] or None,
        limit_rows=int(os.getenv("LIMIT_ROWS", "0")) or None,
    )
    print(stats)
