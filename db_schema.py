# db_schema.py
from __future__ import annotations
import sqlite3
from typing import Iterable, Dict, Any

# --------- helpers ---------

def _fk_on(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA foreign_keys = ON;")

def _create_schema_migrations(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)

def table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone()
    return bool(row)

def column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())

def list_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}

def add_column_if_missing(con: sqlite3.Connection, table: str, column: str, col_def: str) -> None:
    if not column_exists(con, table, column):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def};")

def create_index_if_col_exists(con: sqlite3.Connection, table: str, column: str, index_name: str, desc: bool=False, unique: bool=False) -> None:
    if column_exists(con, table, column):
        order = " DESC" if desc else ""
        uniq = "UNIQUE " if unique else ""
        con.execute(f"CREATE {uniq}INDEX IF NOT EXISTS {index_name} ON {table}({column}{order});")

# --------- schema that matches your news.db dump ---------

def ensure_common_schema(con: sqlite3.Connection) -> None:
    _fk_on(con)
    _create_schema_migrations(con)

    # ARTICLES (aligns with your dump: source_domain + source_type; body, not content)
    con.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_domain   TEXT NOT NULL,
            source_type     TEXT NOT NULL,        -- e.g., 'api'
            canonical_url   TEXT NOT NULL UNIQUE,
            title           TEXT NOT NULL,
            section         TEXT,
            author          TEXT,
            published_at    TEXT,                 -- ISO 8601
            fetched_at      TEXT NOT NULL,        -- ISO 8601 (UTC)
            lang            TEXT,
            summary         TEXT,
            body            TEXT,
            tags_json       TEXT,                 -- JSON array of strings
            text_hash       TEXT,
            keyphrases_json TEXT,
            entities_json   TEXT,
            content_hash    TEXT,
            is_duplicate_of INTEGER
        );
    """)

    # Non-destructive add-columns (safe if already exist)
    for col, coldef in [
        ("source_domain",   "TEXT NOT NULL DEFAULT ''"),
        ("source_type",     "TEXT NOT NULL DEFAULT 'api'"),
        ("canonical_url",   "TEXT"),
        ("title",           "TEXT"),
        ("section",         "TEXT"),
        ("author",          "TEXT"),
        ("published_at",    "TEXT"),
        ("fetched_at",      "TEXT NOT NULL DEFAULT (datetime('now'))"),
        ("lang",            "TEXT"),
        ("summary",         "TEXT"),
        ("body",            "TEXT"),
        ("tags_json",       "TEXT"),
        ("text_hash",       "TEXT"),
        ("keyphrases_json", "TEXT"),
        ("entities_json",   "TEXT"),
        ("content_hash",    "TEXT"),
        ("is_duplicate_of", "INTEGER"),
    ]:
        add_column_if_missing(con, "articles", col, coldef)

    # Indexes present in your DB (guarded)
    create_index_if_col_exists(con, "articles", "published_at", "idx_articles_published_at")
    create_index_if_col_exists(con, "articles", "published_at", "ix_articles_published_desc", desc=True)
    create_index_if_col_exists(con, "articles", "source_type",  "ix_articles_type")

    # Partial unique index on content_hash when not null cannot be re-created via IF NOT EXISTS with WHERE in old SQLite.
    # If you already have it, this will be a no-op; if not, try create (wrapped).
    try:
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_articles_content_hash
            ON articles(content_hash)
            WHERE content_hash IS NOT NULL;
        """)
    except sqlite3.OperationalError:
        # Older SQLite (pre-3.8.0) doesn't support partial indexes; skip silently.
        pass

    # ARTICLE_TOPICS (matches your dump; no FK in original, we keep it optional)
    con.execute("""
        CREATE TABLE IF NOT EXISTS article_topics (
            article_id   INTEGER NOT NULL,
            topic        TEXT    NOT NULL,
            PRIMARY KEY (article_id, topic)
        );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS ix_article_topics_article ON article_topics(article_id);")
    con.execute("CREATE INDEX IF NOT EXISTS ix_article_topics_topic   ON article_topics(topic);")

    # CHUNKS (aligns to your dump)
    con.execute("""
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
    create_index_if_col_exists(con, "chunks", "published_at", "ix_chunks_pub", desc=True)

    # Bridge to YT (present in your DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS article_youtube_map (
            article_id  INTEGER,
            video_id    TEXT,
            PRIMARY KEY(article_id, video_id)
        );
    """)

def ensure_youtube_schema(con: sqlite3.Connection) -> None:
    with con:
        ensure_common_schema(con)

        # YOUTUBE_VIDEOS (aligns to your dump; keep names as-is)
        con.execute("""
            CREATE TABLE IF NOT EXISTS youtube_videos (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id         TEXT UNIQUE,
                channel_id       TEXT,
                channel_title    TEXT,
                title            TEXT,
                description      TEXT,
                published_at     TEXT,
                duration_secs    INTEGER,
                lang             TEXT,
                captions_type    TEXT,
                transcript_text  TEXT,
                chapters_json    TEXT,
                yt_metadata_json TEXT,
                text_hash        TEXT,
                fetched_at       TEXT
            );
        """)
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_youtube_video_id
            ON youtube_videos(video_id);
        """)

# --------- convenience (safe with your schema) ---------

def get_or_create_article(con: sqlite3.Connection, *, canonical_url: str, **fields: Any) -> int:
    """
    Idempotently get/create an article by canonical_url, updating provided fields that exist in schema.
    Returns article_id.
    """
    assert canonical_url, "canonical_url required"
    with con:
        row = con.execute("SELECT id FROM articles WHERE canonical_url = ?;", (canonical_url,)).fetchone()
        cols = list_columns(con, "articles")
        if row:
            if fields:
                updates = {k: v for k, v in fields.items() if k in cols and k != "id"}
                if updates:
                    sets = ", ".join(f"{k}=?" for k in updates)
                    con.execute(f"UPDATE articles SET {sets} WHERE canonical_url = ?;", (*updates.values(), canonical_url))
            return row[0]
        # insert new
        insert_fields: Dict[str, Any] = {"canonical_url": canonical_url}
        insert_fields.update({k: v for k, v in fields.items() if k in cols and k != "id"})
        cols_sql = ", ".join(insert_fields.keys())
        qmarks = ", ".join("?" for _ in insert_fields)
        con.execute(f"INSERT INTO articles ({cols_sql}) VALUES ({qmarks});", tuple(insert_fields.values()))
        return con.execute("SELECT last_insert_rowid();").fetchone()[0]

def map_article_to_topic(con: sqlite3.Connection, article_id: int, topic: str) -> None:
    topic = (topic or "").strip()
    if not topic or not article_id:
        return
    with con:
        con.execute(
            "INSERT OR IGNORE INTO article_topics(article_id, topic) VALUES (?, ?);",
            (article_id, topic),
        )
