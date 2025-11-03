# migrations_add_hash_and_topics.py
import sqlite3

DB = "news.db"

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1].lower() == col.lower() for r in cur.fetchall())

con = sqlite3.connect(DB)
cur = con.cursor()

# Be safe on columns: only add if missing
if not column_exists(cur, "articles", "content_hash"):
    cur.execute("ALTER TABLE articles ADD COLUMN content_hash TEXT")

if not column_exists(cur, "articles", "is_duplicate_of"):
    cur.execute("ALTER TABLE articles ADD COLUMN is_duplicate_of INTEGER")

# Create mapping table + indexes in one go
cur.executescript("""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS article_topics (
  article_id   INTEGER NOT NULL,
  topic        TEXT    NOT NULL,
  PRIMARY KEY (article_id, topic)
);

CREATE INDEX IF NOT EXISTS ix_article_topics_topic
  ON article_topics(topic);

-- Partial unique index: keeps hashes unique when present
CREATE UNIQUE INDEX IF NOT EXISTS ux_articles_content_hash
  ON articles(content_hash)
  WHERE content_hash IS NOT NULL;
""")

con.commit()
con.close()
print("Migration complete.")
