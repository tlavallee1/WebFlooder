# hn_adapter.py
import json, sqlite3, urllib.request, urllib.parse, time, hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

def _domain(u):
    try: return urlparse(u).netloc.lower()
    except: return ""

def _iso(ts):
    try: return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except: return None

def _content_hash(url, title):
    h = hashlib.sha1()
    h.update((url.strip()+"\n"+title.strip()).encode("utf-8","ignore"))
    return h.hexdigest()

def ingest_hn_top(con: sqlite3.Connection, *, max_items=50, log_fn=print) -> dict:
    log = log_fn or (lambda *_: None)
    stats = {"fetched":0, "inserted":0, "duplicates":0}

    with urllib.request.urlopen("https://hacker-news.firebaseio.com/v0/topstories.json") as r:
        ids = json.loads(r.read().decode("utf-8"))

    for sid in ids[:max_items]:
        with urllib.request.urlopen(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json") as r:
            item = json.loads(r.read().decode("utf-8"))
        title = (item.get("title") or "").strip()
        link  = (item.get("url") or "").strip()
        pub   = _iso(item.get("time"))
        if not title or not link:
            continue
        stats["fetched"] += 1
        try:
            with con:
                con.execute("""
                  INSERT INTO articles (
                    source_domain, source_type, canonical_url,
                    title, section, author, published_at, fetched_at,
                    lang, summary, body, tags_json, text_hash, keyphrases_json, entities_json, content_hash, is_duplicate_of
                  ) VALUES (?, 'hn', ?, ?, NULL, NULL, ?, datetime('now'), NULL, NULL, NULL, NULL, NULL, NULL, ?, NULL)
                """, (_domain(link), link, title, pub, _content_hash(link, title)))
            stats["inserted"] += 1
            log(f"[hn] + {title} | {link}")
        except sqlite3.IntegrityError:
            stats["duplicates"] += 1

    return stats
