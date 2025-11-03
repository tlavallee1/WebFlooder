# prominence.py
import math, sqlite3, datetime as dt

DB = "news.db"

def compute_prominence(days_back=14, decay_base=0.85, min_articles=2, limit=10):
    """
    Scores each 'topic' (the user-entered search terms you already track in article_topics)
    with: prominence = volume * recency_sum * (1 + 0.2*log1p(domain_diversity))
    """
    now = dt.datetime.utcnow()
    since = (now - dt.timedelta(days=days_back)).isoformat(timespec="seconds") + "Z"

    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
      SELECT at.topic,
             a.source_domain,
             a.published_at
      FROM article_topics at
      JOIN articles a ON a.id = at.article_id
      WHERE a.published_at IS NOT NULL
        AND a.published_at >= ?
    """, (since,))
    rows = cur.fetchall()
    con.close()

    # Aggregate
    by_topic = {}
    for topic, domain, published_at in rows:
        t = by_topic.setdefault(topic, {"count":0, "domains":set(), "recency_sum":0.0})
        t["count"] += 1
        if domain: t["domains"].add(domain)
        # recency weight
        try:
            pub = dt.datetime.fromisoformat(published_at.replace("Z","+00:00"))
        except Exception:
            pub = now
        days_old = max(0, (now - pub).days)
        t["recency_sum"] += (decay_base ** days_old)

    scored = []
    for topic, agg in by_topic.items():
        if agg["count"] < min_articles:
            continue
        diversity = len(agg["domains"])
        score = agg["count"] * agg["recency_sum"] * (1.0 + 0.2 * math.log1p(diversity))
        scored.append((score, topic, agg["count"], agg["recency_sum"], diversity))

    scored.sort(reverse=True)
    return scored[:limit]

if __name__ == "__main__":
    top = compute_prominence()
    if not top:
        print("[prominence] No topics in window.")
    else:
        print("[prominence] Top topics (last 14 days):")
        for score, topic, n, rec, div in top:
            print(f"  - {topic:30s} score={score:8.2f}  volume={n:3d}  recency={rec:6.2f}  domains={div}")
