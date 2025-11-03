# brief_builder.py
import json, sqlite3, datetime as dt
from typing import Dict, Any, List
import rag_store as rs

DB_PATH = "news.db"

def top_signals(days:int=14, top_n:int=8) -> Dict[str,List[str]]:
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute(f"""SELECT keyphrases_json, entities_json FROM articles
                    WHERE published_at >= datetime('now','-{days} day')""")
    kps, ents = [], []
    import json as _j
    for kj, ej in cur.fetchall():
        if kj: kps += _j.loads(kj)
        if ej: ents += _j.loads(ej)
    from collections import Counter
    return {
        "keyphrases": [k for k,_ in Counter([x.lower() for x in kps]).most_common(top_n)],
        "entities":   [e for e,_ in Counter([x.lower() for x in ents]).most_common(top_n)]
    }

def build_brief(topic:str, format_:str="blog", since:str=None, until:str=None, audience:str="general") -> Dict[str,Any]:
    since = since or (dt.date.today() - dt.timedelta(days=14)).isoformat()
    until = until or dt.date.today().isoformat()
    # Retrieve top chunks for the topic (augmented with your signals)
    sig = top_signals(days=(dt.date.fromisoformat(until) - dt.date.fromisoformat(since)).days, top_n=8)
    query = f"{topic} " + " ".join(sig["keyphrases"][:4] + sig["entities"][:4])
    hits = rs.search(query, k=12)

    # Prepare sources (unique by URL, newest first)
    seen = set(); sources = []
    for h in sorted(hits, key=lambda x: (x.get("published_at") or ""), reverse=True):
        if h["url"] in seen: continue
        seen.add(h["url"])
        sources.append({
            "chunk_id": h["chunk_id"],
            "title": h["title"],
            "url": h["url"],
            "published_at": h["published_at"],
            "text_preview": h["text_preview"]
        })

    brief = {
        "topic": topic,
        "format": format_,
        "audience": audience,
        "tone": "neutral, precise, punchy",
        "constraints": {"max_words": 900, "reading_level": "9th grade"},
        "timebox": {"since": since, "until": until},
        "signals": sig,
        "sources": sources
    }
    return brief

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv)>1 else "Trump campaign finance updates"
    print(json.dumps(build_brief(topic), indent=2, ensure_ascii=False))
