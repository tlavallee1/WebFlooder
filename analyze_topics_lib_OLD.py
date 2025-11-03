# analyze_topics_lib.py (robust output version, stdlib-only)
import sqlite3, json, csv, re, math, html, unicodedata
from datetime import datetime, timezone
from collections import Counter, defaultdict

DB_PATH = "news.db"
EXPORT_PREFIX = "analysis_export"

# ----------------- time helpers -----------------
def iso_to_dt(s):
    try:
        return datetime.fromisoformat(s.replace("Z","")).astimezone(timezone.utc)
    except Exception:
        return None

def age_days(dt):
    if not dt: return 9999
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds()/86400.0)

def exp_recency_weight(days, half_life=7.0):
    return 0.5 ** (days / half_life)

# ----------------- DB shape helpers -----------------
def ensure_columns(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    cols = {r[1] for r in cur.fetchall()}
    if "keyphrases_json" not in cols:
        cur.execute("ALTER TABLE articles ADD COLUMN keyphrases_json TEXT")
    if "entities_json" not in cols:
        cur.execute("ALTER TABLE articles ADD COLUMN entities_json TEXT")
    conn.commit()

# ----------------- text utils -----------------
_COMMON_STOP = set("""
the a an and or of in on to for with from by as is are was were be been being it its this
that those these their his her our your they them he she you we i at not but if then than
into about over under after before during within without across per new says said vs via
amid among around against despite because while where when which who whom whose what
how there here out up down off only still more most very much many any some such each
may might can could should would will just also even every past present future one two
three four five six seven eight nine ten hundred thousand million billion
monday tuesday wednesday thursday friday saturday sunday
january february march april may june july august september october november december
today yesterday tomorrow
home world news politics business economy markets tech sports sport culture opinion live
update updates breaking exclusive video photos photo gallery analysis explainer explainer:
editorial feature features agency agencies source sources report reports reported reporting
""".split())

_GENERIC_LEADERS = set("The A An This That Those These".split())

def _normalize(text: str) -> str:
    if not text:
        return ""
    # HTML & unicode normalization, strip diacritics, fold whitespace
    t = html.unescape(text)
    t = unicodedata.normalize("NFKC", t)
    # remove accents but keep base letters
    t = "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()

def _tokens_for_phrases(text: str):
    # keep letters, numbers, internal hyphens; split on others
    text = re.sub(r"[^A-Za-z0-9\- ]+", " ", text).lower()
    toks = [w for w in text.split() if 2 <= len(w) <= 30 and w not in _COMMON_STOP]
    return toks

def _bigrams(tokens):
    for i in range(len(tokens)-1):
        yield tokens[i], tokens[i+1]

# ----------------- better keyphrases (unigrams + bigrams) -----------------
def simple_keyphrases(title, summary, body, top_k=10):
    title_n = _normalize(title)
    sum_n   = _normalize(summary)
    body_n  = _normalize(body[:4000])  # cap for speed

    # tokens
    t_title = _tokens_for_phrases(title_n)
    t_short = _tokens_for_phrases((title_n + " " + sum_n).strip())
    t_all   = _tokens_for_phrases((title_n + " " + sum_n + " " + body_n).strip())

    uni = Counter(t_all)
    # Bigram counts & naive PMI-ish weighting
    bg_counts = Counter(_bigrams(t_all))
    uni_short = Counter(t_short)

    # title boost for unigrams present in title
    for tok in t_title:
        uni[tok] += 1.5

    # compute bigram scores: freq * lift (co-occurrence / independent)
    total = max(1, sum(uni.values()))
    bi_scores = {}
    for (a,b), c in bg_counts.items():
        if a in _COMMON_STOP or b in _COMMON_STOP: 
            continue
        p_ab = c / total
        p_a  = max(1/total, uni.get(a,0)/total)
        p_b  = max(1/total, uni.get(b,0)/total)
        lift = p_ab / (p_a*p_b)
        # boost if appears in title/summary
        bonus = 1.0 + 0.5*(uni_short.get(a,0)>0) + 0.5*(uni_short.get(b,0)>0)
        bi_scores[f"{a} {b}"] = c * max(1.0, math.log1p(lift)) * bonus

    # merge and rank: prefer bigrams, then strong unigrams
    cand = []
    for w, s in uni.items():
        cand.append((w, float(s)))
    for bg, s in bi_scores.items():
        cand.append((bg, float(s)*1.25))  # prefer bigrams slightly

    # dedupe near-duplicates: keep the longer phrase if it contains the shorter
    cand.sort(key=lambda x: x[1], reverse=True)
    picked = []
    for term, score in cand:
        t = term.strip()
        if not t: 
            continue
        if any(t in p[0] or p[0] in t for p in picked):
            continue
        picked.append((t, score))
        if len(picked) >= top_k:
            break

    return [t for t,_ in picked]

# ----------------- improved entity proxy -----------------
_ent_token = re.compile(r"[A-Z][\w\-]*(?:\s+[A-Z][\w\-]*)+")

def light_entities(title, summary, max_k=10):
    text = _normalize(title) + " " + _normalize(summary)
    # capture multi-word capitalized chunks
    ents = []
    for m in _ent_token.finditer(text):
        span = m.group(0).strip().rstrip(",.:;!?")
        if not span: 
            continue
        # filter sentence-leading generic tokens like "The"
        if span.split()[0] in _GENERIC_LEADERS and len(span.split()) == 1:
            continue
        # discard too generic chunks
        low = span.lower()
        if any(w.lower() in _COMMON_STOP for w in span.split()):
            # keep if at least one word is uncommon/long
            if max(len(w) for w in span.split()) < 4:
                continue
        ents.append(span)
    # collapse near-duplicates (case-insensitive)
    norm = {}
    for e in ents:
        key = e.lower()
        if key not in norm or len(e) > len(norm[key]):
            norm[key] = e
    # frequency by normalized key
    counts = Counter(e.lower() for e in ents)
    ranked = sorted(norm.items(), key=lambda kv: (counts[kv[0]], len(kv[1])), reverse=True)
    return [v for _,v in ranked[:max_k]]

# ----------------- enrichment backfill -----------------
def backfill_enrichment_quick(conn, days_window=14):
    cur = conn.cursor()
    cur.execute(f"""
      SELECT id, title, COALESCE(summary,''), COALESCE(body,''), published_at
      FROM articles
      WHERE published_at IS NOT NULL
        AND published_at >= datetime('now','-{days_window} day')
      ORDER BY published_at DESC
    """)
    rows = cur.fetchall()
    updated = 0
    for aid, title, summary, body, pub in rows:
        row = cur.execute("SELECT keyphrases_json, entities_json FROM articles WHERE id=?", (aid,)).fetchone()
        needs_kp = not row or not row[0]
        needs_ent = not row or not row[1]
        if needs_kp or needs_ent:
            if needs_kp:
                kps = simple_keyphrases(title, summary, body, top_k=10)
                cur.execute("UPDATE articles SET keyphrases_json=? WHERE id=?", (json.dumps(kps, ensure_ascii=False), aid))
            if needs_ent:
                ents = light_entities(title, summary, max_k=10)
                cur.execute("UPDATE articles SET entities_json=? WHERE id=?", (json.dumps(ents, ensure_ascii=False), aid))
            updated += 1
    conn.commit()
    return updated

# ----------------- scoring & topic merge -----------------
def _iter_items(conn, field_name, days_window, half_life):
    cur = conn.cursor()
    cur.execute(f"""
      SELECT id, title, {field_name}, published_at
      FROM articles
      WHERE published_at IS NOT NULL
        AND published_at >= datetime('now','-{days_window} day')
    """)
    for aid, title, raw_list, pub in cur.fetchall():
        if not raw_list: 
            continue
        try:
            items_list = json.loads(raw_list)
        except Exception:
            continue
        w = exp_recency_weight(age_days(iso_to_dt(pub)), half_life=half_life)
        yield aid, title, items_list, w

def prominence_scores(conn, field_name, days_window=14, half_life=7.0):
    score = Counter()
    for _aid, _title, items, w in _iter_items(conn, field_name, days_window, half_life):
        for t in items:
            key = t.strip().lower()
            if key:
                score[key] += w
    return score

def merge_topics(conn, days_window=14, half_life=7.0, top_n=20, examples_per=3):
    # scores from both channels
    kp_score = prominence_scores(conn, "keyphrases_json", days_window, half_life)
    ent_score = prominence_scores(conn, "entities_json",   days_window, half_life)

    # unify keys; keep a “best string” representative (longest form seen)
    rep = {}
    def touch(term):
        k = term.lower().strip()
        if not k: return
        if k not in rep or len(term) > len(rep[k]):
            rep[k] = term

    for k in kp_score.keys(): touch(k)
    for k in ent_score.keys(): touch(k)

    # merge overlapping keys (substring containment) into canonical buckets
    keys = sorted(rep.keys(), key=len, reverse=True)
    parent = {}
    for k in keys:
        parent[k] = k
    for i, k in enumerate(keys):
        for j in range(i+1, len(keys)):
            s = keys[j]
            if s in k:  # smaller included in bigger
                parent[s] = k

    bucket_score = defaultdict(float)
    channel_mix  = defaultdict(lambda: {"kp":0.0,"ent":0.0})
    for k, sc in kp_score.items():
        bucket = parent.get(k, k)
        bucket_score[bucket] += sc
        channel_mix[bucket]["kp"] += sc
    for k, sc in ent_score.items():
        bucket = parent.get(k, k)
        bucket_score[bucket] += sc
        channel_mix[bucket]["ent"] += sc

    # gather example titles per bucket
    examples = defaultdict(list)
    for aid, title, items, w in _iter_items(conn, "keyphrases_json", days_window, half_life):
        for t in items:
            b = parent.get(t.lower().strip(), t.lower().strip())
            if len(examples[b]) < examples_per:
                examples[b].append(title)
    for aid, title, items, w in _iter_items(conn, "entities_json", days_window, half_life):
        for t in items:
            b = parent.get(t.lower().strip(), t.lower().strip())
            if len(examples[b]) < examples_per:
                examples[b].append(title)

    ranked = sorted(bucket_score.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    results = []
    for k, sc in ranked:
        results.append({
            "topic": rep.get(k, k),
            "score": sc,
            "from_keyphrases": channel_mix[k]["kp"],
            "from_entities":   channel_mix[k]["ent"],
            "examples": examples[k]
        })
    return results

# ----------------- main entry -----------------
def run_analysis(days_window=14, top_n=20, half_life=7.0):
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)
    upd = backfill_enrichment_quick(conn, days_window=days_window)

    # original per-channel tables
    kp = prominence_scores(conn, "keyphrases_json", days_window=days_window, half_life=half_life).most_common(top_n)
    ent = prominence_scores(conn, "entities_json",   days_window=days_window, half_life=half_life).most_common(top_n)

    # merged topics
    topics = merge_topics(conn, days_window=days_window, half_life=half_life, top_n=top_n, examples_per=3)

    # ---- write CSVs (per-channel) ----
    with open(f"{EXPORT_PREFIX}_keyphrases.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["keyphrase","prominence"])
        for k,s in kp: w.writerow([k,s])

    with open(f"{EXPORT_PREFIX}_entities.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["entity","prominence"])
        for k,s in ent: w.writerow([k,s])

    # ---- merged topics CSV + JSON + human-readable report ----
    with open(f"{EXPORT_PREFIX}_topics.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["rank","topic","score","from_keyphrases","from_entities","example_1","example_2","example_3"])
        for i, t in enumerate(topics, start=1):
            ex = (t["examples"] + ["","",""])[:3]
            w.writerow([i, t["topic"], t["score"], t["from_keyphrases"], t["from_entities"], *ex])

    with open(f"{EXPORT_PREFIX}_topics.json","w",encoding="utf-8") as f:
        json.dump({
            "generated_at_utc": datetime.utcnow().isoformat()+"Z",
            "days_window": days_window,
            "half_life": half_life,
            "top_n": top_n,
            "updated_rows": upd,
            "topics": topics
        }, f, ensure_ascii=False, indent=2)

    with open(f"{EXPORT_PREFIX}_report.txt","w",encoding="utf-8") as f:
        f.write(f"# Topic Analysis Report\nGenerated (UTC): {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Window: last {days_window} day(s) | Half-life: {half_life} d | TopN: {top_n}\n")
        f.write(f"Backfilled rows: {upd}\n\n")
        for i, t in enumerate(topics, start=1):
            f.write(f"{i:2d}. {t['topic']}  (score={t['score']:.2f} | kp={t['from_keyphrases']:.2f} | ent={t['from_entities']:.2f})\n")
            for ex in t["examples"]:
                f.write(f"      - {ex}\n")
            f.write("\n")

    conn.close()
    return upd, kp, ent, topics

# Optional quick run (uncomment if you want to run as script)
# if __name__ == "__main__":
#     upd, kp, ent, topics = run_analysis()
#     print(f"Updated rows: {upd}")
#     print("Top merged topics:")
#     for i, t in enumerate(topics[:10], 1):
#         print(i, t["topic"], f"(score={t['score']:.2f})")
