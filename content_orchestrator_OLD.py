# content_orchestrator.py
# Orchestrates: (1) detect latest “big issue” from DB, (2) build master prompt with RAG
# (3) ask ChatGPT for a task plan + multi-level content (tweet/post/blog) using your summaries/quotes/facts.
from __future__ import annotations
import os, sqlite3, json, math, datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple

# --- Key loading + client helper --------------------------------------------
import os, configparser
from typing import Optional

def load_chat_key() -> Optional[str]:
    """
    Tries to read an OpenAI-style key from:
      1) KEYS_INI / keys.ini -> [openai] api_key=
      2) ENV: OPENAI_API_KEY
    Returns None if not found.
    """
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
    k = os.getenv("OPENAI_API_KEY", "").strip()
    return k or None

def get_openai_client(log_fn=None):
    """
    Returns an OpenAI client configured with the API key from load_chat_key().
    Raises a clear RuntimeError if not found.
    """
    key = load_chat_key()
    if not key:
        msg = ("[openai] No API key found. Set OPENAI_API_KEY or create keys.ini with:\n"
               "[openai]\napi_key=sk-...")
        if log_fn:
            try: log_fn(msg)
            except Exception: pass
        raise RuntimeError(msg)
    try:
        from openai import OpenAI
        return OpenAI(api_key=key)
    except Exception as e:
        msg = f"[openai] failed to initialize client: {e}"
        if log_fn:
            try: log_fn(msg)
            except Exception: pass
        raise

def _parse_iso_to_utc(s: str | None) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")  # make ISO offset explicit
    try:
        d = dt.datetime.fromisoformat(s)
    except Exception:
        return None
    if d.tzinfo is None:
        # assume UTC if no tz info present
        d = d.replace(tzinfo=dt.timezone.utc)
    else:
        d = d.astimezone(dt.timezone.utc)
    return d

# ---- Chat client (OpenAI-style) ---------------------------------------------
# Reuse your existing OpenAI setup; falls back to env var OPENAI_API_KEY
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

def _get_openai_client(api_key: Optional[str] = None, log_fn=None):
    """
    Returns an OpenAI client. If api_key is None, tries keys.ini/env.
    Raises a clear RuntimeError if not found.
    """
    key = (api_key or load_chat_key() or "").strip()
    if not key:
        msg = (
            "[openai] No API key found.\n"
            "Set the env var OPENAI_API_KEY or create keys.ini with:\n"
            "[openai]\napi_key = sk-....\n"
            "Optionally set KEYS_INI env var to point at your keys.ini path."
        )
        if log_fn:
            try: log_fn(msg)
            except Exception: pass
        raise RuntimeError("Missing OPENAI_API_KEY. Set env var or pass api_key=...")

    try:
        from openai import OpenAI
        if log_fn:
            ini_path = os.getenv("KEYS_INI", "keys.ini")
            src = "explicit arg" if api_key else ("keys.ini" if os.path.exists(ini_path) else "env")
            log_fn(f"[openai] initializing client (source={src})")
        return OpenAI(api_key=key)
    except Exception as e:
        msg = f"[openai] failed to initialize client: {e}"
        if log_fn:
            try: log_fn(msg)
            except Exception: pass
        raise


def _chat(client, model: str, system: str, user: str) -> str:
    # Simple wrapper; adjust model to your preference
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role":"system", "content":system},
            {"role":"user",   "content":user}
        ],
        temperature=0.6,
    )
    return resp.choices[0].message.content.strip()

# ---- Config knobs ------------------------------------------------------------
@dataclass
class ContentOptions:
    category: str = "politics"           # freeform category/tag for styling
    tone: str = "neutral, informative"   # e.g., “neutral, informative”, “edgy”, “optimistic”
    commentary_level: str = "balanced"   # “straight-news”, “balanced”, “opinionated”
    fact_finding: str = "light"          # “light”, “moderate”, “deep”
    audience: str = "general US audience"
    call_to_action: str = "Follow for updates."
    # content levels to produce
    make_tweet: bool = True
    make_post: bool  = True   # FB/LinkedIn style
    make_blog: bool  = True   # long-form
    # retrieval/time window
    days_back: int = 10       # scan recent N days to find the “big issue”
    topk_topic_articles: int = 8
    topk_facts: int = 12
    topk_quotes: int = 6
    topk_rag_chunks: int = 8  # cap of chunks to surface in prompt
    rag_model: str = "text-embedding-3-large"  # used only for info display; retrieval is DB-side
    chat_model: str = "gpt-4.1-mini"           # your default
    # --- in ContentOptions ---
    tone: str = "edgy-assertive"
    humor_level: int = 2          # 0–3 (dry → spicy)
    civility_floor: str = "no-insults"   # "no-insults" | "light-jabs" | "spicy"
    stance_summary: str = (
    "Blue-dog moderate Democrat: pro-institutions, rule-of-law, fiscal realism, "
    "civil-liberties-centered; skeptical of executive overreach and culture-war theatrics."
    )

def _style_instructions(opts: ContentOptions) -> str:
    jab_rules = {
        "no-insults": "Do not insult individuals. Critique actions, policies, and results only.",
        "light-jabs": "You may use brief, witty jabs at ideas—not people. No name-calling.",
        "spicy": "You may use sharp, witty barbs at ideas—not people. Avoid demeaning language."
    }[opts.civility_floor]

    humor = {0:"mostly straight",1:"wry",2:"snark-forward",3:"bold and cheeky"}[max(0,min(3,opts.humor_level))]

    return f"""
VOICE & STANCE:
- Stance: {opts.stance_summary}
- Tone: {opts.tone}, {humor} humor.
- {jab_rules}
- Be punchy. Prefer short, declarative lines. Prioritize verifiable facts over vibes.
- Always ground claims in the evidence chunks, summaries, facts, and quotes supplied.
- If a strong claim lacks grounding in evidence, either soften it or flag it as opinion.
"""

# ---- DB helpers --------------------------------------------------------------
def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Score topics by recency & volume (log-scaled count * recency decay)
def _pick_big_issue(con: sqlite3.Connection, days_back: int) -> Optional[Tuple[str, float]]:
    """
    Returns (topic, score) for the most 'recently hot' topic within the last `days_back` days.
    Scoring = sum over articles of exp(-age_days/3).
    """
    days_back = int(days_back or 7)

    cur = con.cursor()
    # Note: bind the date modifier as "-N days"
    cur.execute("""
        SELECT t.topic, a.published_at
        FROM article_topics t
        JOIN articles a ON a.id = t.article_id
        WHERE a.published_at IS NOT NULL
          AND date(substr(a.published_at, 1, 10)) >= date('now', ?)
    """, (f"-{days_back} days",))
    rows = cur.fetchall()
    if not rows:
        return None

    now = dt.datetime.now(dt.timezone.utc)  # UTC-aware 'now'
    scores: Dict[str, float] = {}

    for r in rows:
        topic = r["topic"]
        pub = _parse_iso_to_utc(r["published_at"]) or now
        age_days = max(0.0, (now - pub).total_seconds() / 86400.0)
        recency = math.exp(-age_days / 3.0)  # half-life-ish ~3 days
        scores[topic] = scores.get(topic, 0.0) + recency

    if not scores:
        return None
    return max(scores.items(), key=lambda kv: kv[1])


def _articles_for_topic(con: sqlite3.Connection, topic: str, limit: int) -> List[sqlite3.Row]:
    cur = con.cursor()
    cur.execute("""
        SELECT a.id, a.title, a.published_at, a.canonical_url,
               a.summary_256, a.summary_1k, a.source_domain, a.body_clean
        FROM articles a
        JOIN article_topics t ON t.article_id = a.id
        WHERE t.topic = ?
        ORDER BY (a.published_at IS NULL), a.published_at DESC, a.id DESC
        LIMIT ?
    """, (topic, int(limit)))
    return cur.fetchall()

def _facts_for_articles(con: sqlite3.Connection, article_ids: List[int], limit: int) -> List[sqlite3.Row]:
    if not article_ids:
        return []
    cur = con.cursor()
    qmarks = ",".join("?" for _ in article_ids)
    cur.execute(f"""
        SELECT article_id, sentence, cited_url
        FROM facts
        WHERE article_id IN ({qmarks})
        LIMIT ?
    """, (*article_ids, int(limit)))
    return cur.fetchall()

def _quotes_for_articles(con: sqlite3.Connection, article_ids: List[int], limit: int) -> List[sqlite3.Row]:
    if not article_ids:
        return []
    cur = con.cursor()
    qmarks = ",".join("?" for _ in article_ids)
    cur.execute(f"""
        SELECT article_id, quote
        FROM quotes
        WHERE article_id IN ({qmarks})
        LIMIT ?
    """, (*article_ids, int(limit)))
    return cur.fetchall()

def _rag_chunks_for_topic(con: sqlite3.Connection, topic: str, topk: int) -> List[sqlite3.Row]:
    """
    Lightweight similarity via inner-join on chunk hash isn’t available,
    so we use a simple text LIKE prefilter over chunks (cheap but effective),
    then cap to topk. (If you stored ANN indices elsewhere, swap this out.)
    """
    cur = con.cursor()
    like = f"%{topic.strip('%')}%"
    cur.execute("""
        SELECT c.article_id, c.seq, c.text, a.title, a.canonical_url, a.published_at
        FROM chunks c
        JOIN articles a ON a.id = c.article_id
        WHERE c.text LIKE ?
        ORDER BY (a.published_at IS NULL), a.published_at DESC, c.article_id DESC, c.seq ASC
        LIMIT ?
    """, (like, int(topk)))
    return cur.fetchall()

# ---- Prompt builder ----------------------------------------------------------
def _style_instructions(opts: ContentOptions) -> str:
    """Render stance/tone rules for edgy-but-grounded copy."""
    # Safe defaults if fields aren't present on opts yet
    stance = getattr(opts, "stance_summary", 
        "Blue-dog moderate Democrat: pro-institutions, rule-of-law, fiscally pragmatic; "
        "skeptical of executive overreach and culture-war theatrics.")
    tone   = getattr(opts, "tone", "edgy-assertive")
    humor_level = int(getattr(opts, "humor_level", 2))
    civility = getattr(opts, "civility_floor", "light-jabs")

    humor_name = {0:"mostly straight", 1:"wry", 2:"snark-forward", 3:"bold & cheeky"}.get(humor_level, "wry")
    jab_rules = {
        "no-insults":  "Do not insult people. Critique actions, policies, and results only.",
        "light-jabs":  "Witty jabs at ideas are ok. No name-calling or personal slurs.",
        "spicy":       "Sharp barbs at ideas are ok. Avoid demeaning language toward people."
    }.get(civility, "Witty jabs at ideas are ok. No name-calling or personal slurs.")

    return f"""
VOICE & STANCE
- Stance: {stance}
- Tone: {tone}; {humor_name} humor.
- {jab_rules}
- Be punchy. Favor short, declarative lines.
- Ground all claims in provided evidence (facts, quotes, RAG chunks). If a strong claim lacks grounding, mark it as opinion.
"""


def _build_master_prompt(
    issue_topic: str,
    topic_articles: List[sqlite3.Row],
    facts: List[sqlite3.Row],
    quotes: List[sqlite3.Row],
    rag_chunks: List[sqlite3.Row],
    opts: ContentOptions
) -> str:
    # Small helpers
    def safe(s): 
        return (s or "").strip().replace("\n", " ")

    # Evidence rollups (with labels we can cite inside outputs)
    art_lines = []
    for r in topic_articles:
        aid = r["id"] if "id" in r.keys() else None
        label = f"[A{aid}]" if aid is not None else ""
        art_lines.append(
            f"- {label} {safe(r.get('title') if hasattr(r, 'get') else r['title'])} "
            f"({r['source_domain']}; {r['published_at'] or 'n/a'}) — "
            f"{safe(r['summary_256'] if 'summary_256' in r.keys() else '')}"
        )

    fact_lines = []
    for fr in facts:
        url = fr["cited_url"] if "cited_url" in fr.keys() else ""
        fact_lines.append(f"- {safe(fr['sentence'])} [src: {url}]")

    quote_lines = [f"“{safe(qr['quote'])}”" for qr in quotes]

    chunk_lines = []
    for ch in rag_chunks:
        # Prefer explicit labels if available so the model can cite [A{id}/C{seq}]
        aid = ch["article_id"] if "article_id" in ch.keys() else None
        seq = ch["seq"] if "seq" in ch.keys() else None
        label = f"[A{aid}/C{seq}]" if (aid is not None and seq is not None) else ""
        preview = safe(ch["text"])[:240]
        src = ch["canonical_url"] if "canonical_url" in ch.keys() else ""
        chunk_lines.append(f"- {label} {preview}…  [src: {src}]")

    # Style/stance block
    style = _style_instructions(opts)

    # Evidence minimums (soft constraints for the model)
    min_facts      = int(getattr(opts, "min_facts", 3))
    min_quotes     = int(getattr(opts, "min_quotes", 1))
    min_citations  = int(getattr(opts, "min_citations", 3))

    return f"""
You are a senior editor creating content plans and drafts for multiple formats.

Primary issue/topic (auto-detected): **{issue_topic}**

Audience: {getattr(opts, 'audience', 'civically engaged general audience')}
Category: {getattr(opts, 'category', 'politics')}
Desired tone: {getattr(opts, 'tone', 'edgy-assertive')}
Commentary level: {getattr(opts, 'commentary_level', 'analysis-with-opinion')}
Extra fact-finding depth: {getattr(opts, 'fact_finding', 'light')}
Call to action: {getattr(opts, 'call_to_action', 'Support institutions and rule-of-law.')}

{style}

EVIDENCE PACKS (use these; do not hallucinate)
[Evidence A: Recent article rollup]
{chr(10).join(art_lines) if art_lines else "(none)"}

[Evidence B: Extracted facts]
{chr(10).join(fact_lines) if fact_lines else "(none)"}

[Evidence C: Curated quotes]
{chr(10).join(quote_lines) if quote_lines else "(none)"}

[Evidence D: RAG chunk previews]
{chr(10).join(chunk_lines) if chunk_lines else "(none)"}

OUTPUT CONTRACT
1) First, propose a short task plan (“subtasks”), each with a purpose and expected outputs.
2) Then produce:
   - A) 1 tweet/X post (≤ 280 chars) + 2 alternates (different angles).
   - B) 1 short social post (120–220 words) for FB/LinkedIn; include ≥1 short quote or fact.
   - C) A blog outline (H2/H3) AND a 600–900 word blog draft.
3) Weave in **≥{min_facts} concrete facts** from [Evidence B] and **≥{min_quotes} short quotes** from [Evidence C] when relevant.
4) Include **≥{min_citations} inline citations** like [A123/C4] pointing to the specific article/chunk used.
5) Add a **Sources** section with 2–5 URLs from the evidence you actually used.
6) If any strong claim lacks clear support, label it as opinion or add an **Open Question**.

Format strictly as:
## PLAN
- Task 1: ...
- Task 2: ...
## DRAFTS
### Tweet
...
### Tweet Alt 2
...
### Tweet Alt 3
...
### Social Post
...
### Blog Outline
...
### Blog Draft
...
### Sources
- ...
## OPEN QUESTIONS
- ...
"""


# ---- Top-level runner --------------------------------------------------------
def run_content_orchestration(
    db_path: str = "news.db",
    *,
    options: Optional[ContentOptions] = None,
    api_key: Optional[str] = None,
    chat_model: Optional[str] = None,
    verbose_log = print
) -> Dict[str, Any]:
    """
    Returns a dict with:
      'topic': str
      'prompt': str
      'outputs': str  # the ChatGPT formatted content
      'stats': {...}
    """
    import os, json, textwrap
    from datetime import datetime

    def _stringify(obj, width=100):
        try:
            if isinstance(obj, (dict, list)):
                s = json.dumps(obj, ensure_ascii=False, indent=2)
            elif obj is None:
                s = ""
            else:
                s = str(obj)
            out = []
            for line in s.splitlines():
                if len(line) > width:
                    out.extend(textwrap.wrap(line, width=width))
                else:
                    out.append(line)
            return "\n".join(out)
        except Exception:
            return str(obj)

    def _log_section(title: str, payload):
        bar = "-" * 72
        verbose_log(f"[orchestrator] {bar}")
        verbose_log(f"[orchestrator] {title}")
        verbose_log(f"[orchestrator] {bar}")
        s = _stringify(payload, width=120)
        if not s.strip():
            verbose_log("[orchestrator] (empty)")
            return
        # chunk very long blobs so logs stay readable
        for chunk in textwrap.wrap(s, width=1000, replace_whitespace=False, drop_whitespace=False):
            verbose_log(chunk)

    opts = options or ContentOptions()
    con = _connect(db_path)

    # 1) Find latest “big issue”
    pick = _pick_big_issue(con, days_back=opts.days_back)
    if not pick:
        con.close()
        raise RuntimeError("No topics found in the selected window.")
    topic, score = pick
    verbose_log(f"[orchestrator] topic='{topic}' score={score:.3f}")

    # 2) Collect evidence
    arts = _articles_for_topic(con, topic, limit=opts.topk_topic_articles)
    art_ids = [r["id"] for r in arts]
    facts = _facts_for_articles(con, art_ids, limit=opts.topk_facts)
    quotes = _quotes_for_articles(con, art_ids, limit=opts.topk_quotes)
    rag = _rag_chunks_for_topic(con, topic, topk=opts.topk_rag_chunks)

    verbose_log(f"[orchestrator] evidence: articles={len(arts)} facts={len(facts)} quotes={len(quotes)} rag_chunks={len(rag)}")

    # 3) Build master prompt
    master = _build_master_prompt(topic, arts, facts, quotes, rag, opts)

    # 4) Ask ChatGPT for plan + drafts
    client = _get_openai_client(api_key=api_key)
    model = chat_model or opts.chat_model
    system = "You are a precise, citation-minded editor who grounds everything in provided evidence."
    outputs = _chat(client, model=model, system=system, user=master)
    verbose_log(f"[orchestrator] Chat outputs received ({len(outputs)} chars)")

    # 5) Persist this run (audit)
    _persist_run(con, topic, master, outputs)

    # 6) Auto-write artifacts to disk (always)
    stamp  = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    outdir = os.path.join("orchestrator_out", stamp)
    os.makedirs(outdir, exist_ok=True)

    # friendly helpers
    def _write_text(name, text):
        path = os.path.join(outdir, name)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text if text is not None else "")
            verbose_log(f"[orchestrator] wrote {path}")
        except Exception as e:
            verbose_log(f"[orchestrator] failed to write {path}: {e}")

    def _write_json(name, payload):
        path = os.path.join(outdir, name)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            verbose_log(f"[orchestrator] wrote {path}")
        except Exception as e:
            verbose_log(f"[orchestrator] failed to write {path}: {e}")

    # evidence packs for JSON
    evidence = {
        "articles": [dict(r) for r in arts],                 # id, title, urls, etc.
        "facts":    [dict(x) for x in facts],                # sentence, cited_url
        "quotes":   [dict(x) for x in quotes],               # quote
        "rag":      [dict(x) for x in rag],                  # chunk text + meta
    }

    # JSON artifacts
    _write_json("evidence.json", evidence)
    _write_json("run.json", {
        "topic": topic,
        "score": score,
        "db_path": db_path,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "model": model,
        "counts": {
            "articles": len(arts),
            "facts": len(facts),
            "quotes": len(quotes),
            "rag_chunks": len(rag),
        },
    })

    # Text artifacts
    _write_text("topic.txt", str(topic))
    _write_text("prompt.txt", master)
    _write_text("outputs.txt", outputs)

    # Human-readable report
    try:
        sep = "\n" + ("=" * 88) + "\n"
        report_path = os.path.join(outdir, "report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"# Content Orchestrator Report\nGenerated: {datetime.utcnow().isoformat()}Z\nOutput dir: {outdir}\n")
            f.write(sep)
            f.write("## TOPIC\n")
            f.write(f"{topic}\n\nscore={score:.3f}\n")
            f.write(sep)

            f.write("## PROMPT (master)\n")
            f.write(master + "\n")
            f.write(sep)

            f.write("## OUTPUTS (ChatGPT)\n")
            f.write(outputs + "\n")
            f.write(sep)

            f.write("## EVIDENCE: Articles\n")
            for a in arts:
                f.write(f"- [{a['id']}] {a.get('title','(untitled)')} | {a.get('canonical_url','')}\n")
            f.write(sep)

            f.write("## EVIDENCE: Facts (up to {})\n".format(len(facts)))
            for i, d in enumerate(facts, 1):
                f.write(f"  {i}. {d.get('sentence','')}  [src: {d.get('cited_url') or '(unknown)'}]\n")
            f.write(sep)

            f.write("## EVIDENCE: Quotes (up to {})\n".format(len(quotes)))
            for q in quotes:
                f.write(f'  “{q.get("quote","")}”\n')
            f.write(sep)

            f.write("## EVIDENCE: RAG Chunks (top {})\n".format(len(rag)))
            for i, ch in enumerate(rag, 1):
                txt = (ch.get("chunk_text","") or "").strip()
                preview = (txt[:600] + ("…" if len(txt) > 600 else ""))
                f.write(f"  [{i}] a#{ch.get('article_id','?')} seq={ch.get('seq','?')} wc≈{len(txt.split())}\n")
                f.write(f"      {preview}\n\n")
            f.write(sep)

        verbose_log(f"[orchestrator] wrote {report_path}")
    except Exception as e:
        verbose_log(f"[orchestrator] failed to write report.txt: {e}")

    # Also mirror key sections into logs
    _log_section("TOPIC", {"topic": topic, "score": score})
    _log_section("PROMPT (master)", master)
    _log_section("OUTPUTS (ChatGPT)", outputs)
    _log_section("EVIDENCE: articles", [{"id": a["id"], "title": a["title"] if "title" in a.keys() else None} for a in arts])
    _log_section("EVIDENCE: facts", facts)
    _log_section("EVIDENCE: quotes", quotes)
    _log_section("EVIDENCE: rag (previews)", [
        {"article_id": ch.get("article_id"), "seq": ch.get("seq"), "preview": (ch.get("chunk_text","") or "")[:200]}
        for ch in rag
    ])

    stats = {
        "articles": len(arts),
        "facts": len(facts),
        "quotes": len(quotes),
        "rag_chunks": len(rag),
        "score": score,
        "ts": _now_utc_iso(),
        "outdir": outdir,
        "model": model,
    }

    con.close()

    return {
        "topic": topic,
        "prompt": master,
        "outputs": outputs,
        "stats": stats,
    }


def _persist_run(con: sqlite3.Connection, topic: str, prompt: str, outputs: str) -> None:
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS content_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            prompt TEXT NOT NULL,
            outputs TEXT NOT NULL
        )
    """)
    cur.execute("INSERT INTO content_runs(topic, prompt, outputs) VALUES (?,?,?)",
                (topic, prompt, outputs))
    con.commit()

# ---- CLI quick test ----------------------------------------------------------
if __name__ == "__main__":
    opts = ContentOptions(
        category="politics",
        tone="neutral, analytical",
        commentary_level="balanced",
        fact_finding="moderate",
        audience="general US audience",
        call_to_action="Follow for updates.",
        days_back=10,
        topk_topic_articles=8,
        topk_facts=12,
        topk_quotes=6,
        topk_rag_chunks=8,
        chat_model="gpt-4.1-mini",
    )
    res = run_content_orchestration(options=opts)
    print("\n=== SELECTED TOPIC ===\n", res["topic"])
    print("\n=== MASTER PROMPT (truncated) ===\n", res["prompt"][:1200], "…")
    print("\n=== OUTPUTS ===\n", res["outputs"][:2000], "…")
