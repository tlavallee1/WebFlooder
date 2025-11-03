#!/usr/bin/env python3
r"""
creator_full_blog.py — Agentic blog creator with REAL RAG over news.db

Pipeline:
  subtasker_agent → query_builder_agent → HYBRID RAG (FTS5 + embeddings)
  → drafting_agent (grounded) → consolidator_agent → YAML front matter + Markdown

Keys:
  --keys keys.ini   or   env OPENAI_API_KEY

DB:
  --db path/to/news.db  (expects tables: articles, chunks, chunk_vectors)
  Auto-creates chunks_fts (FTS5) + triggers if missing, and backfills.

Style:
  --profanity clean|mild|spicy|bleeped  (default: clean)
  --grade-level N|auto  (default: 12)  → guides sentence length/word choice

Usage (PowerShell):
  python .\creator_full_blog.py `
    --title "Signals vs. Systems" `
    --topic "The Fentanyl Tariff Deal" `
    --angle "optics outpacing verification" `
    --db .\news.db `
    --keys .\keys.ini `
    --profanity spicy `
    --grade-level 11 `
    --print-social `
    -o .\out\post.md
"""
from __future__ import annotations
import argparse, configparser, datetime as dt, json, math, os, re, sqlite3, sys, textwrap, hashlib
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import time, logging, json

from openai import OpenAI

def _traced_chat(client, *, model: str, messages: list, trace_log: list,
                 temperature: float = 0.7, max_tokens: int = 1200, **kwargs) -> str:
    """
    Returns ONLY the assistant text (str). Appends full prompt/response to trace_log.
    """
    start = time.time()
    resp = client.chat.completions.create(
        model=model, messages=messages,
        temperature=temperature, max_tokens=max_tokens, **kwargs
    )
    elapsed = time.time() - start
    out = resp.choices[0].message.content if (resp.choices and resp.choices[0].message) else ""

    usage = None
    try:
        usage = {
            "prompt_tokens": getattr(resp.usage, "prompt_tokens", None),
            "completion_tokens": getattr(resp.usage, "completion_tokens", None),
            "total_tokens": getattr(resp.usage, "total_tokens", None),
        }
    except Exception:
        pass

    trace_log.append({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "elapsed_s": round(elapsed, 3),
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
        "response": out,
        "usage": usage,
        "kwargs": {k: v for k, v in kwargs.items()} if kwargs else {}
    })
    return out  # ← IMPORTANT: str, not tuple


def _setup_logging(enabled: bool):
    logger = logging.getLogger()
    # remove old handlers so VSCode terminal shows output reliably
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S"
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO if enabled else logging.WARNING)
    logger.propagate = False

def _log(on: bool, level: str, msg: str):
    if not on:
        return
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.log(lvl, msg)


# ---------- Lightweight blog-oriented agents ----------

def subtasker_agent(task_prompt: str, client: OpenAI, num_subtasks: int = 5,
                    log_dir: str = "Agent_Logs", trace_log: list | None = None) -> list:
    system_message = (
        "You are a senior blog editor and planning agent. "
        "Break a single blog assignment into sharply distinct subtasks that together form a compelling analysis post. "
        "Stay strictly on-topic. Avoid overlap. No fluff."
    )
    user_prompt = f"""Task:
{task_prompt}

Break this into exactly {num_subtasks} numbered subtasks for a persuasive, evidence-based blog post.
Cover (where applicable): snappy lead & hook; verification/mechanisms; stakeholders & incentives; historical benchmarks;
counterpoints/limitations; metrics-to-watch (90-day scoreboard); synthesis & call-to-action. If {num_subtasks} < sections, merge smartly.

Rules:
- One sentence per subtask, <= 18 words, imperative voice, no overlap, no numbering in the sentence itself.
- Must be directly relevant to the task and independently executable.

Return ONLY a numbered list, e.g.:
1. Write a snappy lead that frames the tension and stakes.
2. ...
"""
    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_prompt},
    ]

    if trace_log is not None:
        text = _traced_chat(client, model="gpt-3.5-turbo", messages=messages,
                            trace_log=trace_log, temperature=0.4, max_tokens=800)
    else:
        resp = client.chat.completions.create(model="gpt-3.5-turbo", messages=messages,
                                              temperature=0.4, max_tokens=800)
        text = resp.choices[0].message.content

    raw = text.strip()
    subtasks = []
    for i, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line: continue
        parts = line.split(".", 1)
        instruction = parts[1].strip() if len(parts) == 2 else line.lstrip("1234567890). ").strip()
        if instruction:
            subtasks.append({"id": f"task_{i}", "instruction": instruction, "context": task_prompt})
        if len(subtasks) >= num_subtasks:
            break
    return subtasks

def query_builder_agent(subtask_instruction: str, client: OpenAI, num_queries: int = 3,
                        full_prompt: str = "", trace_log: list | None = None) -> list:
    system_msg = (
        "You are a research query designer for investigative blog writing. "
        "Queries must be concrete, entity-rich, and verification-focused—good for search and vector recall. "
        "Prefer nouns, entities, metrics, mechanisms, and time windows. Avoid opinion words."
    )
    user_msg = f"""Full task:
{full_prompt}

Current subtask:
{subtask_instruction}

Generate exactly {num_queries} diversified retrieval queries. Use different angles, e.g.:
- verification & measurement ('compliance rate', 'interdictions', 'price/availability signal'),
- mechanisms & incentives ('enforcement mechanism', 'verification protocol', 'counterparty incentive'),
- benchmarks & history ('past agreement outcomes', 'comparative baseline 2018–2022'),
- counterpoints & limitations,
- metrics to watch (next 90 days).

Good patterns:
- include entities, dates, places, mechanism keywords
- optional operators like site:, filetype:, or quoted phrases

Return ONLY a numbered list:
1. Query text...
2. Query text...
"""
    messages = [{"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}]

    if trace_log is not None:
        text = _traced_chat(client, model="gpt-3.5-turbo", messages=messages,
                            trace_log=trace_log, temperature=0.5, max_tokens=500)
    else:
        resp = client.chat.completions.create(model="gpt-3.5-turbo", messages=messages,
                                              temperature=0.5, max_tokens=500)
        text = resp.choices[0].message.content

    raw = text.strip()
    queries = []
    for i, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line: continue
        parts = line.split(".", 1)
        qtext = parts[1].strip() if len(parts) == 2 else line.lstrip("1234567890). ").strip()
        if qtext:
            queries.append({"query": qtext, "intent": "lookup"})
        if len(queries) >= num_queries:
            break
    return queries

def drafting_agent(subtask: dict, client: OpenAI, model="gpt-4o",
                   style_hint: str = "", level: str = "clean",
                   freq: str = "scarce", per_section: int = 0,
                   trace_log: list | None = None) -> str:
    raws = subtask.get("retrievals", [])
    blocks = []
    for r in raws:
        if isinstance(r, str):
            blocks.append(r)
        elif isinstance(r, dict) and r.get("response"):
            blocks.append(r["response"])
        elif isinstance(r, dict) and r.get("text"):
            meta = " | ".join(x for x in [r.get("title",""), r.get("url",""), r.get("published_at","")] if x)
            hdr = f"[SOURCE] {meta}\n" if meta else ""
            blocks.append(hdr + r["text"])
    joined = "\n\n".join(blocks).strip()
    task_ctx = subtask.get("context","")

    system_msg = (
        "You are a senior editorial writer for a policy analysis blog. "
        "Write with clarity, edge, and receipts. Favor verification, mechanisms, incentives, and practical tradeoffs. "
        "Cite sources using the provided [SOURCE] lines when drawing facts."
    )
    if style_hint:
        system_msg += "\n" + style_hint

    if level in ("spicy", "bleeped", "mild"):
        target = {"scarce": 1, "moderate": 2, "heavy": 3}.get(freq, max(0, int(per_section)))
    else:
        target = 0
    profanity_specific = ""
    if level == "bleeped":
        profanity_specific = "When profanity is used, always **bleep** it (e.g., f**k, sh*t). "
    elif level == "spicy":
        profanity_specific = "Profanity, if used, is uncensored. "
    elif level == "mild":
        profanity_specific = "Only light profanity (e.g., damn, hell). "

    user_msg = f"""
Assignment context (tone/scope):
{task_ctx}

Subtask to write:
{subtask['instruction']}

Write a multi-paragraph section that:
- Leads with the most decision-relevant point for THIS subtask.
- Weaves in specific facts from the snippets with brief attributions.
- Explains mechanisms/measurement; avoid generic hype.
- Ends with a one-sentence mini-takeaway.
- Aim for 140–240 words unless detail requires more.

Profanity usage target for THIS section: {target}.
{profanity_specific}Distribute any profanities naturally (not all in one sentence).
Return ONLY the prose (no headings).
""".strip()

    messages = [{"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}]

    if trace_log is not None:
        text = _traced_chat(client, model=model, messages=messages,
                            trace_log=trace_log, temperature=0.68, max_tokens=1200)
    else:
        resp = client.chat.completions.create(model=model, messages=messages,
                                              temperature=0.68, max_tokens=1200)
        text = resp.choices[0].message.content
    return text.strip()


def consolidator_agent(prompt_text: str, subtasks: list, client: OpenAI,
                       model="gpt-4o", style_hint: str = "",
                       level: str = "clean", freq: str = "scarce",
                       per_section: int = 0, trace_log: list | None = None) -> str:
    combined = "\n\n---\n\n".join(s.get("draft","") for s in subtasks if s.get("draft"))
    system_msg = (
        "You are a veteran magazine features editor. "
        "Combine the drafts into one cohesive analysis post. "
        "Voice: confident, sharp, plainspoken; avoid jargon unless necessary and define it once."
    )
    if style_hint:
        system_msg += "\n" + style_hint

    overall = {
        "scarce":  max(1, len(subtasks)//2),
        "moderate": max(2, len(subtasks)),
        "heavy":    max(3, int(1.5*len(subtasks))),
        "custom":   max(0, int(per_section)) * max(1, len(subtasks))
    }.get(freq, 0)

    profanity_rule = ""
    if level == "bleeped":
        profanity_rule = (f"Profanity may appear throughout but must be bleeped (e.g., f**k, sh*t). "
                          f"Natural distribution; overall target ≈ {overall}. ")
    elif level == "spicy":
        profanity_rule = (f"Profanity may appear throughout (uncensored). "
                          f"Keep it purposeful; overall target ≈ {overall}. ")
    elif level == "mild":
        profanity_rule = (f"Light profanity may appear sparingly; overall target ≈ {max(1, len(subtasks)//2)}. ")

    user_msg = f"""
Original task (scope/tone):
{prompt_text}

Section drafts (separated by ---):
{combined}

Now produce a single blog post that:
- Opens with a snappy 1–2 sentence lead that frames the stakes.
- Follows with a short hook that questions the headline narrative.
- Flows logically; remove duplication; tighten language.
- Preserves facts and '(see: SOURCE ...)' attributions where present.
- Ends with 3–6 concrete metrics to watch over the next 90 days.

{profanity_rule}
Constraints:
- Profanity must never use slurs or harass protected classes.
- ~900–1,400 words unless the content requires more.
- Return ONLY the final post body (no YAML; no extra commentary).
""".strip()

    messages = [{"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}]

    if trace_log is not None:
        text = _traced_chat(client, model=model, messages=messages,
                            trace_log=trace_log, temperature=0.62, max_tokens=5000)
    else:
        resp = client.chat.completions.create(model=model, messages=messages,
                                              temperature=0.62, max_tokens=5000)
        text = resp.choices[0].message.content
    return text.strip()



# ---------- Config models ----------

from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class BlogConfig:
    # Content / metadata
    title: str
    topic: str
    angle: str = ""
    audience: str = "informed general"
    tone: str = "analytical"
    author: str = "Editorial Desk"
    category: str = "analysis"
    tags: List[str] = field(default_factory=list)
    hero_image: Optional[str] = None
    include_social_blurb: bool = True

    # Agent knobs
    num_subtasks: int = 5
    queries_per_subtask: int = 3
    retrieval_model: str = "text-embedding-3-large"  # embedding model
    drafting_model: str = "gpt-4o"
    temperature: float = 0.7

    # RAG knobs
    db_path: str = "news.db"
    lexical_pool: int = 120
    top_k: int = 18
    alpha: float = 0.45
    time_decay_days: Optional[int] = None  # e.g., 90 or None

    # Style knobs
    profanity_level: str = "clean"         # clean|mild|spicy|bleeped
    grade_level: str = "12"                # N or 'auto'

    # NEW: profanity distribution
    profanity_frequency: str = "moderate"  # scarce|moderate|heavy|custom
    profanity_per_section: int = 0         # used when frequency == custom


# ---------- Utility / style helpers ----------



def _sanitize_fts_query(q: str) -> str:
    """
    Clean a free-form query for FTS5 MATCH so tokens like 'site:' or 'metric:' don't
    get interpreted as column names. Keeps quoted phrases; strips unmatched quotes.
    """
    # Drop leading field-like prefixes 'foo:' but keep the token that follows
    s = re.sub(r"\b\w+:(?=\S)", "", q)

    # Collapse excessive punctuation that could confuse the parser
    s = re.sub(r"[<>~=*]+", " ", s)

    # If quote count is odd, strip quotes to avoid parse errors
    if s.count('"') % 2 == 1:
        s = s.replace('"', "")

    # Trim; if empty, fall back to original as a phrase later
    s = s.strip()
    return s

def _slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    return s.strip("-")[:80]

def _now_iso() -> str:
    return dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _front_matter(cfg: BlogConfig, summary: str) -> str:
    tags = cfg.tags or [cfg.category, cfg.tone, cfg.topic]
    fm = {
        "title": cfg.title,
        "slug": _slugify(cfg.title),
        "date": _now_iso(),
        "author": cfg.author,
        "category": cfg.category,
        "tags": tags,
        "summary": summary,
    }
    if cfg.hero_image:
        fm["hero_image"] = cfg.hero_image
    lines = ["---"]
    for k, v in fm.items():
        if isinstance(v, list):
            lines.append(f"{k}: [{', '.join(json.dumps(x) for x in v)}]")
        else:
            lines.append(f"{k}: {json.dumps(v)}")
    lines.append("---")
    return "\n".join(lines)

def _social_blurb(cfg: BlogConfig, hook: str) -> str:
    base = f"Reality-check {cfg.topic}. {hook} Receipts > press releases."
    w = base.split()
    return " ".join(w[:42]) + ("" if len(w) <= 42 else "…")

def _profanity_style(level: str, freq: str, per_section: int) -> str:
    """
    Returns explicit guidance for the model on *how* and *how often* to swear.
    Never permits slurs or harassment.
    """
    # base rule
    if level == "clean":
        base = "Style: no profanity. Use precision and wit; absolutely avoid slurs and harassment."
    elif level == "mild":
        base = ("Style: allow mild and standard profanity for emphasis. "
                "No slurs or harassment.")
    elif level == "spicy":
        base = ("Style: Include exagerated profanity without restraint - for emphasis and to promote a harsh conversational style. "
                "Avoid slurs and harassment.")
    elif level == "bleeped":
        base = ("Style: profanity is allowed but must be **bleeped** (e.g., f**k, sh*t). "
                "No slurs or harassment.")
    else:
        base = "Style: no profanity."

    # frequency rule
    if level in ("spicy", "bleeped", "mild"):
        if freq == "scarce":
            freq_rule = "Distribution: about 0–1 profanities per section on average; some sections may have none."
        elif freq == "moderate":
            freq_rule = "Distribution: around 2–3 profanities per section; vary placement naturally."
        elif freq == "heavy":
            freq_rule = "Distribution: up to 4–6 profanities per section, spread across the post."
        elif freq == "custom":
            n = max(0, int(per_section))
            freq_rule = f"Distribution: target ~{n} profanities in each section."
        else:
            freq_rule = ""
    else:
        freq_rule = ""

    # safety/quality rails
    rails = "Constraints: never use slurs; never target protected classes; do not harass individuals. "
             
    return f"{base}\n{freq_rule}\n{rails}"

def _readability_style(grade_level: str) -> str:
    """
    Guides clarity and cadence. 'auto' lets the model choose. Otherwise:
    - shorter sentences, common words, minimal nested clauses, concrete nouns/verbs.
    """
    if str(grade_level).lower() == "auto":
        return ("Readability: choose a natural cadence for policy-curious adults; "
                "prefer clarity over flourish; define any necessary jargon once.")
    try:
        g = int(grade_level)
        g = max(2, min(18, g))
        return (f"Readability: target roughly grade {g}. Prefer short sentences, concrete nouns/verbs, "
                "limit subordinate clauses, explain jargon once, and keep average sentence length appropriate "
                "to that level.")
    except Exception:
        return ("Readability: choose a natural cadence for policy-curious adults; "
                "prefer clarity over flourish; define any necessary jargon once.")

def _apply_profanity_filter(text: str, level: str) -> str:
    """
    Small safety net. Model should obey style, but:
    - 'bleeped': censor vowels in common profanities
    - 'clean'  : soften/remove the same set
    We do NOT transform slurs—those are disallowed by instruction.
    """
    bad = [
        r"\bfuck(ing|er|ers|ed|s)?\b", r"\bshit(ty|s)?\b", r"\bass(hole|holes)?\b",
        r"\bdamn\b", r"\bhell\b", r"\bpiss(ed)?\b", r"\bcrap\b"
    ]
    def bleep(m):
        w = m.group(0)
        return re.sub(r"[aeiouAEIOU]", "*", w)
    def soften(m):
        w = m.group(0)
        if w.lower() in ("damn","hell","crap"):
            return ""
        return w[0] + "—"
    if level == "bleeped":
        for pat in bad: text = re.sub(pat, bleep, text, flags=re.IGNORECASE)
    elif level == "clean":
        for pat in bad: text = re.sub(pat, soften, text, flags=re.IGNORECASE)
    return text

# ---------- Keys.ini loading ----------

def _load_openai_key(keys_path: Optional[str]) -> str:
    env_key = os.environ.get("OPENAI_API_KEY")
    def parse_ini(path: str) -> Optional[str]:
        if not os.path.isfile(path): return None
        cp = configparser.ConfigParser(); cp.read(path)
        if cp.has_section("openai") and cp.has_option("openai","api_key"):
            return cp.get("openai","api_key").strip()
        if cp.has_option("DEFAULT","OPENAI_API_KEY"):
            return cp.get("DEFAULT","OPENAI_API_KEY").strip()
        if cp.has_section("keys") and cp.has_option("keys","openai_api_key"):
            return cp.get("keys","openai_api_key").strip()
        for sec in cp.sections():
            for k in ("OPENAI_API_KEY","openai_api_key","api_key"):
                if cp.has_option(sec,k):
                    v = cp.get(sec,k).strip()
                    if v.startswith("sk-"): return v
        return None
    if keys_path:
        k = parse_ini(keys_path)
        if k: return k
        raise RuntimeError(f"OpenAI key not found in: {keys_path}")
    k = parse_ini(os.path.join(os.getcwd(),"keys.ini"))
    if k: return k
    if env_key: return env_key
    raise RuntimeError("OPENAI_API_KEY not found in --keys, ./keys.ini, or environment.")

# ---------- SQLite FTS & Hybrid Retrieval ----------

_FTS_CREATE = """
CREATE VIRTUAL TABLE chunks_fts USING fts5(
  text,
  content='chunks',
  content_rowid='rowid',
  tokenize = 'porter'
);
"""
_TRIGGERS = [
    ("chunks_ai", """
     CREATE TRIGGER chunks_ai AFTER INSERT ON chunks BEGIN
       INSERT INTO chunks_fts(rowid, text) VALUES (new.rowid, new.text);
     END;"""),
    ("chunks_ad", """
     CREATE TRIGGER chunks_ad AFTER DELETE ON chunks BEGIN
       INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.rowid, old.text);
     END;"""),
    ("chunks_au", """
     CREATE TRIGGER chunks_au AFTER UPDATE ON chunks BEGIN
       INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.rowid, old.text);
       INSERT INTO chunks_fts(rowid, text) VALUES(new.rowid, new.text);
     END;"""),
]

def ensure_fts(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks_fts';")
    if not cur.fetchone():
        cur.execute(_FTS_CREATE)
        cur.execute("INSERT INTO chunks_fts(rowid, text) SELECT rowid, text FROM chunks;")
        conn.commit()
    for name, sql in _TRIGGERS:
        cur.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name=?;", (name,))
        if not cur.fetchone():
            cur.execute(sql)
    conn.commit()

def embed_query(client: OpenAI, text: str, model: str) -> List[float]:
    resp = client.embeddings.create(model=model, input=text)
    return resp.data[0].embedding  # type: ignore[return-value]

def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a)!=len(b): return 0.0
    dot=0.0; na=0.0; nb=0.0
    for i in range(len(a)):
        x=a[i]; y=b[i]; dot+=x*y; na+=x*x; nb+=y*y
    if na==0 or nb==0: return 0.0
    return dot / math.sqrt(na*nb)

def _deserialize_vec(val) -> Optional[List[float]]:
    if val is None: return None
    if isinstance(val, (bytes, bytearray)):
        import struct
        return list(struct.unpack("<" + "f"*(len(val)//4), val))
    if isinstance(val, str):
        try:
            arr = json.loads(val)
            if isinstance(arr, list): return [float(x) for x in arr]
        except Exception:
            return None
    return None

def hybrid_retrieve(
    conn: sqlite3.Connection,
    client: OpenAI,
    query: str,
    *,
    embedding_model: str,
    lexical_pool: int,
    top_k: int,
    alpha: float,
    time_decay_days: Optional[int] = None
) -> List[Dict[str, str]]:
    cur = conn.cursor()
    match_q = _sanitize_fts_query(query)
    sql = """
    SELECT c.rowid, c.article_id, c.seq, c.text, a.title, a.canonical_url, a.published_at
    FROM chunks_fts f
    JOIN chunks c ON c.rowid = f.rowid
    JOIN articles a ON a.id = c.article_id
    WHERE chunks_fts MATCH ? 
    ORDER BY rank
    LIMIT ?;
    """
    try:
        cur.execute(sql, (match_q if match_q else query, lexical_pool))
    except sqlite3.OperationalError:
        # Fallback: phrase search over the literal query (quotes escaped)
        phrase = '"' + query.replace('"', ' ') + '"'
        cur.execute(sql, (phrase, lexical_pool))

    rows = cur.fetchall()

    if not rows: return []

    ranks = list(range(1, len(rows)+1))
    rmin, rmax = 1.0, float(len(rows))
    def bm25_norm(idx: int) -> float:
        if rmax==rmin: return 1.0
        return 1.0 - ((idx+1 - rmin) / (rmax - rmin))

    qvec = embed_query(client, query, model=embedding_model)

    cand = []
    for i, (rowid, aid, seq, text, title, url, pub) in enumerate(rows):
        e = cur.execute("SELECT embedding FROM chunk_vectors WHERE article_id=? AND seq=?;", (aid, seq)).fetchone()
        if not e: continue
        vec = _deserialize_vec(e[0])
        if not vec: continue
        cos = _cosine(qvec, vec)
        score = alpha * bm25_norm(i) + (1.0 - alpha) * cos

        if time_decay_days and pub:
            try:
                dt_pub = dt.datetime.fromisoformat(str(pub).replace("Z",""))
                age_days = max((dt.datetime.now() - dt_pub).days, 0)
                decay = math.exp(-age_days / float(time_decay_days))
                score *= decay
            except Exception:
                pass

        cand.append((score, text, title or "", url or "", str(pub) if pub else ""))

    cand.sort(reverse=True, key=lambda x: x[0])

    seen = set(); out=[]
    for score, text, title, url, pub in cand:
        key = hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
        if key in seen: continue
        seen.add(key)
        out.append({"text": text, "title": title, "url": url, "published_at": pub})
        if len(out) >= top_k: break
    return out

# ---------- Orchestration ----------

def generate_blog_with_rag(
    cfg: BlogConfig,
    brief: Optional[str],
    keys_path: Optional[str],
    debug: bool = False,
    trace: bool = False,
    save_trace: Optional[str] = None,
) -> Dict[str, str]:
    start_ts = time.time()
    trace_log: list = []
    trace_obj = {
        "title": cfg.title,
        "topic": cfg.topic,
        "angle": cfg.angle,
        "profanity": cfg.profanity_level,
        "grade_level": cfg.grade_level,
        "retrieval": {
            "db": cfg.db_path,
            "lexical_pool": cfg.lexical_pool,
            "top_k": cfg.top_k,
            "alpha": cfg.alpha,
            "time_decay_days": cfg.time_decay_days,
        },
        "stages": []
    }

    # --- Setup
    _log(debug or trace, "info", "🔑 Loading keys…")
    api_key = _load_openai_key(keys_path)
    os.environ["OPENAI_API_KEY"] = api_key
    client = OpenAI(api_key=api_key)
    _log(debug or trace, "info", "✅ OpenAI client ready")

    if not os.path.isfile(cfg.db_path):
        raise FileNotFoundError(f"DB not found: {cfg.db_path}")

    _log(debug or trace, "info", f"🗄️  Opening DB: {cfg.db_path}")
    conn = sqlite3.connect(cfg.db_path)
    conn.row_factory = sqlite3.Row
    ensure_fts(conn)
    _log(debug, "info", "🧭 Planning subtasks…")

    style_guidance = _profanity_style(cfg.profanity_level, cfg.profanity_frequency, cfg.profanity_per_section)
    readability_guidance = _readability_style(cfg.grade_level)

    # --- Build the master prompt (used by planner/drafter)
    master_prompt = textwrap.dedent(f"""
        Write a detailed, multi-section blog post (opinion/analysis) on:
        Topic: {cfg.topic}
        Angle: {cfg.angle or "challenge the headline; judge by verification, not vibes"}
        Audience: {cfg.audience}
        Tone: {cfg.tone}
        {style_guidance}
        {readability_guidance}

        Structure goals:
        - Catchy lead; a provocative hook that questions the headline narrative.
        - Evidence-focused body USING the provided source snippets.
        - Historical/benchmark context and verification concerns.
        - Counterpoints and limitations.
        - Conclude with "So what, now what" and concrete metrics to watch.

        Constraints:
        - Prefer verifiable claims and concrete mechanisms over slogans.

        {("Additional guidance:\n" + brief) if brief else ""}
    """).strip()

    # --- Planning
    _log(debug or trace, "info", "🧭 Planning subtasks…")
    subtasks = subtasker_agent(master_prompt, client=client,
                               num_subtasks=cfg.num_subtasks,
                               log_dir="Agent_Logs",
                               trace_log=trace_log)
    _log(debug, "info", f"   → {len(subtasks)} subtasks")
    if trace:
        for i, s in enumerate(subtasks, 1):
            _log(True, "debug", f"     [{i}] {s['instruction']}")
    trace_obj["stages"].append({
        "stage": "plan",
        "count_subtasks": len(subtasks),
        "subtasks": [s["instruction"] for s in subtasks]
    })

    # --- Queries + RAG
    hook_hints = []
    for idx, s in enumerate(subtasks, 1):
        _log(debug, "info", f"🔎 Subtask {idx}/{len(subtasks)}: building queries…")
        queries = query_builder_agent(
            subtask_instruction=s["instruction"],
            client=client,
            num_queries=cfg.queries_per_subtask,
            full_prompt=master_prompt,
            trace_log=trace_log
        )
        s["queries"] = queries

        snippets: List[Dict[str, str]] = []
        for j, q in enumerate(queries or [], 1):
            qtext = q.get("query") if isinstance(q, dict) else str(q)
            _log(debug, "info", f"     Q{j}: {qtext}")
            _log(debug, "info", f"       · RAG for Q{j} (lex_pool={cfg.lexical_pool}, top_k={cfg.top_k})")
            res = hybrid_retrieve(
                conn, client, qtext,
                embedding_model=cfg.retrieval_model,
                lexical_pool=cfg.lexical_pool,
                top_k=cfg.top_k,
                alpha=cfg.alpha,
                time_decay_days=cfg.time_decay_days
            )
            _log(debug, "info", f"         → {len(res)} snippets")
            snippets.extend(res)

        # dedup across queries
        seen = set(); grounded = []
        for sn in snippets:
            key = hashlib.sha1(sn["text"].encode("utf-8")).hexdigest()[:16]
            if key in seen: continue
            seen.add(key)
            meta = " | ".join(x for x in [sn["title"], sn["url"], sn["published_at"]] if x)
            cite_hdr = f"[SOURCE] {meta}\n" if meta else ""
            grounded.append(cite_hdr + sn["text"])
        s["retrievals"] = grounded[:cfg.top_k]
        _log(debug, "info", f"     kept {len(s['retrievals'])} grounded snippets for this subtask")

        if queries:
            hook_hints.append(queries[0].get("query") if isinstance(queries[0], dict) else str(queries[0]))


        if trace:
            trace_obj["stages"].append({
                "stage": "retrieve",
                "subtask": s["instruction"],
                "queries": [q["query"] for q in (queries or [])],
                "kept_snippets": len(s["retrievals"])
            })

    # --- Drafting
    _log(debug or trace, "info", "✍️  Drafting sections…")
    for i, s in enumerate(subtasks, 1):
        _log(debug or trace, "info", f"     drafting section {i}/{len(subtasks)}")
        s["draft"] = drafting_agent(
            s, client=client, model=cfg.drafting_model,
            style_hint=style_guidance,
            level=cfg.profanity_level,
            freq=cfg.profanity_frequency,
            per_section=cfg.profanity_per_section,
            trace_log=trace_log
        )
        trace_obj["stages"].append({"stage": "draft", "count": len(subtasks)})

    # --- Consolidation
    _log(debug or trace, "info", "🧵 Consolidating post…")
    final_body = consolidator_agent(
        master_prompt, subtasks, client=client, model=cfg.drafting_model,
        style_hint=style_guidance, level=cfg.profanity_level,
        freq=cfg.profanity_frequency, per_section=cfg.profanity_per_section,trace_log=trace_log, 
    )

    # Post-process profanity (bleep/clean where applicable)
    final_body = _apply_profanity_filter(final_body, cfg.profanity_level)

    # --- Wrap up
    hook = hook_hints[0] if hook_hints else f"The real story behind {cfg.topic} is in the verification math."
    summary = f"{cfg.topic}: " + " ".join(hook.split()[:24])
    fm = _front_matter(cfg, summary)
    md = f"{fm}\n\n# {cfg.title}\n\n{final_body}\n"

    elapsed = time.time() - start_ts
    _log(debug or trace, "info", f"✅ Done in {elapsed:.1f}s")

    trace_obj["llm_calls"] = trace_log

    if save_trace:
        try:
            with open(save_trace, "w", encoding="utf-8") as f:
                json.dump(trace_obj, f, ensure_ascii=False, indent=2)
            _log(True, "info", f"🧾 Trace written: {save_trace}")
        except Exception as e:
            _log(True, "warning", f"Could not write trace JSON: {e}")

    conn.close()

    result = {"markdown": md}
    if cfg.include_social_blurb:
        result["social"] = _social_blurb(cfg, hook)
    return result



# ---------- CLI ----------

def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Agentic Blog Creator with SQLite RAG (news.db).")
    p.add_argument("--title", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--angle", default="")
    p.add_argument("--audience", default="informed general")
    p.add_argument("--tone", default="analytical")
    p.add_argument("--author", default="Editorial Desk")
    p.add_argument("--category", default="analysis")
    p.add_argument("--tags", default="", help="Comma-separated")
    p.add_argument("--hero-image", default=None)

    # Agent knobs
    p.add_argument("--num-subtasks", type=int, default=5)
    p.add_argument("--queries-per-subtask", type=int, default=3)
    p.add_argument("--retrieval-model", default="text-embedding-3-large")
    p.add_argument("--drafting-model", default="gpt-4o")
    p.add_argument("--temperature", type=float, default=0.7)

    # RAG knobs
    p.add_argument("--db", default="news.db")
    p.add_argument("--lexical-pool", type=int, default=120)
    p.add_argument("--top-k", type=int, default=18)
    p.add_argument("--alpha", type=float, default=0.45)
    p.add_argument("--time-decay-days", type=int, default=0, help="0 disables decay")

    # Style knobs
    #p.add_argument("--profanity", choices=["clean","mild","spicy","bleeped"], default="clean")
    p.add_argument("--grade-level", default="12", help="Target grade level (e.g., 9,12,16) or 'auto'")

    p.add_argument("--brief", default=None, help="Optional extra guidance")
    p.add_argument("--keys", default=None, help="Path to keys.ini (or use env)")
    p.add_argument("--output", "-o", default=None, help="Write markdown to file")
    p.add_argument("--print-social", action="store_true")

    p.add_argument("--debug", action="store_true", help="Print stage progress and counts")
    p.add_argument("--trace", action="store_true", help="Verbose: also print subtasks, queries, retrieval stats")
    p.add_argument("--save-trace", default=None, help="Write subtasks/queries/snippets trace to JSON file")

    p.add_argument("--profanity", choices=["clean","mild","spicy","bleeped"], default="clean",
               help="Profanity/edge level.")
    p.add_argument("--profanity-frequency", choices=["scarce","moderate","heavy","custom"], default="moderate",
                help="How often profanity appears across the post.")
    p.add_argument("--profanity-per-section", type=int, default=0,
                help="If frequency=custom, target profanities per section (0+).")


    return p.parse_args(argv)

def main(argv=None) -> int:
    args = _parse_args(argv)

    # Show logs when user asks for them (use --debug)
    _setup_logging(args.debug or args.trace)

    cfg = BlogConfig(
        title=args.title,
        topic=args.topic,
        angle=args.angle,
        audience=args.audience,
        tone=args.tone,
        author=args.author,
        category=args.category,
        tags=[t.strip() for t in args.tags.split(",") if t.strip()],
        hero_image=args.hero_image,
        num_subtasks=args.num_subtasks,
        queries_per_subtask=args.queries_per_subtask,
        drafting_model=args.drafting_model,
        retrieval_model=args.retrieval_model,
        temperature=args.temperature,
        db_path=args.db,
        lexical_pool=args.lexical_pool,
        top_k=args.top_k,
        alpha=args.alpha,
        time_decay_days=(args.time_decay_days or None),
        grade_level=args.grade_level,
        profanity_level=args.profanity,
        profanity_frequency=args.profanity_frequency,
        profanity_per_section=args.profanity_per_section,
    )

    # ---- One-shot run summary (always prints if --debug/--trace) ----
    _log(True if (args.debug or args.trace) else False, "info",
         ("▶ Run summary | "
          f"title={cfg.title!r} | topic={cfg.topic!r} | angle={cfg.angle!r} | "
          f"db={cfg.db_path!r} | models(draft={cfg.drafting_model}, embed={cfg.retrieval_model}) | "
          f"rag(lex_pool={cfg.lexical_pool}, top_k={cfg.top_k}, alpha={cfg.alpha}, "
          f"time_decay={cfg.time_decay_days}) | "
          f"style(profanity={cfg.profanity_level}, grade={cfg.grade_level})"))

    result = generate_blog_with_rag(
        cfg,
        brief=args.brief,
        keys_path=args.keys,
        debug=args.debug,     # enable progress logs
        trace=args.trace,     # (kept for compatibility; not required for minimal logs)
        save_trace=args.save_trace,
    )

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result["markdown"])
        print(f"✅ Wrote: {args.output}")
    else:
        sys.stdout.write(result["markdown"])
        sys.stdout.write("\n")

    if args.print_social and "social" in result:
        print("\n---\n# Social Blurb\n")
        print(result["social"])

    return 0

if __name__ == "__main__":
    raise SystemExit(main())


