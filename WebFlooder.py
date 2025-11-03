import os, json, math, threading, queue, datetime as _dt
from datetime import date, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import sqlite3

# Project modules (must exist)
#import analyze_topics_lib as atl
import guardian_adapter as wf
import gdelt_adapter as gd
from db_schema import ensure_youtube_schema
#from fulltext_fetch import fill_bodies
#import rag_store as rs
#import brief_builder as bb
from youtube_adapter import YouTubeAdapter
from db_schema import map_article_to_topic
from fulltext_fetch import fetch_and_fill_recent_gdelt
import rss_adapter as ra
import hn_adapter as hn

import rss_adapter as rss            
from fulltext_fetch import fetch_and_fill_recent
from content_prep import run_content_prep



def _safe_print(msg: str):
    try:
        print(msg if msg.endswith("\n") else msg + "\n", end="")
    except Exception:
        pass

# Guardian sections (label,id); “All sections” first
GUARDIAN_SECTIONS = [
    ("All sections", ""),
    ("U.S. news", "us-news"),
    ("World", "world"),
    ("UK news", "uk-news"),
    ("Politics", "politics"),
    ("Business", "business"),
    ("Technology", "technology"),
    ("Science", "science"),
    ("Environment", "environment"),
    ("Sport", "sport"),
    ("Culture", "culture"),
    ("Books", "books"),
    ("Film", "film"),
    ("Music", "music"),
    ("TV & Radio", "tv-and-radio"),
    ("Fashion", "fashion"),
    ("Life & Style", "lifeandstyle"),
    ("Travel", "travel"),
    ("Opinion (Comment is Free)", "commentisfree"),
    ("Australia news", "australia-news"),
    ("Media", "media"),
    ("Education", "education"),
    ("Society", "society"),
    ("Law", "law"),
    ("Money", "money"),
]

class LogHandler:
    """Thread-safe log relay from worker to Text widget."""
    def __init__(self, text_widget: tk.Text):
        self.text = text_widget
        self.q = queue.Queue()

    def write(self, msg: str):
        try:
            self.q.put(msg if msg.endswith("\n") else msg + "\n")
        except Exception:
            _safe_print(msg)

    def poll(self):
        try:
            while True:
                msg = self.q.get_nowait()
                self.text.configure(state="normal")
                self.text.insert("end", msg)
                self.text.see("end")
                self.text.configure(state="disabled")
        except queue.Empty:
            pass

class WebFlooderGUI(tk.Tk):
    def __init__(self):
        super().__init__()

        from config_keys import load_keys, get_key

        # Load once and keep on self so it’s always available
        self.keys = load_keys() or {}

        # Convenience: cache these strings
        self._yt_key = (self.keys.get("youtube") or "").strip()
        self.guardian_api_key = (self.keys.get("guardian") or "").strip()



        # ---------- Window ----------
        self.title("WebFlooder")
        self.geometry("1000x900")
        self.minsize(820, 560)

        # ---------- Tk variables (define BEFORE building UI) ----------
        # Source toggles
        self.src_guardian_var = tk.BooleanVar(value=False)
        self.src_gdelt_var    = tk.BooleanVar(value=False)
        self.src_youtube_var  = tk.BooleanVar(value=False)
        self.src_rss_var  = tk.BooleanVar(value=False)
        self.prep_enabled_var = tk.BooleanVar(value=False)

        # Global inputs
        self.topics_var = tk.StringVar(value='"Donald Trump";"President Trump"')
        self.from_var   = tk.StringVar(value="")
        self.to_var     = tk.StringVar(value="")
        self.weeks_var  = tk.StringVar(value="2 weeks")
        self.target_count_var = tk.IntVar(value=50)
        self.lang_var   = tk.StringVar(value="Any")

        # Guardian inputs
        self.section_var = tk.StringVar(value="All sections")
        self.guardian_page_size = tk.IntVar(value=50)

        # GDELT inputs
        self.gdelt_slice_days     = tk.IntVar(value=3)
        self.gdelt_per_slice_cap  = tk.IntVar(value=20)
        self.gdelt_sort           = tk.StringVar(value="DateDesc")
        self.gdelt_timeout        = tk.IntVar(value=20)
        self.gdelt_allow_http     = tk.BooleanVar(value=True)
        self.gdelt_fetch_body     = tk.BooleanVar(value=True)

        # YouTube inputs
        self.yt_api_key_var         = tk.StringVar(self, value="")   # prefilled below from keys.ini
        self.yt_video_id_var        = tk.StringVar(self, value="")
        self.yt_fetch_captions_var  = tk.BooleanVar(self, value=True)

        #self.yt_api_key_var        = tk.StringVar(value="")
        self.yt_mode               = tk.StringVar(value="search")  # search|channel|playlist|video
        self.yt_ident              = tk.StringVar(value="")        # query/URL/ID depending on mode
        self.yt_max                = tk.IntVar(value=20)
        self.yt_lang               = tk.StringVar(value="Any")
        #self.yt_fetch_captions_var = tk.BooleanVar(value=True)
        #self.yt_video_id_var       = tk.StringVar(value="")        # quick video test for mode=video

        # Defaults: fetch inline bodies (best effort) off, centralized fulltext pass on
        self.rss_fetch_body_var     = tk.BooleanVar(value=False)
        self.rss_fulltext_pass_var  = tk.BooleanVar(value=True)
        self.rss_max_items_var      = tk.IntVar(value=30)

        # ---- RSS controls (new) ----
        self.rss_fetch_body_var   = tk.BooleanVar(value=True)
        self.rss_per_host_delay   = tk.DoubleVar(value=0.7)   # polite throttle
        self.rss_min_chars        = tk.IntVar(value=200)      # skip boilerplate

        # Feeds list (semicolon-separated)
        # RSS defaults: high-yield, body-fetchable feeds (no hard paywalls)
        self.rss_feeds_var = tk.StringVar(value=(
            "http://feeds.bbci.co.uk/news/world/rss.xml;"
            "https://www.npr.org/rss/rss.php?id=1001;"
            "https://rss.cnn.com/rss/cnn_us.rss;"
            "https://feeds.foxnews.com/foxnews/politics;"
            "https://www.cbsnews.com/latest/rss/main;"
            "https://news.yahoo.com/rss;"
            "https://www.politico.com/rss/politics-news.xml;"
            "https://thehill.com/feed/"
        ))

        # --- Content Prep controls ---
        self.prep_run_var             = tk.BooleanVar(value=True)   # run prep after ingest
        self.prep_min_words_var       = tk.IntVar(value=120)
        self.prep_min_chars_var       = tk.IntVar(value=220)
        self.prep_min_words_para_var  = tk.IntVar(value=8)
        self.prep_chunk_chars_var     = tk.IntVar(value=1500)
        self.prep_delete_short_var    = tk.BooleanVar(value=False)
        self.prep_limit_rows_var      = tk.IntVar(value=0)          # 0 = no limit
        self.prep_topk_var = tk.IntVar(value=6)
        self.prep_make_snippets_var = tk.BooleanVar(value=True)
        self.prep_index_refresh_var = tk.BooleanVar(value=True)
        self.prep_do_vectorize_var   = tk.BooleanVar(value=False)
        self.prep_recompute_vecs_var = tk.BooleanVar(value=False)
        self.prep_batch_var          = tk.IntVar(value=64)
        self.prep_model_var          = tk.StringVar(value="text-embedding-3-large")
        self.rag_enable_var     = getattr(self, "rag_enable_var",     tk.BooleanVar(value=False))
        self.rag_model_var      = getattr(self, "rag_model_var",      tk.StringVar(value="text-embedding-3-large"))
        self.rag_batch_var      = getattr(self, "rag_batch_var",      tk.IntVar(value=64))
        self.rag_recompute_var  = getattr(self, "rag_recompute_var",  tk.BooleanVar(value=False))

        # Analysis inputs
        self.days_var      = tk.IntVar(value=14)
        self.topn_var      = tk.IntVar(value=20)
        self.halflife_var  = tk.DoubleVar(value=7.0)
        self.view_limit_var = tk.IntVar(value=10)
        self.brief_format_var = tk.StringVar(value="blog")

        # prefill UI fields exactly once
        if not self.yt_api_key_var.get().strip():
            self.yt_api_key_var.set(self.keys.get("youtube", ""))

        if hasattr(self, "guardian_api_key_var") and not self.guardian_api_key_var.get().strip():
            self.guardian_api_key_var.set(self.keys.get("guardian", ""))

        # pass into adapters if they support set_api_key(...)
        if hasattr(self, "yt_adapter") and hasattr(self.yt_adapter, "set_api_key"):
            self.yt_adapter.set_api_key(self.yt_api_key_var.get().strip())

        if hasattr(wf, "set_api_key"):
            wf.set_api_key(self.guardian_api_key_var.get().strip() if hasattr(self, "guardian_api_key_var") else get_key("guardian",""))

        # ---------- Build UI ----------
        self._build_ui()

        # ---------- Logger ----------
        self.logger = LogHandler(self.log_text)

        # ---------- DB connection (one place) ----------
        self.conn = sqlite3.connect("news.db", check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        ensure_youtube_schema(self.conn)

        # ---------- Load self.keys then create adapters ----------
        #self.keys = load_self.keys_from_file_env()
        self.yt_api_key_var.set(self.keys.get("youtube", ""))
        # Guardian: we don’t keep a field in UI; adapter likely reads its own setting
        self.guardian_api_key = self.keys.get("guardian", "")

        # YouTube adapter requires a fetcher; provide a safe minimal one
        def _yt_fetcher(video_id: str, api_key: str | None = None,
                        fetch_captions: bool = True, lang: str = "Any", logger=self.logger.write) -> dict:
            # Fallback stub: lets the adapter run even if it calls fetcher directly.
            now = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            return {
                "video_id": video_id,
                "channel_id": None,
                "channel_title": None,
                "title": f"Video {video_id}",
                "description": None,
                "published_at": now,
                "duration_secs": None,
                "lang": None if lang == "Any" else lang,
                "captions_type": "none",
                "transcript_text": "",
                "chapters": [],
                "yt_metadata": {}
            }

        self.yt_adapter = YouTubeAdapter(conn=self.conn, fetcher=_yt_fetcher, logger=self.logger.write)
        # Pass API key if the adapter supports it
        if hasattr(self.yt_adapter, "set_api_key") and self._yt_key:
            self.yt_adapter.set_api_key(self._yt_key)

        # Guardian key handoff if supported
        if hasattr(wf, "set_api_key") and callable(wf.set_api_key) and self.guardian_api_key:
            try:
                wf.set_api_key(self.guardian_api_key)
            except Exception as e:
                self.logger.write(f"[guardian] could not set api key via set_api_key: {e}")

        self.logger.write("[init] GUI ready, DB opened, schemas ensured, adapters configured.\n")

        # Poll the log queue
        self.after(100, self._poll_logs)

        # Init default dates from weeks
        self._apply_weeks_combo()

    # ---------- UI construction ----------
    def _build_ui(self):
        # Container for input sections
        inputs = ttk.LabelFrame(self, text="News Harvester", padding=10)
        inputs.pack(anchor="nw", padx=10, pady=8)

        def _fixed_row(parent):
            row = ttk.Frame(parent)
            row.grid(sticky="w", padx=0, pady=0)
            # Do NOT give weights; keeps size stable like your other rows
            return row

        # ---- Global ----
        global_outer = ttk.LabelFrame(inputs, text="Global Inputs", padding=(10,8))
        global_outer.grid(row=0, column=0, sticky="nw")

        # Row: topics
        ttk.Label(global_outer, text="Topics (semicolon-separated):").grid(row=0, column=0, sticky="w", padx=(0,8), pady=(0,4))
        ttk.Entry(global_outer, textvariable=self.topics_var, width=72).grid(row=0, column=1, columnspan=7, sticky="w", pady=(0,4))

        # Row: dates + range + articles/language (one row)
        global_row = _fixed_row(global_outer)
        global_row.grid(row=1, column=0, columnspan=8, sticky="w", pady=(2,2))

        ttk.Label(global_row, text="Range:").grid(row=0, column=0, sticky="w")
        weeks_combo = ttk.Combobox(
            global_row, textvariable=self.weeks_var, state="readonly", width=10,
            values=["Custom","1 week","2 weeks","3 weeks","4 weeks","6 weeks","8 weeks","12 weeks","26 weeks","52 weeks"]
        )
        weeks_combo.grid(row=0, column=1, sticky="w", padx=(4,12))
        weeks_combo.bind("<<ComboboxSelected>>", lambda e: self._apply_weeks_combo())

        ttk.Label(global_row, text="From (YYYY-MM-DD):").grid(row=0, column=2, sticky="w")
        from_entry = ttk.Entry(global_row, textvariable=self.from_var, width=12)
        from_entry.grid(row=0, column=3, sticky="w", padx=(4,12))
        from_entry.bind("<FocusOut>", lambda e: self._normalize_iso(self.from_var))

        ttk.Label(global_row, text="To:").grid(row=0, column=4, sticky="w")
        to_entry = ttk.Entry(global_row, textvariable=self.to_var, width=12)
        to_entry.grid(row=0, column=5, sticky="w", padx=(4,12))
        to_entry.bind("<FocusOut>", lambda e: self._normalize_iso(self.to_var))

        ttk.Label(global_row, text="Articles / topic:").grid(row=0, column=6, sticky="w", padx=(16,8))
        ttk.Spinbox(global_row, from_=10, to=500, increment=10, textvariable=self.target_count_var, width=10)\
            .grid(row=0, column=7, sticky="w")

        ttk.Label(global_row, text="Language:").grid(row=0, column=8, sticky="w", padx=(16,8))
        ttk.Combobox(global_row, textvariable=self.lang_var, state="readonly", width=12,
                     values=["Any","en","es","fr","de","ru","ar","zh","pt","it","tr","he","uk","ja","hi"])\
            .grid(row=0, column=9, sticky="w")

        # ---- Guardian (one row) ----
        guardian_outer = ttk.LabelFrame(inputs, text="Guardian", padding=(10,8))
        guardian_outer.grid(row=1, column=0, sticky="nw", pady=(8,0))
        g_row = _fixed_row(guardian_outer)

        ttk.Checkbutton(g_row, text="", variable=self.src_guardian_var,
                        command=lambda: self._toggle_row(g_row, self.src_guardian_var)).grid(row=0, column=0, sticky="w", padx=(0,12))

        ttk.Label(g_row, text="Section:").grid(row=0, column=1, sticky="w", padx=(0,6))
        ttk.Combobox(g_row, textvariable=self.section_var, state="readonly", width=28,
                     values=[label for (label, _id) in GUARDIAN_SECTIONS]).grid(row=0, column=2, sticky="w")

        self._toggle_row(g_row, self.src_guardian_var)

        # ---- GDELT (one row) ----
        gdelt_outer = ttk.LabelFrame(inputs, text="GDELT", padding=(10,8))
        gdelt_outer.grid(row=2, column=0, sticky="nw", pady=(8,0))
        d_row = _fixed_row(gdelt_outer)

        ttk.Checkbutton(d_row, text="", variable=self.src_gdelt_var,
                        command=lambda: self._toggle_row(d_row, self.src_gdelt_var)).grid(row=0, column=0, sticky="w", padx=(0,12))

        """
        ttk.Label(d_row, text="Slice (days):").grid(row=0, column=1, sticky="w", padx=(0,6))
        ttk.Spinbox(d_row, from_=1, to=14, increment=1, textvariable=self.gdelt_slice_days, width=6)\
            .grid(row=0, column=2, sticky="w")

        ttk.Label(d_row, text="Per-slice cap:").grid(row=0, column=3, sticky="w", padx=(16,6))
        ttk.Spinbox(d_row, from_=5, to=200, increment=5, textvariable=self.gdelt_per_slice_cap, width=8)\
            .grid(row=0, column=4, sticky="w")

        ttk.Label(d_row, text="Sort:").grid(row=0, column=5, sticky="w", padx=(16,6))
        ttk.Combobox(d_row, textvariable=self.gdelt_sort, state="readonly", width=12,
                     values=["DateDesc", "Relevance"]).grid(row=0, column=6, sticky="w")

        ttk.Label(d_row, text="Timeout (s):").grid(row=0, column=7, sticky="w", padx=(16,6))
        ttk.Spinbox(d_row, from_=5, to=120, increment=5, textvariable=self.gdelt_timeout, width=8)\
            .grid(row=0, column=8, sticky="w")

        ttk.Checkbutton(d_row, text="Allow HTTP fallback", variable=self.gdelt_allow_http)\
            .grid(row=0, column=9, sticky="w", padx=(16,0))

        ttk.Checkbutton(d_row, text="Fetch full body", variable=self.gdelt_fetch_body)\
            .grid(row=0, column=10, sticky="w", padx=(16,0))
        """
        self._toggle_row(d_row, self.src_gdelt_var)

        # ---- YouTube (one row) ----
        yt_outer = ttk.LabelFrame(inputs, text="YouTube", padding=(10,8))
        yt_outer.grid(row=3, column=0, sticky="nw", pady=(8,0))
        y_row = _fixed_row(yt_outer)

        ttk.Checkbutton(y_row, text="", variable=self.src_youtube_var,
                        command=lambda: self._toggle_row(y_row, self.src_youtube_var)).grid(row=0, column=0, sticky="w", padx=(0,12))

        ttk.Label(y_row, text="Mode:").grid(row=0, column=1, sticky="w")
        ttk.Combobox(y_row, textvariable=self.yt_mode, state="readonly", width=10,
                     values=["search","channel","playlist","video"]).grid(row=0, column=2, sticky="w", padx=(4,10))

        ttk.Label(y_row, text="Query / URL / ID:").grid(row=0, column=3, sticky="w")
        ttk.Entry(y_row, textvariable=self.yt_ident, width=25).grid(row=0, column=4, columnspan=2, sticky="w", padx=(4,10))

        ttk.Label(y_row, text="Max videos:").grid(row=0, column=6, sticky="w")
        ttk.Spinbox(y_row, from_=1, to=200, textvariable=self.yt_max, width=6).grid(row=0, column=7, sticky="w", padx=(4,10))

        ttk.Label(y_row, text="Captions lang:").grid(row=0, column=8, sticky="w")
        ttk.Combobox(y_row, textvariable=self.yt_lang, state="readonly", width=8,
                     values=["Any","en","es","fr","de","ru","ar","pt","it","tr","he","uk","ja","hi","zh"])\
            .grid(row=0, column=9, sticky="w", padx=(4,10))

        ttk.Checkbutton(y_row, text="Fetch captions", variable=self.yt_fetch_captions_var)\
            .grid(row=0, column=10, sticky="w", padx=(4,10))

        self._toggle_row(y_row, self.src_youtube_var)

        # ---- RSS / Atom (one row) ----
        rss_outer = ttk.LabelFrame(inputs, text="RSS Feeds", padding=(10,8))
        rss_outer.grid(row=4, column=0, sticky="nw", pady=(8,0))  # adjust 'row' index if needed
        r_row = _fixed_row(rss_outer)

        # Enable/disable row
        ttk.Checkbutton(
            r_row, text="", variable=self.src_rss_var,
            command=lambda: self._toggle_row(r_row, self.src_rss_var)
        ).grid(row=0, column=0, sticky="w", padx=(0,12))

        # Feeds entry
        ttk.Label(r_row, text="Feeds").grid(row=0, column=1, sticky="w")
        ttk.Entry(r_row, textvariable=self.rss_feeds_var, width=60)\
            .grid(row=0, column=2, columnspan=6, sticky="w", padx=(6,10))

        # Max items
        ttk.Label(r_row, text="Max items").grid(row=0, column=8, sticky="w", padx=(6,4))
        ttk.Spinbox(r_row, from_=5, to=200, increment=5, textvariable=self.rss_max_items_var, width=6)\
            .grid(row=0, column=9, sticky="w")
        
        ttk.Checkbutton(r_row, text="Fetch body", variable=self.rss_fetch_body_var)\
        .grid(row=0, column=10, sticky="w", padx=(12,6))

        ttk.Checkbutton(r_row, text="Text cleanup", variable=self.rss_fulltext_pass_var)\
        .grid(row=0, column=11, sticky="w", padx=(6,0))

        # Start disabled until the source is checked
        self._toggle_row(r_row, self.src_rss_var)

        # --- Content Prep (own container) -------------------------------------------
        prep_container = ttk.Frame(inputs)
        prep_container.grid(row=97, column=0, columnspan=12,
                            sticky="ew", padx=6, pady=(8, 6))
        for c in range(12):
            prep_container.columnconfigure(c, weight=1)

        ttk.Label(prep_container, text="Content Prep", style="Header.TLabel")\
        .grid(row=0, column=0, sticky="w", pady=(0, 4))

        prep_frame = ttk.LabelFrame(prep_container, text="Prepare & Cache", padding=(8, 8))
        prep_frame.grid(row=1, column=0, columnspan=12, sticky="ew")
        for c in range(12):
            prep_frame.columnconfigure(c, weight=1)

        # One fixed row like the others, with a left-side enable checkbox
        prep_row = ttk.Frame(prep_frame)
        prep_row.grid(row=0, column=0, columnspan=12, sticky="w")

        # Enable/disable row
        ttk.Checkbutton(
            prep_row, text="", variable=self.prep_enabled_var,
            command=lambda: self._toggle_row(prep_row, self.prep_enabled_var)
        ).grid(row=0, column=0, sticky="w", padx=(0,12))

        # Controls (shifted right by 1 column)
        ttk.Label(prep_row, text="Min words/article:").grid(row=0, column=1, sticky="w")
        ttk.Entry(prep_row, textvariable=self.prep_min_words_var, width=8)\
        .grid(row=0, column=2, sticky="w", padx=(4, 16))

        ttk.Label(prep_row, text="Min chars/article:").grid(row=0, column=3, sticky="w")
        ttk.Entry(prep_row, textvariable=self.prep_min_chars_var, width=8)\
        .grid(row=0, column=4, sticky="w", padx=(4, 16))

        ttk.Label(prep_row, text="Top-K keyphrases:").grid(row=0, column=5, sticky="w")
        ttk.Entry(prep_row, textvariable=self.prep_topk_var, width=6)\
        .grid(row=0, column=6, sticky="w", padx=(4, 16))

        ttk.Checkbutton(prep_row, text="Build snippets", variable=self.prep_make_snippets_var)\
        .grid(row=0, column=7, sticky="w", padx=(4, 16))
        ttk.Checkbutton(prep_row, text="Refresh vector index", variable=self.prep_index_refresh_var)\
        .grid(row=0, column=8, sticky="w", padx=(4, 16))

        # Start disabled until the checkbox is checked
        self._toggle_row(prep_row, self.prep_enabled_var)

        # ---- RAG / Vectorization (one row) ----
        rag_outer = ttk.LabelFrame(inputs, text="RAG / Vectorization", padding=(10,8))
        rag_outer.grid(row=98, column=0, sticky="nw", pady=(8,0))   # adjust row index if needed
        rag_row = _fixed_row(rag_outer)

        # Enable/disable row (leftmost checkbox)
        ttk.Checkbutton(
            rag_row, text="", variable=self.rag_enable_var,
            command=lambda: self._toggle_row(rag_row, self.rag_enable_var)
        ).grid(row=0, column=0, sticky="w", padx=(0,12))

        # Model
        ttk.Label(rag_row, text="Model:").grid(row=0, column=1, sticky="w")
        ttk.Entry(rag_row, textvariable=self.rag_model_var, width=28)\
            .grid(row=0, column=2, sticky="w", padx=(4,12))

        # Batch size
        ttk.Label(rag_row, text="Batch:").grid(row=0, column=3, sticky="w")
        ttk.Spinbox(rag_row, from_=8, to=512, increment=8, textvariable=self.rag_batch_var, width=6)\
            .grid(row=0, column=4, sticky="w", padx=(4,12))

        # Recompute
        ttk.Checkbutton(rag_row, text="Recompute all", variable=self.rag_recompute_var)\
            .grid(row=0, column=5, sticky="w", padx=(4,12))

        # Optional: show where vectors live / status (non-editable)
        ttk.Label(rag_row, text="Index:").grid(row=0, column=6, sticky="w")
        ttk.Label(rag_row, text="vectors.sqlite (chunks)", foreground="#777")\
            .grid(row=0, column=7, sticky="w", padx=(4,0))

        # Start disabled until the source is checked
        self._toggle_row(rag_row, self.rag_enable_var)

        # ---- Controls (Run/Stop) ----
        controls = ttk.Frame(inputs)
        controls.grid(row=99, column=0, sticky="ew", pady=(8,0))
        self.run_btn  = ttk.Button(controls, text="Run",  command=self.on_run)
        self.stop_btn = ttk.Button(controls, text="Stop", command=self.on_stop, state="disabled")
        self.run_btn.grid(row=0, column=0, sticky="w", padx=(0,8))
        self.stop_btn.grid(row=0, column=1, sticky="w")

        # ---- Progress + Log ----
        prog = ttk.Frame(self, padding=(10,0,10,0)); prog.pack(fill="x")
        self.progress = ttk.Progressbar(prog, mode="indeterminate")
        self.progress.pack(fill="x")

        log_frame = ttk.LabelFrame(self, text="Log", padding=8)
        log_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.log_text = tk.Text(log_frame, wrap="word", height=18, state="disabled")
        self.log_text.pack(fill="both", expand=True)

    # ---------- Helpers ----------
    def _poll_logs(self):
        if hasattr(self, "logger"):
            self.logger.poll()
        self.after(100, self._poll_logs)

    def _toggle_row(self, row: ttk.Frame, var: tk.BooleanVar):
        enabled = bool(var.get())
        for child in row.winfo_children():
            # keep the enabling checkbox clickable
            if isinstance(child, ttk.Checkbutton) and child.cget("text") == "":
                child.configure(state="normal")
                continue
            try:
                child.configure(state=("normal" if enabled else "disabled"))
            except tk.TclError:
                pass

    def _normalize_iso(self, sv: tk.StringVar):
        s = (sv.get() or "").strip()
        try:
            y, m, d = s.split("-")
            _ = date(int(y), int(m), int(d))
            sv.set(f"{int(y):04d}-{int(m):02d}-{int(d):02d}")
        except Exception:
            # Leave user input alone if not valid; analysis/ingest can still run without dates
            pass

    def _apply_weeks_combo(self):
        val = self.weeks_var.get()
        if val == "Custom":
            return
        try:
            n = int(val.split()[0])
        except Exception:
            n = 2
        today = date.today()
        start = today - timedelta(days=n*7 - 1)
        self.from_var.set(start.isoformat())
        self.to_var.set(today.isoformat())

    def _slug(self, s: str) -> str:
        import re
        s = re.sub(r'["“”‘’]', "", s)
        s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
        return s or "topic"

    def _first_topic(self) -> str:
        raw = self.topics_var.get().strip()
        parts = [t.strip() for t in raw.split(";") if t.strip()]
        return parts[0] if parts else ""

    def _timebox(self):
        since = self.from_var.get().strip() or None
        until = self.to_var.get().strip() or None
        return since, until

    def _compute_paging(self, desired: int) -> tuple[int, int]:
        desired = max(1, int(desired))
        page_size = min(50, max(10, desired if desired < 50 else 50))
        pages = max(1, math.ceil(desired / page_size))
        return pages, page_size

    # ---------- Buttons ----------
    def on_run(self):
        self._stop_flag = False
        self.run_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.progress.start(80)

        topics_raw = self.topics_var.get().strip()
        topics = [t.strip() for t in topics_raw.split(";") if t.strip()]
        if not topics:
            messagebox.showerror("Input error", "Please enter at least one topic (semicolon-separated).")
            self.progress.stop(); self.run_btn.config(state="normal"); self.stop_btn.config(state="disabled")
            return

        # Guardian section
        section_lookup = {label: sid for (label, sid) in GUARDIAN_SECTIONS}
        section = section_lookup.get(self.section_var.get(), "") or None

        from_date = self.from_var.get().strip() or None
        to_date   = self.to_var.get().strip() or None

        desired = max(1, int(self.target_count_var.get()))
        pages, page_size = self._compute_paging(desired)
        gdelt_max = min(50, desired)

        # Existing sources
        use_guardian = bool(self.src_guardian_var.get())
        use_gdelt    = bool(self.src_gdelt_var.get())
        use_youtube  = bool(self.src_youtube_var.get())

        # NEW sources
        use_rss = bool(getattr(self, "src_rss_var", tk.BooleanVar(value=False)).get())
        
        # YouTube controls
        yt_mode  = self.yt_mode.get()
        yt_ident = self.yt_ident.get().strip()
        yt_max   = int(self.yt_max.get())
        yt_lang  = self.yt_lang.get()
        yt_key   = (self.yt_api_key_var.get() or "").strip()
        fetch_caps = bool(self.yt_fetch_captions_var.get())

        # NEW source controls
        rss_feeds     = (getattr(self, "rss_feeds_var", tk.StringVar(value="")).get() or "").strip()
        rss_feed_list = [u.strip() for u in rss_feeds.split(";") if u.strip()]
        rss_max_items = int(getattr(self, "rss_max_items_var", tk.IntVar(value=30)).get())

        yt_ident_label = yt_ident if yt_ident else ("<per-topic searches>" if yt_mode == "search" else "<none>")
        self.logger.write(
            "[plan] sources:"
            f" guardian={use_guardian}, gdelt={use_gdelt}, youtube={use_youtube},"
            f" rss={use_rss} "
            f"topics={topics} | dates=({from_date or '∅'},{to_date or '∅'}) | target/article={desired} | "
            f"yt(mode={yt_mode}, ident={yt_ident_label}, max={yt_max}, lang={yt_lang}) | "
            f"rss(max_items={rss_max_items}, feeds={len(rss_feed_list)})"
        )

        #if not (use_guardian or use_gdelt or use_youtube or use_rss):
        #    messagebox.showinfo("No source selected",
        #                        "Please tick at least one source (Guardian, GDELT, YouTube, RSS).")
        #    self.progress.stop(); self.run_btn.config(state="normal"); self.stop_btn.config(state="disabled")
        #    return

        start_ts = _dt.datetime.now().timestamp()

        def worker():
            total_inserted = 0
            try:
                # ---------- Guardian ----------
                if use_guardian and not self._stop_flag:
                    self.logger.write(f"[plan] Guardian: ~{desired} articles/topic → {pages} page(s) × {page_size}")
                    for topic in topics:
                        if self._stop_flag: break
                        self.logger.write(f"[guardian topic] {topic}")
                        try:
                            inserted = wf.run_ingest(
                                topics=[topic],
                                pages=pages,
                                page_size=page_size,
                                section=section,
                                from_date=from_date,
                                to_date=to_date,
                                log_fn=self.logger.write,
                            )
                            total_inserted += int(inserted or 0)
                        except Exception as e:
                            self.logger.write(f"[guardian error] {e}")

                # ---------- GDELT ----------
                if use_gdelt and not self._stop_flag:
                    self.logger.write(f"[plan] GDELT: up to {gdelt_max} records/topic (DateDesc)")
                    for topic in topics:
                        if self._stop_flag: break
                        self.logger.write(f"[gdelt topic] {topic}")
                        try:
                            stats = gd.ingest_gdelt(
                                query=topic,
                                since=from_date,
                                until=to_date,
                                max_records=gdelt_max,
                                slice_days=int(self.gdelt_slice_days.get()),
                                per_slice_cap=int(self.gdelt_per_slice_cap.get()),
                                max_seconds=int(self.gdelt_timeout.get()),
                                log_fn=self.logger.write,
                                stop_cb=lambda: self._stop_flag,
                            )
                            self.logger.write(
                                f"[gdelt stats] fetched={stats.get('fetched',0)} "
                                f"inserted={stats.get('inserted',0)} "
                                f"duplicates={stats.get('duplicates',0)}"
                            )
                            total_inserted += int(stats.get("inserted", 0))
                        except Exception as e:
                            self.logger.write(f"[gdelt error] {e}")

                # After GDELT, optionally fetch page bodies for fresh GDELT URLs
                if use_gdelt and not self._stop_flag and bool(self.gdelt_fetch_body.get()):
                    try:
                        from fulltext_fetch import fetch_and_fill_recent_gdelt
                        db_path = getattr(self, "db_path", "news.db")
                        self.logger.write("[fulltext] Fetching bodies for recent GDELT URLs (limit=150) ...")
                        filled = fetch_and_fill_recent_gdelt(
                            db_path=db_path,
                            limit=150,
                            log_fn=self.logger.write,
                        )
                        self.logger.write(f"[fulltext] filled bodies for {filled} article(s).")
                    except Exception as e:
                        self.logger.write(f"[fulltext error] {e}")

                # ---------- RSS ----------
                if use_rss and not self._stop_flag:
                    try:
                        raw = (self.rss_feeds_var.get() or "").strip()
                        # If entry box empty, fall back to the same default list
                        if not raw:
                            raw = (
                                "http://feeds.bbci.co.uk/news/world/rss.xml;"
                                "https://www.npr.org/rss/rss.php?id=1001;"
                                "https://rss.cnn.com/rss/cnn_us.rss;"
                                "https://feeds.foxnews.com/foxnews/politics;"
                                "https://www.cbsnews.com/latest/rss/main;"
                                "https://news.yahoo.com/rss;"
                                "https://www.politico.com/rss/politics-news.xml;"
                                "https://thehill.com/feed/"
                            )

                        feeds = [u.strip() for u in raw.split(";") if u.strip()]
                        self.logger.write(f"[plan] RSS: {len(feeds)} feed(s), max_items={int(self.rss_max_items_var.get())} per run")

                        import rss_adapter as rsx
                        stats = rsx.ingest_rss_multi(
                            self.conn,
                            feeds,
                            max_items_per_feed=int(self.rss_max_items_var.get()),
                            # Let adapter try to fetch bodies inline (lightweight).
                            # If you prefer only the centralized fulltext pass, set this False.
                            fetch_body=False, #bool(self.rss_fetch_body_var.get()),
                            per_host_delay=0.5,                # gentle throttle
                            log_fn=self.logger.write,
                        )
                        self.logger.write(
                            f"[rss stats] feeds={stats.get('feeds',0)} "
                            f"fetched={stats.get('fetched',0)} "
                            f"inserted={stats.get('inserted',0)} "
                            f"duplicates={stats.get('duplicates',0)}"
                        )

                        # Optional: centralized full-text fill for RSS
                        if use_rss and not self._stop_flag and bool(self.rss_fulltext_pass_var.get()):
                            try:
                                from fulltext_fetch import fetch_and_fill_recent_rss

                                filled = fetch_and_fill_recent_rss(
                                    db_path=getattr(self, "db_path", "news.db"),
                                    limit=200,
                                    log_fn=self.logger.write,
                                    # tune thresholds as you like:
                                    min_chars_to_write=800,
                                    min_words=100,
                                    delete_short=True,
                                    # and pass the paragraph gates the “brutal” version expects:
                                    para_min_len=120,
                                    para_min_words=10,
                                )
                                self.logger.write(f"[rss fulltext] filled bodies for {filled} article(s).")
                            except Exception as e:
                                self.logger.write(f"[rss fulltext error] {e}")

                    except Exception as e:
                        self.logger.write(f"[rss error] {e}")


                # ---------- YouTube ----------
                if use_youtube and not self._stop_flag:
                    if not hasattr(self, "yt_adapter"):
                        self.logger.write("[youtube] adapter not configured (self.yt_adapter missing).\n")
                    elif not self._yt_key:
                        self.logger.write("[youtube] No YouTube API key (load from keys.ini/env at startup).\n")
                    else:
                        fetch_caps = bool(self.yt_fetch_captions_var.get()) if hasattr(self, "yt_fetch_captions_var") else True
                        mode       = (self.yt_mode.get() if hasattr(self, "yt_mode") else "search")
                        ident      = (self.yt_ident.get().strip() if hasattr(self, "yt_ident") else "")
                        max_v      = int(self.yt_max.get()) if hasattr(self, "yt_max") else 20
                        lang       = (self.yt_lang.get() if hasattr(self, "yt_lang") else "Any")

                        try:
                            if mode == "video":
                                vid = (self.yt_video_id_var.get() if hasattr(self, "yt_video_id_var") else "").strip() or ident
                                if not vid:
                                    self.logger.write("[youtube] Mode=video but no Video ID; skipping.\n")
                                else:
                                    topic = (topic or "").strip() if "topic" in locals() else ""
                                    self.logger.write(f"[youtube] ingest video_id={vid}\n")
                                    ins = self.yt_adapter.ingest_by_video_id(
                                        vid, fetch_captions=fetch_caps, lang=lang
                                    )
                                    art_id = self.yt_adapter.mirror_video_into_articles(vid)
                                    self.logger.write(
                                        f"[youtube] inserted={int(ins or 0)} mirrored_article_id={art_id}\n"
                                    )
                                    total_inserted += int(ins or 0)
                                    try:
                                        if art_id and topic:
                                            con = getattr(self, "conn", None) or getattr(self.yt_adapter, "conn", None)
                                            if con is not None:
                                                map_article_to_topic(con, art_id, topic)
                                                self.logger.write(f"[youtube] topic_mapped article_id={art_id} topic={topic}\n")
                                            else:
                                                self.logger.write("[youtube][warn] no sqlite connection found for topic mapping\n")
                                    except Exception as e:
                                        self.logger.write(f"[youtube][warn] topic mapping failed: {e}\n")

                            elif mode == "search":
                                queries = [ident] if ident else topics
                                for q in queries:
                                    if self._stop_flag:
                                        break
                                    self.logger.write(f"[youtube search] q={q!r} max={max_v} lang={lang}\n")
                                    stats = self.yt_adapter.ingest_from_search_query(
                                        query=q, api_key=self._yt_key, max_videos=max_v, lang=lang,
                                        fetch_captions=fetch_caps, since=from_date, until=to_date,
                                        log_fn=self.logger.write, stop_cb=lambda: self._stop_flag,
                                    )
                                    self.logger.write(
                                        f"[youtube stats] query={q!r} fetched={stats.get('fetched',0)} "
                                        f"inserted={stats.get('inserted',0)} duplicates={stats.get('duplicates',0)}\n"
                                    )
                                    total_inserted += int(stats.get("inserted", 0))

                            elif mode == "channel":
                                if not ident:
                                    self.logger.write("[youtube] Mode=channel but no channel ID/URL; skipping.\n")
                                else:
                                    self.logger.write(f"[youtube channel] ident={ident} max={max_v} lang={lang}\n")
                                    stats = self.yt_adapter.ingest_from_channel(
                                        ident, api_key=self._yt_key, max_videos=max_v, lang=lang,
                                        fetch_captions=fetch_caps, since=from_date, until=to_date,
                                        log_fn=self.logger.write, stop_cb=lambda: self._stop_flag,
                                    )
                                    self.logger.write(
                                        f"[youtube stats] channel fetched={stats.get('fetched',0)} "
                                        f"inserted={stats.get('inserted',0)} duplicates={stats.get('duplicates',0)}\n"
                                    )
                                    total_inserted += int(stats.get("inserted", 0))

                            elif mode == "playlist":
                                if not ident:
                                    self.logger.write("[youtube] Mode=playlist but no playlist ID/URL; skipping.\n")
                                else:
                                    self.logger.write(f"[youtube playlist] ident={ident} max={max_v} lang={lang}\n")
                                    stats = self.yt_adapter.ingest_from_playlist(
                                        ident, api_key=self._yt_key, max_videos=max_v, lang=lang,
                                        fetch_captions=fetch_caps, since=from_date, until=to_date,
                                        log_fn=self.logger.write, stop_cb=lambda: self._stop_flag,
                                    )
                                    self.logger.write(
                                        f"[youtube stats] playlist fetched={stats.get('fetched',0)} "
                                        f"inserted={stats.get('inserted',0)} duplicates={stats.get('duplicates',0)}\n"
                                    )
                                    total_inserted += int(stats.get("inserted", 0))
                            else:
                                self.logger.write(f"[youtube] Unknown mode={mode!r}\n")

                        except Exception as e:
                            self.logger.write(f"[youtube error] {e}\n")

                # --- Content Prep (optional; runs if checkbox is checked) -------------------
                if hasattr(self, "prep_enabled_var") and bool(self.prep_enabled_var.get()):
                    self.logger.write("[prep] Preparing content (clean → summaries → chunks → quotes/facts)…")

                    # Import the runner from your content_prep.py
                    from content_prep import run_content_prep as _run_prep

                    # Collect UI-configured values (with safe defaults if fields are absent)
                    db_path   = getattr(self, "db_path", "news.db")
                    min_chars = int(self.prep_min_chars_var.get() or 200) if hasattr(self, "prep_min_chars_var") else 200
                    min_words = int(self.prep_min_words_var.get() or 100) if hasattr(self, "prep_min_words_var") else 100

                    # Optional tunables; you can wire these to UI later if you want
                    min_words_per_paragraph = 8
                    chunk_chars             = 1500
                    max_quotes              = 8
                    max_facts               = 8
                    delete_short            = False      # keep rows; mark/clean only
                    limit_rows              = None       # process all available this pass

                    # Build desired kwargs, then filter by the function signature (defensive)
                    desired_kwargs = {
                        "db_path": db_path,
                        "per_host_delay": 0.0,  # ignored by current impl, kept for symmetry
                        "min_chars": min_chars,
                        "min_words": min_words,
                        "min_words_per_paragraph": min_words_per_paragraph,
                        "chunk_chars": chunk_chars,
                        "max_quotes": max_quotes,
                        "max_facts": max_facts,
                        "delete_short": delete_short,
                        "limit_rows": limit_rows,
                        "log_fn": self.logger.write,
                        "stop_cb": lambda: bool(getattr(self, "_stop_flag", False)),
                    }
                    import inspect
                    sig = inspect.signature(_run_prep)
                    filtered_kwargs = {k: v for k, v in desired_kwargs.items() if k in sig.parameters}

                    # Run prep
                    stats = _run_prep(**filtered_kwargs) or {}
                    self.logger.write(
                        "[prep] Done. processed={processed} updated={updated} deleted={deleted}".format(
                            processed=stats.get("processed", 0),
                            updated=stats.get("updated", 0),
                            deleted=stats.get("deleted", 0),
                        )
                    )

                # --- RAG Prep -------------------
                from rag_prep import run_rag_prep

                # ... inside on_run's worker after harvesting/prep, gated by your checkbox:
                if bool(self.rag_enable_var.get()):
                    # --- Resolve DB path robustly to a plain string ---
                    db_path = None
                    # 1) if you've stored it earlier as a string
                    if hasattr(self, "db_path") and isinstance(getattr(self, "db_path"), str):
                        db_path = self.db_path.strip() or None

                    # 2) or if you keep it in a Tk StringVar like self.db_var / self.db_path_var
                    if not db_path:
                        for attr in ("db_var", "db_path_var"):
                            if hasattr(self, attr):
                                try:
                                    val = getattr(self, attr).get().strip()
                                    if val:
                                        db_path = val
                                        break
                                except Exception:
                                    pass

                    # 3) final fallback
                    db_path = db_path or "news.db"

                    # (optional) log it once for sanity
                    self.logger.write(f"[rag] using DB: {db_path}")

                    # --- Now call the vectorizer with the resolved path ---
                    from rag_prep import run_rag_prep

                    topics_any = None
                    try:
                        raw_topics = (self.rag_topics_any_var.get() if hasattr(self, "rag_topics_any_var") else "")
                        topics_any = [t.strip() for t in raw_topics.split(";") if t.strip()] or None
                    except Exception:
                        topics_any = None

                    stats = run_rag_prep(
                        db_path=db_path,
                        model=(self.rag_model_var.get().strip() if hasattr(self, "rag_model_var") else "text-embedding-3-small") or "text-embedding-3-small",
                        batch_size=int(self.rag_batch_var.get() if hasattr(self, "rag_batch_var") else 64),
                        recompute_all=bool(self.rag_recompute_var.get() if hasattr(self, "rag_recompute_var") else False),
                        date_from=(self.rag_date_from_var.get().strip() if hasattr(self, "rag_date_from_var") else "") or None,
                        date_to=(self.rag_date_to_var.get().strip() if hasattr(self, "rag_date_to_var") else "") or None,
                        topics_any=topics_any,
                        limit_rows=None,
                        log_fn=self.logger.write,
                        stop_cb=lambda: self._stop_flag,
                    )
                    self.logger.write(f"[rag] {stats}")
           

                elapsed = _dt.datetime.now().timestamp() - start_ts
                self.logger.write(f"[summary] Inserted {total_inserted} new items across selected sources. Elapsed: {elapsed:.1f}s")
            except Exception as e:
                import traceback
                self.logger.write("[worker error]\n" + "".join(traceback.format_exception(e)))
            finally:
                self.after(0, self._done)

        threading.Thread(target=worker, daemon=True).start()


    def on_stop(self):
        self._stop_flag = True
        self.logger.write("[ui] Stop requested...")

    def _done(self):
        self.progress.stop()
        self.run_btn.config(state="normal")
        self.stop_btn.config(state="disabled")

    """
    def on_analyze(self):
        self.analyze_btn.config(state="disabled")
        self.progress.start(80)

        days = max(1, int(self.days_var.get()))
        topn = max(5, int(self.topn_var.get()))
        halflife = float(self.halflife_var.get())

        def _format_topics(topics, max_show=5):
            lines = []
            if not topics:
                return "  (no topics returned)"
            for i, t in enumerate(topics[:max_show], start=1):
                label = score = support = None
                examples = None
                if isinstance(t, dict):
                    label = t.get("label") or t.get("topic") or t.get("name") or t.get("key") or t.get("term")
                    score = t.get("score") or t.get("weight") or t.get("prominence") or t.get("value")
                    support = t.get("support") or t.get("count") or t.get("articles") or t.get("n")
                    examples = t.get("examples") or t.get("sample_ids") or t.get("sample_urls")
                elif isinstance(t, (list, tuple)):
                    if len(t) >= 1: label = t[0]
                    if len(t) >= 2: score = t[1]
                    if len(t) >= 3: support = t[2]
                else:
                    label = str(t)
                head = f"{i}. {label}" if label else f"{i}. (unnamed topic)"
                tail_bits = []
                if score is not None:
                    try: tail_bits.append(f"score={float(score):.2f}")
                    except Exception: tail_bits.append(f"score={score}")
                if support is not None:
                    tail_bits.append(f"support={support}")
                lines.append(head + ("  " + " | ".join(tail_bits) if tail_bits else ""))
                if examples:
                    if isinstance(examples, (list, tuple)):
                        ex_list = [str(x) for x in examples[:3]]
                    else:
                        ex_list = [str(examples)]
                    lines.append("     examples: " + "; ".join(ex_list))
            if len(topics) > max_show:
                lines.append(f"  ... and {len(topics) - max_show} more")
            return "\n".join(lines)

        def worker():
            import time, traceback
            t0 = time.time()
            try:
                self.logger.write("\n[analysis] Starting analysis...")
                self.logger.write(f"[analysis] Parameters: days_window={days}, top_n={topn}, half_life={halflife}")

                upd, kp, ent, topics = atl.run_analysis(days_window=days, top_n=topn, half_life=halflife)

                self.logger.write(f"[analysis] Enrichment updated rows: {upd}")

                self.logger.write("[analysis] Top keyphrases:")
                if kp:
                    for k, s in kp:
                        try: self.logger.write(f"  - {k:40}  {float(s):.2f}")
                        except Exception: self.logger.write(f"  - {k:40}  {s}")
                else:
                    self.logger.write("  (none)")

                self.logger.write("\n[analysis] Top entities:")
                if ent:
                    for k, s in ent:
                        try: self.logger.write(f"  - {k:40}  {float(s):.2f}")
                        except Exception: self.logger.write(f"  - {k:40}  {s}")
                else:
                    self.logger.write("  (none)")

                self.logger.write("\n[analysis] Top topics (best-effort view):")
                self.logger.write(_format_topics(topics))

                kp_path = os.path.abspath("analysis_export_keyphrases.csv")
                ent_path = os.path.abspath("analysis_export_entities.csv")
                self.logger.write(f"\n[analysis] Wrote:\n  - {kp_path}\n  - {ent_path}")

                self.logger.write(f"[analysis] Done in {time.time()-t0:.2f}s")
            except Exception as e:
                self.logger.write(f"[analysis error] {e}")
                self.logger.write(traceback.format_exc(limit=6))
            finally:
                self.after(0, lambda: (self.progress.stop(), self.analyze_btn.config(state="normal")))

        threading.Thread(target=worker, daemon=True).start()
    """

    """
    def on_update_index(self):
        self.update_idx_btn.config(state="disabled")
        self.progress.start(80)

        days = max(1, int(self.days_var.get()))

        def worker():
            try:
                upd, kp, ent, topics = atl.run_analysis(days_window=days, top_n=10, half_life=float(self.halflife_var.get()))
                self.logger.write(f"[index] Enrichment updated on {upd} recent rows.")
                built = rs.build_chunks(days=days)
                self.logger.write(f"[index] Built {built} chunks from last {days} day(s).")
                total = rs.build_vector_index()
                self.logger.write(f"[index] Vector index written for {total} chunks.")
            except Exception as e:
                self.logger.write(f"[index error] {e}")
            finally:
                self.after(0, lambda: (self.progress.stop(), self.update_idx_btn.config(state="normal")))

        threading.Thread(target=worker, daemon=True).start()
    """
    """
    def on_preview_retrieval(self):
        topic = self._first_topic()
        if not topic:
            messagebox.showerror("Input error", "Please enter at least one topic.")
            return

        self.preview_btn.config(state="disabled")
        self.progress.start(80)
        days = max(1, int(self.days_var.get()))
        since, until = self._timebox()

        def worker():
            try:
                sig = bb.top_signals(days=days, top_n=8)
                query = f"{topic} " + " ".join(sig["keyphrases"][:4] + sig["entities"][:4])
                hits = rs.search(query, k=12)

                date_tag = until or _dt.date.today().isoformat()
                folder = os.path.join("out", f"{date_tag}_{self._slug(topic)}")
                os.makedirs(folder, exist_ok=True)
                outpath = os.path.join(folder, "retrieval.json")
                with open(outpath, "w", encoding="utf-8") as f:
                    json.dump({"topic": topic, "query": query, "hits": hits}, f, ensure_ascii=False, indent=2)

                lines = [f"Topic: {topic}", f"Query: {query}", f"Hits: {len(hits)}", ""]
                for i, h in enumerate(hits, 1):
                    lines += [
                        f"[{i}] {h['title']}",
                        f"Date: {h.get('published_at')}",
                        f"Score: {h.get('score'):.3f}",
                        f"URL: {h.get('url')}",
                        f"Preview: {h.get('text_preview')}",
                        "-"*88
                    ]
                content = "\n".join(lines)
                self.after(0, lambda: self._open_text_viewer("Preview Retrieval", content))
                self.logger.write(f"[preview] Wrote {outpath}")
            except Exception as e:
                self.logger.write(f"[preview error] {e}")
            finally:
                self.after(0, lambda: (self.progress.stop(), self.preview_btn.config(state="normal")))

        threading.Thread(target=worker, daemon=True).start()

    def on_build_brief(self):
        topic = self._first_topic()
        if not topic:
            messagebox.showerror("Input error", "Please enter at least one topic.")
            return

        fmt = self.brief_format_var.get() or "blog"
        self.build_brief_btn.config(state="disabled")
        self.progress.start(80)
        since, until = self._timebox()

        def worker():
            try:
                brief = bb.build_brief(topic=topic, format_=fmt, since=since, until=until, audience="general")
                date_tag = brief["timebox"]["until"]
                folder = os.path.join("out", f"{date_tag}_{self._slug(topic)}")
                os.makedirs(folder, exist_ok=True)

                with open(os.path.join(folder, "brief.json"), "w", encoding="utf-8") as f:
                    json.dump(brief, f, ensure_ascii=False, indent=2)
                with open(os.path.join(folder, "retrieval.json"), "w", encoding="utf-8") as f:
                    json.dump({"sources": brief["sources"]}, f, ensure_ascii=False, indent=2)

                pretty = json.dumps(brief, ensure_ascii=False, indent=2)
                self.after(0, lambda: self._open_text_viewer("Brief", pretty))
                self.logger.write(f"[brief] Wrote {os.path.join(folder,'brief.json')}\n[brief] Wrote {os.path.join(folder,'retrieval.json')}")
            except Exception as e:
                self.logger.write(f"[brief error] {e}")
            finally:
                self.after(0, lambda: (self.progress.stop(), self.build_brief_btn.config(state="normal")))

        threading.Thread(target=worker, daemon=True).start()
    
    def on_view_articles(self):
        days = max(1, int(self.days_var.get()))
        limit = max(1, int(self.view_limit_var.get()))
        try:
            items = self._fetch_articles(days_window=days, limit=limit)
            if not items:
                self.logger.write(f"[view] No articles found in the last {days} days.")
                return
            self._open_article_viewer(items, days=days)
        except Exception as e:
            self.logger.write(f"[view error] {e}")

    def on_fetch_fulltext(self):
        # Leave this here in case you have a button wired elsewhere
        def worker():
            try:
                filled = fill_bodies(limit=75)
                self.logger.write(f"[fulltext] filled bodies for {filled} GDELT articles.")
            finally:
                self.after(0, self.progress.stop)
        self.progress.start(80)
        threading.Thread(target=worker, daemon=True).start()
    
    # ---------- viewers ----------
    def _open_text_viewer(self, title: str, content: str):
        win = tk.Toplevel(self); win.title(title); win.geometry("900x650")
        frm = ttk.Frame(win, padding=8); frm.pack(fill="both", expand=True)
        txt = tk.Text(frm, wrap="word"); sb = ttk.Scrollbar(frm, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=sb.set, font=("Consolas", 10))
        txt.pack(side="left", fill="both", expand=True); sb.pack(side="right", fill="y")
        txt.insert("1.0", content); txt.mark_set("insert", "1.0"); txt.focus_set()

    def _fetch_articles(self, days_window: int = 14, limit: int = 10):
        conn = sqlite3.connect("news.db")
        cur = conn.cursor()
        cur.execute(f
            #SELECT title, published_at, canonical_url,
            #       COALESCE(keyphrases_json,'[]'), COALESCE(entities_json,'[]'),
            #       COALESCE(body,''), COALESCE(summary,'')
            #FROM articles
            #WHERE published_at IS NOT NULL
            #  AND published_at >= datetime('now','-{days_window} day')
            #ORDER BY (published_at IS NULL), published_at DESC
            #LIMIT ?
        , (limit,))
        rows = cur.fetchall()
        conn.close()

        items = []
        for t, p, u, kjs, ejs, body, summary in rows:
            try:    kps = json.loads(kjs) if kjs else []
            except: kps = []
            try:    ents = json.loads(ejs) if ejs else []
            except: ents = []
            items.append({
                "title": t or "", "published_at": p or "", "url": u or "",
                "keyphrases": kps, "entities": ents,
                "text": body if (body or "").strip() else summary
            })
        return items

    def _open_article_viewer(self, items, days: int):
        win = tk.Toplevel(self)
        win.title(f"Parsed Articles (last {days} days)")
        win.geometry("900x650")

        top = ttk.Frame(win, padding=6); top.pack(fill="x")
        ttk.Label(top, text=f"Showing {len(items)} article(s) • last {days} day(s)").pack(side="left")

        frm = ttk.Frame(win, padding=(6,0,6,6)); frm.pack(fill="both", expand=True)
        txt = tk.Text(frm, wrap="word", font=("Consolas", 10))
        sb = ttk.Scrollbar(frm, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=sb.set)
        txt.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        sep = "\n" + "-"*88 + "\n\n"
        chunks = []
        for i, it in enumerate(items, 1):
            header = (
                f"[{i}] {it['title']}\n"
                f"Date: {it['published_at']} | URL: {it['url']}\n"
                f"Keyphrases: {', '.join(it['keyphrases'][:12]) if it['keyphrases'] else '(none)'}\n"
                f"Entities: {', '.join(it['entities'][:12]) if it['entities'] else '(none)'}\n"
            )
            body = it["text"] or "(no text available)"
            chunks.append(header + "\n" + body + sep)
        txt.insert("1.0", "".join(chunks))
        txt.mark_set("insert", "1.0"); txt.focus_set()
    """
# -------------------- Main --------------------
if __name__ == "__main__":
    app = WebFlooderGUI()
    app.mainloop()
