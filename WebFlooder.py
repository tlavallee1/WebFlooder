import os, json, math, threading, queue, datetime as _dt
from typing import Optional
import subprocess
import sys
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
try:
    import creator_full_blog as creator
except Exception:
    creator = None



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


class SourceParametersDialog(tk.Toplevel):
    """Modal dialog to edit parameters for all sources (Guardian, GDELT, YouTube, RSS).

    This dialog intentionally only edits parameter values (not per-source enable
    checkboxes) and copies values back to the parent when saved.
    """
    def __init__(self, parent: "WebFlooderGUI"):
        super().__init__(parent)
        self.transient(parent)
        self.title("Source Parameters")
        self.parent = parent
        self.resizable(False, False)

        # Notebook for source tabs
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        # --- Global tab (moved from main UI) ---
        gbl = ttk.Frame(nb, padding=8)
        nb.add(gbl, text="Global")

        # Use the parent's Tk variables directly so changes persist and readback is reliable.
        # If the parent is missing a variable (unlikely), create it on the parent and use that.
        if hasattr(parent, "topics_var"):
            self.topics_var = parent.topics_var
        else:
            parent.topics_var = tk.StringVar(parent, value="")
            self.topics_var = parent.topics_var

        if hasattr(parent, "weeks_var"):
            self.weeks_var = parent.weeks_var
        else:
            parent.weeks_var = tk.StringVar(parent, value="2 weeks")
            self.weeks_var = parent.weeks_var

        if hasattr(parent, "from_var"):
            self.from_var = parent.from_var
        else:
            parent.from_var = tk.StringVar(parent, value="")
            self.from_var = parent.from_var

        if hasattr(parent, "to_var"):
            self.to_var = parent.to_var
        else:
            parent.to_var = tk.StringVar(parent, value="")
            self.to_var = parent.to_var

        if hasattr(parent, "target_count_var"):
            self.target_count = parent.target_count_var
        else:
            parent.target_count_var = tk.IntVar(parent, value=50)
            self.target_count = parent.target_count_var

        if hasattr(parent, "lang_var"):
            self.lang_var = parent.lang_var
        else:
            parent.lang_var = tk.StringVar(parent, value="Any")
            self.lang_var = parent.lang_var

        ttk.Label(gbl, text="Topics (semicolon-separated):").grid(row=0, column=0, sticky="w", padx=(0,8), pady=(0,4))
        ttk.Entry(gbl, textvariable=self.topics_var, width=72).grid(row=0, column=1, columnspan=7, sticky="w", pady=(0,4))

        def _apply_weeks_local(ev=None):
            val = self.weeks_var.get()
            if val == "Custom":
                return
            try:
                parts = val.split()
                n = int(parts[0]) if parts else 2
                unit = parts[1] if len(parts) > 1 else "weeks"
            except Exception:
                n = 2
                unit = "weeks"
            today = date.today()
            if unit.startswith("day"):
                # n days: include today and go back n-1 days
                start = today - timedelta(days=max(0, n - 1))
            else:
                # assume weeks
                start = today - timedelta(days=n * 7 - 1)
            self.from_var.set(start.isoformat())
            self.to_var.set(today.isoformat())

        global_row = ttk.Frame(gbl)
        global_row.grid(row=1, column=0, columnspan=8, sticky="w", pady=(2,2))
        ttk.Label(global_row, text="Range:").grid(row=0, column=0, sticky="w")
        weeks_combo = ttk.Combobox(
            global_row, textvariable=self.weeks_var, state="readonly", width=10,
            values=[
                "Custom",
                "1 day", "2 days", "3 days", "4 days", "5 days", "6 days", "7 days",
                "1 week","2 weeks","3 weeks","4 weeks","6 weeks","8 weeks","12 weeks","26 weeks","52 weeks",
            ]
        )
        weeks_combo.grid(row=0, column=1, sticky="w", padx=(4,12))
        weeks_combo.bind("<<ComboboxSelected>>", _apply_weeks_local)

        ttk.Label(global_row, text="From (YYYY-MM-DD):").grid(row=0, column=2, sticky="w")
        from_entry = ttk.Entry(global_row, textvariable=self.from_var, width=12)
        from_entry.grid(row=0, column=3, sticky="w", padx=(4,12))
        from_entry.bind("<FocusOut>", lambda e: self._normalize_iso(self.from_var) if hasattr(self, '_normalize_iso') else None)

        ttk.Label(global_row, text="To:").grid(row=0, column=4, sticky="w")
        to_entry = ttk.Entry(global_row, textvariable=self.to_var, width=12)
        to_entry.grid(row=0, column=5, sticky="w", padx=(4,12))
        to_entry.bind("<FocusOut>", lambda e: self._normalize_iso(self.to_var) if hasattr(self, '_normalize_iso') else None)

        ttk.Label(global_row, text="Articles / topic:").grid(row=0, column=6, sticky="w", padx=(16,8))
        ttk.Spinbox(global_row, from_=10, to=500, increment=10, textvariable=self.target_count, width=10).grid(row=0, column=7, sticky="w")

        ttk.Label(global_row, text="Language:").grid(row=0, column=8, sticky="w", padx=(16,8))
        ttk.Combobox(global_row, textvariable=self.lang_var, state="readonly", width=12,
                     values=["Any","en","es","fr","de","ru","ar","zh","pt","it","tr","he","uk","ja","hi"]).grid(row=0, column=9, sticky="w")

        # --- Guardian tab ---
        gfrm = ttk.Frame(nb, padding=8)
        nb.add(gfrm, text="Guardian")

        ttk.Label(gfrm, text="Section:").grid(row=0, column=0, sticky="w")
        self.section_var = tk.StringVar(value=getattr(parent, "section_var").get())
        ttk.Combobox(gfrm, textvariable=self.section_var, state="readonly", width=36,
                     values=[label for (label, _id) in GUARDIAN_SECTIONS]).grid(row=0, column=1, sticky="w", padx=(8,0))

        ttk.Label(gfrm, text="Page size:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.guardian_page_size = tk.IntVar(value=getattr(parent, "guardian_page_size").get())
        ttk.Spinbox(gfrm, from_=10, to=200, increment=5, textvariable=self.guardian_page_size, width=8).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        # --- GDELT tab ---
        dfrm = ttk.Frame(nb, padding=8)
        nb.add(dfrm, text="GDELT")

        self.gdelt_slice_days = tk.IntVar(value=getattr(parent, "gdelt_slice_days").get())
        self.gdelt_per_slice_cap = tk.IntVar(value=getattr(parent, "gdelt_per_slice_cap").get())
        self.gdelt_sort = tk.StringVar(value=getattr(parent, "gdelt_sort").get())
        self.gdelt_timeout = tk.IntVar(value=getattr(parent, "gdelt_timeout").get())
        self.gdelt_allow_http = tk.BooleanVar(value=getattr(parent, "gdelt_allow_http").get())
        self.gdelt_fetch_body = tk.BooleanVar(value=getattr(parent, "gdelt_fetch_body").get())

        ttk.Label(dfrm, text="Slice (days):").grid(row=0, column=0, sticky="w")
        ttk.Spinbox(dfrm, from_=1, to=14, increment=1, textvariable=self.gdelt_slice_days, width=6).grid(row=0, column=1, sticky="w", padx=(8,12))

        ttk.Label(dfrm, text="Per-slice cap:").grid(row=0, column=2, sticky="w")
        ttk.Spinbox(dfrm, from_=5, to=200, increment=5, textvariable=self.gdelt_per_slice_cap, width=6).grid(row=0, column=3, sticky="w", padx=(8,12))

        ttk.Label(dfrm, text="Sort:").grid(row=1, column=0, sticky="w", pady=(8,0))
        ttk.Combobox(dfrm, textvariable=self.gdelt_sort, state="readonly", width=12,
                     values=["DateDesc", "Relevance"]).grid(row=1, column=1, sticky="w", padx=(8,12), pady=(8,0))

        ttk.Label(dfrm, text="Timeout (s):").grid(row=1, column=2, sticky="w", pady=(8,0))
        ttk.Spinbox(dfrm, from_=5, to=120, increment=5, textvariable=self.gdelt_timeout, width=6).grid(row=1, column=3, sticky="w", padx=(8,12), pady=(8,0))

        ttk.Checkbutton(dfrm, text="Allow HTTP fallback", variable=self.gdelt_allow_http).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8,0))
        ttk.Checkbutton(dfrm, text="Fetch full body", variable=self.gdelt_fetch_body).grid(row=2, column=2, columnspan=2, sticky="w", pady=(8,0))

        # --- YouTube tab ---
        yfrm = ttk.Frame(nb, padding=8)
        nb.add(yfrm, text="YouTube")

        self.yt_api_key = tk.StringVar(value=getattr(parent, "yt_api_key_var").get() if hasattr(parent, "yt_api_key_var") else "")
        ttk.Label(yfrm, text="API Key:").grid(row=0, column=0, sticky="w")
        ttk.Entry(yfrm, textvariable=self.yt_api_key, width=48).grid(row=0, column=1, sticky="w", padx=(8,0))

        ttk.Label(yfrm, text="Mode:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.yt_mode = tk.StringVar(value=getattr(parent, "yt_mode").get())
        ttk.Combobox(yfrm, textvariable=self.yt_mode, state="readonly", width=12,
                     values=["search","channel","playlist","video"]).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(yfrm, text="Query/URL/ID:").grid(row=2, column=0, sticky="w", pady=(8,0))
        self.yt_ident = tk.StringVar(value=getattr(parent, "yt_ident").get())
        ttk.Entry(yfrm, textvariable=self.yt_ident, width=40).grid(row=2, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(yfrm, text="Max videos:").grid(row=3, column=0, sticky="w", pady=(8,0))
        self.yt_max = tk.IntVar(value=getattr(parent, "yt_max").get())
        ttk.Spinbox(yfrm, from_=1, to=500, textvariable=self.yt_max, width=6).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(8,0))

        self.yt_fetch_captions = tk.BooleanVar(value=getattr(parent, "yt_fetch_captions_var").get() if hasattr(parent, "yt_fetch_captions_var") else True)
        ttk.Checkbutton(yfrm, text="Fetch captions", variable=self.yt_fetch_captions).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8,0))

        # --- RSS tab ---
        rfrm = ttk.Frame(nb, padding=8)
        nb.add(rfrm, text="RSS")

        # RSS feeds: use a wrapped Text widget (multi-line) and a Defaults button
        self.rss_feeds = tk.StringVar(value=getattr(parent, "rss_feeds_var").get())
        ttk.Label(rfrm, text="Feeds (semicolon-separated):").grid(row=0, column=0, sticky="nw")
        self.rss_feeds_text = tk.Text(rfrm, width=72, height=6, wrap="word")
        self.rss_feeds_text.grid(row=0, column=1, columnspan=2, sticky="w", padx=(8,0))
        # prefill text widget from the StringVar
        try:
            self.rss_feeds_text.insert("1.0", self.rss_feeds.get())
        except Exception:
            pass

        ttk.Button(rfrm, text="Defaults", command=lambda: self._set_rss_defaults()).grid(row=0, column=3, sticky="nw", padx=(8,0))

        self.rss_max_items = tk.IntVar(value=getattr(parent, "rss_max_items_var").get() if hasattr(parent, "rss_max_items_var") else getattr(parent, "rss_max_items_var", tk.IntVar(value=30)).get())
        ttk.Label(rfrm, text="Max items:").grid(row=1, column=0, sticky="w", pady=(8,0))
        ttk.Spinbox(rfrm, from_=1, to=500, increment=1, textvariable=self.rss_max_items, width=6).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        self.rss_fetch_body = tk.BooleanVar(value=getattr(parent, "rss_fetch_body_var").get())
        ttk.Checkbutton(rfrm, text="Fetch body", variable=self.rss_fetch_body).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8,0))

        # Save / Cancel
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(0,8))
        ttk.Button(btns, text="Save", command=self._on_save).pack(side="right", padx=(4,0))
        ttk.Button(btns, text="Cancel", command=self._on_cancel).pack(side="right")

        # If user clicks window close, behave like Save so changes persist
        try:
            self.protocol("WM_DELETE_WINDOW", self._on_save)
        except Exception:
            pass

        # Center and grab
        self.grab_set()
        self.wait_visibility()
        self.focus_force()

    def _on_save(self):
        p = self.parent
        try:
            p.section_var.set(self.section_var.get())
        except Exception:
            pass
        try:
            p.guardian_page_size.set(int(self.guardian_page_size.get()))
        except Exception:
            pass

        # GDELT
        try:
            p.gdelt_slice_days.set(int(self.gdelt_slice_days.get()))
            p.gdelt_per_slice_cap.set(int(self.gdelt_per_slice_cap.get()))
            p.gdelt_sort.set(self.gdelt_sort.get())
            p.gdelt_timeout.set(int(self.gdelt_timeout.get()))
            p.gdelt_allow_http.set(bool(self.gdelt_allow_http.get()))
            p.gdelt_fetch_body.set(bool(self.gdelt_fetch_body.get()))
        except Exception:
            pass

        # YouTube
        try:
            if hasattr(p, "yt_api_key_var"):
                p.yt_api_key_var.set(self.yt_api_key.get())
            if hasattr(p, "yt_mode"):
                p.yt_mode.set(self.yt_mode.get())
            if hasattr(p, "yt_ident"):
                p.yt_ident.set(self.yt_ident.get())
            if hasattr(p, "yt_max"):
                p.yt_max.set(int(self.yt_max.get()))
            if hasattr(p, "yt_fetch_captions_var"):
                p.yt_fetch_captions_var.set(bool(self.yt_fetch_captions.get()))
        except Exception:
            pass

        # RSS
        try:
            if hasattr(p, "rss_feeds_var"):
                try:
                    feeds_value = self.rss_feeds_text.get("1.0", "end").strip()
                    p.rss_feeds_var.set(feeds_value)
                except Exception:
                    p.rss_feeds_var.set(self.rss_feeds.get())
            if hasattr(p, "rss_max_items_var"):
                p.rss_max_items_var.set(int(self.rss_max_items.get()))
            if hasattr(p, "rss_fetch_body_var"):
                p.rss_fetch_body_var.set(bool(self.rss_fetch_body.get()))
        except Exception:
            pass

        self.grab_release()
        # Persist parameters to disk so dialog changes survive restarts
        try:
            if hasattr(self.parent, "save_parameters"):
                self.parent.save_parameters()
        except Exception:
            pass
        self.destroy()

    def _set_rss_defaults(self):
        """Restore the built-in default RSS feed list (without Politico/The Hill)."""
        default = (
            "http://feeds.bbci.co.uk/news/world/rss.xml;"
            "https://www.npr.org/rss/rss.php?id=1001;"
            "https://rss.cnn.com/rss/cnn_us.rss;"
            "https://feeds.foxnews.com/foxnews/politics;"
            "https://www.cbsnews.com/latest/rss/main;"
            "https://news.yahoo.com/rss"
        )
        try:
            self.rss_feeds_text.delete("1.0", "end")
            self.rss_feeds_text.insert("1.0", default)
        except Exception:
            try:
                # fallback: set var
                self.rss_feeds.set(default)
            except Exception:
                pass

    def _on_cancel(self):
        self.grab_release()
        self.destroy()


class ContentPrepDialog(tk.Toplevel):
    """Modal dialog to edit Content Prep, Cache and RAG parameters."""
    def __init__(self, parent: "WebFlooderGUI"):
        super().__init__(parent)
        self.transient(parent)
        self.title("Prep / Cache / RAG Parameters")
        self.parent = parent
        self.resizable(False, False)

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        # --- Prep tab ---
        pfrm = ttk.Frame(nb, padding=8)
        nb.add(pfrm, text="Prep")

        # Enable prep run
        self.prep_run = tk.BooleanVar(value=getattr(parent, "prep_run_var", tk.BooleanVar(value=True)).get() if hasattr(parent, "prep_run_var") else True)
        ttk.Checkbutton(pfrm, text="Run prep after ingest", variable=self.prep_run).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Label(pfrm, text="Min words/article:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.prep_min_words = tk.IntVar(value=getattr(parent, "prep_min_words_var", tk.IntVar(value=120)).get() if hasattr(parent, "prep_min_words_var") else 120)
        ttk.Entry(pfrm, textvariable=self.prep_min_words, width=8).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(pfrm, text="Min chars/article:").grid(row=2, column=0, sticky="w", pady=(8,0))
        self.prep_min_chars = tk.IntVar(value=getattr(parent, "prep_min_chars_var", tk.IntVar(value=220)).get() if hasattr(parent, "prep_min_chars_var") else 220)
        ttk.Entry(pfrm, textvariable=self.prep_min_chars, width=8).grid(row=2, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(pfrm, text="Min words/paragraph:").grid(row=3, column=0, sticky="w", pady=(8,0))
        self.prep_min_words_para = tk.IntVar(value=getattr(parent, "prep_min_words_para_var", tk.IntVar(value=8)).get() if hasattr(parent, "prep_min_words_para_var") else 8)
        ttk.Entry(pfrm, textvariable=self.prep_min_words_para, width=8).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(pfrm, text="Chunk chars:").grid(row=4, column=0, sticky="w", pady=(8,0))
        self.prep_chunk_chars = tk.IntVar(value=getattr(parent, "prep_chunk_chars_var", tk.IntVar(value=1500)).get() if hasattr(parent, "prep_chunk_chars_var") else 1500)
        ttk.Entry(pfrm, textvariable=self.prep_chunk_chars, width=8).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(8,0))

        self.prep_delete_short = tk.BooleanVar(value=getattr(parent, "prep_delete_short_var", tk.BooleanVar(value=False)).get() if hasattr(parent, "prep_delete_short_var") else False)
        ttk.Checkbutton(pfrm, text="Delete short articles", variable=self.prep_delete_short).grid(row=5, column=0, columnspan=2, sticky="w", pady=(8,0))

        ttk.Label(pfrm, text="Limit rows (0 = none):").grid(row=6, column=0, sticky="w", pady=(8,0))
        self.prep_limit_rows = tk.IntVar(value=getattr(parent, "prep_limit_rows_var", tk.IntVar(value=0)).get() if hasattr(parent, "prep_limit_rows_var") else 0)
        ttk.Entry(pfrm, textvariable=self.prep_limit_rows, width=8).grid(row=6, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(pfrm, text="Top-K keyphrases:").grid(row=7, column=0, sticky="w", pady=(8,0))
        self.prep_topk = tk.IntVar(value=getattr(parent, "prep_topk_var", tk.IntVar(value=6)).get() if hasattr(parent, "prep_topk_var") else 6)
        ttk.Entry(pfrm, textvariable=self.prep_topk, width=8).grid(row=7, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Checkbutton(pfrm, text="Build snippets", variable=tk.BooleanVar(value=getattr(parent, "prep_make_snippets_var", tk.BooleanVar(value=True)).get() if hasattr(parent, "prep_make_snippets_var") else True), variablename=None)
        # (we will store and set the real var on save)

        # --- Cache / Vector tab ---
        cfrm = ttk.Frame(nb, padding=8)
        nb.add(cfrm, text="Cache / Vector")

        self.prep_make_snippets = tk.BooleanVar(value=getattr(parent, "prep_make_snippets_var", tk.BooleanVar(value=True)).get() if hasattr(parent, "prep_make_snippets_var") else True)
        ttk.Checkbutton(cfrm, text="Build snippets", variable=self.prep_make_snippets).grid(row=0, column=0, sticky="w")

        self.prep_index_refresh = tk.BooleanVar(value=getattr(parent, "prep_index_refresh_var", tk.BooleanVar(value=True)).get() if hasattr(parent, "prep_index_refresh_var") else True)
        ttk.Checkbutton(cfrm, text="Refresh vector index", variable=self.prep_index_refresh).grid(row=1, column=0, sticky="w", pady=(8,0))

        self.prep_do_vectorize = tk.BooleanVar(value=getattr(parent, "prep_do_vectorize_var", tk.BooleanVar(value=False)).get() if hasattr(parent, "prep_do_vectorize_var") else False)
        ttk.Checkbutton(cfrm, text="Do vectorize", variable=self.prep_do_vectorize).grid(row=2, column=0, sticky="w", pady=(8,0))

        ttk.Label(cfrm, text="Batch size:").grid(row=3, column=0, sticky="w", pady=(8,0))
        self.prep_batch = tk.IntVar(value=getattr(parent, "prep_batch_var", tk.IntVar(value=64)).get() if hasattr(parent, "prep_batch_var") else 64)
        ttk.Entry(cfrm, textvariable=self.prep_batch, width=8).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(cfrm, text="Model:").grid(row=4, column=0, sticky="w", pady=(8,0))
        self.prep_model = tk.StringVar(value=getattr(parent, "prep_model_var", tk.StringVar(value="text-embedding-3-large")).get() if hasattr(parent, "prep_model_var") else "text-embedding-3-large")
        ttk.Entry(cfrm, textvariable=self.prep_model, width=36).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(8,0))

        # --- RAG tab ---
        rfrm = ttk.Frame(nb, padding=8)
        nb.add(rfrm, text="RAG")

        self.rag_enable = tk.BooleanVar(value=getattr(parent, "rag_enable_var", tk.BooleanVar(value=False)).get() if hasattr(parent, "rag_enable_var") else False)
        ttk.Checkbutton(rfrm, text="Enable RAG", variable=self.rag_enable).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Label(rfrm, text="Model:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.rag_model = tk.StringVar(value=getattr(parent, "rag_model_var", tk.StringVar(value="text-embedding-3-large")).get() if hasattr(parent, "rag_model_var") else "text-embedding-3-large")
        ttk.Entry(rfrm, textvariable=self.rag_model, width=36).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(rfrm, text="Batch:").grid(row=2, column=0, sticky="w", pady=(8,0))
        self.rag_batch = tk.IntVar(value=getattr(parent, "rag_batch_var", tk.IntVar(value=64)).get() if hasattr(parent, "rag_batch_var") else 64)
        ttk.Entry(rfrm, textvariable=self.rag_batch, width=8).grid(row=2, column=1, sticky="w", padx=(8,0), pady=(8,0))

        self.rag_recompute = tk.BooleanVar(value=getattr(parent, "rag_recompute_var", tk.BooleanVar(value=False)).get() if hasattr(parent, "rag_recompute_var") else False)
        ttk.Checkbutton(rfrm, text="Recompute all", variable=self.rag_recompute).grid(row=3, column=0, columnspan=2, sticky="w", pady=(8,0))

        ttk.Label(rfrm, text="Date from (YYYY-MM-DD):").grid(row=4, column=0, sticky="w", pady=(8,0))
        self.rag_date_from = tk.StringVar(value=getattr(parent, "rag_date_from_var", tk.StringVar(value="")).get() if hasattr(parent, "rag_date_from_var") else "")
        ttk.Entry(rfrm, textvariable=self.rag_date_from, width=12).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(rfrm, text="Date to (YYYY-MM-DD):").grid(row=5, column=0, sticky="w", pady=(8,0))
        self.rag_date_to = tk.StringVar(value=getattr(parent, "rag_date_to_var", tk.StringVar(value="")).get() if hasattr(parent, "rag_date_to_var") else "")
        ttk.Entry(rfrm, textvariable=self.rag_date_to, width=12).grid(row=5, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(rfrm, text="Topics any (semicolon-separated):").grid(row=6, column=0, sticky="nw", pady=(8,0))
        self.rag_topics_any = tk.StringVar(value=getattr(parent, "rag_topics_any_var", tk.StringVar(value="")).get() if hasattr(parent, "rag_topics_any_var") else "")
        ttk.Entry(rfrm, textvariable=self.rag_topics_any, width=48).grid(row=6, column=1, sticky="w", padx=(8,0), pady=(8,0))

        # Save / Cancel
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(0,8))
        ttk.Button(btns, text="Save", command=self._on_save).pack(side="right", padx=(4,0))
        ttk.Button(btns, text="Cancel", command=self._on_cancel).pack(side="right")

        # Center and grab
        self.grab_set()
        self.wait_visibility()
        self.focus_force()

    def _on_save(self):
        p = self.parent
        # Helper to ensure parent has a tk.Variable and set value
        def _set_var(name, var, ctor=tk.StringVar):
            try:
                if hasattr(p, name):
                    getattr(p, name).set(var.get())
                else:
                    setattr(p, name, ctor(p, value=var.get()))
            except Exception:
                try:
                    setattr(p, name, ctor(p, value=var.get()))
                except Exception:
                    pass

        # Global inputs (moved from main UI)
        try:
            _set_var("topics_var", self.topics_var, ctor=tk.StringVar)
            _set_var("weeks_var", self.weeks_var, ctor=tk.StringVar)
            _set_var("from_var", self.from_var, ctor=tk.StringVar)
            _set_var("to_var", self.to_var, ctor=tk.StringVar)
            _set_var("target_count_var", self.target_count, ctor=tk.IntVar)
            _set_var("lang_var", self.lang_var, ctor=tk.StringVar)
        except Exception:
            pass

        # Prep
        _set_var("prep_run_var", self.prep_run, ctor=tk.BooleanVar)
        _set_var("prep_min_words_var", self.prep_min_words, ctor=tk.IntVar)
        _set_var("prep_min_chars_var", self.prep_min_chars, ctor=tk.IntVar)
        _set_var("prep_min_words_para_var", self.prep_min_words_para, ctor=tk.IntVar)
        _set_var("prep_chunk_chars_var", self.prep_chunk_chars, ctor=tk.IntVar)
        _set_var("prep_delete_short_var", self.prep_delete_short, ctor=tk.BooleanVar)
        _set_var("prep_limit_rows_var", self.prep_limit_rows, ctor=tk.IntVar)
        _set_var("prep_topk_var", self.prep_topk, ctor=tk.IntVar)
        _set_var("prep_make_snippets_var", self.prep_make_snippets, ctor=tk.BooleanVar)

        # Cache / Vector
        _set_var("prep_index_refresh_var", self.prep_index_refresh, ctor=tk.BooleanVar)
        _set_var("prep_do_vectorize_var", self.prep_do_vectorize, ctor=tk.BooleanVar)
        _set_var("prep_batch_var", self.prep_batch, ctor=tk.IntVar)
        _set_var("prep_model_var", self.prep_model, ctor=tk.StringVar)

        # RAG
        _set_var("rag_enable_var", self.rag_enable, ctor=tk.BooleanVar)
        _set_var("rag_model_var", self.rag_model, ctor=tk.StringVar)
        _set_var("rag_batch_var", self.rag_batch, ctor=tk.IntVar)
        _set_var("rag_recompute_var", self.rag_recompute, ctor=tk.BooleanVar)
        _set_var("rag_date_from_var", self.rag_date_from, ctor=tk.StringVar)
        _set_var("rag_date_to_var", self.rag_date_to, ctor=tk.StringVar)
        _set_var("rag_topics_any_var", self.rag_topics_any, ctor=tk.StringVar)

        # Persist parameters after saving into parent
        try:
            if hasattr(p, "save_parameters"):
                p.save_parameters()
        except Exception:
            pass

        self.grab_release()
        self.destroy()

    def _on_cancel(self):
        self.grab_release()
        self.destroy()


class GenerateContentDialog(tk.Toplevel):
    """Modal dialog to configure generation parameters per content type.

    Provides one tab per content type (Blog, Post, Tweet, Podcast, Video).
    On Save, stores a dict for each tab on the parent (e.g. parent.gen_blog_cfg)
    so the main app can later use those parameters when implementing generation.
    """
    def __init__(self, parent: "WebFlooderGUI"):
        super().__init__(parent)
        self.transient(parent)
        self.title("Generate Content Parameters")
        self.parent = parent
        self.resizable(True, True)

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        # --- Helper to build a common small-form (Topic/Angle/Tone/Author/Tags) ---
        def _common_fields(frm, prefix="", initial: Optional[dict] = None):
            """Create common small-form fields and prefill from `initial` if provided.
            NOTE: angle/tone/tags removed per UX request; returns only topic and author."""
            initial = initial or {}
            ttk.Label(frm, text="Topic:").grid(row=0, column=0, sticky="w")
            # default to parent's topics if no explicit initial topic provided
            default_topic = initial.get('topic') if initial.get('topic') is not None else (getattr(parent, "topics_var").get() if hasattr(parent, "topics_var") else "")
            tvar = tk.StringVar(value=default_topic)
            ttk.Entry(frm, textvariable=tvar, width=60).grid(row=0, column=1, sticky="w", padx=(8,0))

            ttk.Label(frm, text="Author:").grid(row=1, column=0, sticky="w", pady=(6,0))
            author = tk.StringVar(value=initial.get('author', "Editorial Desk"))
            ttk.Entry(frm, textvariable=author, width=36).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(6,0))

            return dict(topic=tvar, author=author)

        # Load any saved configs from parent so we prefill dialog with persisted values
        blog_saved = getattr(parent, 'gen_blog_cfg', {}) or {}
        post_saved = getattr(parent, 'gen_post_cfg', {}) or {}
        tweet_saved = getattr(parent, 'gen_tweet_cfg', {}) or {}
        podcast_saved = getattr(parent, 'gen_podcast_cfg', {}) or {}
        video_saved = getattr(parent, 'gen_video_cfg', {}) or {}

        # Shared vars for some controls (define before tab widgets so we can place widgets in different tabs)
        b_prof = tk.StringVar(value=blog_saved.get('profanity_level', "clean"))
        b_include_social = tk.BooleanVar(value=bool(blog_saved.get('include_social', True)))

        # --- Blog tabs split into four focused areas ---
        tab_id = ttk.Frame(nb, padding=8)
        tab_voice = ttk.Frame(nb, padding=8)
        tab_struct = ttk.Frame(nb, padding=8)
        tab_auto = ttk.Frame(nb, padding=8)
        nb.add(tab_id, text="Identity")
        nb.add(tab_voice, text="Voice & Tone")
        nb.add(tab_struct, text="Structure & Sourcing")
        nb.add(tab_auto, text="Automation & Output")
        # Podcast tab
        tab_podcast = ttk.Frame(nb, padding=8)
        nb.add(tab_podcast, text="Podcast")

        # --- Podcast tab controls ---
        ttk.Label(tab_podcast, text="Voice:").grid(row=1, column=0, sticky="w", pady=(8,0))
        # Voice choices with human-readable labels
        voice_items = [
            "Alloy — Male – Bold, expressive",
            "Ash — Neutral – Calm, professional",
            "Ballad — Female – Soft, melodic",
            "Coral — Female – Warm, friendly",
            "Echo — Male – Crisp, articulate",
            "Fable — Female – Storytelling, rich",
            "Onyx — Male – Deep, commanding",
            "Nova — Male — Energetic, sharp",
            "Sage — Female — Smooth, thoughtful",
            "Shimmer — Female — Light, playful",
            "Verse — Male — Poetic, elegant",
            "generdr — Generative Narrator (clear, neutral, broadcast-ready)",
        ]
        # Select default label by matching saved id (like 'alloy') to the label's first token
        saved_id = (podcast_saved.get('voice') or 'alloy').lower()
        default_label = next((lbl for lbl in voice_items if (lbl.split()[0].lower() == saved_id)), voice_items[0])
        p_voice = tk.StringVar(value=default_label)
        ttk.Combobox(tab_podcast, textvariable=p_voice, state="readonly", values=voice_items, width=40).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_podcast, text="Speed (0.5-2.0):").grid(row=2, column=0, sticky="w", pady=(8,0))
        p_speed = tk.DoubleVar(value=float(podcast_saved.get('speed', 1.0)))
        ttk.Spinbox(tab_podcast, from_=0.5, to=2.0, increment=0.1, textvariable=p_speed, width=6).grid(row=2, column=1, sticky="w", padx=(8,0))

        ttk.Label(tab_podcast, text="Pitch (-10 to +10):").grid(row=2, column=2, sticky="w", pady=(8,0), padx=(12,0))
        p_pitch = tk.IntVar(value=int(podcast_saved.get('pitch', 0)))
        ttk.Spinbox(tab_podcast, from_=-10, to=10, increment=1, textvariable=p_pitch, width=6).grid(row=2, column=3, sticky="w", padx=(8,0))

        ttk.Label(tab_podcast, text="Format:").grid(row=2, column=4, sticky="w", pady=(8,0), padx=(12,0))
        p_format = tk.StringVar(value=podcast_saved.get('format', 'mp3'))
        ttk.Combobox(tab_podcast, textvariable=p_format, state="readonly", width=8, values=["mp3", "wav", "flac", "aac", "ogg"]).grid(row=2, column=5, sticky="w", padx=(8,0))

        ttk.Label(tab_podcast, text="Intro template:").grid(row=3, column=0, sticky="nw", pady=(8,0))
        p_intro = tk.Text(tab_podcast, width=48, height=3, wrap="word")
        p_intro.grid(row=3, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))
        default_intro = podcast_saved.get('intro_template', (
            "Welcome to Meerkat Media. Today's briefing: {title}. "
            "We summarize the key facts, explain what matters, and point to original sources. "
            "This episode is brought to you by Meerkat Media."
        ))
        try:
            p_intro.insert("1.0", default_intro)
        except Exception:
            pass

        ttk.Label(tab_podcast, text="Signoff text:").grid(row=4, column=0, sticky="w", pady=(8,0))
        default_signoff = podcast_saved.get('signoff', (
            "Thanks for listening to Meerkat Media. Visit our site for full articles and sources. "
            "Subscribe for updates and follow us on social."
        ))
        p_signoff = tk.StringVar(value=default_signoff)
        ttk.Entry(tab_podcast, textvariable=p_signoff, width=48).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(8,0))

        # Delivery direction: short prompts to steer vocal delivery
        ttk.Label(tab_podcast, text="Delivery direction:").grid(row=5, column=0, sticky="w", pady=(8,0))
        p_direction = tk.StringVar(value=podcast_saved.get('direction', "Calm, friendly, mid-tempo; emphasize numbers."))
        direction_values = [
            "Calm, friendly, mid-tempo; emphasize numbers.",
            "Energetic, upbeat, punchy; lively delivery.",
            "Serious, clear, deliberate; pause after key facts.",
            "Warm storytelling, slight smile; rich tone.",
            "Neutral, broadcast-ready; even pacing.",
            "Emphasize dates and figures; steady, authoritative."
        ]
        ttk.Combobox(tab_podcast, textvariable=p_direction, state="readonly", width=48, values=direction_values).grid(row=5, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))

        # Reverb presets for post-processing (human-friendly labels shown; saved value is a canonical id)
        ttk.Label(tab_podcast, text="Post-process reverb:").grid(row=6, column=0, sticky="w", pady=(8,0))
        # Display labels (user-facing) — add numeric prefixes so it's obvious which preset is active
        reverb_labels = [
            "1) None (no reverb)",
            "2) Large echo",
            "3) Echo",
            "4) Reverb",
            "5) Subtle reverb",
            "6) Ultra subtle reverb",
        ]
        # Default uses previously saved canonical id; map to the numbered label for display
        _rev_saved_id = (podcast_saved.get('reverb') or 'none')
        _rev_map_inv = {
            'none': "1) None (no reverb)",
            'large_echo': "2) Large echo",
            'echo': "3) Echo",
            'reverb': "4) Reverb",
            'subtle': "5) Subtle reverb",
            'ultra_subtle': "6) Ultra subtle reverb",
        }
        p_reverb = tk.StringVar(value=_rev_map_inv.get(_rev_saved_id, "1) None (no reverb)"))
        ttk.Combobox(tab_podcast, textvariable=p_reverb, state="readonly", width=28, values=reverb_labels).grid(row=6, column=1, sticky="w", padx=(8,0), pady=(8,0))

        # Identity & routing
        b_common = _common_fields(tab_id, initial=blog_saved)
        ttk.Label(tab_id, text="Political party:").grid(row=1, column=0, sticky="w", pady=(6,0))
        b_party = tk.StringVar(value=blog_saved.get('political_party', ""))
        ttk.Combobox(tab_id, textvariable=b_party, state="readonly", width=48,
                     values=[
                         "Democratic Party (center-left)",
                         "Progressive Democrat",
                         "Republican Party (center-right)",
                         "Modern MAGA (right-populist, GOP faction)",
                         "Democratic Socialists (left-populist)",
                         "Libertarian Party",
                         "Green Party",
                         "Constitution Party",
                         "None / Neutral",
                     ]).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(6,0))
        b_party_notes = tk.StringVar(value=blog_saved.get('political_party_notes', ""))
        ttk.Entry(tab_id, textvariable=b_party_notes, width=60).grid(row=1, column=2, sticky="w", padx=(8,0), pady=(6,0))

        # Auto-topic controls
        b_auto = tk.BooleanVar(value=bool(blog_saved.get('auto_topic', False)))
        ttk.Checkbutton(tab_id, text="Auto-generate topic", variable=b_auto).grid(row=0, column=2, sticky="w", padx=(8,8))
        ttk.Label(tab_id, text="Days back:").grid(row=0, column=3, sticky="w")
        b_days = tk.IntVar(value=int(blog_saved.get('auto_days', 7)))
        ttk.Spinbox(tab_id, from_=1, to=90, textvariable=b_days, width=6).grid(row=0, column=4, sticky="w", padx=(4,0))

        ttk.Label(tab_id, text="Target readers:").grid(row=2, column=0, sticky="w", pady=(8,0))
        b_target = tk.StringVar(value=blog_saved.get('target_readers', "General audience"))
        ttk.Combobox(tab_id, textvariable=b_target, state="readonly", width=36,
                     values=["General audience","Policy/professional","Tech/finance literate","Your existing followers/subs","Other"]).grid(row=2, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_id, text="CTA at the end:").grid(row=3, column=0, sticky="w", pady=(6,0))
        b_cta = tk.StringVar(value=blog_saved.get('cta', "Subscribe/Share"))
        ttk.Combobox(tab_id, textvariable=b_cta, state="readonly", width=36,
                     values=["None","Subscribe/Share","Specific action (call reps, donate, sign)","Link to longer brief / follow-ups"]).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(6,0))

        # Voice & tone
        ttk.Label(tab_voice, text="Primary goal:").grid(row=0, column=0, sticky="w", pady=(8,0))
        b_purpose = tk.StringVar(value=blog_saved.get('purpose', "Persuade toward a clear stance"))
        ttk.Combobox(tab_voice, textvariable=b_purpose, state="readonly", width=36,
                     values=[
                         "Persuade toward a clear stance",
                         "Explain/teach with authority",
                         "Expose contradictions / hold power to account",
                         "Mobilize action (petitions, calls, votes)",
                         "Entertain with sharp commentary",
                     ]).grid(row=0, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_voice, text="Stance strength:").grid(row=1, column=0, sticky="w", pady=(6,0))
        b_stance_strength = tk.StringVar(value=blog_saved.get('stance_strength', "Mostly one-sided, occasional nuance"))
        ttk.Combobox(tab_voice, textvariable=b_stance_strength, state="readonly", width=36,
                     values=[
                         "Unapologetically one-sided (no concessions)",
                         "Strongly one-sided, tiny caveats if unavoidable",
                         "Mostly one-sided, occasional nuance",
                     ]).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_voice, text="Lines you won't cross:").grid(row=2, column=0, sticky="nw", pady=(6,0))
        b_lines_text = tk.Text(tab_voice, width=48, height=3, wrap="word")
        b_lines_text.grid(row=2, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))
        try:
            b_lines_text.insert("1.0", blog_saved.get('lines_you_wont_cross', ""))
        except Exception:
            pass

        ttk.Label(tab_voice, text="Narrator persona:").grid(row=3, column=0, sticky="w", pady=(8,0))
        b_persona = tk.StringVar(value=blog_saved.get('persona', "Dry, data-forward analyst"))
        ttk.Combobox(tab_voice, textvariable=b_persona, state="readonly", width=36,
                     values=[
                         "Snarky skeptic",
                         "Dry, data-forward analyst",
                         "Sharp prosecutor / cross-examiner",
                         "Reform advocate / policy wonk",
                         "Outsider contrarian",
                         "Other...",
                     ]).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(8,0))
        b_persona_other = tk.StringVar(value=blog_saved.get('persona_other', ""))
        ttk.Entry(tab_voice, textvariable=b_persona_other, width=28).grid(row=3, column=2, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_voice, text="Humor level:").grid(row=4, column=0, sticky="w", pady=(6,0))
        b_humor = tk.StringVar(value=blog_saved.get('humor_level', "Light wit"))
        ttk.Combobox(tab_voice, textvariable=b_humor, state="readonly", width=24,
                     values=["None","Light wit","Edgy/snark allowed","Memes/one-liners welcome"]).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_voice, text="Heat level:").grid(row=5, column=0, sticky="w", pady=(6,0))
        b_heat = tk.StringVar(value=blog_saved.get('heat_level', "Firm but civil"))
        ttk.Combobox(tab_voice, textvariable=b_heat, state="readonly", width=24,
                     values=["Cool/clinical","Firm but civil","Spicy and confrontational"]).grid(row=5, column=1, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_voice, text="Openings you like (brief):").grid(row=6, column=0, sticky="nw", pady=(8,0))
        b_openings = tk.Text(tab_voice, width=48, height=2, wrap="word")
        b_openings.grid(row=6, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))
        try: b_openings.insert("1.0", blog_saved.get('openings', ""))
        except Exception: pass

        ttk.Label(tab_voice, text="Devices to use (comma-separated):").grid(row=7, column=0, sticky="w", pady=(6,0))
        b_devices = tk.StringVar(value=blog_saved.get('devices', "Rhetorical questions, Short punchy sentences, Bullet callouts"))
        ttk.Entry(tab_voice, textvariable=b_devices, width=48).grid(row=7, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_voice, text="Words/phrases to favor/avoid (brief):").grid(row=8, column=0, sticky="nw", pady=(6,0))
        b_favavoid = tk.Text(tab_voice, width=48, height=2, wrap="word")
        b_favavoid.grid(row=8, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))
        try:
            b_favavoid.insert("1.0", blog_saved.get('fav_avoid', ""))
        except Exception:
            pass

        # Profanity moved to Voice & Tone tab for discoverability
        ttk.Label(tab_voice, text="Profanity level:").grid(row=9, column=0, sticky="w", pady=(8,0))
        ttk.Combobox(tab_voice, textvariable=b_prof, state="readonly", values=["clean","mild","spicy","bleeped"], width=12).grid(row=9, column=1, sticky="w", padx=(8,0), pady=(8,0))

    # Structure & sourcing
        ttk.Label(tab_struct, text="Post length (words):").grid(row=0, column=0, sticky="w", pady=(8,0))
        b_length = tk.StringVar(value=str(blog_saved.get('post_length', "900")))
        ttk.Entry(tab_struct, textvariable=b_length, width=12).grid(row=0, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_struct, text="Preferred structure (semicolon-separated):").grid(row=1, column=0, sticky="nw", pady=(6,0))
        b_structure_text = tk.Text(tab_struct, width=48, height=3, wrap="word")
        b_structure_text.grid(row=1, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))
        try:
            b_structure_text.insert("1.0", blog_saved.get('preferred_structure', ""))
        except Exception:
            pass

        ttk.Label(tab_struct, text="Must-have sections:").grid(row=2, column=0, sticky="w", pady=(6,0))
        b_must = tk.StringVar(value=blog_saved.get('must_have_sections', ""))
        ttk.Entry(tab_struct, textvariable=b_must, width=48).grid(row=2, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_struct, text="Freshness requirement:").grid(row=3, column=0, sticky="w", pady=(8,0))
        b_fresh = tk.StringVar(value=blog_saved.get('freshness_requirement', "Mix: freshest for news, stable primers for context"))
        ttk.Combobox(tab_struct, textvariable=b_fresh, state="readonly", width=48,
                     values=[
                         "≤ 24 hours when topical",
                         "≤ 7 days",
                         "Mix: freshest for news, stable primers for context",
                     ]).grid(row=3, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_struct, text="Numbers to prioritize:").grid(row=4, column=0, sticky="w", pady=(6,0))
        b_numbers = tk.StringVar(value=blog_saved.get('numbers_to_prioritize', "All of the above"))
        ttk.Combobox(tab_struct, textvariable=b_numbers, state="readonly", width=36,
                     values=["$ totals","% changes / deltas","Per-capita, inflation-adjusted","Rankings/benchmarks","Before/after comparisons","All of the above"]).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_struct, text="Citation style:").grid(row=5, column=0, sticky="w", pady=(6,0))
        b_cite = tk.StringVar(value=blog_saved.get('citation_style', "Inline bracketed numbers [1], [2] → sources list"))
        ttk.Combobox(tab_struct, textvariable=b_cite, state="readonly", width=48,
                     values=["Inline bracketed numbers [1], [2] → sources list","Inline parenthetical (Outlet, Date)","Footnote-like superscripts","Links on key phrases only"]).grid(row=5, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_struct, text="Legal guardrails (choose):").grid(row=6, column=0, sticky="w", pady=(8,0))
        b_guard_legal = tk.StringVar(value=blog_saved.get('legal_guardrails', "No doxxing/speculation; No medical/financial advice claims"))
        ttk.Combobox(tab_struct, textvariable=b_guard_legal, state="readonly", width=48,
                     values=["No medical/financial advice claims","No doxxing/speculation","No unverified allegations","Keep to public, citable info"]).grid(row=6, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_struct, text="Content you don't want (brief):").grid(row=7, column=0, sticky="nw", pady=(6,0))
        b_bad = tk.Text(tab_struct, width=48, height=2, wrap="word")
        b_bad.grid(row=7, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))
        try: b_bad.insert("1.0", blog_saved.get('content_blocklist', ""))
        except Exception: pass

        ttk.Label(tab_struct, text="Reading experience:").grid(row=8, column=0, sticky="w", pady=(6,0))
        b_reading = tk.StringVar(value=blog_saved.get('reading_experience', "Scannable (short paras, bullets, bold key stats)"))
        ttk.Combobox(tab_struct, textvariable=b_reading, state="readonly", width=48,
                     values=["Scannable (short paras, bullets, bold key stats)","Narrative flow (longer paras)","Hybrid (clean scannability + story)"]).grid(row=8, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))

        # Automation & output
        ttk.Label(tab_auto, text="Num subtasks:").grid(row=0, column=0, sticky="w", pady=(8,0))
        b_num = tk.IntVar(value=int(blog_saved.get('num_subtasks', 5)))
        ttk.Spinbox(tab_auto, from_=1, to=12, textvariable=b_num, width=6).grid(row=0, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_auto, text="Queries / subtask:").grid(row=1, column=0, sticky="w", pady=(8,0))
        b_qps = tk.IntVar(value=int(blog_saved.get('queries_per_subtask', 3)))
        ttk.Spinbox(tab_auto, from_=1, to=8, textvariable=b_qps, width=6).grid(row=1, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_auto, text="Drafting model:").grid(row=2, column=0, sticky="w", pady=(8,0))
        b_draft = tk.StringVar(value=blog_saved.get('drafting_model', "gpt-4o"))
        ttk.Entry(tab_auto, textvariable=b_draft, width=28).grid(row=2, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_auto, text="Embedding model:").grid(row=3, column=0, sticky="w", pady=(8,0))
        b_emb = tk.StringVar(value=blog_saved.get('retrieval_model', "text-embedding-3-large"))
        ttk.Entry(tab_auto, textvariable=b_emb, width=28).grid(row=3, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_auto, text="Temperature:").grid(row=4, column=0, sticky="w", pady=(8,0))
        b_temp = tk.DoubleVar(value=float(blog_saved.get('temperature', 0.7)))
        ttk.Spinbox(tab_auto, from_=0.0, to=1.0, increment=0.05, textvariable=b_temp, width=8).grid(row=4, column=1, sticky="w", padx=(8,0), pady=(8,0))

        ttk.Label(tab_auto, text="DB path:").grid(row=5, column=0, sticky="w", pady=(8,0))
        b_db = tk.StringVar(value=blog_saved.get('db_path', "news.db"))
        ttk.Entry(tab_auto, textvariable=b_db, width=36).grid(row=5, column=1, sticky="w", padx=(8,0), pady=(8,0))

    # (include_social kept as a hidden variable; checkbox removed)

        ttk.Label(tab_auto, text="Auto-web search behavior:").grid(row=8, column=0, sticky="w", pady=(8,0))
        b_auto_web = tk.StringVar(value=blog_saved.get('auto_web_search', "Yes, but cap at N sources (N=3)"))
        ttk.Combobox(tab_auto, textvariable=b_auto_web, state="readonly", width=48,
                     values=["Yes, always","Yes, but cap at N sources (N=3)","Only if fewer than N RAG cites (N=3)","No"]).grid(row=8, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(8,0))
        b_web_cap = tk.IntVar(value=int(blog_saved.get('web_search_cap', 3)))
        ttk.Label(tab_auto, text="Web cap N:").grid(row=8, column=3, sticky="w", pady=(8,0))
        ttk.Spinbox(tab_auto, from_=0, to=20, textvariable=b_web_cap, width=6).grid(row=8, column=4, sticky="w", padx=(4,0), pady=(8,0))

        ttk.Label(tab_auto, text="Failure behavior:").grid(row=9, column=0, sticky="w", pady=(6,0))
        b_fail = tk.StringVar(value=blog_saved.get('failure_behavior', "If you can’t verify a number quickly, omit it"))
        ttk.Combobox(tab_auto, textvariable=b_fail, state="readonly", width=48,
                     values=["If you can’t verify a number quickly, omit it","Allow estimates with explicit uncertainty bounds","Replace with closest verified proxy metric"]).grid(row=9, column=1, columnspan=2, sticky="w", padx=(8,0), pady=(6,0))

        ttk.Label(tab_auto, text="Output format:").grid(row=10, column=0, sticky="w", pady=(6,0))
        b_output = tk.StringVar(value=blog_saved.get('output_format', "Both"))
        ttk.Combobox(tab_auto, textvariable=b_output, state="readonly", width=24,
                     values=["JSON {title, dek, body_md, key_stat, sources[]}","Markdown only","Both"]).grid(row=10, column=1, sticky="w", padx=(8,0), pady=(6,0))

        # Save / Cancel
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(6,8))

        # Prepare the blog tuple for the saver (same ordering as before)
        self._gen_save_args = (
            parent,
            (
                b_common, b_party, b_party_notes, b_num, b_qps, b_draft, b_emb, b_temp, b_db, b_prof, b_include_social,
                b_auto, b_days,
                b_purpose, b_stance_strength, b_lines_text,
                b_persona, b_persona_other, b_humor, b_heat,
                b_length, b_structure_text, b_must,
                b_fresh, b_numbers, b_cite,
                b_openings, b_devices, b_favavoid,
                b_guard_legal, b_bad, b_target, b_reading, b_cta,
                b_auto_web, b_web_cap, b_fail, b_output
            ),
            (
                p_voice, p_speed, p_pitch, p_format, p_intro, p_signoff, p_direction, p_reverb
            ),
        )

        ttk.Button(btns, text="Save", command=lambda: self._on_save(*self._gen_save_args)).pack(side="right", padx=(4,0))
        ttk.Button(btns, text="Cancel", command=self._on_cancel).pack(side="right")

        # If the user clicks the window close button, behave like Save so changes persist
        try:
            self.protocol("WM_DELETE_WINDOW", lambda: self._on_save(*self._gen_save_args))
        except Exception:
            pass

        # Grab
        self.grab_set()
        self.wait_visibility()
        self.focus_force()

        

    def _on_save(self, parent, blog_vals, podcast_vals=None):
        try:
            # Blog
            (b_common, b_party, b_party_notes, b_num, b_qps, b_draft, b_emb, b_temp, b_db, b_prof, b_include_social,
             b_auto, b_days,
             b_purpose, b_stance_strength, b_lines_text,
             b_persona, b_persona_other, b_humor, b_heat,
             b_length, b_structure_text, b_must,
             b_fresh, b_numbers, b_cite,
             b_openings, b_devices, b_favavoid,
             b_guard_legal, b_bad, b_target, b_reading, b_cta,
             b_auto_web, b_web_cap, b_fail, b_output) = blog_vals

            # Collect long/text fields from Text widgets safely
            def _text_get(tw):
                try:
                    return tw.get("1.0", "end").strip()
                except Exception:
                    return ""

            blog_cfg = {
                "topic": b_common["topic"].get(),
                "author": b_common["author"].get(),
                # political_party replaces the old angle/tone/tags fields
                "political_party": b_party.get() if hasattr(b_party, 'get') else "",
                "political_party_notes": b_party_notes.get() if hasattr(b_party_notes, 'get') else "",
                # no tags field in compact common fields; keep empty list
                "tags": [],
                "num_subtasks": int(b_num.get()),
                "queries_per_subtask": int(b_qps.get()),
                "drafting_model": b_draft.get(),
                "retrieval_model": b_emb.get(),
                "temperature": float(b_temp.get()),
                "db_path": b_db.get(),
                "profanity_level": b_prof.get(),
                "include_social": bool(b_include_social.get()),
                "auto_topic": bool(b_auto.get()),
                "auto_days": int(b_days.get()),
                # New fields
                "purpose": b_purpose.get(),
                "stance_strength": b_stance_strength.get(),
                "lines_you_wont_cross": _text_get(b_lines_text),
                "persona": b_persona.get(),
                "persona_other": b_persona_other.get(),
                "humor_level": b_humor.get(),
                "heat_level": b_heat.get(),
                "post_length": b_length.get(),
                "preferred_structure": _text_get(b_structure_text),
                "must_have_sections": b_must.get(),
                "freshness_requirement": b_fresh.get(),
                "numbers_to_prioritize": b_numbers.get(),
                "citation_style": b_cite.get(),
                "openings": _text_get(b_openings),
                "devices": b_devices.get(),
                "fav_avoid": _text_get(b_favavoid),
                "legal_guardrails": b_guard_legal.get(),
                "content_blocklist": _text_get(b_bad),
                "target_readers": b_target.get(),
                "reading_experience": b_reading.get(),
                "cta": b_cta.get(),
                "auto_web_search": b_auto_web.get(),
                "web_search_cap": int(b_web_cap.get()),
                "failure_behavior": b_fail.get(),
                "output_format": b_output.get(),
            }
            parent.gen_blog_cfg = blog_cfg

            # Podcast config (optional)
            try:
                if podcast_vals:
                    (p_voice_v, p_speed_v, p_pitch_v, p_format_v, p_intro_v, p_signoff_v, p_direction_v, p_reverb_v) = podcast_vals
                    def _text_get(tw):
                        try:
                            return tw.get("1.0", "end").strip()
                        except Exception:
                            return ""

                    # Determine enabled from the main UI checkbox (parent.gen_podcast_var)
                    try:
                        enabled_val = bool(getattr(parent, 'gen_podcast_var').get())
                    except Exception:
                        enabled_val = bool(podcast_saved.get('enabled', True))

                    raw_voice = p_voice_v.get() if hasattr(p_voice_v, 'get') else str(p_voice_v)
                    # If the display label contains 'generdr', normalize to the id 'generdr'
                    if 'generdr' in raw_voice.lower():
                        voice_val = 'generdr'
                    else:
                        # keep first token (most voices are single-word ids)
                        voice_val = (raw_voice.split()[0] if raw_voice else raw_voice)

                    # Map human-facing reverb label back to a canonical id for persistence
                    try:
                        raw_reverb = p_reverb_v.get() if hasattr(p_reverb_v, 'get') else str(p_reverb_v)
                    except Exception:
                        raw_reverb = str(p_reverb_v)
                    # Accept both numbered labels (new) and legacy unnumbered labels when mapping back
                    reverb_label_to_id = {
                        "1) None (no reverb)": 'none',
                        "None (no reverb)": 'none',
                        "2) Large echo": 'large_echo',
                        "Large echo": 'large_echo',
                        "3) Echo": 'echo',
                        "Echo": 'echo',
                        "4) Reverb": 'reverb',
                        "Reverb": 'reverb',
                        "5) Subtle reverb": 'subtle',
                        "Subtle reverb": 'subtle',
                        "6) Ultra subtle reverb": 'ultra_subtle',
                        "Ultra subtle reverb": 'ultra_subtle',
                    }
                    podcast_cfg = {
                        "enabled": enabled_val,
                        "voice": voice_val,
                        "speed": float(p_speed_v.get()) if hasattr(p_speed_v, 'get') else float(p_speed_v),
                        "pitch": int(p_pitch_v.get()) if hasattr(p_pitch_v, 'get') else int(p_pitch_v),
                        "format": (p_format_v.get() if hasattr(p_format_v, 'get') else str(p_format_v)) or 'mp3',
                        "intro_template": _text_get(p_intro_v),
                        "signoff": p_signoff_v.get() if hasattr(p_signoff_v, 'get') else str(p_signoff_v),
                        "direction": p_direction_v.get() if hasattr(p_direction_v, 'get') else str(p_direction_v),
                        "reverb": reverb_label_to_id.get(raw_reverb, 'none'),
                    }
                    parent.gen_podcast_cfg = podcast_cfg
            except Exception:
                pass

        except Exception as e:
            try:
                if hasattr(parent, "logger"):
                    parent.logger.write(f"[gen dialog] failed to collect params: {e}")
            except Exception:
                pass
        finally:
            # Persist to disk so generation params survive restarts
            try:
                if hasattr(parent, "save_parameters"):
                    parent.save_parameters()
                    try:
                        if hasattr(parent, 'logger'):
                            parent.logger.write(f"[gen dialog] saved generate params keys.") #: {list(parent.gen_blog_cfg.keys())}")
                    except Exception:
                        pass
            except Exception:
                pass

            self.grab_release()
            self.destroy()

    def _on_cancel(self):
        self.grab_release()
        self.destroy()


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
        self.geometry("1500x800")
        self.minsize(820, 560)

        # ---------- Tk variables (define BEFORE building UI) ----------
        # Source toggles
        self.src_guardian_var = tk.BooleanVar(value=True)
        self.src_gdelt_var    = tk.BooleanVar(value=True)
        self.src_youtube_var  = tk.BooleanVar(value=True)
        self.src_rss_var      = tk.BooleanVar(value=True)
        # Master enable for all source processing (compact control)
        self.sources_enabled_var = tk.BooleanVar(value=False)
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
        # NOTE: Politico and The Hill removed per request
        self.rss_feeds_var = tk.StringVar(value=(
            "http://feeds.bbci.co.uk/news/world/rss.xml;"
            "https://www.npr.org/rss/rss.php?id=1001;"
            "https://rss.cnn.com/rss/cnn_us.rss;"
            "https://feeds.foxnews.com/foxnews/politics;"
            "https://www.cbsnews.com/latest/rss/main;"
            "https://news.yahoo.com/rss"
        ))

        # --- Content Prep controls ---
        self.prep_run_var = tk.BooleanVar(value=True)   # run prep after ingest
        self.prep_min_words_var = tk.IntVar(value=120)
        self.prep_min_chars_var = tk.IntVar(value=220)
        self.prep_min_words_para_var = tk.IntVar(value=8)
        self.prep_chunk_chars_var = tk.IntVar(value=1500)
        self.prep_delete_short_var = tk.BooleanVar(value=False)
        self.prep_limit_rows_var = tk.IntVar(value=0)  # 0 = no limit
        self.prep_topk_var = tk.IntVar(value=6)
        self.prep_make_snippets_var = tk.BooleanVar(value=True)
        self.prep_index_refresh_var = tk.BooleanVar(value=True)
        self.prep_do_vectorize_var = tk.BooleanVar(value=False)
        self.prep_recompute_vecs_var = tk.BooleanVar(value=False)
        self.prep_batch_var = tk.IntVar(value=64)
        self.prep_model_var = tk.StringVar(value="text-embedding-3-large")

        # RAG defaults (keep names consistent; use getattr to preserve pre-existing vars)
        self.rag_enable_var = getattr(self, "rag_enable_var", tk.BooleanVar(value=True))
        self.rag_model_var = getattr(self, "rag_model_var", tk.StringVar(value="text-embedding-3-large"))
        self.rag_batch_var = getattr(self, "rag_batch_var", tk.IntVar(value=64))
        self.rag_recompute_var = getattr(self, "rag_recompute_var", tk.BooleanVar(value=False))

        # --- Generate Content placeholders ---
        # Master enable + individual content type toggles (placeholders)
        # master flag off by default; individual content types on
        self.gen_enabled_var = tk.BooleanVar(value=False)
        self.gen_blog_var = tk.BooleanVar(value=True)
        self.gen_post_var = tk.BooleanVar(value=True)
        self.gen_tweet_var = tk.BooleanVar(value=True)
        self.gen_podcast_var = tk.BooleanVar(value=True)
        self.gen_video_var = tk.BooleanVar(value=True)

        # Analysis inputs
        self.days_var = tk.IntVar(value=14)
        self.topn_var = tk.IntVar(value=20)
        self.halflife_var = tk.DoubleVar(value=7.0)
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

        # YouTube adapter requires a fetcher; provide a safe minimal one nested inside __init__.
        def _yt_fetcher(video_id: str, api_key: Optional[str] = None,
                        fetch_captions: bool = True, lang: str = "Any", logger=None) -> dict:
            # Fallback stub: lets the adapter run even if it calls fetcher directly.
            log_fn = logger or (getattr(self, "logger", None) and self.logger.write)
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
        # Load persisted parameters (if present)
        try:
            # load after UI vars are initialized
            self._load_parameters()
        except Exception:
            try:
                self.logger.write("[config] load parameters skipped (no file or error)")
            except Exception:
                pass

        # Ensure we save parameters on exit
        try:
            self.protocol("WM_DELETE_WINDOW", self._on_close)
        except Exception:
            pass

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

        # (Global inputs moved into the Source Parameters dialog)

        # ---- Sources (compact row) ----
        sources_outer = ttk.LabelFrame(inputs, text="Sources", padding=(10,8))
        sources_outer.grid(row=1, column=0, sticky="nw", pady=(8,0))
        s_row = _fixed_row(sources_outer)

        # Master enable for all source processing
        ttk.Checkbutton(s_row, text="Enable sources", variable=self.sources_enabled_var).grid(row=0, column=0, sticky="w", padx=(0,12))

        # Individual source toggles (compact)
        ttk.Checkbutton(s_row, text="Guardian", variable=self.src_guardian_var).grid(row=0, column=1, sticky="w", padx=(6,8))
        ttk.Checkbutton(s_row, text="GDELT", variable=self.src_gdelt_var).grid(row=0, column=2, sticky="w", padx=(6,8))
        ttk.Checkbutton(s_row, text="YouTube", variable=self.src_youtube_var).grid(row=0, column=3, sticky="w", padx=(6,8))
        ttk.Checkbutton(s_row, text="RSS", variable=self.src_rss_var).grid(row=0, column=4, sticky="w", padx=(6,8))

        # Button to open modal Source Parameters dialog (placed in the Sources row)
        ttk.Button(s_row, text="Source Parameters", command=lambda: self._open_source_params())\
            .grid(row=0, column=5, sticky="w", padx=(8,0))

        # Note: source-specific parameter widgets have been moved to the Source Parameters dialog.
        # --- Content Prep (compact controls) ---------------------------------------
        # Compact frame below Sources: master enable + Preparation, Cache, RAG
        # checkboxes, plus the Prep Parameters button (opens the modal dialog).
        prep_container = ttk.LabelFrame(inputs, text="Content Prep", padding=(10,8))
        prep_container.grid(row=97, column=0, columnspan=12,
                            sticky="ew", padx=6, pady=(8, 6))
        for c in range(12):
            prep_container.columnconfigure(c, weight=1)

        prep_row = ttk.Frame(prep_container)
        prep_row.grid(row=0, column=0, columnspan=12, sticky="w")

        # Master enable for content prep
        ttk.Checkbutton(prep_row, text="Enable content prep", variable=self.prep_enabled_var).grid(row=0, column=0, sticky="w", padx=(0,12))

        # Compact toggles
        ttk.Checkbutton(prep_row, text="Preparation", variable=self.prep_run_var).grid(row=0, column=1, sticky="w", padx=(6,8))
        ttk.Checkbutton(prep_row, text="Cache", variable=self.prep_index_refresh_var).grid(row=0, column=2, sticky="w", padx=(6,8))
        ttk.Checkbutton(prep_row, text="RAG", variable=self.rag_enable_var).grid(row=0, column=3, sticky="w", padx=(6,8))

        # Button to open the full parameters dialog
        ttk.Button(prep_row, text="Prep Parameters", command=lambda: self._open_prep_params()).grid(row=0, column=4, sticky="w", padx=(8,0))
        # Quick access: run the existing exporter (view_db.py) and show the human-readable cleaned file
        ttk.Button(prep_row, text="View Cleaned", command=lambda: self._run_and_show_ready_articles()).grid(row=0, column=5, sticky="w", padx=(8,0))

        # --- Generate Content (placeholders) ------------------------------------
        gen_container = ttk.LabelFrame(inputs, text="Generate Content", padding=(10,8))
        gen_container.grid(row=98, column=0, columnspan=12, sticky="ew", padx=6, pady=(6, 6))
        for c in range(12):
            gen_container.columnconfigure(c, weight=1)

        gen_row = ttk.Frame(gen_container)
        gen_row.grid(row=0, column=0, columnspan=12, sticky="w")

        ttk.Checkbutton(gen_row, text="Enable generate", variable=self.gen_enabled_var).grid(row=0, column=0, sticky="w", padx=(0,12))
        # Rename Blog checkbox to 'Posts' (keeps same variable so generation wiring is unchanged)
        ttk.Checkbutton(gen_row, text="Posts",   variable=self.gen_blog_var).grid(row=0, column=1, sticky="w", padx=(6,8))
        ttk.Checkbutton(gen_row, text="Podcast", variable=self.gen_podcast_var).grid(row=0, column=4, sticky="w", padx=(6,8))
        ttk.Checkbutton(gen_row, text="Video",  variable=self.gen_video_var).grid(row=0, column=5, sticky="w", padx=(6,8))
        ttk.Button(gen_row, text="Gen Parameters", command=lambda: self._open_gen_params()).grid(row=0, column=6, sticky="w", padx=(8,0))

        # ---- Controls (Run/Stop) ----
        controls = ttk.Frame(inputs)
        controls.grid(row=99, column=0, sticky="ew", pady=(8,0))
        self.run_btn  = ttk.Button(controls, text="Run",  command=self.on_run)
        self.stop_btn = ttk.Button(controls, text="Stop", command=self.on_stop, state="disabled")
        self.run_btn.grid(row=0, column=0, sticky="w", padx=(0,8))
        self.stop_btn.grid(row=0, column=1, sticky="w")
    # (Source Parameters button moved into the Sources row)

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
            parts = val.split()
            n = int(parts[0]) if parts else 2
            unit = parts[1] if len(parts) > 1 else "weeks"
        except Exception:
            n = 2
            unit = "weeks"
        today = date.today()
        if unit.startswith("day"):
            start = today - timedelta(days=max(0, n - 1))
        else:
            start = today - timedelta(days=n * 7 - 1)
        self.from_var.set(start.isoformat())
        self.to_var.set(today.isoformat())

    def _slug(self, s: str) -> str:
        import re
        s = re.sub(r'["“”‘’]', "", s)
        # Replace non-alphanumeric runs with a single dash
        s = re.sub(r"[^a-zA-Z0-9]+", "-", s)
        s = re.sub(r"-+", "-", s).strip("-").lower()
        # Truncate to a safe length to avoid Windows MAX_PATH issues
        s = s[:80].strip("-")
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

        # Existing sources (respect master enable)
        master_enabled = bool(getattr(self, "sources_enabled_var", tk.BooleanVar(value=True)).get())
        use_guardian = master_enabled and bool(self.src_guardian_var.get())
        use_gdelt    = master_enabled and bool(self.src_gdelt_var.get())
        use_youtube  = master_enabled and bool(self.src_youtube_var.get())

        # NEW sources
        use_rss = master_enabled and bool(getattr(self, "src_rss_var", tk.BooleanVar(value=False)).get())
        
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
                                "https://news.yahoo.com/rss"
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
                
                # --- Optional: Generate blog/post content if requested ---
                try:
                    if bool(getattr(self, "gen_enabled_var", tk.BooleanVar(value=False)).get()) and bool(getattr(self, "gen_blog_var", tk.BooleanVar(value=False)).get()) and not self._stop_flag:
                        if creator is None:
                            self.logger.write("[generate] creator_full_blog module not available; skipping generation.")
                        else:
                            # Build a BlogConfig from saved dialog params if present
                            blog_params = getattr(self, "gen_blog_cfg", {}) or {}
                            # If auto_topic requested, ask the creator regardless of whether the
                            # topic field contains the global topics placeholder (user may have left it).
                            topic = blog_params.get("topic") or ""
                            # Map political party selection to an abstract 'approach' string
                            # that guides the generator's angle without naming the party.
                            def _party_to_approach(party: str) -> str:
                                if not party:
                                    return ""
                                p = (party or "").lower()
                                try:
                                    if "progressive" in p or "democrat" in p or "democratic" in p:
                                        return "a progressive-leaning, center-left perspective"
                                    if "republican" in p or "maga" in p or "conservative" in p:
                                        return "a conservative, center-right perspective"
                                    if "socialist" in p:
                                        return "a left-populist, social-democratic perspective"
                                    if "libertarian" in p:
                                        return "a libertarian, small-government perspective"
                                    if "green" in p or "environment" in p:
                                        return "an environmentalist perspective"
                                    if "constitution" in p:
                                        return "a constitutionalist perspective"
                                except Exception:
                                    pass
                                return ""

                            if bool(blog_params.get("auto_topic", False)):
                                try:
                                    dbp = blog_params.get("db_path", "news.db")
                                    days_back = int(blog_params.get("auto_days", 7))
                                    try:
                                        self.logger.write(f"[generate] auto-topic requested: querying DB for last {days_back} day(s)...")
                                    except Exception:
                                        pass
                                    suggested = None
                                    try:
                                        suggested = creator.suggest_topic_from_db(
                                            db_path=dbp,
                                            days_back=days_back,
                                            angle=_party_to_approach(blog_params.get("political_party", "")),
                                            tone=blog_params.get("tone", "analytical"),
                                            keys_path=None,
                                            client=None,
                                            log_fn=(getattr(self, 'logger', None).write if hasattr(self, 'logger') else None),
                                        )
                                    except Exception as e:
                                        try:
                                            self.logger.write(f"[generate][auto-topic] suggestion failed: {e}")
                                        except Exception:
                                            pass
                                    if suggested:
                                        topic = suggested
                                        try:
                                            self.logger.write(f"[generate] auto-topic suggestion: {topic}")
                                        except Exception:
                                            pass
                                except Exception as e:
                                    try:
                                        self.logger.write(f"[generate][auto-topic] error: {e}")
                                    except Exception:
                                        pass
                            if not topic:
                                topic = self._first_topic() or "Untitled"

                            # Sanitize and normalize title: remove any front-matter or header lines,
                            # drop trailing "- analysis"/"— Analysis" suffixes, and apply
                            # light capitalization heuristics.
                            def _sanitize_title(raw_title: Optional[str], topic_fallback: str) -> str:
                                import re
                                t = (raw_title or "").strip()
                                if not t:
                                    t = topic_fallback or "Untitled"

                                # If YAML front matter present, drop it and use first content line
                                if t.startswith("---"):
                                    parts = t.split("---", 2)
                                    if len(parts) >= 3:
                                        # parts[2] is content after front matter
                                        body = parts[2]
                                        for line in body.splitlines():
                                            if line.strip():
                                                t = line.strip()
                                                break

                                # If there's an explicit Title: line inside, extract it
                                m = re.search(r"(?im)^title\s*:\s*(.+)$", t)
                                if m:
                                    t = m.group(1).strip()

                                # Take only the first non-empty line
                                t = (t.splitlines()[0] or "").strip()

                                # Remove common trailing analysis suffixes like "- analysis", "— Analysis", "(analysis)"
                                t = re.sub(r"\s*[-–—]\s*analysis\s*$", "", t, flags=re.I)
                                t = re.sub(r"\s*\(\s*analysis\s*\)\s*$", "", t, flags=re.I)

                                # If the title is all lower-case, convert to title case; otherwise respect original casing
                                if t and t == t.lower():
                                    t = t.title()

                                # Ensure first character is capitalized
                                if t:
                                    t = t[0].upper() + t[1:]
                                return t

                            title = _sanitize_title(blog_params.get("title"), topic)
                            try:
                                    cfg = creator.BlogConfig(
                                    title=title,
                                    topic=topic,
                                    angle=_party_to_approach(blog_params.get("political_party","")),
                                    tone=blog_params.get("tone","analytical"),
                                    author=blog_params.get("author","Editorial Desk"),
                                    tags=blog_params.get("tags",[]),
                                    num_subtasks=int(blog_params.get("num_subtasks",5)),
                                    queries_per_subtask=int(blog_params.get("queries_per_subtask",3)),
                                    drafting_model=blog_params.get("drafting_model","gpt-4o"),
                                    retrieval_model=blog_params.get("retrieval_model","text-embedding-3-large"),
                                    temperature=float(blog_params.get("temperature",0.7)),
                                    db_path=blog_params.get("db_path","news.db"),
                                    profanity_level=blog_params.get("profanity_level","clean"),
                                    include_social_blurb=bool(blog_params.get("include_social", True)),
                                    # map UI editorial knobs into BlogConfig where available
                                    purpose=blog_params.get('purpose'),
                                    stance_strength=blog_params.get('stance_strength'),
                                    lines_you_wont_cross=blog_params.get('lines_you_wont_cross'),
                                    persona=blog_params.get('persona'),
                                    persona_other=blog_params.get('persona_other'),
                                    humor_level=blog_params.get('humor_level'),
                                    heat_level=blog_params.get('heat_level'),
                                    post_length=blog_params.get('post_length'),
                                    preferred_structure=blog_params.get('preferred_structure'),
                                    must_have_sections=blog_params.get('must_have_sections'),
                                    freshness_requirement=blog_params.get('freshness_requirement'),
                                    numbers_to_prioritize=blog_params.get('numbers_to_prioritize'),
                                    citation_style=blog_params.get('citation_style'),
                                    openings=blog_params.get('openings'),
                                    devices=blog_params.get('devices'),
                                    fav_avoid=blog_params.get('fav_avoid'),
                                    legal_guardrails=blog_params.get('legal_guardrails'),
                                    content_blocklist=blog_params.get('content_blocklist'),
                                    target_readers=blog_params.get('target_readers'),
                                    reading_experience=blog_params.get('reading_experience'),
                                    cta=blog_params.get('cta'),
                                    auto_web_search=blog_params.get('auto_web_search'),
                                    web_search_cap=int(blog_params.get('web_search_cap', 0)) if blog_params.get('web_search_cap') is not None else None,
                                    failure_behavior=blog_params.get('failure_behavior'),
                                    output_format=blog_params.get('output_format'),
                                )
                            except Exception as e:
                                self.logger.write(f"[generate] failed to build BlogConfig: {e}")
                                cfg = None

                            if cfg is not None:
                                self.logger.write(f"[generate] Starting blog generation: title={cfg.title!r} topic={cfg.topic!r}")
                                try:
                                    # Run generator (this is already on the worker thread)
                                    # pass GUI logger into the generator so it can echo queries and progress
                                    result = creator.generate_blog_with_rag(cfg, brief=None, keys_path=None, debug=True, trace=False, log_fn=getattr(self, 'logger', None).write if hasattr(self, 'logger') else None)
                                    md = result.get("markdown", "")
                                    social = result.get("social", "")

                                    # Persist result to an timestamped folder
                                    slug = self._slug(cfg.title)
                                    date_tag = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                                    base_dir = os.path.dirname(__file__)
                                    outdir = os.path.join(base_dir, "out", f"{date_tag}_{slug}")
                                    os.makedirs(outdir, exist_ok=True)
                                    outpath = os.path.join(outdir, "post.md")
                                    # Write markdown file (log start/completion)
                                    try:
                                        self.logger.write(f"[generate] writing markdown to: {outpath}")
                                    except Exception:
                                        pass
                                    try:
                                        # Clean the generator markdown: remove any header/front-matter
                                        # and produce a file that contains only the title (as H1)
                                        # followed by the blog body text.
                                        def _strip_front_matter_and_title(text: str) -> str:
                                            t = text or ""
                                            # Remove YAML front matter if present (--- ... ---)
                                            if t.startswith("---"):
                                                parts = t.split("---", 2)
                                                if len(parts) >= 3:
                                                    t = parts[2]
                                            # Strip leading metadata lines like "Title: ..." or "Author: ..."
                                            lines = t.splitlines()
                                            i = 0
                                            # skip any leading empty lines
                                            while i < len(lines) and lines[i].strip() == "":
                                                i += 1
                                            # drop simple Key: Value header lines until a blank line
                                            while i < len(lines) and (":" in lines[i] and not lines[i].startswith("#")):
                                                i += 1
                                            # if the first non-empty line is a Markdown title, drop it
                                            if i < len(lines) and lines[i].lstrip().startswith("#"):
                                                i += 1
                                            # return the rest
                                            return "\n".join(lines[i:]).lstrip("\n")

                                        clean_body = _strip_front_matter_and_title(md)
                                        # Compose minimal markdown: plain title (no leading '#') + body
                                        title_text = (cfg.title or "").lstrip("# ").strip()
                                        minimal_md = f"{title_text}\n\n" + (clean_body.strip() + "\n" if clean_body and clean_body.strip() else "\n")
                                        with open(outpath, "w", encoding="utf-8") as fh:
                                            fh.write(minimal_md)
                                        try:
                                            self.logger.write(f"[generate] finished writing markdown: {outpath}")
                                        except Exception:
                                            pass
                                    except Exception as e:
                                        self.logger.write(f"[generate] failed to write markdown to {outpath}: {e}")
                                    if social:
                                        socpath = os.path.join(outdir, "social.txt")
                                        with open(socpath, "w", encoding="utf-8") as fh:
                                            fh.write(social)
                                        self.logger.write(f"[generate] Wrote social blurb: {socpath}")

                                    # If Podcast generation is enabled, create an MP3 using md_to_mp3
                                    if bool(getattr(self, 'gen_podcast_var', tk.BooleanVar(value=False)).get()):
                                        try:
                                            try:
                                                import md_to_mp3
                                            except Exception:
                                                md_to_mp3 = None

                                            if md_to_mp3 is None:
                                                self.logger.write("[generate] md_to_mp3 module not available; skipping podcast generation")
                                            else:
                                                # Read saved podcast config (if any)
                                                podcast_cfg = getattr(self, 'gen_podcast_cfg', {}) or {}
                                                voice_cfg = podcast_cfg.get('voice') or 'alloy'
                                                speed_cfg = float(podcast_cfg.get('speed', 1.0))
                                                pitch_cfg = int(podcast_cfg.get('pitch', 0))
                                                intro_tpl = podcast_cfg.get('intro_template') or (
                                                    "Welcome to Meerkat Media. Today's briefing: {title}. "
                                                    "We summarize the key facts, explain what matters, and point to original sources."
                                                )
                                                signoff_tpl = podcast_cfg.get('signoff') or (
                                                    "\n\nThanks for listening to Meerkat Media. Visit our site for full articles and sources. "
                                                    "Subscribe for updates and follow us on social."
                                                )

                                                # Create a temporary markdown with intro and signoff for narration
                                                podcast_md = os.path.join(outdir, "post_for_podcast.md")
                                                # Safe format of intro (replace {title} if present)
                                                try:
                                                    intro_text = intro_tpl.format(title=(cfg.title or ""))
                                                except Exception:
                                                    intro_text = intro_tpl.replace("{title}", (cfg.title or ""))

                                                try:
                                                    # Prepare podcast markdown: intro (with title) then the cleaned body
                                                    with open(podcast_md, "w", encoding="utf-8") as pf:
                                                        pf.write(intro_text + "\n\n")
                                                        pf.write(clean_body if clean_body else md)
                                                        pf.write("\n\n" + signoff_tpl)

                                                    keys_path = os.path.join(os.path.dirname(__file__), "keys.ini")
                                                    fmt = (podcast_cfg.get('format') or 'mp3').lower()
                                                    out_name = f"post.{fmt}"
                                                    mp3_out = os.path.join(outdir, out_name)

                                                    # Build md_to_mp3 CLI args and include voice/speed/pitch
                                                    args = ["--in", podcast_md, "--out", mp3_out, "--keys", keys_path, "--conversational"]
                                                    try:
                                                        if voice_cfg:
                                                            try:
                                                                import re as _re
                                                                raw_v = str(voice_cfg).strip()
                                                                m = _re.match(r"([A-Za-z0-9]+)", raw_v)
                                                                voice_arg = (m.group(1).lower() if m else raw_v.lower())
                                                            except Exception:
                                                                voice_arg = str(voice_cfg).strip().lower()
                                                            args.extend(["--voice", voice_arg])
                                                        # Include format in args so md_to_mp3 writes requested container
                                                        try:
                                                            if fmt:
                                                                args.extend(["--format", fmt])
                                                        except Exception:
                                                            pass

                                                        if speed_cfg is not None:
                                                            args.extend(["--speed", str(speed_cfg)])
                                                        if pitch_cfg is not None:
                                                            args.extend(["--pitch", str(pitch_cfg)])
                                                        # Include delivery direction if provided
                                                        direction_cfg = podcast_cfg.get('direction') if podcast_cfg else None
                                                        if direction_cfg:
                                                            args.extend(["--direction", str(direction_cfg)])
                                                        # Include reverb preset if provided (canonical id)
                                                        reverb_cfg = podcast_cfg.get('reverb') if podcast_cfg else None
                                                        if reverb_cfg and str(reverb_cfg).lower() not in ('', 'none'):
                                                            args.extend(["--reverb", str(reverb_cfg)])

                                                        # Log & verify input file, then capture stdout/stderr from helper
                                                        if os.path.isfile(podcast_md):
                                                            try:
                                                                sz = os.path.getsize(podcast_md)
                                                                self.logger.write(f"[generate] Calling md_to_mp3.") # with in={podcast_md} (size={sz} bytes) out={mp3_out} args={args}")
                                                            except Exception:
                                                                self.logger.write(f"[generate] Calling md_to_mp3.") # with in={podcast_md} out={mp3_out} args={args}")
                                                        else:
                                                            self.logger.write(f"[generate] md_to_mp3 input file missing: {podcast_md}; skipping helper call")
                                                            rc = 2
                                                            raise RuntimeError("md_to_mp3 input file missing")

                                                        import io as _io
                                                        old_out, old_err = sys.stdout, sys.stderr
                                                        sys.stdout = _io.StringIO()
                                                        sys.stderr = _io.StringIO()
                                                        try:
                                                            rc = md_to_mp3.main(args)  # type: ignore
                                                        finally:
                                                            out_txt = sys.stdout.getvalue()
                                                            err_txt = sys.stderr.getvalue()
                                                            sys.stdout, sys.stderr = old_out, old_err

                                                        if out_txt:
                                                            for ln in out_txt.rstrip().splitlines():
                                                                self.logger.write("[md_to_mp3 stdout] " + ln)
                                                        if err_txt:
                                                            for ln in err_txt.rstrip().splitlines():
                                                                self.logger.write("[md_to_mp3 stderr] " + ln)

                                                        if isinstance(rc, int) and rc == 0:
                                                            self.logger.write(f"[generate] Wrote podcast MP3: {mp3_out}")
                                                        else:
                                                            self.logger.write(f"[generate] md_to_mp3 finished with code: {rc} (no MP3 written)")
                                                    except SystemExit as se:
                                                        try:
                                                            self.logger.write(f"[generate] md_to_mp3 exited with SystemExit: {se}")
                                                        except Exception:
                                                            pass
                                                    except Exception as e:
                                                        try:
                                                            import traceback as _tb
                                                            self.logger.write(f"[generate] md_to_mp3 call failed: {e}")
                                                            for ln in _tb.format_exception(type(e), e, e.__traceback__):
                                                                for sub in ln.rstrip().splitlines():
                                                                    self.logger.write("[generate][trace] " + sub)
                                                        except Exception:
                                                            pass
                                                except Exception as e:
                                                    self.logger.write(f"[generate] could not prepare podcast markdown: {e}")
                                        except Exception:
                                            # keep generation robust: log and continue
                                            try:
                                                self.logger.write("[generate] unexpected error during podcast generation (see log)")
                                            except Exception:
                                                pass

                                    # Post-process: run social_variants to create platform variants next to the blog
                                    try:
                                        try:
                                            import social_variants
                                        except Exception:
                                            social_variants = None
                                        if social_variants is not None:
                                            # Call its main entry with the generated markdown path
                                            try:
                                                social_variants.main(["--input", outpath])
                                                self.logger.write(f"[generate] social_variants ran for: {outpath}")
                                            except Exception as e:
                                                self.logger.write(f"[generate] social_variants failed: {e}")
                                        else:
                                            self.logger.write("[generate] social_variants module not available; skipping post-processing")
                                    except Exception:
                                        pass

                                    # Open the cleaned markdown in the viewer on the main thread.
                                    # Prefer the minimal version we wrote to disk; fall back to generator output.
                                    try:
                                        display_md = minimal_md if 'minimal_md' in locals() else (clean_body if 'clean_body' in locals() and clean_body else md)
                                    except Exception:
                                        display_md = md
                                    self.after(0, lambda m=display_md: self._open_text_viewer("Generated Blog", m))
                                except Exception as e:
                                    import traceback
                                    self.logger.write("[generate error]\n" + "".join(traceback.format_exception(e)))
                except Exception as e:
                    self.logger.write(f"[generate guard error] {e}")
           

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

    def _open_source_params(self):
        """Open the modal dialog for editing source parameters."""
        try:
            SourceParametersDialog(self)
        except Exception as e:
            # Defensive logging to the GUI log if dialog creation fails
            if hasattr(self, "logger"):
                self.logger.write(f"[ui error] could not open Source Parameters dialog: {e}")
            else:
                print(f"[ui error] could not open Source Parameters dialog: {e}")

    def _open_prep_params(self):
        """Open the modal dialog for editing content prep / cache / RAG parameters."""
        try:
            ContentPrepDialog(self)
        except Exception as e:
            if hasattr(self, "logger"):
                self.logger.write(f"[ui error] could not open Prep Parameters dialog: {e}")
            else:
                print(f"[ui error] could not open Prep Parameters dialog: {e}")

    def _open_gen_params(self):
        """Open a placeholder dialog for Generate Content parameters."""
        try:
            GenerateContentDialog(self)
        except Exception as e:
            if hasattr(self, "logger"):
                self.logger.write(f"[ui error] could not open Generate Parameters dialog: {e}")
            else:
                print(f"[ui error] could not open Generate Parameters dialog: {e}")

    # ---------- Parameter persistence helpers ----------
    def _params_path(self) -> str:
        cfg_dir = os.path.join(os.path.dirname(__file__), "config")
        try:
            os.makedirs(cfg_dir, exist_ok=True)
        except Exception:
            pass
        return os.path.join(cfg_dir, "params.json")

    def save_parameters(self, path: Optional[str] = None):
        """Collect UI-controlled parameters and save to JSON file."""
        path = path or self._params_path()
        data = self._collect_parameters()
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
            try:
                self.logger.write(f"[config] saved parameters to {path}")
            except Exception:
                pass
        except Exception as e:
            try:
                self.logger.write(f"[config] failed to save parameters: {e}")
            except Exception:
                pass

    def _load_parameters(self, path: Optional[str] = None):
        """Load persisted parameters from disk and apply to UI variables."""
        path = path or self._params_path()
        if not os.path.isfile(path):
            # nothing to do
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self._apply_parameters(data)
            try:
                self.logger.write(f"[config] loaded parameters from {path}")
            except Exception:
                pass
        except Exception as e:
            try:
                self.logger.write(f"[config] failed to load parameters: {e}")
            except Exception:
                pass

    def _collect_parameters(self) -> dict:
        """Return a serializable dict of parameters to persist."""
        def safe_get(var):
            try:
                return var.get()
            except Exception:
                return None

        params = {
            "sources": {
                "sources_enabled": bool(safe_get(self.sources_enabled_var)),
                "src_guardian": bool(safe_get(self.src_guardian_var)),
                "src_gdelt": bool(safe_get(self.src_gdelt_var)),
                "src_youtube": bool(safe_get(self.src_youtube_var)),
                "src_rss": bool(safe_get(self.src_rss_var)),
            },
            "global": {
                "topics": safe_get(self.topics_var),
                "from": safe_get(self.from_var),
                "to": safe_get(self.to_var),
                "weeks": safe_get(self.weeks_var),
                "target_count": int(safe_get(self.target_count_var) or 0),
                "lang": safe_get(self.lang_var),
            },
            "guardian": {
                "section": safe_get(self.section_var),
                "page_size": int(safe_get(self.guardian_page_size) or 0),
            },
            "gdelt": {
                "slice_days": int(safe_get(self.gdelt_slice_days) or 0),
                "per_slice_cap": int(safe_get(self.gdelt_per_slice_cap) or 0),
                "sort": safe_get(self.gdelt_sort),
                "timeout": int(safe_get(self.gdelt_timeout) or 0),
                "allow_http": bool(safe_get(self.gdelt_allow_http)),
                "fetch_body": bool(safe_get(self.gdelt_fetch_body)),
            },
            "youtube": {
                # Do not persist API keys by default; keep only non-secret fields
                "mode": safe_get(self.yt_mode),
                "ident": safe_get(self.yt_ident),
                "max": int(safe_get(self.yt_max) or 0),
                "lang": safe_get(self.yt_lang),
                "fetch_captions": bool(safe_get(self.yt_fetch_captions_var)),
            },
            "rss": {
                "feeds": safe_get(self.rss_feeds_var),
                "max_items": int(safe_get(self.rss_max_items_var) or 0),
                "fetch_body": bool(safe_get(self.rss_fetch_body_var)),
                "fulltext_pass": bool(safe_get(self.rss_fulltext_pass_var) if hasattr(self, 'rss_fulltext_pass_var') else False),
            },
            "prep": {
                "prep_run": bool(safe_get(self.prep_run_var)),
                "min_words": int(safe_get(self.prep_min_words_var) or 0),
                "min_chars": int(safe_get(self.prep_min_chars_var) or 0),
                "chunk_chars": int(safe_get(self.prep_chunk_chars_var) or 0),
                "make_snippets": bool(safe_get(self.prep_make_snippets_var)),
                "index_refresh": bool(safe_get(self.prep_index_refresh_var)),
                "do_vectorize": bool(safe_get(self.prep_do_vectorize_var)),
                "batch": int(safe_get(self.prep_batch_var) or 0),
                "model": safe_get(self.prep_model_var),
            },
            "rag": {
                "enable": bool(safe_get(self.rag_enable_var)),
                "model": safe_get(self.rag_model_var),
                "batch": int(safe_get(self.rag_batch_var) or 0),
                "recompute": bool(safe_get(self.rag_recompute_var)),
                "date_from": safe_get(getattr(self, 'rag_date_from_var', tk.StringVar(value=''))),
                "date_to": safe_get(getattr(self, 'rag_date_to_var', tk.StringVar(value=''))),
                "topics_any": safe_get(getattr(self, 'rag_topics_any_var', tk.StringVar(value=''))),
            },
            "generate": {
                "enabled": bool(safe_get(self.gen_enabled_var)),
                "blog_cfg": getattr(self, 'gen_blog_cfg', {}),
                "post_cfg": getattr(self, 'gen_post_cfg', {}),
                "tweet_cfg": getattr(self, 'gen_tweet_cfg', {}),
                "podcast_cfg": getattr(self, 'gen_podcast_cfg', {}),
                "video_cfg": getattr(self, 'gen_video_cfg', {}),
            }
        }
        return params

    def _apply_parameters(self, params: dict):
        """Apply a loaded parameter dict to Tk variables and in-memory cfgs."""
        try:
            s = params.get('sources', {})
            if s:
                try: self.sources_enabled_var.set(bool(s.get('sources_enabled', self.sources_enabled_var.get())))
                except Exception: pass
                try: self.src_guardian_var.set(bool(s.get('src_guardian', self.src_guardian_var.get())))
                except Exception: pass
                try: self.src_gdelt_var.set(bool(s.get('src_gdelt', self.src_gdelt_var.get())))
                except Exception: pass
                try: self.src_youtube_var.set(bool(s.get('src_youtube', self.src_youtube_var.get())))
                except Exception: pass
                try: self.src_rss_var.set(bool(s.get('src_rss', self.src_rss_var.get())))
                except Exception: pass

            g = params.get('global', {})
            if g:
                try: self.topics_var.set(g.get('topics', self.topics_var.get()))
                except Exception: pass
                try: self.from_var.set(g.get('from', self.from_var.get()))
                except Exception: pass
                try: self.to_var.set(g.get('to', self.to_var.get()))
                except Exception: pass
                try: self.weeks_var.set(g.get('weeks', self.weeks_var.get()))
                except Exception: pass
                try: self.target_count_var.set(int(g.get('target_count', self.target_count_var.get())))
                except Exception: pass
                try: self.lang_var.set(g.get('lang', self.lang_var.get()))
                except Exception: pass

            guard = params.get('guardian', {})
            if guard:
                try: self.section_var.set(guard.get('section', self.section_var.get()))
                except Exception: pass
                try: self.guardian_page_size.set(int(guard.get('page_size', self.guardian_page_size.get())))
                except Exception: pass

            gd = params.get('gdelt', {})
            if gd:
                try: self.gdelt_slice_days.set(int(gd.get('slice_days', self.gdelt_slice_days.get())))
                except Exception: pass
                try: self.gdelt_per_slice_cap.set(int(gd.get('per_slice_cap', self.gdelt_per_slice_cap.get())))
                except Exception: pass
                try: self.gdelt_sort.set(gd.get('sort', self.gdelt_sort.get()))
                except Exception: pass
                try: self.gdelt_timeout.set(int(gd.get('timeout', self.gdelt_timeout.get())))
                except Exception: pass
                try: self.gdelt_allow_http.set(bool(gd.get('allow_http', self.gdelt_allow_http.get())))
                except Exception: pass
                try: self.gdelt_fetch_body.set(bool(gd.get('fetch_body', self.gdelt_fetch_body.get())))
                except Exception: pass

            yt = params.get('youtube', {})
            if yt:
                try: self.yt_mode.set(yt.get('mode', self.yt_mode.get()))
                except Exception: pass
                try: self.yt_ident.set(yt.get('ident', self.yt_ident.get()))
                except Exception: pass
                try: self.yt_max.set(int(yt.get('max', self.yt_max.get())))
                except Exception: pass
                try: self.yt_lang.set(yt.get('lang', self.yt_lang.get()))
                except Exception: pass
                try: self.yt_fetch_captions_var.set(bool(yt.get('fetch_captions', self.yt_fetch_captions_var.get())))
                except Exception: pass

            rss = params.get('rss', {})
            if rss:
                try: self.rss_feeds_var.set(rss.get('feeds', self.rss_feeds_var.get()))
                except Exception: pass
                try: self.rss_max_items_var.set(int(rss.get('max_items', self.rss_max_items_var.get())))
                except Exception: pass
                try: self.rss_fetch_body_var.set(bool(rss.get('fetch_body', self.rss_fetch_body_var.get())))
                except Exception: pass

            prep = params.get('prep', {})
            if prep:
                try: self.prep_run_var.set(bool(prep.get('prep_run', self.prep_run_var.get())))
                except Exception: pass
                try: self.prep_min_words_var.set(int(prep.get('min_words', self.prep_min_words_var.get())))
                except Exception: pass
                try: self.prep_min_chars_var.set(int(prep.get('min_chars', self.prep_min_chars_var.get())))
                except Exception: pass
                try: self.prep_chunk_chars_var.set(int(prep.get('chunk_chars', self.prep_chunk_chars_var.get())))
                except Exception: pass
                try: self.prep_make_snippets_var.set(bool(prep.get('make_snippets', self.prep_make_snippets_var.get())))
                except Exception: pass
                try: self.prep_index_refresh_var.set(bool(prep.get('index_refresh', self.prep_index_refresh_var.get())))
                except Exception: pass
                try: self.prep_do_vectorize_var.set(bool(prep.get('do_vectorize', self.prep_do_vectorize_var.get())))
                except Exception: pass
                try: self.prep_batch_var.set(int(prep.get('batch', self.prep_batch_var.get())))
                except Exception: pass
                try: self.prep_model_var.set(prep.get('model', self.prep_model_var.get()))
                except Exception: pass

            rag = params.get('rag', {})
            if rag:
                try: self.rag_enable_var.set(bool(rag.get('enable', self.rag_enable_var.get())))
                except Exception: pass
                try: self.rag_model_var.set(rag.get('model', self.rag_model_var.get()))
                except Exception: pass
                try: self.rag_batch_var.set(int(rag.get('batch', self.rag_batch_var.get())))
                except Exception: pass
                try: self.rag_recompute_var.set(bool(rag.get('recompute', self.rag_recompute_var.get())))
                except Exception: pass
                try: self.rag_date_from_var.set(rag.get('date_from', getattr(self, 'rag_date_from_var', tk.StringVar(value='')).get()))
                except Exception: pass
                try: self.rag_date_to_var.set(rag.get('date_to', getattr(self, 'rag_date_to_var', tk.StringVar(value='')).get()))
                except Exception: pass
                try: self.rag_topics_any_var.set(rag.get('topics_any', getattr(self, 'rag_topics_any_var', tk.StringVar(value='')).get()))
                except Exception: pass

            gen = params.get('generate', {})
            if gen:
                try: self.gen_enabled_var.set(bool(gen.get('enabled', self.gen_enabled_var.get())))
                except Exception: pass
                try:
                    if 'blog_cfg' in gen:
                        self.gen_blog_cfg = gen.get('blog_cfg') or {}
                except Exception:
                    pass
                try:
                    if 'post_cfg' in gen:
                        self.gen_post_cfg = gen.get('post_cfg') or {}
                except Exception:
                    pass
                try:
                    if 'tweet_cfg' in gen:
                        self.gen_tweet_cfg = gen.get('tweet_cfg') or {}
                except Exception:
                    pass
                try:
                    if 'podcast_cfg' in gen:
                        self.gen_podcast_cfg = gen.get('podcast_cfg') or {}
                except Exception:
                    pass
                try:
                    if 'video_cfg' in gen:
                        self.gen_video_cfg = gen.get('video_cfg') or {}
                except Exception:
                    pass
        except Exception:
            # best-effort; don't let malformed settings crash the UI
            pass

    def _on_close(self):
        """Save parameters then close the app."""
        try:
            self.save_parameters()
        except Exception:
            pass
        try:
            self.destroy()
        except Exception:
            try:
                self.quit()
            except Exception:
                pass

    def _run_and_show_ready_articles(self):
        """Run the existing exporter script (view_db.py) and open the newest ready_articles_readable.txt in a viewer.

        This leaves `view_db.py` unchanged and invokes it as a subprocess, then finds the newest export
        folder under ./exports and opens the human-readable TXT produced by the exporter.
        """
        def worker():
            try:
                self.logger.write("[export] invoking view_db.py to generate ready articles...\n")
                script_path = os.path.join(os.path.dirname(__file__), "view_db.py")
                # Run with the same Python executable to avoid env mismatch
                proc = subprocess.run([sys.executable, script_path], cwd=os.path.dirname(__file__), capture_output=True, text=True)
                if proc.stdout:
                    self.logger.write(proc.stdout)
                if proc.stderr:
                    self.logger.write("[export stderr] " + proc.stderr)


                # Prefer to parse the exporter stdout for the explicit path the script wrote.
                target = None
                try:
                    for line in (proc.stdout or "").splitlines():
                        line = line.strip()
                        # Example: "Wrote: exports\20251111-001418\ready_articles_readable.txt"
                        if line.lower().startswith("wrote:") and "ready_articles_readable.txt" in line.lower():
                            # extract the path after the colon
                            parts = line.split("", 1)
                            # fallback: split by 'Wrote:'
                            try:
                                _, rest = line.split("Wrote:", 1)
                            except Exception:
                                try:
                                    _, rest = line.split("wrote:", 1)
                                except Exception:
                                    rest = line
                            candidate = rest.strip()
                            # If path is relative, make it absolute relative to project dir
                            if not os.path.isabs(candidate):
                                candidate = os.path.join(os.path.dirname(__file__), candidate)
                            target = os.path.normpath(candidate)
                            break
                except Exception:
                    target = None

                # If parsing stdout didn't find it, fall back to scanning exports/ for the newest file
                if not target:
                    exports_dir = os.path.join(os.path.dirname(__file__), "exports")
                    if not os.path.isdir(exports_dir):
                        self.logger.write(f"[export] exports directory not found at {exports_dir}")
                        return

                    # Find the newest export subdirectory
                    subdirs = [os.path.join(exports_dir, d) for d in os.listdir(exports_dir) if os.path.isdir(os.path.join(exports_dir, d))]
                    if not subdirs:
                        self.logger.write(f"[export] no export subfolders found in {exports_dir}")
                        return
                    latest = max(subdirs, key=os.path.getmtime)
                    target = os.path.join(latest, "ready_articles_readable.txt")

                # Log target and verify
                self.logger.write(f"[export] attempting to open: {target}")
                if not os.path.isfile(target):
                    self.logger.write(f"[export] target file does not exist: {target}")
                    return
                try:
                    size = os.path.getsize(target)
                except Exception:
                    size = None
                self.logger.write(f"[export] target exists; size={size}")

                try:
                    with open(target, "r", encoding="utf-8") as fh:
                        content = fh.read()
                except Exception as e:
                    self.logger.write(f"[export] failed to read {target}: {e}")
                    return

                # Show on the main thread (schedule and log)
                self.after(0, lambda: self._show_ready_in_ui(content, target))
            except Exception as e:
                import traceback
                self.logger.write("[export error]\n" + "".join(traceback.format_exception(e)))

        threading.Thread(target=worker, daemon=True).start()
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
        try:
            win.lift()
            # briefly make topmost to force it in front, then clear the flag
            try:
                win.attributes("-topmost", True)
                self.after(100, lambda: win.attributes("-topmost", False))
            except Exception:
                pass
        except Exception:
            pass

    def _show_ready_in_ui(self, content: str, path: Optional[str] = None):
        """Called on the main/UI thread to show the prepared articles and log the action."""
        try:
            self.logger.write(f"[export] opening viewer for: {path}")
        except Exception:
            pass
        try:
            self._open_text_viewer("Prepared Articles", content)
        except Exception as e:
            try:
                self.logger.write(f"[export] failed to open viewer: {e}")
            except Exception:
                pass
        # Also attempt to open the file with the OS default application as a fallback
        """
        try:
            if path and os.path.isfile(path):
                try:
                    # Windows-specific convenient opener
                    os.startfile(path)
                    try:
                        self.logger.write(f"[export] opened file with OS default: {path}")
                    except Exception:
                        pass
                except Exception:
                    # not a Windows platform or failed; ignore
                    pass
        except Exception:
            pass
        """
        
    def _fetch_articles(self, days_window: int = 14, limit: int = 10):
        conn = sqlite3.connect("news.db")
        cur = conn.cursor()
        # Query recent articles (limit). Kept explicit to avoid syntax issues.
        cur.execute(
            "SELECT title, published_at, canonical_url, "
            "COALESCE(keyphrases_json,'[]'), COALESCE(entities_json,'[]'), "
            "COALESCE(body,''), COALESCE(summary,'') "
            "FROM articles "
            "ORDER BY (published_at IS NULL), published_at DESC "
            "LIMIT ?",
            (limit,)
        )
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
# -------------------- Main --------------------
if __name__ == "__main__":
    app = WebFlooderGUI()
    app.mainloop()
