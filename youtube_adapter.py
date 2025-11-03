# adapters/youtube_adapter.py
from __future__ import annotations
import sqlite3, json, hashlib, time, re
from typing import Callable, Dict, Optional, Any, List
from datetime import datetime
import requests

try:
    from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
    _HAS_YTA = True
except Exception:
    _HAS_YTA = False

# at top of youtube_adapter.py (near imports)
import time
import random
try:
    from youtube_transcript_api import YouTubeRequestFailed
except Exception:
    YouTubeRequestFailed = Exception  # safety fallback


# Robust import whether flat or package layout
try:
    from db_schema import ensure_youtube_schema
except ImportError:
    from ..db_schema import ensure_youtube_schema  # type: ignore

import time, threading
from collections import deque

class _TimedTextLimiter:
    """
    Token-bucket limiter specifically for YouTube timedtext (extremely sensitive).
    Allows ~0.2 RPS by default (1 request every 5s). Thread-safe.
    """
    def __init__(self, min_interval_sec: float = 5.0, burst: int = 1):
        self.min_interval = float(min_interval_sec)
        self.burst = int(burst)
        self._lock = threading.Lock()
        self._hits = deque(maxlen=burst)

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if len(self._hits) < self.burst:
                self._hits.append(now)
                return
            oldest = self._hits[0]
            elapsed = now - oldest
            need = self.min_interval - elapsed
            if need > 0:
                time.sleep(need)
            self._hits.append(time.monotonic())

_TIMEDTEXT_LIMITER = _TimedTextLimiter(min_interval_sec=5.0, burst=1)  # start conservative

# ---------- small helpers ----------
def _sha256(s: str | bytes) -> str:
    if isinstance(s, str):
        s = s.encode("utf-8", errors="ignore")
    return hashlib.sha256(s).hexdigest()

def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def _to_rfc3339(datestr: str | None) -> str | None:
    """Accept YYYY-MM-DD; emit midnight UTC RFC3339."""
    if not datestr:
        return None
    try:
        dt = datetime.strptime(datestr.strip(), "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return None

# timestamp like 1:23:45 or 23:45
_TS_RE = re.compile(r"\b(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\b")

def _hms_to_seconds(h: int, m: int, s: int) -> int:
    return h * 3600 + m * 60 + s

def _parse_chapters_from_description(desc: str) -> list[dict]:
    """
    From lines like:
      00:00 Intro
      05:12 Segment A
    -> [{"title": "...", "start": seconds}, ...]
    (No 'end' because we don't know duration here; analyzer doesn’t need it.)
    """
    if not desc:
        return []
    out = []
    for line in desc.splitlines():
        line = line.strip()
        m = _TS_RE.search(line)
        if not m:
            continue
        h = int(m.group(1) or 0)
        mm = int(m.group(2))
        ss = int(m.group(3))
        start = _hms_to_seconds(h, mm, ss)
        title = line[m.end():].strip(" -–—:") or f"Chapter at {m.group(0)}"
        out.append({"title": title, "start": start})
    return out

def _debug_list_transcripts(video_id: str, logger):
    if not _HAS_YTA:
        logger("[yt dbg] transcript api not available")
        return
    try:
        ts = YouTubeTranscriptApi.list_transcripts(video_id)
        found = []
        for t in ts:
            # each t has .language_code, .is_generated, .is_translatable
            found.append(f"{t.language_code}:{'gen' if getattr(t, 'is_generated', False) else 'off'}:{'X' if getattr(t, 'is_translatable', False) else '-'}")
        logger(f"[yt dbg] {video_id} available={found or '[]'}")
    except Exception as e:
        logger(f"[yt dbg] {video_id} list_transcripts error: {type(e).__name__}: {e}")

def _try_fetch_transcript(video_id: str, preferred_langs: list[str], logger):
    """
    Returns (text, captions_type, lang) or ("", "none", None).

    Strategy (using the transcripts we just listed, not the 'find_*' helpers):
      A) Exact-language matches (official first, then generated)
      B) Translate any non-EN to EN (official first, then generated)
      C) First available (official > generated)
    """
    if not _HAS_YTA:
        return "", "none", None

    def expand(langs: list[str]) -> list[str]:
        out, seen = [], set()
        for c in langs or []:
            c = (c or "").lower()
            if not c or c == "any":
                continue
            if c == "en":
                for v in ("en", "en-us", "en-gb", "en-ca", "en-au"):
                    if v not in seen:
                        out.append(v); seen.add(v)
            else:
                if c not in seen:
                    out.append(c); seen.add(c)
        return out or ["en", "en-us", "en-gb"]

    prefs = expand(preferred_langs)

    try:
        ts_list = YouTubeTranscriptApi.list_transcripts(video_id)
        # Build typed lists so we can control order
        all_transcripts = list(ts_list)  # iterable -> list of Transcript objects
        official = [t for t in all_transcripts if not getattr(t, "is_generated", False)]
        generated = [t for t in all_transcripts if getattr(t, "is_generated", False)]

        def _fetch(t, tag=""):
            """
            Fetch with global rate limit and long backoff on 429/5xx.
            Returns (text, ctype, lang) or ("","","").
            """
            attempts = 5
            base = 3.0   # start modest; we also have the global limiter
            for i in range(attempts):
                # Global limiter: never burst timedtext
                _TIMEDTEXT_LIMITER.wait()
                try:
                    parts = t.fetch()  # youtube_transcript_api call
                    text = " ".join(seg.get("text", "") for seg in parts if seg.get("text")).strip()
                    if text:
                        ctype = "official" if not getattr(t, "is_generated", False) else "asr"
                        return text, ctype, t.language_code
                    return "", "", ""
                except YouTubeRequestFailed as e:
                    msg = str(e)
                    transient = ("Too Many Requests", "429", "503", "timed out", "temporarily", "quota")
                    if any(s in msg for s in transient):
                        # Exponential backoff with jitter, but LONG because YouTube is prickly
                        if i < attempts - 1:
                            delay = base * (2 ** i) + (0.5 * i)
                            logger(f"[yt] fetch retry {i+1}/{attempts} {tag} {t.language_code} in {delay:.2f}s: \n{msg}")
                            time.sleep(delay)
                            continue
                        # final failure
                        logger(f"[yt] fetch error {tag} {video_id} {t.language_code}: {type(e).__name__}: \n{msg}")
                        return "", "", ""
                    else:
                        logger(f"[yt] fetch error {tag} {video_id} {t.language_code}: {type(e).__name__}: \n{msg}")
                        return "", "", ""
                except Exception as e:
                    logger(f"[yt] fetch error {tag} {video_id} {t.language_code}: {type(e).__name__}: {e}")
                    return "", "", ""
            return "", "", ""


        # A1) exact-language official
        for code in prefs:
            for t in official:
                if t.language_code.lower() == code:
                    text, ctype, lang = _fetch(t, tag="exact-official")
                    if text:
                        return text, ctype, lang

        # A2) exact-language generated
        for code in prefs:
            for t in generated:
                if t.language_code.lower() == code:
                    text, ctype, lang = _fetch(t, tag="exact-generated")
                    if text:
                        return text, ctype, lang

        # B1) translate any official -> en
        for t in official:
            if getattr(t, "is_translatable", False):
                try:
                    en_t = t.translate("en")
                    text, _, _ = _fetch(en_t, tag="translate-official")
                    if text:
                        return text, "official", "en"
                except Exception as e:
                    logger(f"[yt] translate error official {video_id} {t.language_code}: {type(e).__name__}: {e}")

        # B2) translate any generated -> en
        for t in generated:
            if getattr(t, "is_translatable", False):
                try:
                    en_t = t.translate("en")
                    text, _, _ = _fetch(en_t, tag="translate-generated")
                    if text:
                        return text, "asr", "en"
                except Exception as e:
                    logger(f"[yt] translate error generated {video_id} {t.language_code}: {type(e).__name__}: {e}")

        # C) first available (official > generated)
        for t in official + generated:
            text, ctype, lang = _fetch(t, tag="first-available")
            if text:
                return text, ctype, lang

        logger(f"[yt] no transcript found for {video_id} after walking listed tracks")
        return "", "none", None

    except (TranscriptsDisabled, NoTranscriptFound):
        logger(f"[yt] transcripts not available for {video_id} (disabled/none)")
        return "", "none", None
    except Exception as e:
        logger(f"[yt] transcript fetch error for {video_id}: {type(e).__name__}: {e}")
        return "", "none", None


class YouTubeAdapter:
    """
    Minimal YouTube adapter.

    Provide a fetcher with signature:
        fetcher(video_id: str, *, api_key: str, fetch_captions: bool, lang: str, logger=None) -> dict

    It must return a dict with keys:
        video_id, channel_id, channel_title, title, description, published_at,
        duration_secs, lang, captions_type, transcript_text, chapters, yt_metadata
    """

    # match URLs or raw 11-char IDs
    _YT_ID_RE = re.compile(r"(?:v=|/shorts/|/videos/|/embed/|youtu\.be/)([A-Za-z0-9_-]{11})")

    def __init__(
        self,
        conn: sqlite3.Connection,
        fetcher: Callable[..., Dict[str, Any]],
        logger: Optional[Callable[[str], None]] = None,
    ):
        self.conn = conn
        self.fetcher = fetcher
        self.logger = logger or (lambda s: None)
        self.api_key: str = ""                 # <-- set via set_api_key(...)
        ensure_youtube_schema(self.conn)
        

    # ---------- configuration ----------
    def set_api_key(self, key: str) -> None:
        self.api_key = (key or "").strip()

    

    # ---------- core DB op ----------
    def upsert_video(self, rec: Dict[str, Any]) -> bool:
        """
        Insert if new; if exists, update selective fields (transcript, captions, chapters, meta).
        Returns True if inserted, False if it existed (update/no-op).
        """
        cur = self.conn.cursor()
        text = (rec.get("transcript_text") or "").strip()
        chapters_json = _json_dumps(rec.get("chapters") or [])
        meta_json = _json_dumps(rec.get("yt_metadata") or {})
        text_hash = _sha256(text) if text else None

        params = (
            rec.get("video_id"),
            rec.get("channel_id"),
            rec.get("channel_title"),
            rec.get("title"),
            rec.get("description"),
            rec.get("published_at"),
            rec.get("duration_secs"),
            rec.get("lang"),
            rec.get("captions_type"),
            text,
            chapters_json,
            meta_json,
            text_hash,
        )

        try:
            with self.conn:
                cur.execute(
                    """
                    INSERT INTO youtube_videos (
                        video_id, channel_id, channel_title, title, description,
                        published_at, duration_secs, lang, captions_type,
                        transcript_text, chapters_json, yt_metadata_json,
                        text_hash, fetched_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
                    """,
                    params,
                )
            return True  # inserted
        except sqlite3.IntegrityError:
            # exists -> update only when we have something useful
            with self.conn:
                cur.execute(
                    """
                    UPDATE youtube_videos
                    SET
                        title          = COALESCE(title, ?),
                        description    = CASE WHEN (description IS NULL OR TRIM(description)='') THEN ? ELSE description END,
                        published_at   = COALESCE(published_at, ?),
                        duration_secs  = COALESCE(duration_secs, ?),
                        lang           = COALESCE(lang, ?),
                        captions_type  = COALESCE(captions_type, ?),

                        transcript_text = CASE
                            WHEN (? IS NOT NULL AND TRIM(?)<>'') THEN ?
                            ELSE transcript_text
                        END,

                        chapters_json   = CASE
                            WHEN (chapters_json IS NULL OR TRIM(chapters_json)='') THEN ?
                            ELSE chapters_json
                        END,
                        yt_metadata_json = CASE
                            WHEN (yt_metadata_json IS NULL OR TRIM(yt_metadata_json)='') THEN ?
                            ELSE yt_metadata_json
                        END,

                        text_hash      = COALESCE(text_hash, ?),
                        fetched_at     = datetime('now')
                    WHERE video_id = ?
                    """,
                    (
                        rec.get("title"),
                        rec.get("description"),
                        rec.get("published_at"),
                        rec.get("duration_secs"),
                        rec.get("lang"),
                        rec.get("captions_type"),

                        text, text, text,

                        chapters_json,
                        meta_json,

                        text_hash,
                        rec.get("video_id"),
                    ),
                )
            return False  # existed (but transcript/etc. now updated if present)



    # ---------- single-video ingest ----------
    def ingest_by_video_id(self, video_id: str, *, fetch_captions: bool = True, lang: str = "Any") -> int:
        if not self.api_key:
            raise ValueError("No YouTube API key set in adapter. Call set_api_key(...) first.")

        meta = self.fetcher(
            video_id,
            api_key=self.api_key,
            fetch_captions=fetch_captions,
            lang=lang,
            logger=self.logger,
        )

        # inside ingest_by_video_id, before the fallback fetch block:
        _debug_list_transcripts(video_id, self.logger)

        # --- fill transcript if missing and requested (REPLACE this whole block with the one below) ---
        if fetch_captions and not (meta.get("transcript_text") or "").strip():
            if not _HAS_YTA:
                self.logger("[yt] captions requested but youtube-transcript-api not installed; skipping transcript fetch")
            else:
                # Build richer language preferences
                prefs: list[str] = []
                if lang and lang.lower() != "any":
                    prefs.append(lang)
                m = meta.get("yt_metadata") or {}
                default_lang = (m.get("defaultLanguage") or m.get("defaultAudioLanguage") or "").lower()
                if default_lang:
                    prefs.append(default_lang)
                # English + common variants last
                prefs += ["en", "en-us", "en-gb"]

                text, ctype, out_lang = _try_fetch_transcript(video_id, prefs, self.logger)
                if text:
                    meta["transcript_text"] = text
                    if not meta.get("captions_type"): meta["captions_type"] = ctype
                    if not meta.get("lang"): meta["lang"] = out_lang
                    self.logger(f"[yt] transcript fetched chars={len(text)} type={ctype} lang={out_lang}")
                else:
                    self.logger(f"[yt] no transcript available via API for vid={video_id} prefs={prefs}")

        inserted = self.upsert_video(meta)
        try:
            row = self.conn.execute(
                "SELECT LENGTH(transcript_text) FROM youtube_videos WHERE video_id=?",
                (video_id,)
            ).fetchone()
            self.logger(f"[yt] post-upsert vid={video_id} transcript_len={row[0] if row else None}")
        except Exception as e:
            self.logger(f"[yt] post-upsert check failed: {e}")

        return 1 if inserted else 0

    # ---------- search helpers ----------
    @staticmethod
    def _extract_video_id(text: str) -> Optional[str]:
        if not text:
            return None
        m = YouTubeAdapter._YT_ID_RE.search(text)
        if m:
            return m.group(1)
        t = text.strip()
        if len(t) == 11 and re.fullmatch(r"[A-Za-z0-9_-]{11}", t):
            return t
        return None

    def _search_video_ids(
        self, *,
        api_key: str,
        query: str,
        max_videos: int = 20,
        since: Optional[str] = None,
        until: Optional[str] = None,
        relevance_lang: Optional[str] = None,
        log_fn: Optional[Callable[[str], None]] = None,
        stop_cb: Optional[Callable[[], bool]] = None,
    ) -> List[str]:
        """
        Use YouTube Data API v3 search.list to resolve a query → videoIds.
        """
        log = log_fn or self.logger
        ids: List[str] = []

        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "key": api_key,
            "q": query,
            "part": "id",
            "type": "video",
            "maxResults": 50,
            "order": "date",
        }
        if relevance_lang and relevance_lang.lower() != "any":
            params["relevanceLanguage"] = relevance_lang

        pa = _to_rfc3339(since)
        pb = _to_rfc3339(until)
        if pa: params["publishedAfter"] = pa
        if pb: params["publishedBefore"] = pb

        next_token = None
        while len(ids) < max_videos:
            if stop_cb and stop_cb():
                log("[yt] stop requested during search paging")
                break

            if next_token:
                params["pageToken"] = next_token
            else:
                params.pop("pageToken", None)

            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                log(f"[yt] search HTTP {r.status_code}: {r.text[:200]}")
                break

            data = r.json()
            for item in data.get("items", []):
                if item.get("id", {}).get("kind") == "youtube#video":
                    vid = item["id"].get("videoId")
                    if vid:
                        ids.append(vid)
                        if len(ids) >= max_videos:
                            break

            next_token = data.get("nextPageToken")
            if not next_token or len(ids) >= max_videos:
                break

            time.sleep(0.15)  # polite QPS pause

        return ids[:max_videos]

    def ingest_from_search_query(
        self, *,
        query: str,
        api_key: str,
        max_videos: int = 20,
        lang: str = "Any",
        fetch_captions: bool = True,
        since: Optional[str] = None,
        until: Optional[str] = None,
        log_fn: Optional[Callable[[str], None]] = None,
        stop_cb: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, int]:
        # in ingest_from_search_query(...)


        log = log_fn or self.logger
        stats = {"fetched": 0, "inserted": 0, "duplicates": 0}
        log(f"[yt] options fetch_captions={fetch_captions} lang={lang} max={max_videos}")

        # 1) If the query is a direct video URL/ID, short-circuit and mirror it.
        one_vid = self._extract_video_id(query)
        if one_vid:
            log(f"[yt] treating query as video id/url → {one_vid}")
            try:
                ins = self.ingest_by_video_id(one_vid, fetch_captions=fetch_captions, lang=lang)
                stats["fetched"] = 1
                stats["inserted"] += int(ins or 0)
            except Exception as e:
                log(f"[yt][warn] ingest failed for {one_vid}: {e}")
                return stats
            # Mirror → articles (so exports see transcript in body)
            try:
                art_id = self.mirror_video_into_articles(one_vid)
                log(f"[yt] mirrored {one_vid} → article_id={art_id}")
            except Exception as e:
                log(f"[yt][warn] mirror failed for {one_vid}: {e}")
            return stats

        # 2) Normal search → ids
        ids: List[str] = []
        try:
            ids = self._search_video_ids(
                api_key=api_key,
                query=query,
                max_videos=max_videos,
                since=since,
                until=until,
                relevance_lang=lang,
                log_fn=log_fn,
                stop_cb=stop_cb,
            )
        except Exception as e:
            log(f"[youtube error] search failed: {e}")
            return stats

        stats["fetched"] = len(ids)
        log(f"[youtube search] resolved {len(ids)} ids")

        # 3) Ingest each id and mirror to articles
        for i, vid in enumerate(ids, 1):
            if stop_cb and stop_cb():
                log("[yt] stop requested during per-video ingest")
                break

            log(f"[yt] ingest {i}/{len(ids)} → {vid}")
            try:
                ins = self.ingest_by_video_id(vid, fetch_captions=fetch_captions, lang=lang)
                stats["inserted"] += int(ins or 0)
            except Exception as e:
                log(f"[yt][warn] ingest failed for {vid}: {e}")
                continue

            # Mirror so transcript lands in articles.body
            try:
                art_id = self.mirror_video_into_articles(vid)
                try:
                    trow = self.conn.execute("SELECT LENGTH(transcript_text) FROM youtube_videos WHERE video_id=?", (vid,)).fetchone()
                    arow = self.conn.execute("SELECT LENGTH(body) FROM articles WHERE id=?", (art_id,)).fetchone() if art_id else (None,)
                    log(f"[yt] mirrored {vid} → article_id={art_id} tlen={trow[0] if trow else None} body_len={arow[0] if arow else None}")
                except Exception:
                    pass
            
            except Exception as e:
                log(f"[yt][warn] mirror failed for {vid}: {e}")

            # after stats["inserted"] update (and after mirroring)
            time.sleep(0.6 + random.uniform(0.0, 0.4))

        return stats


    def ingest_from_channel(self, *args, **kwargs) -> Dict[str, int]:
        (kwargs.get("log_fn") or self.logger)("[yt] channel ingestion not implemented yet.")
        return {"fetched": 0, "inserted": 0, "duplicates": 0}

    def ingest_from_playlist(self, *args, **kwargs) -> Dict[str, int]:
        (kwargs.get("log_fn") or self.logger)("[yt] playlist ingestion not implemented yet.")
        return {"fetched": 0, "inserted": 0, "duplicates": 0}

    def mirror_video_into_articles(self, video_id: str, *, source_domain: str = "youtube.com", lang_fallback: str = "en") -> Optional[int]:
        cur = self.conn.cursor()
        row = cur.execute(
            """SELECT video_id, title, description, published_at, transcript_text, lang, channel_title
            FROM youtube_videos WHERE video_id=?""",
            (video_id,),
        ).fetchone()
        if not row:
            self.logger(f"[yt mirror] no youtube_videos row for {video_id}")
            return None

        _, title, description, published_at, transcript, lang, channel_title = row
        body = (transcript or "").strip()
        summary = (description or "").strip()
        lang = lang or lang_fallback
        canonical_url = f"https://www.youtube.com/watch?v={video_id}"
        fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        text_hash = hashlib.sha256(body.encode("utf-8")).hexdigest() if body else None

        article_id = None
        try:
            with self.conn:
                cur.execute(
                    """
                    INSERT INTO articles (
                        source_domain, source_type, canonical_url, title, section, author,
                        published_at, fetched_at, lang, summary, body, tags_json, text_hash
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_domain, "youtube", canonical_url, title, None, channel_title,
                        published_at, fetched_at, lang, summary, body,
                        json.dumps(["youtube"], ensure_ascii=False), text_hash
                    ),
                )
                article_id = cur.lastrowid
        except sqlite3.IntegrityError:
            # Already exists – look it up
            row2 = cur.execute("SELECT id FROM articles WHERE canonical_url=?", (canonical_url,)).fetchone()
            article_id = row2[0] if row2 else None

        if article_id is not None:
            # NEW: backfill body/summary if still empty
            try:
                with self.conn:
                    cur.execute(
                        """
                        UPDATE articles
                        SET
                            body = CASE
                                WHEN (body IS NULL OR TRIM(body)='') AND ? IS NOT NULL AND TRIM(?)<>'' THEN ?
                                ELSE body END,
                            summary = CASE
                                WHEN (summary IS NULL OR TRIM(summary)='') AND ? IS NOT NULL AND TRIM(?)<>'' THEN ?
                                ELSE summary END,
                            lang = COALESCE(lang, ?),
                            fetched_at = COALESCE(fetched_at, ?),
                            text_hash = CASE
                                WHEN (body IS NULL OR TRIM(body)='') AND ? IS NOT NULL AND TRIM(?)<>'' THEN ?
                                ELSE text_hash END
                        WHERE id = ?
                        """,
                        (
                            body, body, body,           # body fill
                            summary, summary, summary,  # summary fill
                            lang, fetched_at,           # language + timestamp
                            body, body, text_hash,      # text_hash fill
                            article_id,
                        ),
                    )
            except Exception as e:
                self.logger(f"[yt mirror][warn] backfill failed for article_id={article_id}: {e}")

            with self.conn:
                cur.execute(
                    "INSERT OR IGNORE INTO article_youtube_map(article_id, video_id) VALUES(?,?)",
                    (article_id, video_id),
                )

        self.logger(f"[yt mirror] video {video_id} → article_id={article_id} body_len={len(body)}")
        return article_id
