#!/usr/bin/env python3
r"""
md_to_mp3.py — Turn a Markdown blog post into a narrated MP3 using OpenAI TTS.

Usage (PowerShell / VS Code terminal):
  python -u .\md_to_mp3.py --in .\out\post_spicy_heavy.md --out .\out\post_spicy_heavy.mp3 --keys .\keys.ini

Optional knobs:
  --voice alloy|verse|sage|... (model voices)
  --model gpt-4o-mini-tts
  --speed 1.0
  --format mp3|wav|aac|flac|ogg
  --max-chars 2800     (chunking safeguard)
  --no-ssml            (don’t add pauses/emphasis)
  --verbose
"""

import os, re, sys, io, argparse, configparser, tempfile, time
from typing import List
try:
    from openai import OpenAI
except ImportError:
    print("ERROR: `openai` package not found. Install with: pip install openai", file=sys.stderr)
    sys.exit(1)

# pydub is optional (for robust audio concatenation). If missing, we fall back to raw MP3 concat.
try:
    from pydub import AudioSegment  # requires ffmpeg on PATH for formats other than raw wav/mp3
    _HAVE_PYDUB = True
except Exception:
    _HAVE_PYDUB = False

# ------------------------ Utilities ------------------------

def make_conversational(text: str) -> str:
    """
    Light, safe rewrite toward conversational speech:
    - add common contractions
    - prefer 'but'/'and' over heavy transitions
    - break up a few long sentences with commas
    """
    rules = [
        (r"\bdo not\b", "don't"),
        (r"\bdoes not\b", "doesn't"),
        (r"\bdid not\b", "didn't"),
        (r"\bcannot\b", "can't"),
        (r"\bcan not\b", "cannot"),   # normalize
        (r"\bare not\b", "aren't"),
        (r"\bis not\b", "isn't"),
        (r"\bam not\b", "I'm not"),
        (r"\bI am\b", "I'm"),
        (r"\bI have\b", "I've"),
        (r"\bwe are\b", "we're"),
        (r"\byou are\b", "you're"),
        (r"\bthey are\b", "they're"),
        (r"\bit is\b", "it's"),
        (r"\bthere is\b", "there's"),
        (r"\bthat is\b", "that's"),
        (r"\bwho is\b", "who's"),
        (r"\bwhat is\b", "what's"),
        (r"\blet us\b", "let's"),
        # soften heavy transitions
        (r"\bHowever,\s+", "But "),
        (r"\bTherefore,\s+", "So "),
        (r"\bMoreover,\s+", "And "),
        (r"\bNevertheless,\s+", "Still, "),
    ]
    import re
    out = text
    for pat, repl in rules:
        out = re.sub(pat, repl, out, flags=re.IGNORECASE)

    # Lightly insert commas to slow run-ons (very conservative)
    out = re.sub(r"(\bwhich\b)", r", \1", out)
    out = re.sub(r"(\bbecause\b)", r", \1", out)

    return out

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"{ts} | {msg}", flush=True)

def load_openai_key(keys_path: str) -> str:
    """
    Expect keys.ini with either:
      [openai]
      api_key = sk-...
    or:
      [DEFAULT]
      OPENAI_API_KEY = sk-...
    """
    if not keys_path or not os.path.isfile(keys_path):
        raise FileNotFoundError(f"keys.ini not found: {keys_path!r}")
    cfg = configparser.ConfigParser()
    cfg.read(keys_path)
    # Try common places
    api = (
        cfg.get("openai", "api_key", fallback=None)
        or cfg.get("DEFAULT", "OPENAI_API_KEY", fallback=None)
        or cfg.get("openai", "OPENAI_API_KEY", fallback=None)
    )
    if not api:
        raise RuntimeError("Could not find OpenAI API key in keys.ini (try [openai] api_key=...).")
    return api.strip()

def strip_yaml_front_matter(text: str) -> str:
    # Remove leading --- ... --- block
    if text.startswith("---"):
        parts = text.split("\n")
        try:
            # find second '---' line
            end = next(i for i, ln in enumerate(parts[1:], start=1) if ln.strip() == "---")
            return "\n".join(parts[end+1:])
        except StopIteration:
            return text
    return text

def markdown_to_plain(md: str, add_ssml: bool = True) -> str:
    """
    Very lightweight Markdown -> narration-friendly text:
    - strip code blocks, images, links (keep link text), inline code
    - convert headings to emphasized lines + pauses
    - collapse extra whitespace
    """
    # Remove fenced code blocks
    md = re.sub(r"```.+?```", "", md, flags=re.DOTALL)
    # Remove images ![alt](url)
    md = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", md)
    # Convert links [text](url) -> text (optionally keep URL in parens)
    md = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", md)
    # Inline code `code`
    md = re.sub(r"`([^`]+)`", r"\1", md)

    # Headings -> uppercase + pause cue
    def _h(m):
        hashes = m.group(1)
        txt = m.group(2).strip()
        line = txt.upper()
        if add_ssml:
            return f"\n\n{line}.\n\n"
        else:
            return f"\n\n{line}\n\n"
    md = re.sub(r"^(#{1,6})\s+(.*)$", _h, md, flags=re.MULTILINE)

    # Strip any remaining markdown bullets/emphasis in a gentle way
    md = re.sub(r"[*_]{1,3}", "", md)
    md = re.sub(r"^\s*[-+*]\s+", "• ", md, flags=re.MULTILINE)

    # Replace multiple newlines with sentence breaks
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r"[ \t]+", " ", md)
    md = md.strip()

    # Add small pauses after bullet lines (readability)
    if add_ssml:
        md = re.sub(r"(^• .+?$)", r"\1.", md, flags=re.MULTILINE)

    return md

def chunk_text(text: str, max_chars: int = 2800) -> List[str]:
    """
    Chunk on sentence-ish boundaries without breaking words.
    """
    if len(text) <= max_chars:
        return [text]
    # Split on sentence enders while keeping them
    parts = re.split(r"(\.|\?|\!)(\s+)", text)
    chunks, buf = [], ""
    for i in range(0, len(parts), 3):
        seg = parts[i]
        ender = parts[i+1] if i+1 < len(parts) else ""
        space = parts[i+2] if i+2 < len(parts) else " "
        piece = (seg + ender + space)
        if len(buf) + len(piece) > max_chars and buf:
            chunks.append(buf.strip())
            buf = piece
        else:
            buf += piece
    if buf.strip():
        chunks.append(buf.strip())
    return chunks

def concat_mp3_bytes(byte_segments: List[bytes]) -> bytes:
    """
    Naive MP3 concatenation (often works, same params). If pydub exists, prefer that.
    """
    return b"".join(byte_segments)

def concat_audio_with_pydub(files: List[str]) -> AudioSegment:
    segs = [AudioSegment.from_file(f) for f in files]
    out = AudioSegment.silent(duration=0)
    for s in segs:
        out += s
    return out

# ------------------------ TTS core ------------------------

def tts_chunk(client: OpenAI, text: str, *, model: str, voice: str, speed: float, fmt: str) -> bytes:
    """
    Call OpenAI TTS for one chunk. Returns raw audio bytes in requested format.
    Tries modern arg names first (response_format), falls back if unsupported.
    """
    # Prefer streaming response when available
    try:
        with client.audio.speech.with_streaming_response.create(
            model=model,
            voice=voice,
            input=text,
            response_format=fmt,   # <— correct arg name
            speed=speed            # some SDKs accept this; if not, we'll catch below
        ) as resp:
            return resp.read()
    except TypeError:
        # Retry without speed (some SDK builds don’t support it)
        with client.audio.speech.with_streaming_response.create(
            model=model,
            voice=voice,
            input=text,
            response_format=fmt
        ) as resp:
            return resp.read()
    except AttributeError:
        # Older SDK without .with_streaming_response — fall back to non-streaming
        try:
            resp = client.audio.speech.create(
                model=model,
                voice=voice,
                input=text,
                response_format=fmt,
                speed=speed
            )
        except TypeError:
            resp = client.audio.speech.create(
                model=model,
                voice=voice,
                input=text,
                response_format=fmt
            )

        # Try to extract bytes in a version-agnostic way
        for attr in ("read", "bytes", "content"):
            if hasattr(resp, attr):
                val = getattr(resp, attr)
                return val() if callable(val) else val
        # As a last resort, try .audio.data[0].b64_* shapes (older APIs)
        try:
            import base64
            b64 = getattr(resp, "audio").data[0].b64_audio  # type: ignore[attr-defined]
            return base64.b64decode(b64)
        except Exception:
            raise RuntimeError("Could not extract audio bytes from TTS response (SDK variant not recognized).")


# ------------------------ CLI / Orchestration ------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Convert a Markdown blog post to a narrated MP3 using OpenAI TTS.")
    ap.add_argument("--in", dest="in_path", required=True, help="Input Markdown file")
    ap.add_argument("--out", dest="out_path", required=True, help="Output audio file (.mp3 recommended)")
    ap.add_argument("--keys", dest="keys_path", required=True, help="Path to keys.ini with OpenAI key")
    ap.add_argument("--model", default="gpt-4o-mini-tts", help="OpenAI TTS model (default: gpt-4o-mini-tts)")
    ap.add_argument("--voice", default="alloy", help="Voice name (e.g., alloy, verse, sage)")
    ap.add_argument("--speed", type=float, default=1.0, help="Playback speed (0.5–2.0)")
    ap.add_argument("--format", dest="fmt", default="mp3", choices=["mp3","wav","flac","aac","ogg"], help="Audio format")
    ap.add_argument("--max-chars", type=int, default=2800, help="Max chars per TTS chunk")
    ap.add_argument("--no-ssml", action="store_true", help="Disable added pauses/emphasis cues")
    ap.add_argument("--verbose", action="store_true", help="Print extra progress")
    ap.add_argument("--conversational", action="store_true",
                help="Lightly rewrite text with contractions and casual connectors for more natural speech.")
    args = ap.parse_args(argv)

    in_path = args.in_path
    out_path = args.out_path
    fmt = args.fmt

    if not os.path.isfile(in_path):
        print(f"ERROR: input file not found: {in_path}", file=sys.stderr)
        return 2

    api_key = load_openai_key(args.keys_path)
    os.environ["OPENAI_API_KEY"] = api_key
    client = OpenAI(api_key=api_key)

    # Read and prep text
    log("Reading Markdown…")
    with open(in_path, "r", encoding="utf-8") as f:
        md = f.read()

    md = strip_yaml_front_matter(md)
    text = markdown_to_plain(md, add_ssml=(not args.no_ssml))
    if args.conversational:
        log("Applying conversational rewrite…")
        text = make_conversational(text)

    if args.verbose:
        log(f"Plain text length: {len(text)} chars")

    chunks = chunk_text(text, max_chars=args.max_chars)
    log(f"Creating TTS for {len(chunks)} chunk(s)… (model={args.model}, voice={args.voice}, speed={args.speed})")

    # Generate audio per chunk
    byte_segments: List[bytes] = []
    temp_files: List[str] = []
    t0 = time.time()

    for i, chunk in enumerate(chunks, 1):
        log(f"  [{i}/{len(chunks)}] Synthesizing… {min(len(chunk), 80)} chars preview: {chunk[:80]!r}")
        try:
            audio_bytes = tts_chunk(
                client, chunk,
                model=args.model, voice=args.voice,
                speed=args.speed, fmt=fmt
            )
            byte_segments.append(audio_bytes)
            # Also write to temp file so we can optionally pydub-concat
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}")
            tf.write(audio_bytes)
            tf.close()
            temp_files.append(tf.name)
        except Exception as e:
            print(f"ERROR during TTS for chunk {i}: {e}", file=sys.stderr)
            # Best effort: continue or abort? We abort to keep result consistent
            for p in temp_files:
                try: os.unlink(p)
                except Exception: pass
            return 3

    # Concatenate
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if _HAVE_PYDUB:
        try:
            log("Concatenating audio with pydub…")
            final = concat_audio_with_pydub(temp_files)
            final.export(out_path, format=fmt)
        except Exception as e:
            log(f"pydub concat failed ({e}); falling back to raw byte concat…")
            with open(out_path, "wb") as outf:
                outf.write(concat_mp3_bytes(byte_segments))
    else:
        log("pydub not installed; concatenating by raw bytes… (for MP3 this usually works)")
        with open(out_path, "wb") as outf:
            outf.write(concat_mp3_bytes(byte_segments))

    # Cleanup
    for p in temp_files:
        try: os.unlink(p)
        except Exception: pass

    dt = time.time() - t0
    log(f"✅ Wrote: {out_path}  ({len(chunks)} chunk(s), {dt:.1f}s)")
    if not _HAVE_PYDUB:
        log("Tip: install pydub + ffmpeg for robust concat:  pip install pydub  (and add ffmpeg to PATH)")
    return 0

if __name__ == "__main__":
    
    raise SystemExit(main())
