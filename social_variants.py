#!/usr/bin/env python3
"""Generate social / email variants from a blog post using OpenAI.

Usage:
  python social_variants.py --input out/20251111-001418_topic/post.md

Produces files next to the input file:
  facebook.txt
  reddit.md
  instagram.txt
  email.txt
  tweet1.txt
  tweet2.txt
  raw_response.txt   (full model output for debugging)

The script prefers an OPENAI_API_KEY environment variable. You can also pass
--api-key on the command line (will set the env var for this run).
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from typing import Dict


def _read_input(path: str) -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _strip_front_matter(md: str) -> str:
    # Remove YAML front matter if present
    if md.startswith("---"):
        parts = md.split("---", 2)
        if len(parts) == 3:
            return parts[2].strip()
    return md


def _call_openai(prompt: str, model: str, temp: float = 0.7, max_tokens: int = 1200) -> str:
    try:
        # Defer import so calling code can set OPENAI_API_KEY first if desired
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError("OpenAI Python client not installed. Install via 'pip install openai'.") from e

    client = OpenAI()
    messages = [
        {
            "role": "system",
            "content": (
                "You are a creative social copywriter. Given a blog post body, produce short, platform-appropriate copy."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temp,
        max_tokens=max_tokens,
    )

    try:
        return resp.choices[0].message.content
    except Exception:
        # Fallback: stringify
        return str(resp)


def _parse_sections(text: str) -> Dict[str, str]:
    # Expect sections delimited by lines starting with '### NAME'
    sections: Dict[str, str] = {}
    parts = re.split(r"^###\s+([A-Z_0-9]+)\s*$", text, flags=re.MULTILINE)
    # re.split yields [pre, name1, body1, name2, body2, ...]
    if len(parts) >= 3:
        pre = parts[0].strip()
        for i in range(1, len(parts) - 1, 2):
            name = parts[i].strip()
            body = parts[i + 1].strip()
            sections[name] = body
        if pre and "RAW" not in sections:
            sections.setdefault("RAW", pre)
    else:
        # No markers: return whole as RAW
        sections["RAW"] = text.strip()
    return sections


def _write_files(base_dir: str, mapping: Dict[str, str], prefix: str = "social"):
    os.makedirs(base_dir, exist_ok=True)
    # Map expected keys to filenames
    out_map = {
        "FACEBOOK": "facebook.txt",
        "REDDIT_TITLE": "reddit_title.txt",
        "REDDIT_BODY": "reddit.md",
        "INSTAGRAM": "instagram.txt",
        "EMAIL_SUBJECT": "email_subject.txt",
        "EMAIL_BODY": "email.txt",
        "TWEET1": "tweet1.txt",
        "TWEET2": "tweet2.txt",
        "RAW": "raw_response.txt",
    }
    written = []
    for key, fname in out_map.items():
        val = mapping.get(key, "")
        if not val:
            continue
        path = os.path.join(base_dir, fname)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(val.strip() + "\n")
        written.append(path)
    return written


def main(argv=None):
    p = argparse.ArgumentParser(prog="social_variants.py")
    p.add_argument("--input", "-i", required=True, help="Path to blog markdown file")
    p.add_argument("--api-key", help="OpenAI API key (optional). If provided, sets OPENAI_API_KEY for this run")
    p.add_argument("--model", default="gpt-4o", help="Model to use (default: gpt-4o)")
    p.add_argument("--temp", type=float, default=0.7, help="Sampling temperature")
    p.add_argument("--max-tokens", type=int, default=1200, help="Max tokens for completion")
    args = p.parse_args(argv)

    if args.api_key:
        os.environ["OPENAI_API_KEY"] = args.api_key

    try:
        raw = _read_input(args.input)
    except Exception as e:
        print(f"Failed to read input: {e}")
        sys.exit(2)

    md = _strip_front_matter(raw)

    # Extract a short title if the markdown has a top-level heading
    title = ""
    m = re.search(r"^#\s+(.+)$", raw, flags=re.MULTILINE)
    if m:
        title = m.group(1).strip()

    prompt = (
        "Produce social variants for the following blog post. Return sections delimited by lines starting with '### NAME' (e.g. '### FACEBOOK').\n"
        "Required sections: FACEBOOK, REDDIT_TITLE, REDDIT_BODY, INSTAGRAM, EMAIL_SUBJECT, EMAIL_BODY, TWEET1, TWEET2.\n"
        "Make each section platform-appropriate: Facebook=short paragraph+call-to-action; Reddit=concise title and a detailed post body suitable for r/politics or r/news (do not include subreddit name); Instagram=short caption + 3-10 suggested hashtags at the end; Email=subject line and 2-3 paragraph email body for subscribers; Tweets=2 short tweets (280 chars max each).\n"
        "Do NOT include any political party names in the title or body. Keep the tone aligned with the blog.\n\n"
        f"Blog title: {title}\n\nBlog body:\n{md}\n\nReturn only the delimited sections."
    )

    try:
        out = _call_openai(prompt, model=args.model, temp=args.temp, max_tokens=args.max_tokens)
    except Exception as e:
        print(f"OpenAI request failed: {e}")
        sys.exit(3)

    sections = _parse_sections(out)

    base_dir = os.path.dirname(os.path.abspath(args.input))
    written = _write_files(base_dir, sections)

    # Always write the raw response as well
    raw_path = os.path.join(base_dir, "raw_response.txt")
    with open(raw_path, "w", encoding="utf-8") as fh:
        fh.write(out)
    if raw_path not in written:
        written.append(raw_path)

    print("Wrote:")
    for w in written:
        print(" -", w)


if __name__ == "__main__":
    main()
