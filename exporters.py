# exporters.py
from typing import List, Dict

def sources_footer(sources: List[Dict]) -> str:
    lines = ["\n## Sources"]
    for i, s in enumerate(sources, 1):
        lines.append(f"[^{i}] {s['title']} — {s['published_at']} — {s['url']}")
    return "\n".join(lines)

def blog_skeleton(brief: Dict, draft_body: str) -> str:
    fm = [
        "---",
        f'title: "{brief["topic"]}"',
        f'date: {brief["timebox"]["until"]}',
        f'tags: [{", ".join([repr(k) for k in brief["signals"]["keyphrases"][:5]])}]',
        "layout: post",
        "---",
        ""
    ]
    return "\n".join(fm) + draft_body + sources_footer(brief["sources"])
