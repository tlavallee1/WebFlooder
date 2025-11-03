# config_keys.py
import os
import configparser

_CACHE = None

def _candidate_paths():
    # 1) CWD, 2) alongside the running script
    yield os.path.join(os.getcwd(), "keys.ini")
    yield os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys.ini")

def _load_ini():
    cfg = configparser.ConfigParser()
    for path in _candidate_paths():
        if os.path.exists(path):
            cfg.read(path, encoding="utf-8")
            return cfg
    return cfg  # empty

def load_keys():
    """
    Returns a simple dict: {"guardian": "...", "youtube": "..."}.
    Precedence: ENV > [guardian]/[youtube] sections > [keys] fallback.
    """
    global _CACHE
    if _CACHE is not None:
        return dict(_CACHE)

    cfg = _load_ini()
    # Section-style (preferred)
    guardian = cfg.get("guardian", "api_key", fallback=None)
    youtube  = cfg.get("youtube",  "api_key", fallback=None)

    # Fallback flat section: [keys] guardian=..., youtube=...
    guardian = guardian or cfg.get("keys", "guardian", fallback="")
    youtube  = youtube  or cfg.get("keys", "youtube",  fallback="")

    # ENV overrides
    guardian = os.getenv("GUARDIAN_API_KEY", guardian or "")
    youtube  = os.getenv("YOUTUBE_API_KEY",  youtube  or "")

    _CACHE = {"guardian": guardian, "youtube": youtube}
    return dict(_CACHE)

def get_key(name: str, default: str = "") -> str:
    return load_keys().get(name, "") or default
