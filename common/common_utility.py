import os
import sys
import json
import logging
from pathlib import Path
from functools import lru_cache
from logging.handlers import RotatingFileHandler

# ---------------------------
# Root Resolver
# ---------------------------

CACHE_FILE = Path(__file__).parent / ".file_root_cache.json"

def _load_disk_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_disk_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

@lru_cache(maxsize=64)
def get_file_root_path(file_name: str = "app.py", start_path: str | None = None) -> str:
    start_path = Path(start_path or Path(__file__).resolve().parents[3])

    disk_cache = _load_disk_cache()
    cached = disk_cache.get(file_name)
    if cached:
        p = Path(cached)
        if p.exists() and (start_path == p or start_path in p.parents):
            return str(p)

    for root, _, files in os.walk(start_path):
        if file_name in files:
            found = Path(root) / file_name
            disk_cache[file_name] = str(found)
            _save_disk_cache(disk_cache)
            return str(found)

    raise FileNotFoundError(f"{file_name} not found starting from {start_path}")

def resolve_path(*relative_path: str) -> str:
    return str(Path(get_file_root_path()).parent.joinpath(*relative_path))

# ---------------------------
# Ultra-Fast ANSI System
# ---------------------------

COLOR_DIRECTORY = {
    "black": "\033[30m", "red": "\033[31m", "green": "\033[32m", "yellow": "\033[33m",
    "blue": "\033[34m", "magenta": "\033[35m", "cyan": "\033[36m", "white": "\033[37m",
    "bright_black": "\033[90m", "bright_red": "\033[91m", "bright_green": "\033[92m",
    "bright_yellow": "\033[93m", "bright_blue": "\033[94m", "bright_magenta": "\033[95m",
    "bright_cyan": "\033[96m", "bright_white": "\033[97m",
    "bg_black": "\033[40m", "bg_red": "\033[41m", "bg_green": "\033[42m",
    "bg_yellow": "\033[43m", "bg_blue": "\033[44m", "bg_magenta": "\033[45m",
    "bg_cyan": "\033[46m", "bg_white": "\033[47m",
    "bg_bright_black": "\033[100m", "bg_bright_red": "\033[101m",
    "bg_bright_green": "\033[102m", "bg_bright_yellow": "\033[103m",
    "bg_bright_blue": "\033[104m", "bg_bright_magenta": "\033[105m",
    "bg_bright_cyan": "\033[106m", "bg_bright_white": "\033[107m",
    "bold": "\033[1m", "dim": "\033[2m", "italic": "\033[3m",
    "underline": "\033[4m", "blink": "\033[5m", "reverse": "\033[7m",
    "hidden": "\033[8m", "strike": "\033[9m", "reset": "\033[0m",
}

RESET = COLOR_DIRECTORY["reset"]

# Precompile replacements once
_ANSI_REPLACEMENTS = {f"{{{k}}}": v for k, v in COLOR_DIRECTORY.items()}

# ---------------------------
# Debug Flags
# ---------------------------

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
DEBUG_TO_FILE = os.getenv("DEBUG_TO_FILE", "false").lower() == "true"

# ---------------------------
# File Logger (Hot Path Ready)
# ---------------------------

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "debug.log"

_logger = None

def _init_file_logger():
    global _logger
    if _logger:
        return _logger

    logger = logging.getLogger("DEBUG")
    logger.setLevel(logging.DEBUG)

    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )

    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False

    _logger = logger
    return logger

if DEBUG_TO_FILE:
    _init_file_logger()

# ---------------------------
# Lightning-Fast Debug Print
# ---------------------------

def debug_print(message, color="white", tag="DEBUG"):
    if not DEBUG_MODE:
        return

    try:
        frame = sys._getframe(1)  # 100x faster than inspect.stack()
        location = f"{os.path.basename(frame.f_code.co_filename)}:{frame.f_lineno}"
    except Exception:
        location = "Unknown:??"

    base = COLOR_DIRECTORY.get(color.lower(), RESET)

    msg = str(message)
    for key, ansi in _ANSI_REPLACEMENTS.items():
        msg = msg.replace(key, ansi)

    final = f"[{tag}] [{location}] {msg}"

    print(f"{base}{final}{RESET}")

    if DEBUG_TO_FILE:
        _logger.debug(final)

# ---------------------------
# Fast Structured Print
# ---------------------------

def custom_print(message, color="yellow", type_=None, caller_info=True):
    base = COLOR_DIRECTORY.get(color.lower(), RESET)

    if type_:
        type_colors = {
            "error": "red", "warning": "yellow", "info": "blue", "success": "green"
        }
        base = COLOR_DIRECTORY.get(type_colors.get(type_, color), RESET)
        message = f"[{type_.upper()}] {message}"

    if caller_info:
        try:
            f = sys._getframe(1)
            message = f"[{os.path.basename(f.f_code.co_filename)}:{f.f_lineno}] {message}"
        except Exception:
            pass

    msg = str(message)
    for key, ansi in _ANSI_REPLACEMENTS.items():
        msg = msg.replace(key, ansi)

    print(f"{base}{msg}{RESET}")

custom_print("Debug mode is " + ("enabled" if DEBUG_MODE else "disabled"), type_="info")