"""
Shared runtime bootstrap for web and worker processes.
"""

from __future__ import annotations

import logging
from datetime import datetime
from threading import Lock, Thread

from database import init_db
from hf_utils import get_smart_engine
from scraper import start_browser_warmup


_WARMUPS_LOCK = Lock()
_WARMUPS_STARTED = False
_LOGGING_CONFIGURED = False


def configure_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _LOGGING_CONFIGURED = True


def ensure_database_ready() -> None:
    try:
        init_db()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] init_db at startup failed: {exc}")


def start_runtime_warmups() -> None:
    global _WARMUPS_STARTED
    with _WARMUPS_LOCK:
        if _WARMUPS_STARTED:
            return
        _WARMUPS_STARTED = True
    Thread(target=get_smart_engine, daemon=True).start()
    Thread(target=start_browser_warmup, daemon=True).start()
