"""
Bounded executor for interactive discovery searches.

This remains web-process local on purpose: interactive searches should start
immediately without waiting on the background worker queue. The persisted search
state in SQLite is the source of truth for progress and rendered results.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock

from config import DISCOVERY_INTERACTIVE_WORKERS


_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_LOCK = Lock()


def _executor() -> ThreadPoolExecutor:
    global _EXECUTOR
    with _EXECUTOR_LOCK:
        if _EXECUTOR is None:
            _EXECUTOR = ThreadPoolExecutor(
                max_workers=max(1, int(DISCOVERY_INTERACTIVE_WORKERS)),
                thread_name_prefix="pricepulse-discovery",
            )
        return _EXECUTOR


def submit_discovery_job(fn, *args, **kwargs) -> Future:
    return _executor().submit(fn, *args, **kwargs)
