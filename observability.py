"""
observability.py - lightweight structured logging helpers for PricePulse.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


def log_event(event: str, *, level: str = "info", **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "event": event,
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[key] = value
    logger = logging.getLogger("pricepulse")
    log_fn = getattr(logger, level.lower(), logger.info)
    try:
        log_fn(json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        logger.info("%s %s", event, fields)
