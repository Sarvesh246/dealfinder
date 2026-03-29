"""
Request/runtime helpers shared by route modules.
"""

from __future__ import annotations

import hmac
import secrets
import time
from datetime import date
from threading import Lock

from flask import request


_COOLDOWN_LOCK = Lock()
_COOLDOWN_STATE: dict[tuple[str, str, str], float] = {}


def client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.remote_addr or "unknown").strip() or "unknown"


def _cooldown_key(bucket: str, extra: str = "") -> tuple[str, str, str]:
    return (bucket, client_ip(), extra)


def consume_cooldown(bucket: str, seconds: int, *, extra: str = "") -> bool:
    if seconds <= 0:
        return False
    now = time.monotonic()
    key = _cooldown_key(bucket, extra)
    with _COOLDOWN_LOCK:
        expires_at = _COOLDOWN_STATE.get(key, 0.0)
        if expires_at > now:
            return True
        _COOLDOWN_STATE[key] = now + seconds
        stale_keys = [k for k, expires in _COOLDOWN_STATE.items() if expires <= now]
        for stale_key in stale_keys:
            _COOLDOWN_STATE.pop(stale_key, None)
    return False


def manual_check_ui_token(secret_key) -> str:
    key = secret_key
    if isinstance(key, str):
        key = key.encode("utf-8")
    msg = f"manual_check:{date.today().isoformat()}".encode("utf-8")
    return hmac.new(key, msg, "sha256").hexdigest()[:32]


def manual_check_authorized(secret_key, cron_secret: str) -> bool:
    if not cron_secret:
        return True
    q = request.args.get("token", "")
    h = request.headers.get("X-Cron-Token", "")
    if secrets.compare_digest(q, cron_secret) or secrets.compare_digest(h, cron_secret):
        return True
    ui = request.args.get("ui_token", "")
    if ui and secrets.compare_digest(ui, manual_check_ui_token(secret_key)):
        return True
    return False


def internal_job_authorized(shared_secret: str) -> bool:
    if not shared_secret:
        return False
    token = request.args.get("token", "")
    header = request.headers.get("X-Internal-Job-Token", "")
    return secrets.compare_digest(token, shared_secret) or secrets.compare_digest(header, shared_secret)
