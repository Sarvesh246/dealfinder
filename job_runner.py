"""
HTTP-invoked background job runner for Vercel-compatible deployments.
"""

from __future__ import annotations

import logging
import os
import socket
import threading

import requests

from config import APP_BASE_URL, ENABLE_STARTUP_BACKFILL, INTERNAL_JOB_SECRET, JOB_RUNNER_MODE
from database import get_runtime_diagnostics
from observability import log_event
from scheduler import check_all_products_managed, process_manual_check_queue, run_initial_backfill_managed


def http_job_runner_enabled() -> bool:
    return JOB_RUNNER_MODE == "http"


def _runner_id(label: str = "http-runner") -> str:
    return f"{label}:{socket.gethostname()}:{os.getpid()}"


def _backfill_pending() -> bool:
    if not ENABLE_STARTUP_BACKFILL:
        return False
    runtime = get_runtime_diagnostics()
    return not runtime.get("last_backfill")


def run_backfill_job() -> dict[str, object]:
    worker_id = _runner_id("backfill")
    ok = run_initial_backfill_managed(worker_id)
    return {"ok": bool(ok), "worker_id": worker_id, "job": "startup_backfill"}


def run_dispatch_job(*, mode: str = "scheduled") -> dict[str, object]:
    worker_id = _runner_id("dispatch")
    ran_backfill = False
    if _backfill_pending():
        ran_backfill = bool(run_initial_backfill_managed(worker_id))

    if mode == "manual":
        ok = process_manual_check_queue(worker_id)
        return {
            "ok": bool(ok),
            "worker_id": worker_id,
            "mode": mode,
            "ran_backfill": ran_backfill,
            "job": "manual_price_check" if ok else "idle",
        }

    manual_ok = process_manual_check_queue(worker_id)
    if manual_ok:
        return {
            "ok": True,
            "worker_id": worker_id,
            "mode": mode,
            "ran_backfill": ran_backfill,
            "job": "manual_price_check",
        }

    scheduled_ok = check_all_products_managed(worker_id, trigger_type="scheduled")
    return {
        "ok": bool(scheduled_ok),
        "worker_id": worker_id,
        "mode": mode,
        "ran_backfill": ran_backfill,
        "job": "periodic_price_check" if scheduled_ok else "idle",
    }


def trigger_internal_dispatch(*, mode: str = "manual", base_url: str | None = None) -> bool:
    if not http_job_runner_enabled():
        return False
    root = (base_url or APP_BASE_URL or "").rstrip("/")
    if not root or not INTERNAL_JOB_SECRET:
        return False

    def _call() -> None:
        try:
            url = f"{root}/internal/jobs/dispatch?{urlencode({'mode': mode})}"
            if mode in {"manual", "scheduled"}:
                url = f"{root}/internal/jobs/dispatch/{mode}"
            response = requests.get(
                url,
                headers={"X-Internal-Job-Token": INTERNAL_JOB_SECRET},
                timeout=1.5,
            )
            log_event(
                "job.dispatch.trigger",
                mode=mode,
                status_code=response.status_code,
                ok=response.ok,
            )
        except Exception as exc:
            logging.warning("Internal dispatch trigger failed: %s", exc)
            log_event(
                "job.dispatch.trigger",
                level="warning",
                mode=mode,
                ok=False,
                error=str(exc),
            )

    threading.Thread(target=_call, daemon=True).start()
    return True
