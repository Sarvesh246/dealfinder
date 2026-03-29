"""
Internal authenticated job routes for Vercel-compatible background execution.
"""

from __future__ import annotations

from flask import abort, jsonify, request

from config import INTERNAL_JOB_SECRET, JOB_RUNNER_MODE
from job_runner import run_backfill_job, run_dispatch_job
from route_runtime import internal_job_authorized

from . import main_bp


def _ensure_internal_authorized():
    if JOB_RUNNER_MODE != "http":
        abort(404)
    if not internal_job_authorized(INTERNAL_JOB_SECRET):
        abort(401)


@main_bp.route("/internal/jobs/dispatch", defaults={"mode": "scheduled"}, methods=["GET", "POST"], endpoint="internal_dispatch_jobs")
@main_bp.route("/internal/jobs/dispatch/<mode>", methods=["GET", "POST"])
def internal_dispatch_jobs(mode):
    _ensure_internal_authorized()
    mode = ((mode or request.args.get("mode", "scheduled")).strip().lower() or "scheduled")
    if mode not in {"scheduled", "manual"}:
        mode = "scheduled"
    payload = run_dispatch_job(mode=mode)
    return jsonify(
        {
            "status": "ok" if payload.get("ok") else "idle",
            "job_runner_mode": JOB_RUNNER_MODE,
            **payload,
        }
    )


@main_bp.route("/internal/jobs/backfill", methods=["GET", "POST"], endpoint="internal_backfill_job")
def internal_backfill_job():
    _ensure_internal_authorized()
    payload = run_backfill_job()
    return jsonify(
        {
            "status": "ok" if payload.get("ok") else "idle",
            "job_runner_mode": JOB_RUNNER_MODE,
            **payload,
        }
    )
