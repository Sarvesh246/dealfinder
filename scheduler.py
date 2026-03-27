"""
scheduler.py - worker-owned APScheduler configuration and price revalidation loops.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler

from alerts import send_alerts
from database import (
    acquire_worker_lease,
    add_price_history,
    add_product_source_candidate,
    begin_worker_job,
    claim_next_manual_check_request,
    clear_product_source_candidates,
    complete_manual_check_request,
    compute_best_price,
    finish_worker_job,
    get_all_product_sources_for_revalidation,
    get_best_source_url,
    get_product_by_id,
    get_product_sources_needing_backfill,
    get_runtime_diagnostics,
    heartbeat_worker_lease,
    requeue_manual_check_request,
    release_worker_lease,
    set_alert_sent,
    update_product_source,
)
from observability import log_event
from scraper import SearchExecutionContext, revalidate_product_source


WORKER_LEASE_SECONDS = int(os.getenv("WORKER_LEASE_SECONDS", "90"))
WORKER_HEARTBEAT_SECONDS = int(os.getenv("WORKER_HEARTBEAT_SECONDS", "20"))
MANUAL_CHECK_POLL_SECONDS = int(os.getenv("MANUAL_CHECK_POLL_SECONDS", "10"))
_RUN_LOCK = threading.Lock()


def _status_from_price(price, target_price):
    if price is None:
        return "watching"
    if target_price is None:
        return "watching"
    return "deal_found" if float(price) <= float(target_price) else "watching"


def _persist_revalidation_result(ps, result, now: str) -> bool:
    verified = result.get("verified", [])
    ambiguous = result.get("ambiguous", [])
    clear_product_source_candidates(ps["product_id"], ps["source_id"])
    if verified:
        best = verified[0]
        update_product_source(
            ps["id"],
            discovered_url=best["url"],
            current_price=best.get("price"),
            status=_status_from_price(best.get("price"), ps.get("target_price")),
            verification_state="verified",
            verification_reason=best.get("verification_reason"),
            health_state=best.get("health_state", "healthy"),
            matched_product_name=best.get("matched_product_name") or best.get("name_found"),
            fingerprint_brand=best.get("fingerprint_brand"),
            fingerprint_family=best.get("fingerprint_family"),
            fingerprint_model=best.get("fingerprint_model"),
            fingerprint_json=best.get("fingerprint_json"),
            match_label=best.get("match_label", "verified_exact"),
            last_verified=now,
            last_checked=now,
        )
        if best.get("price") is not None:
            add_price_history(ps["id"], best["price"])
        log_event(
            "verification.result",
            product_id=ps["product_id"],
            product_name=ps["product_name"],
            source=ps["source_name"],
            outcome="verified",
            price=best.get("price"),
            match_label=best.get("match_label", "verified_exact"),
        )
        return True
    if ambiguous:
        lead = ambiguous[0]
        update_product_source(
            ps["id"],
            discovered_url=None,
            current_price=None,
            status="pending_confirmation",
            verification_state="pending_confirmation",
            verification_reason=lead.get("verification_reason"),
            health_state=lead.get("health_state", "healthy"),
            matched_product_name=lead.get("matched_product_name") or lead.get("name_found"),
            fingerprint_brand=lead.get("fingerprint_brand"),
            fingerprint_family=lead.get("fingerprint_family"),
            fingerprint_model=lead.get("fingerprint_model"),
            fingerprint_json=lead.get("fingerprint_json"),
            match_label=lead.get("match_label", "verified_related"),
            last_verified=now,
            last_checked=now,
        )
        for candidate in ambiguous:
            add_product_source_candidate(
                ps["product_id"],
                ps["source_id"],
                candidate["url"],
                product_source_id=ps["id"],
                candidate_name=candidate.get("matched_product_name") or candidate.get("name_found"),
                candidate_price=candidate.get("price"),
                verification_state="ambiguous",
                verification_reason=candidate.get("verification_reason"),
                health_state=candidate.get("health_state", "healthy"),
                fingerprint_brand=candidate.get("fingerprint_brand"),
                fingerprint_family=candidate.get("fingerprint_family"),
                fingerprint_model=candidate.get("fingerprint_model"),
                match_label=candidate.get("match_label", "verified_related"),
                fingerprint_json=candidate.get("fingerprint_json"),
            )
        log_event(
            "verification.result",
            product_id=ps["product_id"],
            product_name=ps["product_name"],
            source=ps["source_name"],
            outcome="ambiguous",
            candidates=len(ambiguous),
            reason=lead.get("verification_reason"),
        )
        return False
    final_status = "quarantined" if result.get("status") == "quarantined" else "not_found"
    update_product_source(
        ps["id"],
        discovered_url=None if final_status != "quarantined" else ps["discovered_url"],
        current_price=None,
        status=final_status,
        verification_state=final_status,
        verification_reason=result.get("status", "revalidation_failed"),
        health_state="quarantined" if final_status == "quarantined" else "healthy",
        last_verified=now,
        last_checked=now,
    )
    log_event(
        "verification.result",
        product_id=ps["product_id"],
        product_name=ps["product_name"],
        source=ps["source_name"],
        outcome=final_status,
        reason=result.get("status", "revalidation_failed"),
    )
    return False


def _finalize_checked_products(checked_products, *, allow_alerts: bool) -> None:
    for pid in checked_products:
        before = get_product_by_id(pid)
        best = compute_best_price(pid)
        product = get_product_by_id(pid)
        if not product:
            continue

        alert_mode = product["alert_mode"] or "target_threshold"
        target = product["target_price"]
        alert_sent = product["alert_sent"]
        name = product["name"]
        previous_price = before["current_price"] if before else None

        if alert_mode == "any_drop":
            if allow_alerts and best is not None and previous_price is not None and float(best) < float(previous_price):
                best_url = get_best_source_url(pid)
                alert_delivered = send_alerts(
                    name,
                    best,
                    alert_mode="any_drop",
                    previous_price=previous_price,
                    url=best_url,
                )
                log_event(
                    "alert.delivery",
                    channel="multi",
                    product_id=pid,
                    product_name=name,
                    alert_mode="any_drop",
                    previous_price=previous_price,
                    current_price=best,
                    delivered=alert_delivered,
                )
                if alert_delivered:
                    logging.info(
                        f"[{datetime.now()}] ANY-DROP ALERT - sent for {name}: "
                        f"${previous_price:.2f} -> ${best:.2f}"
                    )
            continue

        if best is not None and target is not None and best <= target and allow_alerts:
            if alert_sent == 0:
                best_url = get_best_source_url(pid)
                alert_delivered = send_alerts(
                    name,
                    best,
                    alert_mode="target_threshold",
                    target_price=target,
                    url=best_url,
                )
                log_event(
                    "alert.delivery",
                    channel="multi",
                    product_id=pid,
                    product_name=name,
                    alert_mode="target_threshold",
                    current_price=best,
                    target_price=target,
                    delivered=alert_delivered,
                )
                if alert_delivered:
                    set_alert_sent(pid, 1)
                    logging.info(f"[{datetime.now()}] DEAL FOUND - alert sent for: {name}")
            continue

        if alert_sent == 1:
            set_alert_sent(pid, 0)
            logging.info(
                f"[{datetime.now()}] Price above target for {name} "
                f"- alert flag reset."
            )


def _run_revalidation_pass(rows, *, label: str, allow_alerts: bool) -> None:
    if not rows:
        logging.info(f"[{datetime.now()}] No product sources to {label.lower()}.")
        return

    checked_products = set()
    context = SearchExecutionContext()
    for ps in rows:
        ps = dict(ps)
        now = datetime.now().isoformat()
        logging.info(f"[{datetime.now()}] {label}: {ps['product_name']} on {ps['source_name']}")
        result = revalidate_product_source(ps, context=context)
        ok = _persist_revalidation_result(ps, result, now)
        if ok and result.get("verified"):
            price = result["verified"][0].get("price")
            if price is not None:
                if ps.get("alert_mode") == "any_drop" or ps.get("target_price") is None:
                    logging.info(
                        f"[{datetime.now()}] {ps['product_name']} @ {ps['source_name']}: "
                        f"${price:.2f} (any-drop tracking)"
                    )
                else:
                    logging.info(
                        f"[{datetime.now()}] {ps['product_name']} @ {ps['source_name']}: "
                        f"${price:.2f} (target ${ps['target_price']:.2f})"
                    )
        else:
            logging.info(
                f"[{datetime.now()}] {ps['product_name']} on {ps['source_name']} -> "
                f"{result.get('status', 'unverified')}"
            )
        checked_products.add(ps["product_id"])

    _finalize_checked_products(checked_products, allow_alerts=allow_alerts)


def _run_managed_job(
    worker_id: str,
    *,
    job_name: str,
    trigger_type: str,
    requested_by: str | None = None,
    request_id: int | None = None,
    rows: list[Any] | None = None,
    label: str,
    allow_alerts: bool,
) -> bool:
    if not _RUN_LOCK.acquire(blocking=False):
        return False
    run_id: int | None = None
    try:
        if not acquire_worker_lease(worker_id, WORKER_LEASE_SECONDS):
            log_event("job.skip", job_name=job_name, trigger_type=trigger_type, reason="lease_not_acquired")
            return False
        run_id = begin_worker_job(worker_id, job_name, trigger_type, requested_by=requested_by)
        if run_id is None:
            log_event("job.skip", job_name=job_name, trigger_type=trigger_type, reason="worker_busy")
            return False

        log_event(
            "job.start",
            worker_id=worker_id,
            run_id=run_id,
            job_name=job_name,
            trigger_type=trigger_type,
            requested_by=requested_by,
        )
        _run_revalidation_pass(rows or [], label=label, allow_alerts=allow_alerts)
        finish_worker_job(worker_id, run_id, "completed")
        if request_id is not None:
            complete_manual_check_request(request_id, worker_id, "completed")
        log_event(
            "job.finish",
            worker_id=worker_id,
            run_id=run_id,
            job_name=job_name,
            trigger_type=trigger_type,
            status="completed",
        )
        return True
    except Exception as exc:
        logging.exception("Managed worker job failed", exc_info=exc)
        if run_id is not None:
            finish_worker_job(worker_id, run_id, "failed", str(exc))
        if request_id is not None:
            complete_manual_check_request(request_id, worker_id, "failed", str(exc))
        log_event(
            "job.finish",
            level="error",
            worker_id=worker_id,
            run_id=run_id,
            job_name=job_name,
            trigger_type=trigger_type,
            status="failed",
            error=str(exc),
        )
        return False
    finally:
        _RUN_LOCK.release()


def run_initial_backfill_managed(worker_id: str) -> bool:
    rows = get_product_sources_needing_backfill()
    return _run_managed_job(
        worker_id,
        job_name="startup_backfill",
        trigger_type="startup",
        rows=rows,
        label="Backfilling",
        allow_alerts=False,
    )


def check_all_products_managed(
    worker_id: str,
    *,
    trigger_type: str = "scheduled",
    requested_by: str | None = None,
    request_id: int | None = None,
) -> bool:
    rows = get_all_product_sources_for_revalidation()
    job_name = "manual_price_check" if trigger_type == "manual" else "periodic_price_check"
    return _run_managed_job(
        worker_id,
        job_name=job_name,
        trigger_type=trigger_type,
        requested_by=requested_by,
        request_id=request_id,
        rows=rows,
        label="Revalidating",
        allow_alerts=True,
    )


def worker_heartbeat(worker_id: str) -> bool:
    if heartbeat_worker_lease(worker_id, WORKER_LEASE_SECONDS):
        return True
    return acquire_worker_lease(worker_id, WORKER_LEASE_SECONDS)


def process_manual_check_queue(worker_id: str) -> bool:
    diagnostics = get_runtime_diagnostics()
    if diagnostics.get("current_job_name"):
        return False
    request_row = claim_next_manual_check_request(worker_id)
    if not request_row:
        return False
    if not check_all_products_managed(
        worker_id,
        trigger_type="manual",
        requested_by=request_row["requested_by"],
        request_id=request_row["id"],
    ):
        requeue_manual_check_request(request_row["id"])
        return False
    return True


def run_initial_backfill() -> None:
    logging.info(f"[{datetime.now()}] -- Initial source backfill started --")
    rows = get_product_sources_needing_backfill()
    _run_revalidation_pass(rows, label="Backfilling", allow_alerts=False)
    logging.info(f"[{datetime.now()}] -- Initial source backfill complete --")


def check_all_products() -> None:
    logging.info(f"[{datetime.now()}] -- Price check started --")
    rows = get_all_product_sources_for_revalidation()
    _run_revalidation_pass(rows, label="Revalidating", allow_alerts=True)
    logging.info(f"[{datetime.now()}] -- Price check complete --")


def create_scheduler() -> BackgroundScheduler:
    interval_hours = int(os.getenv("CHECK_INTERVAL_HOURS", "6"))

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        func=check_all_products,
        trigger="interval",
        hours=interval_hours,
        id="price_check",
        name="Periodic price check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    logging.info(
        f"[{datetime.now()}] Scheduler ready - will run every {interval_hours}h."
    )
    return scheduler


def create_worker_scheduler(worker_id: str) -> BackgroundScheduler:
    interval_hours = int(os.getenv("CHECK_INTERVAL_HOURS", "6"))
    scheduler = BackgroundScheduler(daemon=True, job_defaults={"coalesce": True, "max_instances": 1})
    scheduler.add_job(
        func=lambda: worker_heartbeat(worker_id),
        trigger="interval",
        seconds=max(10, WORKER_HEARTBEAT_SECONDS),
        id="worker_heartbeat",
        name="Worker heartbeat",
        replace_existing=True,
        misfire_grace_time=30,
    )
    scheduler.add_job(
        func=lambda: process_manual_check_queue(worker_id),
        trigger="interval",
        seconds=max(5, MANUAL_CHECK_POLL_SECONDS),
        id="manual_check_queue",
        name="Manual check queue",
        replace_existing=True,
        misfire_grace_time=30,
    )
    scheduler.add_job(
        func=lambda: check_all_products_managed(worker_id, trigger_type="scheduled"),
        trigger="interval",
        hours=interval_hours,
        id="periodic_price_check",
        name="Periodic price check",
        replace_existing=True,
        misfire_grace_time=300,
    )
    if os.getenv("ENABLE_STARTUP_BACKFILL", "1").lower() in ("1", "true", "yes"):
        scheduler.add_job(
            func=lambda: run_initial_backfill_managed(worker_id),
            trigger="date",
            run_date=datetime.now() + timedelta(seconds=5),
            id="startup_backfill",
            name="Startup backfill",
            replace_existing=True,
            misfire_grace_time=120,
        )
    logging.info(
        f"[{datetime.now()}] Worker scheduler ready - price checks every {interval_hours}h, "
        f"heartbeat every {WORKER_HEARTBEAT_SECONDS}s."
    )
    return scheduler


def shutdown_worker(worker_id: str) -> None:
    release_worker_lease(worker_id)
