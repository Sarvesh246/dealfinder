"""
scheduler.py - APScheduler configuration and price revalidation loops.
"""

import logging
import os
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from alerts import send_alerts
from database import (
    add_price_history,
    add_product_source_candidate,
    clear_product_source_candidates,
    compute_best_price,
    get_all_product_sources_for_revalidation,
    get_product_by_id,
    get_product_sources_needing_backfill,
    set_alert_sent,
    update_product_source,
)
from scraper import revalidate_product_source


def _status_from_price(price, target_price):
    if price is None:
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
            status=_status_from_price(best.get("price"), ps["target_price"]),
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
    return False


def _finalize_checked_products(checked_products, *, allow_alerts: bool) -> None:
    for pid in checked_products:
        best = compute_best_price(pid)
        product = get_product_by_id(pid)
        if not product:
            continue

        target = product["target_price"]
        alert_sent = product["alert_sent"]
        name = product["name"]

        if best is not None and best <= target and allow_alerts:
            if alert_sent == 0:
                from database import get_best_source_url

                best_url = get_best_source_url(pid)
                send_alerts(name, best, target, best_url)
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
    for ps in rows:
        now = datetime.now().isoformat()
        logging.info(f"[{datetime.now()}] {label}: {ps['product_name']} on {ps['source_name']}")
        result = revalidate_product_source(dict(ps))
        ok = _persist_revalidation_result(ps, result, now)
        if ok and result.get("verified"):
            price = result["verified"][0].get("price")
            if price is not None:
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
