"""
Tracking routes: add product, direct links, product detail, sources, redirects, and manual checks.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

from flask import current_app, flash, make_response, redirect, render_template, request, url_for

from config import (
    CHECK_COOLDOWN_SECONDS,
    CHECK_CRON_SECRET,
    STRICT_SOURCE_WORKERS,
)
from database import (
    add_price_history,
    add_product,
    add_product_source,
    clear_product_source_candidates,
    compute_best_price,
    delete_product,
    delete_product_sources_by_source,
    ensure_generic_direct_source,
    enqueue_manual_check_request,
    find_source_for_url,
    get_certified_catalog_sources,
    get_discovery_result_by_id,
    get_discovery_search,
    get_product_by_id,
    get_price_history,
    get_product_source_by_id,
    get_product_source_candidate,
    get_product_source_candidates,
    get_product_sources,
    get_runtime_diagnostics,
    get_source_by_id,
    mark_candidate_selected,
    update_product_source,
)
from observability import log_event
from pricing_status import status_for_price
from product_verifier import QueryType, parse_product_spec
from route_runtime import client_ip, consume_cooldown, manual_check_authorized
from route_support import (
    apply_source_matches_for_product,
    direct_link_error_message,
    final_outbound_url,
    matches_from_direct_link_inspection,
    persist_source_matches,
    promoted_tracking_spec,
    redirect_back_or,
    sources_from_posted_ids,
)
from scraper import (
    SearchExecutionContext,
    canonicalize_listing_url,
    inspect_direct_link,
    revalidate_product_source,
)
from source_capabilities import filter_supported_sources
from template_utils import external_domain

from . import main_bp


@main_bp.route("/open/source/<int:product_source_id>", endpoint="open_product_source")
def open_product_source(product_source_id):
    product_source = get_product_source_by_id(product_source_id)
    if not product_source or not product_source["discovered_url"]:
        flash("That source link is no longer available.", "error")
        return redirect_back_or("index")

    original_url = product_source["discovered_url"]
    final_url = final_outbound_url(original_url)
    if not final_url:
        log_event(
            "source.redirect",
            outcome="invalid_url",
            source_kind="product_source",
            product_source_id=product_source_id,
            original_url=original_url,
        )
        flash("That source link is malformed and could not be opened.", "error")
        return redirect_back_or("product_detail", product_id=product_source["product_id"])

    log_event(
        "source.redirect",
        outcome="redirect",
        source_kind="product_source",
        product_source_id=product_source_id,
        product_id=product_source["product_id"],
        source_id=product_source["source_id"],
        domain=external_domain(final_url, fallback=product_source["domain"]),
        canonicalized=(final_url != (original_url or "").strip()),
    )
    return redirect(final_url, code=302)


@main_bp.route("/open/discovery-result/<int:result_id>", endpoint="open_discovery_result")
def open_discovery_result(result_id):
    result = get_discovery_result_by_id(result_id)
    if not result or not result["product_url"]:
        flash("That result link is no longer available.", "error")
        return redirect_back_or("discover_page")

    original_url = result["product_url"]
    final_url = final_outbound_url(original_url)
    if not final_url:
        log_event(
            "source.redirect",
            outcome="invalid_url",
            source_kind="discovery_result",
            discovery_result_id=result_id,
            original_url=original_url,
        )
        flash("That result link is malformed and could not be opened.", "error")
        return redirect_back_or("discover_results", search_id=result["search_id"])

    log_event(
        "source.redirect",
        outcome="redirect",
        source_kind="discovery_result",
        discovery_result_id=result_id,
        search_id=result["search_id"],
        source_id=result["source_id"],
        domain=external_domain(final_url),
        canonicalized=(final_url != (original_url or "").strip()),
    )
    return redirect(final_url, code=302)


@main_bp.route("/add", methods=["GET"], endpoint="add_page")
def add_page():
    response = make_response(
        render_template(
            "add.html",
            sources=get_certified_catalog_sources(),
            active_mode=request.args.get("mode", "search").strip() or "search",
            link_prefill_url=request.args.get("product_url", "").strip(),
            link_prefill_target=request.args.get("target_price", "").strip(),
        )
    )
    response.headers["X-PricePulse-Add-UI"] = "search-and-link-v1"
    return response


@main_bp.route("/add", methods=["POST"], endpoint="add_product_route")
def add_product_route():
    name = request.form.get("name", "").strip()
    target_str = request.form.get("target_price", "").strip()
    search_all = request.form.get("search_all_sources") == "1"
    source_ids = [str(source["id"]) for source in get_certified_catalog_sources()] if search_all else request.form.getlist("source_ids")

    if not name:
        flash("A product name is required.", "error")
        return redirect(url_for("add_page"))

    try:
        target_price = float(target_str)
        if target_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Please enter a valid target price greater than $0.", "error")
        return redirect(url_for("add_page"))

    if not source_ids:
        flash("Select at least one source to search, or choose All sources.", "error")
        return redirect(url_for("add_page"))

    spec = parse_product_spec(name)
    if spec.query_type == QueryType.CATEGORY.value:
        flash(
            "That search is still broad. Pick a specific result from Discovery before creating a tracker.",
            "info",
        )
        redirect_args = {"query": name, "max_price": target_str}
        if search_all:
            redirect_args["search_all_sources"] = "1"
        else:
            redirect_args["source_ids"] = source_ids
        return redirect(url_for("discover_page", **redirect_args))

    selected_sources = sources_from_posted_ids(search_all, source_ids, query_or_spec=spec)
    if not selected_sources:
        raw_selected = sources_from_posted_ids(search_all, source_ids)
        if raw_selected:
            skipped = ", ".join(source["name"] for source in raw_selected)
            flash(
                "The selected stores do not support this product type at our current quality bar: "
                + skipped,
                "error",
            )
        else:
            flash("Select at least one source to search, or choose All sources.", "error")
        return redirect(url_for("add_page"))

    product_id = add_product(name, target_price)
    if not product_id:
        flash("Could not save the product — please try again.", "error")
        return redirect(url_for("add_page"))

    product = dict(get_product_by_id(product_id))
    outcomes = apply_source_matches_for_product(product, selected_sources)
    total = len(selected_sources)
    verified = outcomes["verified"]
    pending = outcomes["pending_confirmation"]
    unavailable = (
        outcomes.get("blocked", 0)
        + outcomes.get("timeout", 0)
        + outcomes.get("unavailable", 0)
        + outcomes.get("error", 0)
    )
    if search_all:
        flash(
            f'Now tracking "{name}" — verified on {verified} of {total} sources'
            + (f", {pending} need confirmation" if pending else "")
            + (f", {unavailable} temporarily unavailable" if unavailable else "")
            + " (all registered stores).",
            "success",
        )
    else:
        flash(
            f'Now tracking "{name}" — verified on {verified} of {total} sources'
            + (f", {pending} need confirmation" if pending else "")
            + (f", {unavailable} temporarily unavailable" if unavailable else ""),
            "success",
        )
    if pending:
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    return redirect(url_for("index"))


@main_bp.route("/track/link", methods=["POST"], endpoint="track_link_route")
def track_link_route():
    raw_url = request.form.get("product_url", "").strip()
    target_str = request.form.get("target_price", "").strip()

    if not raw_url:
        flash("Paste a product URL to start tracking from a direct link.", "error")
        return redirect(url_for("add_page", mode="link"))

    try:
        target_price = float(target_str) if target_str else None
        if target_price is not None and target_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Enter a valid notify price, or leave it blank for any lower verified price.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    matched_source = find_source_for_url(raw_url)
    source_row = matched_source or ensure_generic_direct_source()
    if not source_row:
        flash("Could not initialize direct-link tracking right now.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    inspection = inspect_direct_link(
        raw_url,
        source=dict(matched_source) if matched_source else None,
        context=SearchExecutionContext(),
    )
    if not inspection.get("ok"):
        flash(direct_link_error_message(str(inspection.get("reason") or "")), "error")
        return redirect(
            url_for(
                "add_page",
                mode="link",
                product_url=canonicalize_listing_url(raw_url) or raw_url,
                target_price=target_str,
            )
        )

    tracking_name = inspection.get("title") or canonicalize_listing_url(raw_url)
    alert_mode = "target_threshold" if target_price is not None else "any_drop"
    product_id = add_product(
        tracking_name,
        target_price,
        alert_mode=alert_mode,
        origin_type="direct_link",
    )
    if not product_id:
        flash("Could not save that link for tracking.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    product = dict(get_product_by_id(product_id))
    host = (inspection.get("domain") or urlparse(raw_url).netloc or "").replace("www.", "")
    source_label_override = None if matched_source else host
    source_domain_override = None if matched_source else host
    matches = matches_from_direct_link_inspection(inspection)
    outcome = persist_source_matches(
        product,
        dict(source_row),
        matches,
        tracking_mode="direct_url",
        source_label_override=source_label_override,
        source_domain_override=source_domain_override,
    )
    compute_best_price(product_id)

    if outcome == "pending_confirmation":
        flash(f'Added "{tracking_name}" — confirm the match before tracking alerts start.', "success")
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    if alert_mode == "any_drop":
        flash(
            f'Now tracking "{tracking_name}" from its direct link. You’ll be alerted on any new verified lower price.',
            "success",
        )
    else:
        flash(
            f'Now tracking "{tracking_name}" from its direct link at your ${target_price:,.2f} notify price.',
            "success",
        )
    return redirect(url_for("index"))


@main_bp.route("/product/<int:product_id>", endpoint="product_detail")
def product_detail(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    return render_template(
        "product.html",
        product=dict(product),
        product_sources=get_product_sources(product_id),
        price_history=get_price_history(product_id),
        pending_candidates_count=len(get_product_source_candidates(product_id)),
    )


@main_bp.route("/product/<int:product_id>/confirmations", endpoint="product_confirmations_page")
def product_confirmations_page(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    candidates = get_product_source_candidates(product_id)
    if not candidates:
        flash("No pending confirmations for this product.", "success")
        return redirect(url_for("product_detail", product_id=product_id))

    grouped: dict[int, dict] = {}
    for row in candidates:
        source_id = row["source_id"]
        group = grouped.setdefault(
            source_id,
            {
                "source_id": source_id,
                "source_name": row["source_name"],
                "logo_color": row["logo_color"],
                "candidates": [],
            },
        )
        group["candidates"].append(dict(row))

    return render_template(
        "confirmations.html",
        product=dict(product),
        groups=list(grouped.values()),
    )


@main_bp.route("/product/<int:product_id>/confirm/<int:candidate_id>", methods=["POST"], endpoint="confirm_product_candidate")
def confirm_product_candidate(product_id, candidate_id):
    product = get_product_by_id(product_id)
    candidate = get_product_source_candidate(candidate_id)
    if not product or not candidate or candidate["product_id"] != product_id:
        flash("Confirmation candidate not found.", "error")
        return redirect(url_for("index"))

    ps_id = candidate["product_source_id"]
    if not ps_id:
        ps_id = add_product_source(
            product_id,
            candidate["source_id"],
            enabled=1,
            status="pending_confirmation",
            verification_state="pending_confirmation",
        )
    status = (
        "quarantined"
        if candidate["candidate_price"] is None
        else status_for_price(
            candidate["candidate_price"],
            product["target_price"],
            product["alert_mode"] or "target_threshold",
        )
    )
    now = datetime.now().isoformat()
    update_product_source(
        ps_id,
        discovered_url=candidate["candidate_url"],
        current_price=candidate["candidate_price"],
        status=status,
        verification_state="verified",
        verification_reason="user_confirmed",
        health_state="healthy" if candidate["candidate_price"] is not None else "quarantined",
        matched_product_name=candidate["candidate_name"],
        fingerprint_brand=candidate["fingerprint_brand"],
        fingerprint_family=candidate["fingerprint_family"],
        fingerprint_model=candidate["fingerprint_model"],
        fingerprint_json=candidate["fingerprint_json"],
        match_label="verified_related",
        last_verified=now,
        last_checked=now,
    )
    if candidate["candidate_price"] is not None:
        add_price_history(ps_id, candidate["candidate_price"])
    mark_candidate_selected(candidate_id)
    clear_product_source_candidates(product_id, candidate["source_id"])
    compute_best_price(product_id)
    flash(f'Confirmed match for {candidate["source_name"]}.', "success")
    return redirect(url_for("product_detail", product_id=product_id))


@main_bp.route("/history/<int:product_id>", endpoint="history")
def history(product_id):
    return redirect(url_for("product_detail", product_id=product_id))


@main_bp.route("/product/<int:product_id>/sources", methods=["GET"], endpoint="product_sources_page")
def product_sources_page(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    current_ps = get_product_sources(product_id)
    active_ids = {ps["source_id"] for ps in current_ps}
    compatible_sources, _ = filter_supported_sources(
        get_certified_catalog_sources(),
        product["raw_query"] or product["name"],
    )
    return render_template(
        "sources.html",
        product=dict(product),
        all_sources=compatible_sources,
        active_ids=active_ids,
    )


@main_bp.route("/product/<int:product_id>/sources", methods=["POST"], endpoint="product_sources_save")
def product_sources_save(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    compatible_sources, _ = filter_supported_sources(
        get_certified_catalog_sources(),
        product["raw_query"] or product["name"],
    )
    available_source_map = {int(row["id"]): row for row in compatible_sources}
    new_source_ids = set()
    for sid in request.form.getlist("source_ids"):
        try:
            parsed_id = int(sid)
        except ValueError:
            continue
        if parsed_id in available_source_map:
            new_source_ids.add(parsed_id)

    current_ps = get_product_sources(product_id)
    current_map = {ps["source_id"]: ps for ps in current_ps}

    for sid in list(current_map.keys()):
        if sid not in new_source_ids:
            delete_product_sources_by_source(product_id, sid)

    newly_added = []
    for sid in new_source_ids:
        if sid not in current_map:
            newly_added.append(sid)
            add_product_source(product_id, sid, enabled=1, status="not_found")

    if newly_added:
        outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0, "blocked": 0, "timeout": 0, "unavailable": 0, "error": 0}
        context = SearchExecutionContext()

        def task(source_id):
            source = available_source_map.get(source_id)
            if not source:
                return None
            ps_rows = get_product_sources(product_id)
            ps_row = next((row for row in ps_rows if row["source_id"] == source_id), None)
            matches = discover_product_matches(
                product["raw_query"] or product["name"],
                dict(source),
                target_price=product["target_price"],
                context=context,
            )
            return dict(source), ps_row, matches

        with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(newly_added)))) as executor:
            futures = {executor.submit(task, sid): idx for idx, sid in enumerate(newly_added)}
            ordered = [None] * len(futures)
            for future in as_completed(futures):
                try:
                    ordered[futures[future]] = future.result()
                except Exception as exc:
                    logging.error(f"[{datetime.now()}] Source add verification failed: {exc}")
                    ordered[futures[future]] = None

        for item in [row for row in ordered if row is not None]:
            source_dict, ps_row, matches = item
            outcome = persist_source_matches(dict(product), dict(source_dict), matches, existing_ps=ps_row)
            if outcome in outcomes:
                outcomes[outcome] += 1
        compute_best_price(product_id)
        unavailable = outcomes["blocked"] + outcomes["timeout"] + outcomes["unavailable"] + outcomes["error"]
        flash(
            f"Sources updated — {outcomes['verified']} verified, "
            f"{outcomes['pending_confirmation']} pending confirmation"
            + (f", {unavailable} temporarily unavailable" if unavailable else "")
            + ".",
            "success",
        )
        if outcomes["pending_confirmation"]:
            return redirect(url_for("product_confirmations_page", product_id=product_id))
        return redirect(url_for("product_detail", product_id=product_id))

    compute_best_price(product_id)
    flash("Sources updated.", "success")
    return redirect(url_for("product_detail", product_id=product_id))


@main_bp.route("/product/<int:product_id>/rediscover", methods=["POST"], endpoint="rediscover_route")
def rediscover_route(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    ps_list = get_product_sources(product_id)
    outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0, "blocked": 0, "timeout": 0, "unavailable": 0, "error": 0}
    context = SearchExecutionContext()

    def rediscover_task(ps_row):
        source_dict = {
            "id": ps_row["source_id"],
            "name": ps_row["source_name"],
            "domain": ps_row["domain"],
            "search_url_template": ps_row["search_url_template"],
        }
        if ps_row.get("tracking_mode") == "direct_url":
            matches = revalidate_product_source(dict(ps_row), context=context)
        else:
            matches = discover_product_matches(
                product["raw_query"] or product["name"],
                source_dict,
                target_price=product["target_price"],
                context=context,
            )
        return ps_row, source_dict, matches

    with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(ps_list) or 1))) as executor:
        futures = {executor.submit(rediscover_task, ps): idx for idx, ps in enumerate(ps_list)}
        ordered = [None] * len(futures)
        for future in as_completed(futures):
            try:
                ordered[futures[future]] = future.result()
            except Exception as exc:
                logging.error(f"[{datetime.now()}] Re-discovery failed: {exc}")
                ordered[futures[future]] = None

    for item in [row for row in ordered if row is not None]:
        ps_row, source_dict, matches = item
        if ps_row.get("tracking_mode") == "direct_url":
            outcome = persist_source_matches(
                dict(product),
                source_dict,
                matches,
                existing_ps=ps_row,
                tracking_mode="direct_url",
                source_label_override=ps_row.get("source_label_override"),
                source_domain_override=ps_row.get("source_domain_override"),
            )
        else:
            outcome = persist_source_matches(dict(product), source_dict, matches, existing_ps=ps_row)
        if outcome in outcomes:
            outcomes[outcome] += 1

    compute_best_price(product_id)
    unavailable = outcomes["blocked"] + outcomes["timeout"] + outcomes["unavailable"] + outcomes["error"]
    flash(
        f"Re-discovery complete — {outcomes['verified']} verified, "
        f"{outcomes['pending_confirmation']} pending confirmation"
        + (f", {unavailable} temporarily unavailable" if unavailable else "")
        + ".",
        "success",
    )
    if outcomes["pending_confirmation"]:
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    return redirect(url_for("product_detail", product_id=product_id))


@main_bp.route("/delete/<int:product_id>", methods=["POST"], endpoint="delete_product_route")
def delete_product_route(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    display = product["name"] or "Product"
    if delete_product(product_id):
        flash(f'Removed "{display}" from tracking.', "success")
    else:
        flash("Could not delete the product — please try again.", "error")
    return redirect(url_for("index"))


@main_bp.route("/check", endpoint="manual_check")
def manual_check():
    if not manual_check_authorized(current_app.secret_key, CHECK_CRON_SECRET):
        logging.warning(f"[{datetime.now()}] /check rejected: unauthorized")
        flash("Not authorized to run a price check.", "error")
        return redirect(url_for("index"))
    if consume_cooldown("manual_check", CHECK_COOLDOWN_SECONDS):
        flash("A full price check just ran. Please wait a minute before starting another one.", "info")
        return redirect(url_for("index"))

    runtime = get_runtime_diagnostics()
    request_id, created = enqueue_manual_check_request(requested_by=client_ip())
    if created:
        log_event(
            "job.requested",
            job_name="manual_price_check",
            requested_by=client_ip(),
            request_id=request_id,
            worker_online=runtime.get("worker_online", False),
        )
        if runtime.get("worker_online"):
            flash("Price check requested. The worker will pick it up shortly.", "success")
        else:
            flash(
                "Price check queued. A worker needs to be running to process it automatically.",
                "warning",
            )
    else:
        if runtime.get("current_job_name") == "manual_price_check" or runtime.get("running_manual_checks", 0):
            flash("A background price check is already running.", "info")
        else:
            flash("A price check is already queued for the worker.", "info")
    return redirect(url_for("index"))
