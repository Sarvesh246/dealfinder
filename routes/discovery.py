"""
Discovery routes and track-from-discovery flow.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from flask import flash, jsonify, redirect, render_template, request, url_for

from config import (
    DISCOVERY_SOURCE_WORKERS,
    DISCOVERY_STATUS_POLL_MS,
    DISCOVERY_VERIFY_WORKERS,
    TRACK_RESULT_COOLDOWN_SECONDS,
)
from discovery_runtime import submit_discovery_job
from database import (
    add_product,
    compute_best_price,
    create_discovery_search,
    get_categories_tree,
    get_category_by_id,
    get_discovery_result_by_id,
    get_discovery_results,
    get_discovery_search,
    get_discovery_source_runs,
    get_parent_categories,
    get_product_by_id,
    get_runtime_diagnostics,
    get_source_by_id,
    get_available_sources,
    init_db,
    replace_discovery_results,
    seed_discovery_source_runs,
    update_discovery_search_state,
    upsert_discovery_source_run,
)
from hf_utils import get_smart_engine
from observability import log_event
from product_verifier import QueryType, parse_product_spec
from scraper import LAST_DISCOVERY_STATS, SearchExecutionContext, discover_deals_for_queries, verify_candidate_listing
from route_runtime import consume_cooldown
from route_support import (
    apply_source_matches_for_product,
    direct_url_matches_from_clicked_result,
    discover_prefill_from_request,
    matches_from_clicked_discovery_result,
    persist_source_matches,
    promoted_tracking_spec,
    sources_from_posted_ids,
)

from . import main_bp


def _coverage_outcome(stats: dict | None, rows: list[dict], domain_failure: str | None = None) -> tuple[str, str | None]:
    stats = stats or {}
    failure_reason = domain_failure or ((stats.get("failure_reason") or "").strip() or None)
    if rows:
        return "ok", failure_reason
    if failure_reason in {"bot_wall", "cooldown"}:
        return "blocked", failure_reason
    if failure_reason == "timeout":
        return "timeout", failure_reason
    if failure_reason in {"provider_unavailable", "provider_invalid", "provider_error", "http_error", "request_error", "selenium_error", "fetch_failed"}:
        return "unavailable", failure_reason
    if failure_reason in {"unexpected_error"}:
        return "error", failure_reason
    return "no_results", failure_reason


def _coverage_strategy(stats: dict | None) -> str:
    method = (stats or {}).get("fetch_method") or "direct"
    if method in {"provider_html"}:
        return "provider_html"
    if method in {"provider_rendered"}:
        return "provider_rendered"
    if method.startswith("selenium"):
        return "browser"
    return "direct"


def _build_coverage_summary(source_runs: list[dict]) -> dict | None:
    if not source_runs:
        return None
    total = len(source_runs)
    affected = [run for run in source_runs if run.get("outcome") in {"blocked", "timeout", "unavailable", "error"}]
    if not affected:
        return {
            "total": total,
            "affected_count": 0,
            "has_partial_coverage": False,
            "message": "",
            "details": [],
        }
    grouped: dict[str, list[dict]] = {}
    for run in affected:
        grouped.setdefault(run.get("outcome") or "unavailable", []).append(run)
    detail_rows = []
    for outcome, items in grouped.items():
        detail_rows.append(
            {
                "outcome": outcome,
                "stores": [item.get("source_name") for item in items if item.get("source_name")],
                "reason": next((item.get("failure_reason") for item in items if item.get("failure_reason")), None),
                "count": len(items),
            }
        )
    detail_rows.sort(key=lambda row: (-row["count"], row["outcome"]))
    return {
        "total": total,
        "affected_count": len(affected),
        "has_partial_coverage": True,
        "message": f"Partial coverage: {len(affected)} of {total} stores were temporarily unavailable.",
        "details": detail_rows,
    }


def _normalize_discovery_results(raw_results) -> tuple[list[dict], bool]:
    results = []
    has_ai_scores = False
    for raw_result in raw_results:
        item = dict(raw_result)
        if item.get("also_available_at"):
            try:
                item["also_available_at"] = json.loads(item["also_available_at"])
            except (json.JSONDecodeError, TypeError):
                item["also_available_at"] = None
        if item.get("deal_score", 0) > 0:
            has_ai_scores = True
        results.append(item)

    groups: dict[int, list[int]] = {}
    for idx, result in enumerate(results):
        group_id = result.get("group_id")
        if group_id is not None:
            groups.setdefault(group_id, []).append(idx)
    for members in groups.values():
        best_idx = min(members, key=lambda idx: results[idx].get("current_price", float("inf")))
        for idx in members:
            results[idx]["group_size"] = len(members)
            results[idx]["is_best_in_group"] = idx == best_idx
    return results, has_ai_scores


def _discovery_progress(search: dict, source_runs: list[dict]) -> dict:
    total = int(search.get("sources_total") or len(source_runs) or 0)
    finished = int(search.get("sources_finished") or 0)
    if not finished and source_runs:
        finished = sum(1 for run in source_runs if (run.get("outcome") or "") != "checking")
    pending = max(0, total - finished)
    status = (search.get("status") or "completed").strip().lower()
    percent = int(round((finished / total) * 100)) if total else (100 if status == "completed" else 0)
    return {
        "status": status,
        "total": total,
        "finished": finished,
        "pending": pending,
        "percent": max(0, min(100, percent)),
        "active": status in {"queued", "running"},
        "completed": status == "completed",
        "failed": status == "failed",
    }


def _discovery_empty_state(search: dict, source_runs: list[dict], result_count: int) -> dict:
    coverage_summary = _build_coverage_summary(source_runs)
    raw_seen = any(int(run.get("raw_count") or 0) > 0 for run in source_runs)
    if result_count:
        return {"title": "", "hint": ""}
    if coverage_summary and coverage_summary["has_partial_coverage"]:
        title = (
            f'No active deals were found from the stores we could check'
            f'{" under $" + format(search["max_price"], ".2f") if search.get("max_price") else ""}'
            f' for "{search["query"]}" right now'
        )
        hint = "Some stores were temporarily unavailable, so coverage may be incomplete. Try again shortly or broaden your search."
    elif raw_seen:
        title = (
            f'No active deals across our supported stores'
            f'{" under $" + format(search["max_price"], ".2f") if search.get("max_price") else ""}'
            f' for "{search["query"]}" right now'
        )
        hint = "The listings we checked did not show a qualifying sale price at the moment. Try raising your budget or broadening your search."
    else:
        title = (
            f'No active deals are showing across our supported stores'
            f'{" under $" + format(search["max_price"], ".2f") if search.get("max_price") else ""}'
            f' for "{search["query"]}" right now'
        )
        hint = "Try broadening your search or raising your budget."
    return {"title": title, "hint": hint}


def _build_discovery_view_model(search_id: int) -> dict | None:
    search_row = get_discovery_search(search_id)
    if not search_row:
        return None
    search = dict(search_row)
    raw_results = get_discovery_results(search_id)
    results, has_ai_scores = _normalize_discovery_results(raw_results)
    query_spec = parse_product_spec(search["query"])
    source_runs = [dict(row) for row in get_discovery_source_runs(search_id)]
    coverage_summary = _build_coverage_summary(source_runs)
    progress = _discovery_progress(search, source_runs)
    empty_state = _discovery_empty_state(search, source_runs, len(results))
    if coverage_summary and coverage_summary["has_partial_coverage"]:
        log_event(
            "source.coverage.partial",
            search_id=search_id,
            query=search["query"],
            affected_count=coverage_summary["affected_count"],
            total_sources=coverage_summary["total"],
        )
    return {
        "search": search,
        "results": results,
        "ai_enhanced": has_ai_scores,
        "query_spec": query_spec,
        "is_category_search": query_spec.query_type == QueryType.CATEGORY.value,
        "coverage_summary": coverage_summary,
        "source_runs": source_runs,
        "progress": progress,
        "empty_state": empty_state,
    }


def _process_discovery_rows(
    *,
    query: str,
    sources: list[dict],
    rows: list[dict],
    condition_filter: str,
    product_filter: str,
    brand_filter: str,
    context: SearchExecutionContext,
) -> tuple[list[dict], bool]:
    if not rows:
        return [], False
    engine = get_smart_engine()
    processed = engine.process_discovery_results(
        query,
        [dict(row) for row in rows],
        condition_filter=condition_filter,
        product_filter=product_filter,
        brand_filter=brand_filter,
    )
    if not processed:
        return [], engine.available

    strict_spec = parse_product_spec(query)
    source_map = {int(source["id"]): dict(source) for source in sources}
    with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_VERIFY_WORKERS, len(processed) or 1))) as executor:
        futures = {}
        for idx, row in enumerate(processed):
            source_dict = source_map.get(int(row["source_id"]))
            if not source_dict:
                row["verification_label"] = "related"
                continue
            futures[
                executor.submit(
                    verify_candidate_listing,
                    strict_spec,
                    source_dict,
                    {
                        "product_url": row["product_url"],
                        "product_name": row["product_name"],
                        "current_price": row["current_price"],
                    },
                    context=context,
                )
            ] = idx
        for future in as_completed(futures):
            idx = futures[future]
            try:
                verification = future.result()
            except Exception as exc:
                logging.error(
                    f"[{datetime.now()}] Discovery verification error for {processed[idx].get('product_url')}: {exc}"
                )
                verification = None
            processed[idx]["verification_label"] = (
                verification.match_label if verification is not None else "related"
            )
    return processed, engine.available


def _run_discovery_search_job(
    *,
    search_id: int,
    query: str,
    search_queries: tuple[str, ...],
    sources: list[dict],
    max_price: float | None,
    filter_condition: str,
    filter_product_type: str,
    filter_brand: str,
) -> None:
    started_at = datetime.now().isoformat()
    update_discovery_search_state(
        search_id,
        status="running",
        started_at=started_at,
        completed_at=None,
        sources_total=len(sources),
        sources_finished=0,
        result_count=0,
    )
    log_event("search.start", route="discover_search", query=query, search_id=search_id)

    context = SearchExecutionContext()
    cumulative_rows: list[dict] = []
    seen_urls: set[str] = set()
    latest_rows: list[dict] = []
    ai_enhanced = False

    def discover_source(source_row):
        source_dict = dict(source_row)
        started = time.perf_counter()
        deals = discover_deals_for_queries(
            tuple(search_queries),
            source_dict,
            max_price=max_price,
            context=context,
        )
        for deal in deals:
            deal["source_id"] = source_row["id"]
            deal["source_name"] = source_row["name"]
        stats = dict(LAST_DISCOVERY_STATS.get(f"{source_dict['domain']}::discover_deals") or {})
        duration_ms = int((time.perf_counter() - started) * 1000)
        outcome, failure_reason = _coverage_outcome(
            stats,
            deals,
            context.domain_failure_reason(source_dict["domain"]),
        )
        return {
            "source": source_row,
            "deals": deals,
            "stats": stats,
            "duration_ms": duration_ms,
            "outcome": outcome,
            "failure_reason": failure_reason,
        }

    try:
        with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_SOURCE_WORKERS, len(sources) or 1))) as executor:
            futures = {executor.submit(discover_source, source): idx for idx, source in enumerate(sources)}
            finished = 0
            for future in as_completed(futures):
                idx = futures[future]
                source = sources[idx]
                try:
                    payload = future.result()
                except Exception as exc:
                    logging.error(
                        f"[{datetime.now()}] Discovery error on {source['name']} for query {query!r}: {exc}"
                    )
                    payload = {
                        "source": source,
                        "deals": [],
                        "stats": {},
                        "duration_ms": 0,
                        "outcome": "error",
                        "failure_reason": "worker_error",
                    }

                upsert_discovery_source_run(
                    search_id,
                    payload["source"]["id"],
                    outcome=payload["outcome"],
                    fetch_strategy=_coverage_strategy(payload["stats"]),
                    failure_reason=payload["failure_reason"],
                    raw_count=int(payload["stats"].get("scraped_count") or 0),
                    eligible_count=int(payload["stats"].get("after_dedupe_count") or 0),
                    returned_count=len(payload["deals"]),
                    duration_ms=payload["duration_ms"],
                )
                finished += 1
                update_discovery_search_state(search_id, sources_finished=finished)

                added_new_rows = False
                for deal in payload["deals"]:
                    url = (deal.get("product_url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    cumulative_rows.append(deal)
                    added_new_rows = True

                if added_new_rows or finished == len(sources):
                    latest_rows, ai_enhanced = _process_discovery_rows(
                        query=query,
                        sources=sources,
                        rows=cumulative_rows,
                        condition_filter=filter_condition,
                        product_filter=filter_product_type,
                        brand_filter=filter_brand,
                        context=context,
                    )
                    replace_discovery_results(search_id, latest_rows)
                    update_discovery_search_state(search_id, result_count=len(latest_rows))

        completed_at = datetime.now().isoformat()
        update_discovery_search_state(
            search_id,
            status="completed",
            completed_at=completed_at,
            sources_finished=len(sources),
            result_count=len(latest_rows),
        )
        log_event(
            "search.finish",
            route="discover_search",
            query=query,
            search_id=search_id,
            result_count=len(latest_rows),
            ai_enhanced=ai_enhanced,
            worker_online=get_runtime_diagnostics().get("worker_online", False),
        )
    except Exception as exc:
        logging.exception(f"[{datetime.now()}] Discovery job failed for search_id={search_id}: {exc}")
        update_discovery_search_state(
            search_id,
            status="failed",
            completed_at=datetime.now().isoformat(),
            result_count=len(latest_rows),
        )
        log_event(
            "search.finish",
            route="discover_search",
            query=query,
            search_id=search_id,
            result_count=len(latest_rows),
            ai_enhanced=ai_enhanced,
            failed=True,
        )


@main_bp.route("/discover", endpoint="discover_page")
def discover_page():
    tree = get_categories_tree()
    parents = get_parent_categories()
    sources = get_available_sources()
    if not sources:
        logging.warning(
            f"[{datetime.now()}] discover_page: no sources in DB — re-running init_db"
        )
        try:
            init_db()
            sources = get_available_sources()
        except Exception as exc:
            logging.error(f"[{datetime.now()}] discover init_db retry failed: {exc}")
    return render_template(
        "discover.html",
        categories=tree,
        parent_categories=parents,
        sources=sources,
        prefill=discover_prefill_from_request(),
    )


@main_bp.route("/discover/search", methods=["POST"], endpoint="discover_search")
def discover_search():
    query = request.form.get("query", "").strip()
    max_price_str = request.form.get("max_price", "").strip()
    category_id = request.form.get("category_id", "").strip() or None

    if not query:
        flash("Enter a search term to find deals.", "error")
        return redirect(url_for("discover_page"))

    try:
        max_price = float(max_price_str) if max_price_str else None
        if max_price is not None and max_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Please enter a valid maximum price.", "error")
        return redirect(url_for("discover_page"))

    cat_id_int = None
    if category_id:
        try:
            cat_id_int = int(category_id)
        except ValueError:
            pass

    engine = get_smart_engine()
    query_spec = parse_product_spec(query)
    search_terms = query
    if cat_id_int and query_spec.query_type == "category" and not query_spec.model_token:
        category = get_category_by_id(cat_id_int)
        if category and category["search_keywords"]:
            search_terms = category["search_keywords"].split(",")[0].strip()
    elif engine.available and query_spec.query_type == "category" and not query_spec.model_token:
        search_terms = engine.enhance_query(query)

    search_all = request.form.get("search_all_sources") == "1"
    posted_source_ids = request.form.getlist("source_ids")
    sources = sources_from_posted_ids(search_all, posted_source_ids)
    if not sources:
        flash(
            "Select at least one store to search, or turn on All registered stores.",
            "error",
        )
        return redirect(url_for("discover_page"))

    filter_condition = request.form.get("filter_condition", "new_only")
    if filter_condition not in ("new_only", "include_refurb", "all"):
        filter_condition = "new_only"
    filter_product_type = request.form.get("filter_product_type", "primary_only")
    if filter_product_type not in ("primary_only", "include_accessories"):
        filter_product_type = "primary_only"
    filter_brand = request.form.get("filter_brand", "exact")
    if filter_brand not in ("exact", "similar"):
        filter_brand = "exact"

    search_id = create_discovery_search(
        query,
        cat_id_int,
        max_price,
        filter_condition=filter_condition,
        filter_product_type=filter_product_type,
        filter_brand=filter_brand,
        status="queued",
        sources_total=len(sources),
        sources_finished=0,
    )
    if not search_id:
        flash("Could not start discovery — please try again.", "error")
        return redirect(url_for("discover_page"))

    if query_spec.query_type in {QueryType.EXACT_MODEL.value, QueryType.NAMED_PRODUCT.value}:
        search_queries = query_spec.search_aliases or (search_terms,)
    else:
        search_queries = (search_terms,)
    try:
        seed_discovery_source_runs(search_id, sources)
        submit_discovery_job(
            _run_discovery_search_job,
            search_id=search_id,
            query=query,
            search_queries=tuple(search_queries),
            sources=[dict(source) for source in sources],
            max_price=max_price,
            filter_condition=filter_condition,
            filter_product_type=filter_product_type,
            filter_brand=filter_brand,
        )
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Could not start background discovery for {query!r}: {exc}")
        update_discovery_search_state(
            search_id,
            status="failed",
            completed_at=datetime.now().isoformat(),
        )
        flash("Could not start discovery right now. Please try again.", "error")
        return redirect(url_for("discover_page"))
    return redirect(url_for("discover_results", search_id=search_id))


@main_bp.route("/discover/results/<int:search_id>", endpoint="discover_results")
def discover_results(search_id):
    view = _build_discovery_view_model(search_id)
    if not view:
        flash("Search not found.", "error")
        return redirect(url_for("discover_page"))
    return render_template(
        "discover_results.html",
        **view,
        poll_interval_ms=DISCOVERY_STATUS_POLL_MS,
    )


@main_bp.route("/discover/status/<int:search_id>", endpoint="discover_status")
def discover_status(search_id):
    view = _build_discovery_view_model(search_id)
    if not view:
        return jsonify({"status": "not_found"}), 404
    return jsonify(
        {
            "status": view["progress"]["status"],
            "result_count": len(view["results"]),
            "progress": view["progress"],
            "coverage_summary": view["coverage_summary"],
            "source_runs": view["source_runs"],
            "results_html": render_template("partials/discovery_results_body.html", **view),
            "progress_html": render_template("partials/discovery_progress.html", **view),
            "poll_interval_ms": DISCOVERY_STATUS_POLL_MS,
        }
    )


@main_bp.route("/discover/track/<int:result_id>", methods=["POST"], endpoint="discover_track")
def discover_track(result_id):
    result = get_discovery_result_by_id(result_id)
    if not result:
        flash("Deal not found.", "error")
        return redirect(url_for("discover_page"))

    result = dict(result)
    if consume_cooldown("discover_track", TRACK_RESULT_COOLDOWN_SECONDS, extra=str(result_id)):
        flash(
            "That result is already being processed. Please wait a moment before tracking it again.",
            "info",
        )
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    search = get_discovery_search(result["search_id"]) if result.get("search_id") else None
    search = dict(search) if search else None
    spec, tracking_name = promoted_tracking_spec(search, result)
    if spec is None or not tracking_name:
        flash(
            "That result is still too broad to track directly. Choose a more specific listing.",
            "error",
        )
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    price = result.get("current_price")
    target_price = search.get("max_price") if search and search.get("max_price") is not None else price
    target_price = target_price if target_price is not None else 0
    source_id = result["source_id"]
    source = get_source_by_id(source_id) if source_id else None
    if not source or not result.get("product_url"):
        flash("That result cannot be tracked because the source listing is missing.", "error")
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    matches = matches_from_clicked_discovery_result(result, spec, dict(source))
    tracking_mode = "search_verified"
    if not matches["verified"]:
        selected_listing = direct_url_matches_from_clicked_result(result, dict(source))
        if selected_listing:
            matches = {
                "verified": selected_listing.get("verified", []),
                "ambiguous": selected_listing.get("ambiguous", []),
            }
            tracking_mode = "direct_url"
            tracking_name = selected_listing.get("tracking_name") or tracking_name

    if not matches["verified"] and not matches["ambiguous"]:
        flash(
            "Could not verify that listing for tracking. Please rerun discovery or choose a different result.",
            "error",
        )
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    product_id = add_product(tracking_name, target_price)
    if not product_id:
        flash("Could not save the product.", "error")
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    product = dict(get_product_by_id(product_id))
    outcome = persist_source_matches(
        product,
        dict(source),
        matches,
        tracking_mode=tracking_mode,
    )
    compute_best_price(product_id)
    if outcome == "pending_confirmation":
        flash(f'Created "{tracking_name}" with matches awaiting confirmation.', "success")
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    flash(f'Now tracking "{tracking_name}".', "success")
    return redirect(url_for("index"))
