"""
Discovery routes and track-from-discovery flow.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from flask import flash, redirect, render_template, request, url_for

from config import DISCOVERY_SOURCE_WORKERS, DISCOVERY_VERIFY_WORKERS, TRACK_RESULT_COOLDOWN_SECONDS
from database import (
    add_discovery_result,
    add_product,
    compute_best_price,
    create_discovery_search,
    get_categories_tree,
    get_category_by_id,
    get_discovery_result_by_id,
    get_discovery_results,
    get_discovery_search,
    get_parent_categories,
    get_product_by_id,
    get_runtime_diagnostics,
    get_source_by_id,
    get_available_sources,
    init_db,
    update_discovery_search_count,
)
from hf_utils import get_smart_engine
from observability import log_event
from product_verifier import QueryType, parse_product_spec
from scraper import SearchExecutionContext, discover_deals_for_queries, verify_candidate_listing
from route_runtime import consume_cooldown
from route_support import (
    apply_source_matches_for_product,
    discover_prefill_from_request,
    matches_from_clicked_discovery_result,
    persist_source_matches,
    promoted_tracking_spec,
    sources_from_posted_ids,
)

from . import main_bp


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
    log_event("search.start", route="discover_search", query=query, category_id=category_id or None)

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
    )
    if not search_id:
        flash("Could not start discovery — please try again.", "error")
        return redirect(url_for("discover_page"))

    if query_spec.query_type in {QueryType.EXACT_MODEL.value, QueryType.NAMED_PRODUCT.value}:
        search_queries = query_spec.search_aliases or (search_terms,)
    else:
        search_queries = (search_terms,)

    all_raw: list[dict] = []
    context = SearchExecutionContext()

    def discover_source(source_row):
        source_dict = dict(source_row)
        deals = discover_deals_for_queries(
            tuple(search_queries),
            source_dict,
            max_price=max_price,
            context=context,
        )
        for deal in deals:
            deal["source_id"] = source_row["id"]
            deal["source_name"] = source_row["name"]
        return deals

    with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_SOURCE_WORKERS, len(sources) or 1))) as executor:
        futures = {executor.submit(discover_source, source): idx for idx, source in enumerate(sources)}
        source_rows_by_index = [[] for _ in sources]
        for future in as_completed(futures):
            idx = futures[future]
            try:
                source_rows_by_index[idx] = future.result()
            except Exception as exc:
                source = sources[idx]
                logging.error(
                    f"[{datetime.now()}] Discovery error on {source['name']} for query {query!r}: {exc}"
                )
                source_rows_by_index[idx] = []

    for rows in source_rows_by_index:
        all_raw.extend(rows)

    scraped_before_rank = len(all_raw)
    ai_enhanced = False

    if all_raw:
        all_raw = engine.process_discovery_results(
            query,
            all_raw,
            condition_filter=filter_condition,
            product_filter=filter_product_type,
            brand_filter=filter_brand,
        )
        ai_enhanced = engine.available
        strict_spec = parse_product_spec(query)
        source_map = {int(source["id"]): dict(source) for source in sources}
        with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_VERIFY_WORKERS, len(all_raw) or 1))) as executor:
            futures = {}
            for idx, row in enumerate(all_raw):
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
                        f"[{datetime.now()}] Discovery verification error for {all_raw[idx].get('product_url')}: {exc}"
                    )
                    verification = None
                all_raw[idx]["verification_label"] = (
                    verification.match_label if verification is not None else "related"
                )

    for row in all_raw:
        add_discovery_result(
            search_id=search_id,
            source_id=row["source_id"],
            product_name=row["product_name"],
            current_price=row["current_price"],
            original_price=row.get("original_price"),
            discount_percent=row.get("discount_percent", 0),
            product_url=row["product_url"],
            relevance_score=row.get("relevance_score", 0),
            deal_score=row.get("deal_score", 0),
            group_id=row.get("group_id"),
            also_available_at=row.get("also_available_at"),
            discount_confirmed=1 if row.get("discount_confirmed", True) else 0,
            verification_label=row.get("verification_label", "related"),
        )

    update_discovery_search_count(search_id, len(all_raw))

    if not all_raw:
        if scraped_before_rank:
            flash(
                f'No active deals across our supported stores matched your current filters for "{query}" right now. '
                "Try including refurbished items, accessories, similar brands, or a different search term.",
                "error",
            )
        else:
            flash(
                f'No active deals are showing across our supported stores for "{query}" right now. '
                "Try broadening your search or raising your budget.",
                "error",
            )
    else:
        ai_note = " (AI-ranked)" if ai_enhanced else ""
        flash(f'Found {len(all_raw)} deals for "{query}"{ai_note}!', "success")

    log_event(
        "search.finish",
        route="discover_search",
        query=query,
        result_count=len(all_raw),
        ai_enhanced=ai_enhanced,
        worker_online=get_runtime_diagnostics().get("worker_online", False),
    )
    return redirect(url_for("discover_results", search_id=search_id))


@main_bp.route("/discover/results/<int:search_id>", endpoint="discover_results")
def discover_results(search_id):
    search = get_discovery_search(search_id)
    if not search:
        flash("Search not found.", "error")
        return redirect(url_for("discover_page"))

    raw_results = get_discovery_results(search_id)
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

    query_spec = parse_product_spec(search["query"])
    return render_template(
        "discover_results.html",
        search=dict(search),
        results=results,
        ai_enhanced=has_ai_scores,
        query_spec=query_spec,
        is_category_search=(query_spec.query_type == QueryType.CATEGORY.value),
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
    outcome = persist_source_matches(product, dict(source), matches)
    compute_best_price(product_id)
    if outcome == "pending_confirmation":
        flash(f'Created "{tracking_name}" with matches awaiting confirmation.', "success")
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    flash(f'Now tracking "{tracking_name}".', "success")
    return redirect(url_for("index"))
