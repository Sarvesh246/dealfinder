"""
Shared route-support logic for tracking and discovery flows.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from flask import redirect, request, url_for

from config import STRICT_SOURCE_WORKERS
from database import (
    add_price_history,
    add_product_source,
    add_product_source_candidate,
    clear_product_source_candidates,
    compute_best_price,
    get_available_sources,
    get_product_source_by_id,
    update_product_source,
)
from pricing_status import status_for_price
from product_verifier import QueryType, parse_product_spec, verification_result_to_fields
from scraper import (
    SearchExecutionContext,
    discover_product_matches,
    verify_candidate_listing,
)
from template_utils import canonical_external_url


def first_discover_listing(rows):
    """discover_product returns a list of hits (cheapest-first); DB stores one per source."""
    if not rows:
        return None
    return rows[0] if isinstance(rows, list) else rows


def match_row_from_verification(candidate, verification):
    row = {
        "url": candidate.get("product_url") or candidate.get("url"),
        "price": (
            verification.current_price
            if verification.current_price is not None
            else candidate.get("current_price") or candidate.get("price")
        ),
        "name_found": (
            verification.product_name
            or candidate.get("product_name")
            or candidate.get("name_found")
        ),
    }
    row.update(verification_result_to_fields(verification))
    return row


def matches_from_clicked_discovery_result(result, spec, source):
    candidate = {
        "product_url": result.get("product_url"),
        "product_name": result.get("product_name"),
        "current_price": result.get("current_price"),
    }
    verification = verify_candidate_listing(spec, source, candidate)
    if verification and verification.status in {"verified", "ambiguous"}:
        row = match_row_from_verification(candidate, verification)
        return {
            "verified": [row] if verification.status == "verified" else [],
            "ambiguous": [row] if verification.status == "ambiguous" else [],
        }

    label = (result.get("verification_label") or "").strip().lower()
    if label not in {"verified_exact", "verified_named", "verified_related"}:
        return {"verified": [], "ambiguous": []}

    price = result.get("current_price")
    if price is None:
        return {"verified": [], "ambiguous": []}

    fallback_row = {
        "url": result.get("product_url"),
        "price": price,
        "name_found": result.get("product_name"),
        "verification_state": "verified" if label in {"verified_exact", "verified_named"} else "ambiguous",
        "verification_reason": "reused_discovery_verification",
        "health_state": "healthy",
        "matched_product_name": result.get("product_name"),
        "fingerprint_brand": spec.brand,
        "fingerprint_family": spec.family,
        "fingerprint_model": spec.model_token,
        "fingerprint_json": None,
        "match_label": label,
    }
    return {
        "verified": [fallback_row] if label in {"verified_exact", "verified_named"} else [],
        "ambiguous": [fallback_row] if label == "verified_related" else [],
    }


def matches_from_direct_link_inspection(inspection):
    verification = inspection.get("verification")
    if not verification:
        return {"verified": [], "ambiguous": []}
    row = {
        "url": inspection.get("url"),
        "price": inspection.get("price"),
        "name_found": inspection.get("title"),
    }
    row.update(verification_result_to_fields(verification))
    return {
        "verified": [row] if verification.status == "verified" else [],
        "ambiguous": [row] if verification.status == "ambiguous" else [],
    }


def direct_link_error_message(reason: str) -> str:
    return {
        "invalid_url": "Paste a full product URL to start tracking from a link.",
        "not_product_url": "That link does not look like a product page. Paste the product page URL instead.",
        "fetch_failed": "We couldn't fetch that product page right now. Please try again in a moment.",
        "weak_listing": "That page didn't expose a stable product title we can track reliably.",
        "price_not_found": "That page didn't expose a reliable current price, so it can't be tracked yet.",
    }.get(reason, "That link could not be verified for reliable tracking.")


def discover_prefill_from_request():
    source_ids = []
    for raw_sid in request.args.getlist("source_ids"):
        try:
            source_ids.append(int(raw_sid))
        except (TypeError, ValueError):
            continue
    return {
        "query": request.args.get("query", "").strip(),
        "max_price": request.args.get("max_price", "").strip(),
        "search_all_sources": request.args.get("search_all_sources", "").strip() == "1",
        "source_ids": source_ids,
    }


def promoted_tracking_spec(search, result):
    result_title = (result.get("product_name") or "").strip()
    search_query = ((search or {}).get("query") or "").strip()
    result_spec = parse_product_spec(result_title or search_query)
    search_spec = parse_product_spec(search_query) if search_query else result_spec
    if search_spec.query_type == QueryType.CATEGORY.value:
        if result_spec.query_type == QueryType.CATEGORY.value:
            return None, None
        return result_spec, (result_spec.raw_query or result_title)
    return search_spec, (search_query or result_title)


def persist_source_matches(
    product,
    source,
    matches,
    *,
    existing_ps=None,
    tracking_mode="search_verified",
    source_label_override=None,
    source_domain_override=None,
):
    product_data = dict(product) if hasattr(product, "keys") and not isinstance(product, dict) else product
    product_id = product["id"]
    source_id = source["id"]
    clear_product_source_candidates(product_id, source_id)

    def ensure_row():
        nonlocal existing_ps
        if existing_ps:
            return existing_ps["id"]
        ps_id = add_product_source(
            product_id,
            source_id,
            enabled=1,
            status="not_found",
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        existing_ps = get_product_source_by_id(ps_id) if ps_id else None
        return ps_id

    verified = matches.get("verified", [])
    ambiguous = matches.get("ambiguous", [])
    now = datetime.now().isoformat()

    if verified:
        best = verified[0]
        ps_id = ensure_row()
        if not ps_id:
            return "error"
        update_product_source(
            ps_id,
            discovered_url=best["url"],
            current_price=best.get("price"),
            status=status_for_price(
                best.get("price"),
                product_data.get("target_price"),
                product_data.get("alert_mode", "target_threshold"),
            ),
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
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        if best.get("price") is not None:
            add_price_history(ps_id, best["price"])
        return "verified"

    if ambiguous:
        ps_id = ensure_row()
        if not ps_id:
            return "error"
        primary = ambiguous[0]
        update_product_source(
            ps_id,
            discovered_url=None,
            current_price=None,
            status="pending_confirmation",
            verification_state="pending_confirmation",
            verification_reason=primary.get("verification_reason"),
            health_state=primary.get("health_state", "healthy"),
            matched_product_name=primary.get("matched_product_name") or primary.get("name_found"),
            fingerprint_brand=primary.get("fingerprint_brand"),
            fingerprint_family=primary.get("fingerprint_family"),
            fingerprint_model=primary.get("fingerprint_model"),
            fingerprint_json=primary.get("fingerprint_json"),
            match_label=primary.get("match_label", "verified_related"),
            last_verified=now,
            last_checked=now,
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        for candidate in ambiguous:
            add_product_source_candidate(
                product_id,
                source_id,
                candidate["url"],
                product_source_id=ps_id,
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
        return "pending_confirmation"

    ps_id = ensure_row()
    if not ps_id:
        return "error"
    update_product_source(
        ps_id,
        discovered_url=None,
        current_price=None,
        status="not_found",
        verification_state="not_found",
        verification_reason="no_verified_match",
        health_state="healthy",
        matched_product_name=None,
        fingerprint_brand=None,
        fingerprint_family=None,
        fingerprint_model=None,
        fingerprint_json=None,
        match_label="related",
        last_verified=now,
        last_checked=now,
        tracking_mode=tracking_mode,
        source_label_override=source_label_override,
        source_domain_override=source_domain_override,
    )
    return "not_found"


def apply_source_matches_for_product(product, sources):
    outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0}
    if not sources:
        compute_best_price(product["id"])
        return outcomes
    context = SearchExecutionContext()

    def task(source_row):
        source_dict = dict(source_row)
        matches = discover_product_matches(
            product["raw_query"] or product["name"],
            source_dict,
            target_price=product["target_price"],
            context=context,
        )
        return source_dict, matches

    with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(sources) or 1))) as executor:
        futures = {executor.submit(task, source): idx for idx, source in enumerate(sources)}
        by_index = [None] * len(futures)
        for future in as_completed(futures):
            idx = futures[future]
            try:
                by_index[idx] = future.result()
            except Exception as exc:
                logging.error(
                    f"[{datetime.now()}] Strict source matching failed for {dict(sources[idx]).get('name')}: {exc}"
                )
                by_index[idx] = None

    for source_dict, matches in [row for row in by_index if row is not None]:
        outcome = persist_source_matches(product, source_dict, matches)
        if outcome in outcomes:
            outcomes[outcome] += 1
    compute_best_price(product["id"])
    return outcomes


def sources_from_posted_ids(search_all: bool, posted_ids: list[str]) -> list:
    available_sources = {int(row["id"]): row for row in get_available_sources()}
    if search_all:
        return list(available_sources.values())
    seen: set[int] = set()
    out = []
    for raw in posted_ids:
        try:
            sid = int(raw)
        except (ValueError, TypeError):
            continue
        if sid in seen:
            continue
        seen.add(sid)
        row = available_sources.get(sid)
        if row:
            out.append(row)
    return out


def redirect_back_or(endpoint: str, **values):
    target = request.referrer
    if target:
        return redirect(target)
    return redirect(url_for(endpoint, **values))


def final_outbound_url(raw_url: str | None) -> str:
    return canonical_external_url(raw_url)
