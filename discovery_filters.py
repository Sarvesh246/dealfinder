"""
Compatibility wrapper over product_identity for discovery-time filtering.

Pipeline role:
1. Accept raw scraper rows and a user query.
2. Reuse product_identity.py to classify product kind, condition, and confidence.
3. Expose stable helper functions used by scraper.py, hf_utils.py, and tests.

This module exists to keep the discovery pipeline readable and backwards
compatible while the lower-level rules live in product_identity.py and the
strict final gate lives in product_verifier.py.
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from typing import Any

from product_identity import (
    classify_with_intent,
    family_defs_list,
    parse_query_intent,
    passes_confidence_gate,
    passes_identity_gate,
    merge_condition_title_url,
    detect_condition,
    condition_hint_from_url,
    query_has_accessory_intent,
)

# Re-export for tests / imports
_FAMILY_DEFS = family_defs_list()


def classify_listing(
    title: str,
    query_norm: str,
    *,
    family: dict[str, Any] | None,
    accessory_intent: bool,
    product_url: str | None = None,
) -> dict[str, Any]:
    """Returns product_kind, condition_class, structural_relevance, penalties."""
    intent = parse_query_intent(query_norm)
    intent = replace(
        intent,
        family=family if family is not None else intent.family,
        accessory_intent=accessory_intent,
    )
    meta = classify_with_intent(title, product_url, intent)
    return {
        "product_kind": meta["product_kind"],
        "condition_class": meta["condition_class"],
        "structural_relevance": meta["structural_relevance"],
        "penalty_total": meta["penalty_total"],
        "listing_role": meta.get("listing_role"),
        "identity_match": meta.get("identity_match"),
        "trust_score": meta.get("trust_score"),
        "confidence": meta.get("confidence"),
        "query_type": meta.get("query_type"),
    }


def enrich_result_metadata(
    row: dict[str, Any],
    raw_user_query: str,
    *,
    family: dict[str, Any] | None,
    accessory_intent: bool,
) -> None:
    title = row.get("product_name") or ""
    qn = re.sub(r"\s+", " ", (raw_user_query or "").lower().strip())
    meta = classify_listing(
        title,
        qn,
        family=family,
        accessory_intent=accessory_intent,
        product_url=row.get("product_url"),
    )
    row["condition_class"] = meta["condition_class"]
    row["product_kind"] = meta["product_kind"]
    row["structural_relevance"] = meta["structural_relevance"]
    row["listing_penalty"] = meta["penalty_total"]
    row["listing_role"] = meta.get("listing_role", "primary_product")
    row["identity_match"] = meta.get("identity_match", 0.0)
    row["trust_score"] = meta.get("trust_score", 0.5)
    row["confidence"] = meta.get("confidence", 0.5)
    row["query_type"] = meta.get("query_type", "category")
    row["_enrich_query"] = raw_user_query


def passes_eligibility(
    row: dict[str, Any],
    *,
    condition_filter: str,
    product_filter: str,
    brand_filter: str,
    family: dict[str, Any] | None,
    accessory_intent: bool,
    _log_name: str = "",
    query_for_intent: str | None = None,
    min_confidence: float | None = None,
) -> bool:
    """Stage-1 gate before ranking. Optional min_confidence for deal discovery."""
    cond = row.get("condition_class", "unknown")
    kind = row.get("product_kind", "primary_product")
    name_preview = _log_name or (row.get("product_name") or "")[:60]

    if condition_filter == "new_only":
        if cond in ("renewed", "refurbished", "used", "open_box"):
            logging.debug(
                f"[eligibility] EXCLUDE condition={cond!r} filter={condition_filter!r}"
                f" name={name_preview!r}"
            )
            return False
    elif condition_filter == "include_refurb":
        if cond == "used":
            logging.debug(
                f"[eligibility] EXCLUDE condition={cond!r} filter={condition_filter!r}"
                f" name={name_preview!r}"
            )
            return False

    if product_filter == "primary_only" and family and not accessory_intent:
        if kind in ("accessory", "compatible_generic"):
            logging.debug(
                f"[eligibility] EXCLUDE product_kind={kind!r} filter={product_filter!r}"
                f" name={name_preview!r}"
            )
            return False

    if brand_filter == "exact" and family:
        if kind == "other_brand":
            logging.debug(
                f"[eligibility] EXCLUDE other_brand filter={brand_filter!r}"
                f" name={name_preview!r}"
            )
            return False

    q_src = (query_for_intent if query_for_intent is not None else "") or (
        row.get("_enrich_query") or ""
    )
    intent = parse_query_intent(q_src)
    intent = replace(
        intent,
        family=family if family is not None else intent.family,
        accessory_intent=accessory_intent,
    )
    if q_src and not passes_identity_gate(row, intent):
        return False
    if min_confidence is not None:
        if not passes_confidence_gate(row, min_confidence):
            return False
    return True


def resolve_family_and_intent(raw_user_query: str) -> tuple[dict[str, Any] | None, bool]:
    intent = parse_query_intent(raw_user_query)
    return intent.family, intent.accessory_intent
