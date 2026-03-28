"""
Public product-identity API.

This package preserves the historical `product_identity` import surface while
splitting query parsing, family definitions, intent derivation, and
classification into focused modules.
"""

from .classify import (
    classify_with_intent,
    compute_confidence,
    compute_identity_match,
    compute_trust_score,
    has_core_generic,
    identity_threshold_for_query,
    passes_confidence_gate,
    passes_identity_gate,
)
from .family_catalog import family_defs_list
from .intent import (
    match_family_from_query,
    parse_query_intent,
)
from .query import (
    QueryIntent,
    QueryType,
    condition_hint_from_url,
    detect_condition,
    merge_condition_title_url,
    normalize_user_query,
    query_has_accessory_intent,
)

__all__ = [
    "QueryIntent",
    "QueryType",
    "classify_with_intent",
    "compute_confidence",
    "compute_identity_match",
    "compute_trust_score",
    "condition_hint_from_url",
    "detect_condition",
    "family_defs_list",
    "has_core_generic",
    "identity_threshold_for_query",
    "match_family_from_query",
    "merge_condition_title_url",
    "normalize_user_query",
    "parse_query_intent",
    "passes_confidence_gate",
    "passes_identity_gate",
    "query_has_accessory_intent",
]
