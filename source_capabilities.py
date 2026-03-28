"""Capability-aware source selection for scoped certified retailers.

Most certified sources remain broadly eligible. Narrower retailers can opt into
scoped certification here so they are only queried for product families they
have proven they can handle well.
"""

from __future__ import annotations

from product_verifier import QueryType, parse_product_spec

_OFFICEDEPOT_FAMILIES = {
    "standing_desk",
    "office_chair",
    "printer",
    "router",
    "monitor",
    "keyboard",
    "mouse",
    "laptop",
    "tablet",
    "storage",
    "webcam",
}

_SCOPED_SOURCE_CAPABILITIES = {
    "officedepot.com": {
        "query_types": {
            QueryType.EXACT_MODEL.value,
            QueryType.NAMED_PRODUCT.value,
            QueryType.CATEGORY.value,
        },
        "families": _OFFICEDEPOT_FAMILIES,
        "category_families": {
            "standing_desk",
            "office_chair",
            "printer",
            "router",
            "monitor",
            "keyboard",
            "mouse",
        },
    },
}


def _normalize_spec(query_or_spec):
    if hasattr(query_or_spec, "query_type"):
        return query_or_spec
    return parse_product_spec(str(query_or_spec or ""))


def source_supports_query(source_or_domain, query_or_spec) -> bool:
    if hasattr(source_or_domain, "keys"):
        domain = source_or_domain["domain"]
    elif isinstance(source_or_domain, dict):
        domain = source_or_domain.get("domain")
    else:
        domain = str(source_or_domain or "")
    domain = domain.lower().replace("www.", "")
    capability = _SCOPED_SOURCE_CAPABILITIES.get(domain)
    if not capability:
        return True

    spec = _normalize_spec(query_or_spec)
    family = spec.family
    if not family:
        return False
    if spec.query_type not in capability["query_types"]:
        return False
    if spec.query_type == QueryType.CATEGORY.value:
        return family in capability["category_families"]
    return family in capability["families"]


def filter_supported_sources(sources, query_or_spec):
    spec = _normalize_spec(query_or_spec)
    supported = []
    skipped = []
    for source in sources:
        if source_supports_query(source, spec):
            supported.append(source)
        else:
            skipped.append(source)
    return supported, skipped
