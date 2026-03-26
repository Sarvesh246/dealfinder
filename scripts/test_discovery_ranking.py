"""
Quick checks for discovery classification + pipeline (no live scraping).
Run from project root: python scripts/test_discovery_ranking.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discovery_filters import (  # noqa: E402
    enrich_result_metadata,
    merge_condition_title_url,
    passes_eligibility,
    resolve_family_and_intent,
)
from hf_utils import SmartEngine  # noqa: E402
from product_identity import QueryType, parse_query_intent  # noqa: E402


def _row(name: str, price: float = 99.0, discount: float = 0.0) -> dict:
    return {
        "product_name": name,
        "current_price": price,
        "discount_percent": discount,
        "product_url": "https://example.test/p",
        "source_id": 1,
        "source_name": "Test",
    }


def main() -> None:
    assert (
        merge_condition_title_url("Apple AirPods Pro", "https://x.com/p/renewed-item")
        == "renewed"
    )

    qi = parse_query_intent("RTX 5070")
    assert qi.query_type == QueryType.EXACT_MODEL
    assert qi.family and qi.family["id"] == "gpu"

    qi_mon = parse_query_intent("gaming monitor")
    assert qi_mon.query_type in (QueryType.PRODUCT_LINE, QueryType.CATEGORY)

    fam, acc = resolve_family_and_intent("airpods")
    assert fam and fam["id"] == "airpods"
    assert acc is False

    _fam_case, acc_case = resolve_family_and_intent("airpods case")
    assert _fam_case and _fam_case["id"] == "airpods"
    assert acc_case is True

    # Classification
    r1 = _row("Apple AirPods Pro 2 with Wireless MagSafe Charging Case (USB-C)")
    enrich_result_metadata(r1, "airpods", family=fam, accessory_intent=False)
    assert r1["product_kind"] == "primary_product"
    assert r1["condition_class"] == "new"
    assert passes_eligibility(
        r1,
        condition_filter="new_only",
        product_filter="primary_only",
        brand_filter="exact",
        family=fam,
        accessory_intent=False,
    )

    r2 = _row("Silicone Case for AirPods Pro 2 — Navy")
    enrich_result_metadata(r2, "airpods", family=fam, accessory_intent=False)
    assert r2["product_kind"] == "accessory"
    assert not passes_eligibility(
        r2,
        condition_filter="new_only",
        product_filter="primary_only",
        brand_filter="exact",
        family=fam,
        accessory_intent=False,
    )

    r3 = _row("Beats Studio Pro Wireless Earbuds")
    enrich_result_metadata(r3, "airpods", family=fam, accessory_intent=False)
    assert r3["product_kind"] == "other_brand"

    r4 = _row("Apple AirPods (3rd Generation) — Renewed")
    enrich_result_metadata(r4, "airpods", family=fam, accessory_intent=False)
    assert r4["condition_class"] == "renewed"
    assert not passes_eligibility(
        r4,
        condition_filter="new_only",
        product_filter="primary_only",
        brand_filter="exact",
        family=fam,
        accessory_intent=False,
    )
    assert passes_eligibility(
        r4,
        condition_filter="all",
        product_filter="primary_only",
        brand_filter="exact",
        family=fam,
        accessory_intent=False,
    )

    r5 = _row("Apple AirTag 4 Pack")
    enrich_result_metadata(r5, "airpods", family=fam, accessory_intent=False)
    assert r5["product_kind"] == "other_brand"

    # Pipeline without Hugging Face (keyword relevance fallback)
    eng = SmartEngine()
    eng._enabled = False
    eng._client = None

    raw = [
        _row("Apple AirPods Pro 2", 199, 5),
        _row("Silicone Case for AirPods Pro 2 — Navy", 15, 0),
        _row("Beats Studio Pro Wireless Earbuds", 199, 0),
        _row("Apple AirPods (3rd Generation) — Renewed", 89, 40),
        _row("Apple AirPods 4", 129, 0),
        _row("Amazon Fire TV Stick 4K", 29, 10),
    ]
    out = eng.process_discovery_results(
        "airpods",
        raw,
        condition_filter="new_only",
        product_filter="primary_only",
        brand_filter="exact",
    )
    names = [x["product_name"] for x in out]
    assert any("AirPods Pro 2" in n for n in names)
    assert not any("Silicone Case" in n for n in names)
    assert not any("Beats" in n for n in names)
    assert not any("Renewed" in n for n in names)
    # AirPods 4 should rank
    assert "Apple AirPods 4" in names

    print("discovery ranking tests OK:", len(out), "rows after pipeline")


if __name__ == "__main__":
    main()
