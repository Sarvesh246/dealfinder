from discovery_filters import enrich_result_metadata, passes_eligibility, resolve_family_and_intent
from product_identity import detect_condition
from scraper import clean_listing_title


def test_clean_listing_title_strips_badges_and_prices():
    raw = "Overall pick Apple AirPods Pro 3 $199.00 Was $249.00"
    assert clean_listing_title(raw) == "Apple AirPods Pro 3"


def test_detect_condition_handles_glued_refurbished_prefix():
    assert detect_condition("refurbishedsony wh-1000xm5 wireless headphones") == "refurbished"


def test_new_only_excludes_glued_refurbished_title():
    query = "Sony WH-1000XM5"
    family, accessory_intent = resolve_family_and_intent(query)
    row = {
        "product_name": "refurbishedsony wh-1000xm5 wireless noise cancelling headphones",
        "product_url": "https://example.com/products/refurbishedsony-wh-1000xm5",
    }
    enrich_result_metadata(
        row,
        query,
        family=family,
        accessory_intent=accessory_intent,
    )
    assert row["condition_class"] == "refurbished"
    assert not passes_eligibility(
        row,
        condition_filter="new_only",
        product_filter="primary_only",
        brand_filter="exact",
        family=family,
        accessory_intent=accessory_intent,
        query_for_intent=query,
        min_confidence=0.0,
    )
