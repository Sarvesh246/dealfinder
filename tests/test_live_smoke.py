import os

import pytest

from database import DEFAULT_SOURCES
from product_verifier import parse_product_spec
from scraper import _search_listing_candidates


@pytest.mark.live
@pytest.mark.skipif(os.getenv("LIVE_SCRAPER_SMOKE") != "1", reason="live smoke disabled")
@pytest.mark.parametrize(
    "domain",
    ["amazon.com", "bestbuy.com", "newegg.com", "target.com", "costco.com", "walmart.com"],
)
def test_live_candidate_search(domain):
    source = next(s for s in DEFAULT_SOURCES if s["domain"] == domain)
    spec = parse_product_spec("WH-1000XM4")
    rows = _search_listing_candidates(spec.canonical_query, source, max_results=1)
    assert isinstance(rows, list)
