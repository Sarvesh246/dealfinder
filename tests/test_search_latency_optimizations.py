from bs4 import BeautifulSoup

import hf_utils
import scraper
from product_verifier import ListingFingerprint, VerificationResult, parse_product_spec


def _verified_exact(url: str, title: str, price: float) -> VerificationResult:
    fingerprint = ListingFingerprint(
        url=url,
        domain="example.com",
        title=title,
        brand="apple",
        family="airpods",
        model_tokens=("AIRPODSPRO3",),
        normalized_model_tokens=("airpodspro3",),
        variant_tokens=(),
        current_price=price,
        accessory_signal=False,
        compatibility_signal=False,
        bundle_signal=False,
        hard_block_signal=False,
        raw_text=title,
    )
    return VerificationResult(
        status="verified",
        reason="exact_model_verified",
        health_state="healthy",
        product_name=title,
        current_price=price,
        brand="apple",
        family="airpods",
        model_token="AIRPODSPRO3",
        match_label="verified_exact",
        fingerprint=fingerprint,
    )


def test_verify_candidate_listing_reuses_context_cache(monkeypatch):
    context = scraper.SearchExecutionContext()
    spec = parse_product_spec("airpods pro 3")
    source = {"domain": "amazon.com"}
    candidate = {
        "product_url": "https://example.com/airpods-pro-3",
        "product_name": "Apple AirPods Pro 3",
        "current_price": 199.0,
    }
    verification = _verified_exact(candidate["product_url"], candidate["product_name"], 199.0)
    calls = {"fetch": 0}
    soup = BeautifulSoup("<html><h1>Apple AirPods Pro 3</h1><div>$199.00</div></html>", "html.parser")

    def fake_fetch(url, source_domain, *, context=None):
        calls["fetch"] += 1
        return soup, "requests"

    monkeypatch.setattr(scraper, "_fetch_listing_soup", fake_fetch)
    monkeypatch.setattr(scraper, "extract_price_from_soup", lambda soup, price_hint=None: 199.0)
    monkeypatch.setattr(scraper, "fingerprint_listing_document", lambda *args, **kwargs: verification.fingerprint)
    monkeypatch.setattr(scraper, "verify_listing", lambda spec, fingerprint: verification)

    first = scraper.verify_candidate_listing(spec, source, candidate, context=context)
    second = scraper.verify_candidate_listing(spec, source, candidate, context=context)

    assert first == second
    assert calls["fetch"] == 1


def test_search_results_circuit_breaker_stops_repeat_fetch(monkeypatch):
    context = scraper.SearchExecutionContext()
    calls = {"selenium": 0}

    def fake_fetch(url, *, domain="", debug_meta=None, failure_sink=None):
        calls["selenium"] += 1
        if failure_sink is not None:
            failure_sink["reason"] = "bot_wall"
        return None

    monkeypatch.setattr(scraper, "_fetch_soup_selenium_pooled", fake_fetch)

    soup, method = scraper._fetch_search_results_soup(
        "https://www.walmart.com/search?q=airpods",
        "walmart.com",
        context=context,
    )
    assert soup is None
    assert method == "fetch_failed"
    assert context.should_skip_domain("walmart.com")

    soup2, method2 = scraper._fetch_search_results_soup(
        "https://www.walmart.com/search?q=airpods+pro+3",
        "walmart.com",
        context=context,
    )
    assert soup2 is None
    assert method2 == "circuit_breaker"
    assert calls["selenium"] == 1


def test_discover_deals_for_queries_stops_after_first_sufficient_alias(monkeypatch):
    source = {"domain": "amazon.com", "search_url_template": "https://www.amazon.com/s?k={query}"}
    seen_queries = []

    def fake_discover(query, source, max_price=None, max_results=50, *, context=None):
        seen_queries.append(query)
        return [
            {
                "product_name": f"Row {i}",
                "current_price": 100.0 + i,
                "original_price": 120.0 + i,
                "product_url": f"https://example.com/{i}",
            }
            for i in range(8)
        ]

    monkeypatch.setattr(scraper, "discover_deals", fake_discover)

    rows = scraper.discover_deals_for_queries(
        ("alias one", "alias two"),
        source,
        context=scraper.SearchExecutionContext(),
    )

    assert len(rows) == 8
    assert seen_queries == ["alias one"]


def test_discover_deals_for_queries_breaks_after_repeated_empty_js_aliases(monkeypatch):
    source = {"domain": "bestbuy.com", "search_url_template": "https://www.bestbuy.com/site/searchpage.jsp?st={query}"}
    seen_queries = []

    def fake_discover(query, source, max_price=None, max_results=50, *, context=None):
        seen_queries.append(query)
        return []

    monkeypatch.setattr(scraper, "discover_deals", fake_discover)

    rows = scraper.discover_deals_for_queries(
        ("alias one", "alias two", "alias three"),
        source,
        context=scraper.SearchExecutionContext(),
    )

    assert rows == []
    assert seen_queries == ["alias one", "alias two"]


def test_discover_product_matches_stops_after_first_verified_alias(monkeypatch):
    source = {"domain": "amazon.com", "search_url_template": "https://www.amazon.com/s?k={query}"}
    searched = []
    verification = _verified_exact("https://example.com/airpods", "Apple AirPods Pro 3", 199.0)

    def fake_search(search_query, source, *, max_results=50, context=None):
        searched.append(search_query)
        return [{
            "product_url": "https://example.com/airpods",
            "product_name": "Apple AirPods Pro 3",
            "current_price": 199.0,
        }]

    monkeypatch.setattr(scraper, "_search_listing_candidates", fake_search)
    monkeypatch.setattr(
        scraper,
        "_verify_candidates_parallel",
        lambda spec, source, candidates, *, max_workers, context=None: [
            (candidates[0], verification)
        ],
    )

    matches = scraper.discover_product_matches(
        "airpods pro 3",
        source,
        context=scraper.SearchExecutionContext(),
    )

    assert len(matches["verified"]) == 1
    assert searched == ["apple airpods pro 3 earbuds"]


def test_hf_relevance_cache_reuses_previous_scores(monkeypatch):
    engine = hf_utils.SmartEngine()

    class FakeClient:
        def __init__(self):
            self.calls = 0

        def sentence_similarity(self, query, other_sentences, model):
            self.calls += 1
            return [0.91, 0.74]

    fake = FakeClient()
    engine._client = fake
    engine._enabled = True
    results = [
        {"product_name": "Apple AirPods Pro 3"},
        {"product_name": "Apple AirPods 4"},
    ]

    first = engine.score_relevance("airpods pro 3", [dict(r) for r in results])
    second = engine.score_relevance("airpods pro 3", [dict(r) for r in results])

    assert fake.calls == 1
    assert [row["relevance_score"] for row in first] == [row["relevance_score"] for row in second]
