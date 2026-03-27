import time

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
    calls = {"stages": []}

    def fake_probe(search_url, domain, stage, *, mode, search_query, context=None):
        calls["stages"].append(stage)
        return None, "fetch_failed", "bot_wall"

    monkeypatch.setattr(scraper, "_fetch_search_probe_stage", fake_probe)

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
    assert calls["stages"] == ["probe_html", "probe_light_js"]


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


def test_hf_query_enhancement_cache_reuses_previous_response(monkeypatch):
    engine = hf_utils.SmartEngine()

    class FakeMessage:
        content = "apple watch se"

    class FakeChoice:
        message = FakeMessage()

    class FakeResponse:
        choices = [FakeChoice()]

    class FakeClient:
        def __init__(self):
            self.calls = 0

        def chat_completion(self, **kwargs):
            self.calls += 1
            return FakeResponse()

    fake = FakeClient()
    engine._client = fake
    engine._enabled = True

    first = engine.enhance_query("  apple watch se  ")
    second = engine.enhance_query("  apple watch se  ")

    assert first == "apple watch se"
    assert second == "apple watch se"
    assert fake.calls == 1


def test_bestbuy_probe_html_success_skips_selenium_stages(monkeypatch):
    soup = BeautifulSoup("<html><body>bestbuy html</body></html>", "html.parser")
    source = {"domain": "bestbuy.com"}
    stages = []
    adapter = scraper.SourceAdapter(
        domain="bestbuy.com",
        search_extractor=lambda _soup, max_results=50: [
            {
                "product_name": f"Row {i}",
                "current_price": 199.0 + i,
                "original_price": 249.0 + i,
                "product_url": f"https://www.bestbuy.com/site/item-{i}",
            }
            for i in range(8)
        ],
        selenium_preferred=True,
        probe_stages=("probe_html", "probe_light_js", "full_selenium"),
        discovery_threshold=8,
    )

    monkeypatch.setitem(scraper._SOURCE_ADAPTERS, "bestbuy.com", adapter)

    def fake_stage(search_url, domain, stage, *, mode, search_query, context=None):
        stages.append(stage)
        return soup, "requests" if stage == "probe_html" else "selenium", None

    monkeypatch.setattr(scraper, "_fetch_search_probe_stage", fake_stage)

    picked_soup, method, rows = scraper._search_results_probe_ladder(
        "https://www.bestbuy.com/site/searchpage.jsp?st=airpods",
        source,
        mode="discover_deals",
        search_query="airpods pro 3",
        max_results=8,
        max_price=220.0,
        context=scraper.SearchExecutionContext(),
    )

    assert picked_soup == soup
    assert method == "requests"
    assert len(rows) == 8
    assert stages == ["probe_html"]


def test_walmart_probe_ladder_escalates_until_threshold_met(monkeypatch):
    source = {"domain": "walmart.com"}
    soups = {
        "probe_html": BeautifulSoup("<html><body>html</body></html>", "html.parser"),
        "probe_light_js": BeautifulSoup("<html><body>light</body></html>", "html.parser"),
    }
    adapter = scraper.SourceAdapter(
        domain="walmart.com",
        search_extractor=lambda soup, max_results=50: (
            [{
                "product_name": "Only one row",
                "current_price": 100.0,
                "original_price": 150.0,
                "product_url": "https://www.walmart.com/ip/1",
            }]
            if soup is soups["probe_html"]
            else [
                {
                    "product_name": f"Row {i}",
                    "current_price": 100.0 + i,
                    "original_price": 150.0 + i,
                    "product_url": f"https://www.walmart.com/ip/{i}",
                    }
                for i in range(8)
            ]
        ),
        selenium_preferred=True,
        probe_stages=("probe_html", "probe_light_js", "full_selenium"),
        discovery_threshold=6,
    )
    monkeypatch.setitem(scraper._SOURCE_ADAPTERS, "walmart.com", adapter)
    stages = []

    def fake_stage(search_url, domain, stage, *, mode, search_query, context=None):
        stages.append(stage)
        if stage == "full_selenium":
            raise AssertionError("full_selenium should not run once light JS is sufficient")
        return soups[stage], "requests" if stage == "probe_html" else "selenium_light", None

    monkeypatch.setattr(scraper, "_fetch_search_probe_stage", fake_stage)

    picked_soup, method, rows = scraper._search_results_probe_ladder(
        "https://www.walmart.com/search?q=standing+desk",
        source,
        mode="discover_deals",
        search_query="standing desk",
        max_results=8,
        max_price=400.0,
        context=scraper.SearchExecutionContext(),
    )

    assert picked_soup == soups["probe_light_js"]
    assert method == "selenium_light"
    assert len(rows) == 8
    assert stages == ["probe_html", "probe_light_js"]


def test_browser_pool_recycles_driver_by_age(monkeypatch):
    pool = scraper.BrowserPool()
    first_driver = object()
    second_driver = object()
    created = {"count": 0}

    def fake_new_driver():
        created["count"] += 1
        return first_driver if created["count"] == 1 else second_driver

    monkeypatch.setattr(pool, "_new_driver", fake_new_driver)

    handle = pool._get_handle("bestbuy.com")
    with handle.lock:
        driver_one = pool._ensure_driver_locked(handle)
        handle.created_at = time.monotonic() - (scraper.SELENIUM_DRIVER_MAX_AGE_SECONDS + 5)
        driver_two = pool._ensure_driver_locked(handle)

    assert driver_one is first_driver
    assert driver_two is second_driver
