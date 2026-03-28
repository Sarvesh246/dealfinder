from bs4 import BeautifulSoup

from scraper import revalidate_product_source


def test_revalidate_source_rediscovery_path(monkeypatch):
    source_row = {
        "id": 10,
        "product_id": 1,
        "source_id": 1,
        "product_name": "WH-1000XM4",
        "raw_query": "WH-1000XM4",
        "target_price": 200.0,
        "discovered_url": "https://example.com/old",
        "source_name": "Amazon",
        "domain": "amazon.com",
        "search_url_template": "https://www.amazon.com/s?k={query}",
    }

    class _Rejected:
        status = "rejected"
        health_state = "healthy"

    monkeypatch.setattr("scraper.verify_candidate_listing", lambda *args, **kwargs: _Rejected())
    monkeypatch.setattr(
        "scraper.discover_product_matches",
        lambda *args, **kwargs: {
            "verified": [
                {
                    "url": "https://example.com/new",
                    "price": 189.99,
                    "name_found": "Sony WH-1000XM4 Headphones",
                    "verification_reason": "exact_model_verified",
                    "health_state": "healthy",
                    "match_label": "verified_exact",
                    "matched_product_name": "Sony WH-1000XM4 Headphones",
                }
            ],
            "ambiguous": [],
        },
    )

    result = revalidate_product_source(source_row)
    assert result["status"] == "rediscovered"
    assert result["verified"][0]["url"] == "https://example.com/new"


def test_revalidate_source_returns_blocked_fetch_status(monkeypatch):
    import scraper._legacy_impl as legacy

    source_row = {
        "id": 10,
        "product_id": 1,
        "source_id": 2,
        "product_name": "Apple AirPods Pro 3",
        "raw_query": "airpods pro 3",
        "target_price": 199.0,
        "discovered_url": "https://www.bestbuy.com/site/airpods/123.p",
        "source_name": "Best Buy",
        "domain": "bestbuy.com",
        "search_url_template": "https://www.bestbuy.com/site/searchpage.jsp?st={query}",
    }

    monkeypatch.setattr("scraper.verify_candidate_listing", lambda *args, **kwargs: None)
    monkeypatch.setattr(legacy, "_listing_fetch_status", lambda *args, **kwargs: {
        "outcome": "blocked",
        "method": "provider_html",
        "reason": "bot_wall",
    })

    result = revalidate_product_source(source_row)

    assert result["status"] == "blocked"
    assert result["fetch_status"]["method"] == "provider_html"
    assert result["fetch_status"]["reason"] == "bot_wall"


def test_search_probe_stage_uses_provider_when_protected_domain_is_in_cooldown(monkeypatch):
    import scraper._legacy_impl as legacy

    monkeypatch.setattr(legacy, "should_bypass_direct", lambda domain: True)
    monkeypatch.setattr(legacy, "provider_enabled_for", lambda domain: True)
    monkeypatch.setattr(
        legacy,
        "fetch_via_provider",
        lambda *args, **kwargs: (BeautifulSoup("<html><body><div>ok</div></body></html>", "html.parser"), "provider_html", None),
    )

    soup, fetch_method, failure_reason = legacy._fetch_search_probe_stage(
        "https://www.walmart.com/search?q=standing+desk",
        "walmart.com",
        "probe_html",
        mode="discover_deals",
        search_query="standing desk",
        context=legacy.SearchExecutionContext(),
    )

    assert soup is not None
    assert fetch_method == "provider_html"
    assert failure_reason is None


def test_search_results_probe_ladder_downgrades_extractor_failure(monkeypatch):
    import scraper._legacy_impl as legacy

    soup = BeautifulSoup("<html><body><li class='sku-item'>row</li></body></html>", "html.parser")
    source = {
        "domain": "bestbuy.com",
        "search_url_template": "https://www.bestbuy.com/site/searchpage.jsp?st={query}",
    }
    context = legacy.SearchExecutionContext()

    monkeypatch.setattr(
        legacy,
        "_fetch_search_probe_stage",
        lambda *args, **kwargs: (soup, "requests", None),
    )

    def broken_preview(*args, **kwargs):
        raise RuntimeError("preview failed")

    monkeypatch.setattr(legacy, "_preview_probe_rows", broken_preview)

    ladder_soup, fetch_method, rows = legacy._search_results_probe_ladder(
        "https://www.bestbuy.com/site/searchpage.jsp?st=airpods+pro",
        source,
        mode="discover_deals",
        search_query="airpods pro",
        max_results=10,
        context=context,
    )

    assert ladder_soup is None
    assert fetch_method == "fetch_failed"
    assert rows == []
    assert context.get_probe_outcome(
        "bestbuy.com",
        "https://www.bestbuy.com/site/searchpage.jsp?st=airpods+pro",
        "probe_html",
    )["failure_reason"] == "extractor_error"
