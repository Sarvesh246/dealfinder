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
