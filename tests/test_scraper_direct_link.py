from bs4 import BeautifulSoup

import scraper


def test_revalidate_direct_url_never_rediscovers_other_urls(monkeypatch):
    html = """
    <html><head><meta property="og:title" content="Acme ChefMaster Air Fryer 6qt" /></head>
    <body><h1>Acme ChefMaster Air Fryer 6qt</h1><div>$79.99</div></body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    monkeypatch.setattr(scraper, "_fetch_listing_soup", lambda url, source_domain, **kwargs: (soup, "requests"))
    monkeypatch.setattr(scraper, "extract_price_from_soup", lambda soup, price_hint=None: 79.99)
    monkeypatch.setattr(
        scraper,
        "discover_product_matches",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("direct-url revalidation must not rediscover")
        ),
    )

    ps_row = {
        "product_id": 1,
        "source_id": 11,
        "source_name": "shop.example.net",
        "domain": "shop.example.net",
        "search_url_template": "",
        "discovered_url": "https://shop.example.net/product/air-fryer",
        "product_name": "Acme ChefMaster Air Fryer 6qt",
        "matched_product_name": "Acme ChefMaster Air Fryer 6qt",
        "current_price": 89.99,
        "target_price": None,
        "raw_query": "Acme ChefMaster Air Fryer 6qt",
        "canonical_query": "6qt air fryer",
        "brand": None,
        "family": "air_fryer",
        "model_token": None,
        "variant_tokens": "[]",
        "match_mode": "strict",
        "query_type": "named_product",
        "tracking_mode": "direct_url",
        "source_domain_override": "shop.example.net",
    }

    result = scraper.revalidate_product_source(ps_row)

    assert result["status"] == "verified"
    assert result["verified"]
    assert result["verified"][0]["url"] == "https://shop.example.net/product/air-fryer"


def test_revalidate_direct_url_quarantines_changed_pages(monkeypatch):
    html = """
    <html><head><meta property="og:title" content="Replacement Air Fryer Basket for ChefMaster" /></head>
    <body><h1>Replacement Air Fryer Basket for ChefMaster</h1><div>$14.99</div></body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    monkeypatch.setattr(scraper, "_fetch_listing_soup", lambda url, source_domain, **kwargs: (soup, "requests"))
    monkeypatch.setattr(scraper, "extract_price_from_soup", lambda soup, price_hint=None: 14.99)

    ps_row = {
        "product_id": 1,
        "source_id": 11,
        "source_name": "shop.example.net",
        "domain": "shop.example.net",
        "search_url_template": "",
        "discovered_url": "https://shop.example.net/product/air-fryer",
        "product_name": "Acme ChefMaster Air Fryer 6qt",
        "matched_product_name": "Acme ChefMaster Air Fryer 6qt",
        "current_price": 79.99,
        "target_price": None,
        "raw_query": "Acme ChefMaster Air Fryer 6qt",
        "canonical_query": "6qt air fryer",
        "brand": None,
        "family": "air_fryer",
        "model_token": None,
        "variant_tokens": "[]",
        "match_mode": "strict",
        "query_type": "named_product",
        "tracking_mode": "direct_url",
        "source_domain_override": "shop.example.net",
    }

    result = scraper.revalidate_product_source(ps_row)

    assert result["status"] == "quarantined"
    assert result["verified"] == []
    assert result["ambiguous"] == []


def test_inspect_direct_link_uses_clean_title_and_primary_price_hint(monkeypatch):
    html = """
    <html>
      <head>
        <title>Amazon.com: TP-Link Dual-Band AX3000 Wi-Fi 6 Router Archer AX55 : Electronics</title>
        <meta property="og:title" content="Amazon.com: TP-Link Dual-Band AX3000 Wi-Fi 6 Router Archer AX55 : Electronics" />
      </head>
      <body>
        <h1>Amazon.com: TP-Link Dual-Band AX3000 Wi-Fi 6 Router Archer AX55 : Electronics</h1>
        <div id="corePrice_feature_div"><span class="a-offscreen">$65.98</span></div>
        <div id="attach-warranty-card-price">$5.99</div>
        <div>keyboard shortcuts</div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    monkeypatch.setattr(scraper, "_fetch_listing_soup", lambda url, source_domain, **kwargs: (soup, "requests"))

    result = scraper.inspect_direct_link(
        "https://www.amazon.com/TP-Link-WiFi-AX3000-Smart-Router/dp/B09G5W9R6R",
        source={"domain": "amazon.com"},
    )

    assert result["ok"] is True
    assert result["title"] == "TP-Link Dual-Band AX3000 Wi-Fi 6 Router Archer AX55"
    assert result["price"] == 65.98
    assert result["verification"].status == "verified"
    assert result["verification"].match_label == "verified_exact"
