from pathlib import Path

from bs4 import BeautifulSoup

from product_identity import parse_query_intent
from product_verifier import (
    fingerprint_listing_document,
    parse_product_spec,
    verify_listing,
)
from scraper import extract_price_from_soup


FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str, url: str):
    html = (FIXTURES / name).read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    spec = parse_product_spec("WH-1000XM4")
    price = extract_price_from_soup(soup)
    fingerprint = fingerprint_listing_document(
        url,
        soup,
        current_price=price,
        family_hint=spec.family,
    )
    return spec, fingerprint


def test_model_only_query_is_exact_model():
    intent = parse_query_intent("WH-1000XM4")
    spec = parse_product_spec("WH-1000XM4")
    assert intent.query_type.value == "exact_model"
    assert spec.family == "headphones"
    assert spec.brand == "sony"
    assert spec.model_token.lower() == "wh-1000xm4"
    assert spec.canonical_query.lower() == "sony wh-1000xm4 headphones"


def test_exact_headphone_page_is_verified():
    spec, fingerprint = _load_fixture("amazon_exact.html", "https://www.amazon.com/dp/exact-xm4")
    result = verify_listing(spec, fingerprint)
    assert result.status == "verified"
    assert result.match_label == "verified_exact"
    assert result.brand == "sony"


def test_wrong_brand_page_is_rejected():
    spec, fingerprint = _load_fixture("amazon_wrong_brand.html", "https://www.amazon.com/dp/wrong-brand")
    result = verify_listing(spec, fingerprint)
    assert result.status == "rejected"
    assert result.reason in {"brand_mismatch", "different_model", "model_missing"}


def test_target_earbuds_are_rejected():
    spec, fingerprint = _load_fixture("target_earbuds.html", "https://www.target.com/p/earbuds")
    result = verify_listing(spec, fingerprint)
    assert result.status == "rejected"
    assert result.reason in {"compatible_or_wrong_type", "family_mismatch", "accessory_or_bundle"}


def test_walmart_wrong_model_is_rejected():
    spec, fingerprint = _load_fixture("walmart_wrong_model.html", "https://www.walmart.com/ip/xm5")
    result = verify_listing(spec, fingerprint)
    assert result.status == "rejected"
    assert result.reason == "different_model"


def test_supported_exact_fixtures_verify():
    fixture_names = [
        ("bestbuy_exact.html", "https://www.bestbuy.com/site/sony-wh1000xm4/123"),
        ("newegg_exact.html", "https://www.newegg.com/p/exact"),
        ("costco_exact.html", "https://www.costco.com/exact.product.123.html"),
    ]
    for file_name, url in fixture_names:
        spec, fingerprint = _load_fixture(file_name, url)
        result = verify_listing(spec, fingerprint)
        assert result.status == "verified", file_name


def test_exact_product_not_rejected_by_generic_ecosystem_copy():
    html = """
    <html>
      <head>
        <meta property="og:title" content="Apple AirPods Pro 3 Wireless Earbuds, USB-C Charging" />
        <meta name="description" content="Effortless setup for iPhone and seamless Apple pairing." />
      </head>
      <body>
        <h1>Apple AirPods Pro 3 Wireless Earbuds, USB-C Charging</h1>
        <p>Effortless setup for iPhone and seamless Apple pairing.</p>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    spec = parse_product_spec("airpods pro 3")
    fingerprint = fingerprint_listing_document(
        "https://www.amazon.com/dp/airpods-pro-3",
        soup,
        current_price=199.0,
        family_hint=spec.family,
    )
    result = verify_listing(spec, fingerprint)
    assert result.status == "verified"
    assert result.reason == "exact_model_verified"
