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


def test_named_product_queries_parse_as_named_products():
    instant_pot = parse_product_spec("instant pot duo 7-in-1")
    ninja = parse_product_spec("ninja air fryer")
    standing_desk = parse_product_spec("standing desk")
    gpu = parse_product_spec("rtx 4070")

    assert instant_pot.query_type == "named_product"
    assert instant_pot.family == "pressure_cooker"
    assert instant_pot.brand == "instant pot"
    assert "duo" in instant_pot.required_tokens
    assert "7in1" in instant_pot.hard_variant_tokens

    assert ninja.query_type == "named_product"
    assert ninja.family == "air_fryer"
    assert ninja.brand == "ninja"

    assert standing_desk.query_type == "category"
    assert standing_desk.family == "standing_desk"

    assert gpu.query_type == "exact_model"
    assert gpu.family == "gpu"
    assert gpu.brand == "nvidia"


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


def test_apple_watch_se_rejects_series_page_even_if_body_mentions_se():
    html = """
    <html>
      <head>
        <meta property="og:title" content="Apple Watch Series 11 [GPS 42mm] Smartwatch with Jet Black Aluminum Case with Black Sport Band - S/M" />
        <meta name="description" content="Apple Watch Series 11 with health and fitness tracking." />
      </head>
      <body>
        <h1>Apple Watch Series 11 [GPS 42mm] Smartwatch with Jet Black Aluminum Case with Black Sport Band - S/M</h1>
        <div>AppleCare+ for Apple Watch SE - monthly</div>
        <div>AppleCare+ for Apple Watch SE 3rd gen - 2 years</div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    spec = parse_product_spec("apple watch se")
    fingerprint = fingerprint_listing_document(
        "https://www.amazon.com/Apple-Watch-Series-11/dp/series11",
        soup,
        current_price=299.0,
        family_hint=spec.family,
    )
    result = verify_listing(spec, fingerprint)
    assert result.status == "rejected"
    assert result.reason == "different_model"


def test_family_detection_prefers_title_over_navigation_noise():
    html = """
    <html>
      <head>
        <meta property="og:title" content="Instant Pot Duo 7-in-1 Electric Pressure Cooker" />
      </head>
      <body>
        <h1>Instant Pot Duo 7-in-1 Electric Pressure Cooker</h1>
        <nav>Movies & TV, TV & Video, Prime Video, Books</nav>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    fingerprint = fingerprint_listing_document(
        "https://www.amazon.com/Instant-Pot-Programmable-Pressure-Steamer/dp/B01B1VC13K",
        soup,
        current_price=99.99,
        family_hint=None,
    )
    assert fingerprint.family == "pressure_cooker"


def test_named_product_pressure_cooker_verifies_and_rejects_wrong_variant():
    exact_html = """
    <html>
      <head>
        <meta property="og:title" content="Instant Pot Duo 7-in-1 Electric Pressure Cooker, 6 Quart" />
      </head>
      <body>
        <h1>Instant Pot Duo 7-in-1 Electric Pressure Cooker, 6 Quart</h1>
      </body>
    </html>
    """
    wrong_html = """
    <html>
      <head>
        <meta property="og:title" content="Instant Pot Duo Plus 9-in-1 Electric Pressure Cooker" />
      </head>
      <body>
        <h1>Instant Pot Duo Plus 9-in-1 Electric Pressure Cooker</h1>
      </body>
    </html>
    """
    accessory_html = """
    <html>
      <head>
        <meta property="og:title" content="Instant Pot Replacement Lid for Duo Models" />
      </head>
      <body>
        <h1>Instant Pot Replacement Lid for Duo Models</h1>
      </body>
    </html>
    """

    spec = parse_product_spec("instant pot duo 7-in-1")

    exact_fp = fingerprint_listing_document(
        "https://example.com/instant-pot-duo",
        BeautifulSoup(exact_html, "html.parser"),
        current_price=89.99,
        family_hint=spec.family,
    )
    exact_result = verify_listing(spec, exact_fp)
    assert exact_result.status == "verified"
    assert exact_result.match_label == "verified_named"

    wrong_fp = fingerprint_listing_document(
        "https://example.com/instant-pot-duo-plus",
        BeautifulSoup(wrong_html, "html.parser"),
        current_price=119.99,
        family_hint=spec.family,
    )
    wrong_result = verify_listing(spec, wrong_fp)
    assert wrong_result.status == "rejected"
    assert wrong_result.reason == "hard_variant_conflict"

    accessory_fp = fingerprint_listing_document(
        "https://example.com/instant-pot-lid",
        BeautifulSoup(accessory_html, "html.parser"),
        current_price=24.99,
        family_hint=spec.family,
    )
    accessory_result = verify_listing(spec, accessory_fp)
    assert accessory_result.status == "rejected"
    assert accessory_result.reason in {"compatible_or_wrong_type", "accessory_or_bundle"}


def test_gpu_partner_brand_verifies_but_wrong_models_and_accessories_do_not():
    spec = parse_product_spec("rtx 4070")

    exact_html = """
    <html><head><meta property="og:title" content="ASUS Dual GeForce RTX 4070 12GB Graphics Card" /></head>
    <body><h1>ASUS Dual GeForce RTX 4070 12GB Graphics Card</h1></body></html>
    """
    super_html = """
    <html><head><meta property="og:title" content="MSI GeForce RTX 4070 Super 12GB Graphics Card" /></head>
    <body><h1>MSI GeForce RTX 4070 Super 12GB Graphics Card</h1></body></html>
    """
    wrong_gen_html = """
    <html><head><meta property="og:title" content="Gigabyte GeForce RTX 5070 Graphics Card" /></head>
    <body><h1>Gigabyte GeForce RTX 5070 Graphics Card</h1></body></html>
    """
    accessory_html = """
    <html><head><meta property="og:title" content="PCIe GPU Riser Cable Bracket for RTX Builds" /></head>
    <body><h1>PCIe GPU Riser Cable Bracket for RTX Builds</h1></body></html>
    """

    exact_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/asus-rtx-4070",
            BeautifulSoup(exact_html, "html.parser"),
            current_price=549.99,
            family_hint=spec.family,
        ),
    )
    assert exact_result.status == "verified"
    assert exact_result.match_label == "verified_exact"

    super_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/msi-rtx-4070-super",
            BeautifulSoup(super_html, "html.parser"),
            current_price=619.99,
            family_hint=spec.family,
        ),
    )
    assert super_result.status == "rejected"
    assert super_result.reason == "different_model"

    wrong_gen_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/gigabyte-rtx-5070",
            BeautifulSoup(wrong_gen_html, "html.parser"),
            current_price=699.99,
            family_hint=spec.family,
        ),
    )
    assert wrong_gen_result.status == "rejected"
    assert wrong_gen_result.reason == "different_model"

    accessory_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/gpu-riser",
            BeautifulSoup(accessory_html, "html.parser"),
            current_price=29.99,
            family_hint=spec.family,
        ),
    )
    assert accessory_result.status == "rejected"
    assert accessory_result.reason in {"compatible_or_wrong_type", "accessory_or_bundle"}


def test_category_query_labels_primary_products_without_auto_verifying_identity():
    desk_html = """
    <html><head><meta property="og:title" content="FlexiSpot EN1 Electric Standing Desk 48 x 24" /></head>
    <body><h1>FlexiSpot EN1 Electric Standing Desk 48 x 24</h1></body></html>
    """
    converter_html = """
    <html><head><meta property="og:title" content="Standing Desk Converter 36 x 22" /></head>
    <body><h1>Standing Desk Converter 36 x 22</h1></body></html>
    """
    spec = parse_product_spec("standing desk")

    desk_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/flexispot-en1",
            BeautifulSoup(desk_html, "html.parser"),
            current_price=299.99,
            family_hint=spec.family,
        ),
    )
    assert desk_result.status == "ambiguous"
    assert desk_result.match_label == "category_primary"

    converter_result = verify_listing(
        spec,
        fingerprint_listing_document(
            "https://example.com/standing-desk-converter",
            BeautifulSoup(converter_html, "html.parser"),
            current_price=149.99,
            family_hint=spec.family,
        ),
    )
    assert converter_result.status == "rejected"


def test_extract_price_prefers_real_product_price_over_warranty_offer():
    html = """
    <html>
      <body>
        <div id="attach-warranty-card-price">$39.00</div>
        <div id="attach-warranty-option-price-eu-enhanced-1">$1.99/month</div>
        <div id="corePrice_feature_div">
          <span class="a-offscreen">$199.00</span>
        </div>
        <div id="apex_offerDisplay_desktop">
          <span class="a-offscreen">$199.00</span>
        </div>
        <div id="tp_price_block_total_price_ww">
          <span class="a-offscreen">$199.00</span>
        </div>
        <div id="corePriceDisplay_desktop_feature_div">
          <span class="a-offscreen">$249.00</span>
        </div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    assert extract_price_from_soup(soup, price_hint=199.0) == 199.0


def test_extract_price_prefers_matching_amazon_price_hint():
    html = """
    <html>
      <body>
        <div id="attach-warranty-card-price">$51.99</div>
        <div id="attach-warranty-option-price-eu-enhanced-1">$16.99/month</div>
        <div id="corePrice_feature_div">
          <span class="a-offscreen">$328.00</span>
          <span class="a-offscreen">$199.99</span>
        </div>
        <div id="tp_price_block_total_price_ww">
          <span class="a-offscreen">$328.00</span>
        </div>
        <div id="corePriceDisplay_desktop_feature_div">
          <span class="a-offscreen">$349.99</span>
        </div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    assert extract_price_from_soup(soup, price_hint=328.0) == 328.0
