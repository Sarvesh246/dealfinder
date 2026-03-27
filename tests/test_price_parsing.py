from bs4 import BeautifulSoup

import scraper


def test_clean_price_handles_concatenated_sale_and_regular_prices():
    assert scraper.clean_price("$40.00$79.99") == 40.0
    assert scraper.clean_price("69.9979.99") == 69.99
    assert scraper.clean_price("Now $69.99 Was $79.99") == 69.99


def test_target_current_price_prefers_current_price_token():
    soup = BeautifulSoup(
        """
        <div data-test="product-details">
            <a data-test="product-title" href="/p/example-product">Example Product</a>
            <span data-test="current-price">$40.00$79.99</span>
            <span data-test="product-regular-price">Reg $79.99</span>
        </div>
        """,
        "html.parser",
    )
    item = soup.select_one('[data-test="product-details"]')

    assert scraper._target_current_price(item) == 40.0


def test_extract_target_all_uses_fixed_current_price():
    soup = BeautifulSoup(
        """
        <div data-test="product-details">
            <a data-test="product-title" href="/p/example-product">Example Product</a>
            <span data-test="current-price">$69.99$79.99</span>
            <span data-test="product-regular-price">Reg $79.99</span>
        </div>
        """,
        "html.parser",
    )

    rows = scraper._extract_target_all(
        soup,
        apply_quality_pipeline=False,
    )

    assert len(rows) == 1
    assert rows[0]["price"] == 69.99
    assert rows[0]["original_price"] == 79.99


def test_amazon_listing_price_ignores_monthly_price_copy():
    soup = BeautifulSoup(
        """
        <div data-component-type="s-search-result">
            <h2>
                <a class="a-link-normal" href="/dp/B0001">
                    <span class="a-text-normal">Example Router</span>
                </a>
            </h2>
            <div class="offer-block">
                <span class="a-price priceToPay">
                    <span class="a-offscreen">$63.99</span>
                </span>
            </div>
            <div class="monthly-offer">
                <span>or $8.99 / month</span>
                <span class="a-price">
                    <span class="a-offscreen">$8.99</span>
                </span>
            </div>
        </div>
        """,
        "html.parser",
    )
    item = soup.select_one('[data-component-type="s-search-result"]')

    assert scraper._amazon_listing_price(item) == 63.99


def test_extract_amazon_multi_prefers_primary_offer_price_over_plan_price():
    soup = BeautifulSoup(
        """
        <div data-component-type="s-search-result">
            <h2>
                <a class="a-link-normal" href="/dp/B0002">
                    <span class="a-text-normal">Example Headphones</span>
                </a>
            </h2>
            <div class="plan-copy">
                <span>2-year protection plan</span>
                <span class="a-price"><span class="a-offscreen">$14.99</span></span>
            </div>
            <div class="offer-block">
                <span class="a-price priceToPay">
                    <span class="a-offscreen">$199.99</span>
                </span>
            </div>
        </div>
        """,
        "html.parser",
    )

    rows = scraper._extract_amazon_multi(soup, max_results=5)

    assert len(rows) == 1
    assert rows[0]["current_price"] == 199.99


def test_extract_price_from_soup_ignores_lower_renewed_offer_for_new_tracker():
    soup = BeautifulSoup(
        """
        <div id="corePrice_feature_div">
            <span class="a-price priceToPay">
                <span class="a-offscreen">$199.99</span>
            </span>
        </div>
        <div class="renewed-offer">
            <span>Amazon Renewed</span>
            <span class="a-price">
                <span class="a-offscreen">$189.99</span>
            </span>
        </div>
        """,
        "html.parser",
    )

    price = scraper.extract_price_from_soup(
        soup,
        price_hint=199.99,
        condition_hint_text="Apple AirPods Pro 3",
    )

    assert price == 199.99


def test_extract_price_from_soup_allows_renewed_price_when_explicitly_requested():
    soup = BeautifulSoup(
        """
        <div class="renewed-offer">
            <span>Amazon Renewed</span>
            <span class="a-price priceToPay">
                <span class="a-offscreen">$189.99</span>
            </span>
        </div>
        """,
        "html.parser",
    )

    price = scraper.extract_price_from_soup(
        soup,
        price_hint=189.99,
        condition_hint_text="Apple AirPods Pro 3 renewed",
    )

    assert price == 189.99


def test_extract_price_from_soup_ignores_used_accordion_price_on_amazon_pdp():
    soup = BeautifulSoup(
        """
        <div id="corePrice_feature_div">
            <div class="a-spacing-top-mini apex-core-price-identifier">
                <span class="a-price a-text-normal aok-align-center reinventPriceAccordionT2 apex-pricetopay-value" data-a-color="base" data-a-size="l">
                    <span class="a-offscreen">$199.00</span>
                </span>
            </div>
        </div>
        <div data-csa-c-slot-id="usedAccordionRow" data-csa-c-buying-option-type="USED">
            <div class="a-spacing-top-mini apex-core-price-identifier">
                <span class="a-price a-text-normal aok-align-center reinventPriceAccordionT2 apex-pricetopay-value" data-a-color="base" data-a-size="l">
                    <span class="a-offscreen">$187.06</span>
                </span>
            </div>
        </div>
        """,
        "html.parser",
    )

    price = scraper.extract_price_from_soup(
        soup,
        price_hint=189.05,
        condition_hint_text="Apple AirPods Pro 3",
    )

    assert price == 199.00
