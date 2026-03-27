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


def test_target_original_price_reads_comparison_price_block():
    soup = BeautifulSoup(
        """
        <div data-test="product-details">
            <a data-test="@web/ProductCard/title" href="/p/ap2022-true-wireless-bluetooth-headphones/-/A-85978609">
                Apple AirPods Pro 3 Wireless Earbuds with Active Noise Cancellation
            </a>
            <span data-test="current-price"><span>$199.99</span></span>
            <div data-test="comparison-price">reg <span>$249.99</span></div>
            <div data-test="urgency-message">Sale</div>
        </div>
        """,
        "html.parser",
    )
    item = soup.select_one('[data-test="product-details"]')

    assert scraper._target_current_price(item) == 199.99
    assert scraper._target_listing_original_price(item) == 249.99


def test_target_original_price_picks_nearest_higher_value_from_range():
    soup = BeautifulSoup(
        """
        <div data-test="product-details">
            <a data-test="@web/ProductCard/title" href="/p/apple-airpods-4/-/A-93606140">
                Apple AirPods 4 Wireless Earbuds
            </a>
            <span data-test="current-price">$99.99 - $149.99</span>
            <div data-test="comparison-price">reg $129.99 - $179.99</div>
        </div>
        """,
        "html.parser",
    )
    item = soup.select_one('[data-test="product-details"]')

    assert scraper._target_current_price(item) == 99.99
    assert scraper._target_listing_original_price(item) == 129.99


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


def test_extract_bestbuy_multi_supports_product_list_item_tiles():
    soup = BeautifulSoup(
        """
        <ul class="product-grid-view-container p-0">
            <li class="product-list-item grid-view" data-product-id="6376563" data-testid="6376563">
                <div class="sku-block">
                    <a class="product-list-item-link" href="https://www.bestbuy.com/product/apple-airpods-pro-3-white/JJGCQLYK5F">
                        <div class="product-title">
                            Apple - AirPods Pro 3, Wireless Active Noise Cancelling Earbuds - White
                        </div>
                    </a>
                    <div class="pricing-copy">
                        <span>$199.99</span>
                        <span>Save $50</span>
                        <span>Comp. Value: $249.99</span>
                    </div>
                </div>
            </li>
        </ul>
        """,
        "html.parser",
    )

    rows = scraper._extract_bestbuy_multi(soup, max_results=5)

    assert len(rows) == 1
    assert rows[0]["product_name"].startswith("Apple - AirPods Pro 3")
    assert rows[0]["current_price"] == 199.99
    assert rows[0]["original_price"] == 249.99


def test_bestbuy_product_listing_url_accepts_modern_product_paths():
    assert scraper._bestbuy_is_product_listing_url(
        "https://www.bestbuy.com/product/apple-airpods-pro-3-white/JJGCQLYK5F"
    )
    assert scraper._bestbuy_is_product_listing_url("/product/apple-airpods-pro-3-white/JJGCQLYK5F")


def test_bestbuy_canonicalize_listing_url_preserves_site_skuid_and_strips_tracking():
    assert (
        scraper.canonicalize_listing_url(
            "https://www.bestbuy.com/site/example-product.p?skuId=1234567&utm_source=test#frag"
        )
        == "https://www.bestbuy.com/site/example-product.p?skuId=1234567"
    )


def test_bestbuy_canonicalize_listing_url_enriches_modern_product_path_with_sku_hint():
    soup = BeautifulSoup(
        """
        <li class="product-list-item" data-product-id="6376563" data-testid="6376563">
            <a class="product-list-item-link" href="https://www.bestbuy.com/product/apple-airpods-pro-3-white/JJGCQLYK5F">
                Apple - AirPods Pro 3, Wireless Active Noise Cancelling Earbuds - White
            </a>
            <div class="pricing-copy"><span>$199.99</span></div>
        </li>
        """,
        "html.parser",
    )

    tile = soup.select_one("li")
    link = soup.select_one("a")
    url = scraper._bestbuy_canonicalize_extracted_url(link["href"], tile, link)

    assert url == "https://www.bestbuy.com/product/apple-airpods-pro-3-white/JJGCQLYK5F/sku/6376563"


def test_officedepot_canonicalize_listing_url_strips_session_and_query():
    assert (
        scraper.canonicalize_listing_url(
            "https://www.officedepot.com/a/products/4371810/Move-40-Series-by-Bush-Business/;jsessionid=ABC123?pr=#Reviews"
        )
        == "https://www.officedepot.com/a/products/4371810/Move-40-Series-by-Bush-Business"
    )


def test_extract_officedepot_multi_reads_title_sale_and_original_price():
    soup = BeautifulSoup(
        """
        <div class="od-product-card" data-product-id="4371810" pagetype="search">
            <span name="skuTitleGAData" data-value="Move 40 Series by Bush Business Furniture Electric Height-Adjustable Standing Desk, 72&quot; x 30&quot;"></span>
            <div class="od-product-card-region od-product-card-region-body">
                <a class="od-product-card-image" href="/a/products/4371810/Move-40-Series-by-Bush-Business/" title='Move 40 Series by Bush Business Furniture Electric Height-Adjustable Standing Desk, 72" x 30"'></a>
                <a href="/a/products/4371810/Move-40-Series-by-Bush-Business/" title='Move 40 Series by Bush Business Furniture Electric Height-Adjustable Standing Desk, 72" x 30"'>
                    Move 40 Series by Bush Business Furniture Electric Height-Adjustable Standing...
                </a>
                <span class="od-graphql-price-big-price">$584.99</span>
                <span class="od-graphql-price-little-price">$699.99</span>
            </div>
        </div>
        """,
        "html.parser",
    )

    rows = scraper._extract_officedepot_multi(soup, max_results=5)

    assert len(rows) == 1
    assert rows[0]["product_url"] == "https://www.officedepot.com/a/products/4371810/Move-40-Series-by-Bush-Business"
    assert rows[0]["current_price"] == 584.99
    assert rows[0]["original_price"] == 699.99
    assert "Standing Desk" in rows[0]["product_name"]


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
