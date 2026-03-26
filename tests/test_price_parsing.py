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
