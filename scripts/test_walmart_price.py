"""Verify Walmart price extraction with simulated DOM structures."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bs4 import BeautifulSoup
from scraper import _walmart_extract_price

cases = [
    (
        "Split 168+00 (was causing 16800 bug)",
        '<div data-automation-id="product-price">'
        '<span class="price-characteristic" aria-hidden="true">168</span>'
        '<span class="price-mantissa" aria-hidden="true">00</span>'
        '</div>',
        168.0,
    ),
    (
        "itemprop content attr",
        '<div data-automation-id="product-price">'
        '<span itemprop="price" content="168.00">168.00</span>'
        '</div>',
        168.0,
    ),
    (
        "$X.XX dollar-sign fallback",
        '<div data-automation-id="product-price">Some text $168.00 more text</div>',
        168.0,
    ),
    (
        "Budget item cents only in mantissa",
        '<div data-automation-id="product-price">'
        '<span class="price-characteristic">7</span>'
        '<span class="price-mantissa">99</span>'
        '</div>',
        7.99,
    ),
    (
        "Three-digit price",
        '<div data-automation-id="product-price">'
        '<span class="price-characteristic">249</span>'
        '<span class="price-mantissa">00</span>'
        '</div>',
        249.0,
    ),
]

all_pass = True
for label, html, expected in cases:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("div")
    result = _walmart_extract_price(root)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        all_pass = False
    print(f"[{status}] {label}: got={result} expected={expected}")

sys.exit(0 if all_pass else 1)
