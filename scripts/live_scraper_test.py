"""Live scraper diagnostics: discover_deals + discover_product per source, with stats.

Run from project root:  python scripts/live_scraper_test.py
"""
from __future__ import annotations

import logging
import os
import sys

os.environ.setdefault("MAX_RESULTS_PER_SOURCE", "20")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(message)s")

import scraper  # noqa: E402


SOURCES = {
    "amazon.com": {
        "domain": "amazon.com",
        "search_url_template": "https://www.amazon.com/s?k={query}",
    },
    "walmart.com": {
        "domain": "walmart.com",
        "search_url_template": "https://www.walmart.com/search?q={query}",
    },
    "bestbuy.com": {
        "domain": "bestbuy.com",
        "search_url_template": "https://www.bestbuy.com/site/searchpage.jsp?st={query}",
    },
    "target.com": {
        "domain": "target.com",
        "search_url_template": "https://www.target.com/s?searchTerm={query}",
    },
    "costco.com": {
        "domain": "costco.com",
        "search_url_template":
            "https://www.costco.com/CatalogSearch?dept=All&keyword={query}",
    },
    "homedepot.com": {
        "domain": "homedepot.com",
        "search_url_template": "https://www.homedepot.com/s/{query}",
    },
    "lowes.com": {
        "domain": "lowes.com",
        "search_url_template":
            "https://www.lowes.com/search?searchTerm={query}",
    },
    "ebay.com": {
        "domain": "ebay.com",
        "search_url_template":
            "https://www.ebay.com/sch/i.html?_nkw={query}&LH_BIN=1",
    },
    "bhphotovideo.com": {
        "domain": "bhphotovideo.com",
        "search_url_template": "https://www.bhphotovideo.com/c/search?Ntt={query}",
    },
}

# Phase-1 general retailers first (Costco → Home Depot → Lowe’s → eBay), then others.
SOURCE_ORDER = (
    "amazon.com",
    "walmart.com",
    "bestbuy.com",
    "target.com",
    "costco.com",
    "homedepot.com",
    "lowes.com",
    "ebay.com",
    "bhphotovideo.com",
)


def _print_stats_banner(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def _print_row(st: dict) -> None:
    fm = st.get("fetch_method", "-")
    fr = st.get("failure_reason", "-")
    print(
        f"  {st['domain']:<20} "
        f"scraped={st['scraped_count']:<4} "
        f"deduped={st['after_dedupe_count']:<4} "
        f"returned={st['returned_count']:<4} "
        f"dup_rm={st['duplicates_removed']:<3} "
        f"no_name={st['missing_name_rows']:<3} "
        f"no_price={st['missing_price_rows']:<3} "
        f"fetch={fm:<9} "
        f"fail={fr}"
    )


def run_discover_deals_all(q: str, max_price: float, max_results: int) -> None:
    _print_stats_banner("discover_deals (per source)")
    print(
        f"  query={q!r}  max_price={max_price}  max_results={max_results}\n"
    )
    for key in SOURCE_ORDER:
        src = SOURCES[key]
        scraper.discover_deals(q, src, max_price=max_price, max_results=max_results)
        st = scraper.LAST_DISCOVERY_STATS.get(f"{key}::discover_deals")
        if st:
            _print_row(st)
        else:
            print(f"  {key:<20} (no stats recorded)")


def run_discover_product_all(q: str, target_price: float) -> None:
    _print_stats_banner("discover_product (per source)")
    print(f"  query={q!r}  target_price={target_price}\n")
    for key in SOURCE_ORDER:
        src = SOURCES[key]
        scraper.discover_product(q, src, target_price=target_price)
        st = scraper.LAST_DISCOVERY_STATS.get(f"{key}::discover_product")
        if st:
            _print_row(st)
        else:
            print(f"  {key:<20} (no stats recorded)")


def main() -> None:
    q = "wireless mouse"
    scraper.LAST_DISCOVERY_STATS.clear()

    run_discover_deals_all(q, max_price=150.0, max_results=20)
    run_discover_product_all(q, target_price=40.0)

    print()
    print("Done. Full detail also in log lines prefixed with [STATS].")
    print()


if __name__ == "__main__":
    main()
