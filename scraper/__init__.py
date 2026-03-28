"""Public scraper API surface.

This package preserves the historical `scraper` module contract while the
implementation is split into focused submodules. The legacy implementation is
still the active execution context for some orchestration paths, so the package
facade forwards dynamic attribute overrides into that backing module. That keeps
existing monkeypatch-style tests and scripts working while the split settles.
"""

from __future__ import annotations

import sys
from types import ModuleType

from . import _legacy_impl as _legacy

from .adapters import SourceAdapter, _SOURCE_ADAPTERS
from .browser import BrowserPool, SELENIUM_DRIVER_MAX_AGE_SECONDS, start_browser_warmup
from .context import FetchCacheEntry, SearchExecutionContext
from .debug import _debug_save_failure, _debug_text_preview, _html_has_search_result_markers, _scraper_debug_dir
from .discovery import discover_deals, discover_deals_for_queries, discover_product
from .extractors.amazon import _amazon_item_title, _amazon_listing_price, _extract_amazon_all, _extract_amazon_multi
from .extractors.bestbuy import _bestbuy_canonicalize_extracted_url, _bestbuy_is_product_listing_url, _extract_bestbuy_all, _extract_bestbuy_multi
from .extractors.bhphoto import _extract_bhphoto_multi
from .extractors.costco import _extract_costco_all, _extract_costco_multi
from .extractors.ebay import _extract_ebay_all, _extract_ebay_multi
from .extractors.homedepot import _extract_homedepot_all, _extract_homedepot_multi
from .extractors.lowes import _extract_lowes_all, _extract_lowes_multi
from .extractors.newegg import _extract_newegg_all, _extract_newegg_multi
from .extractors.officedepot import _extract_officedepot_all, _extract_officedepot_multi
from .extractors.registry import _MULTI_EXTRACTORS, _SITE_EXTRACTORS
from .extractors.target import _extract_target_all, _extract_target_multi, _target_current_price, _target_item_title, _target_listing_original_price
from .extractors.walmart import _extract_walmart_all, _extract_walmart_multi, _humanize_walmart_search_url, _walmart_extract_price
from .fetch import _fetch_listing_soup, _fetch_search_results_soup, _fetch_soup, _fetch_soup_selenium, _fetch_soup_selenium_pooled
from .pipeline import LAST_DISCOVERY_STATS, _apply_discovery_quality_pipeline, _dedupe_rows_by_url
from .pricing import clean_listing_title, clean_price, extract_price_from_soup
from .probes import (
    _fetch_search_probe_stage,
    _preview_probe_rows,
    _search_listing_candidates,
    _search_results_probe_ladder,
    _strict_search_url,
)
from .tracking import discover_product_matches, get_price_from_url, inspect_direct_link, revalidate_product_source, verify_candidate_listing
from .urls import _looks_like_non_product_url, canonicalize_listing_url

__all__ = [
    "BrowserPool",
    "FetchCacheEntry",
    "LAST_DISCOVERY_STATS",
    "SELENIUM_DRIVER_MAX_AGE_SECONDS",
    "SearchExecutionContext",
    "SourceAdapter",
    "_MULTI_EXTRACTORS",
    "_SITE_EXTRACTORS",
    "_SOURCE_ADAPTERS",
    "_amazon_item_title",
    "_amazon_listing_price",
    "_apply_discovery_quality_pipeline",
    "_bestbuy_canonicalize_extracted_url",
    "_bestbuy_is_product_listing_url",
    "_debug_save_failure",
    "_debug_text_preview",
    "_dedupe_rows_by_url",
    "_extract_amazon_all",
    "_extract_amazon_multi",
    "_extract_bestbuy_all",
    "_extract_bestbuy_multi",
    "_extract_bhphoto_multi",
    "_extract_costco_all",
    "_extract_costco_multi",
    "_extract_ebay_all",
    "_extract_ebay_multi",
    "_extract_homedepot_all",
    "_extract_homedepot_multi",
    "_extract_lowes_all",
    "_extract_lowes_multi",
    "_extract_newegg_all",
    "_extract_newegg_multi",
    "_extract_officedepot_all",
    "_extract_officedepot_multi",
    "_extract_target_all",
    "_extract_target_multi",
    "_extract_walmart_all",
    "_extract_walmart_multi",
    "_fetch_listing_soup",
    "_fetch_search_probe_stage",
    "_fetch_search_results_soup",
    "_fetch_soup",
    "_fetch_soup_selenium",
    "_fetch_soup_selenium_pooled",
    "_html_has_search_result_markers",
    "_humanize_walmart_search_url",
    "_looks_like_non_product_url",
    "_preview_probe_rows",
    "_scraper_debug_dir",
    "_search_listing_candidates",
    "_search_results_probe_ladder",
    "_strict_search_url",
    "_target_current_price",
    "_target_item_title",
    "_target_listing_original_price",
    "_walmart_extract_price",
    "canonicalize_listing_url",
    "clean_listing_title",
    "clean_price",
    "discover_deals",
    "discover_deals_for_queries",
    "discover_product",
    "discover_product_matches",
    "extract_price_from_soup",
    "get_price_from_url",
    "inspect_direct_link",
    "revalidate_product_source",
    "start_browser_warmup",
    "verify_candidate_listing",
]


def __getattr__(name: str):
    if hasattr(_legacy, name):
        return getattr(_legacy, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_legacy)))


class _ScraperModule(ModuleType):
    def __setattr__(self, name: str, value) -> None:
        super().__setattr__(name, value)
        if not name.startswith("__"):
            setattr(_legacy, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        if hasattr(_legacy, name):
            delattr(_legacy, name)


sys.modules[__name__].__class__ = _ScraperModule

