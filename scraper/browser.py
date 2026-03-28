"""Browser pool and Selenium fetch helpers."""

from config import SELENIUM_DRIVER_MAX_AGE_SECONDS
from ._legacy_impl import BrowserPool, _BrowserHandle, _default_warmup_url, _fetch_soup_selenium, _fetch_soup_selenium_pooled, start_browser_warmup

