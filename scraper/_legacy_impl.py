"""
scraper.py — Product discovery and price extraction.

Two main functions:
  1. discover_product(product_name, source, target_price=...) — search a retailer;
     returns a list of listing dicts (url, price, name_found, status_hint), best-first.
  2. get_price_from_url(url, source_name)   — fetch price from a known product page

Extraction pipeline: JSON-LD → meta tags → HTML class/id patterns.
Requests+BeautifulSoup first, Selenium headless Chrome as fallback.
"""

import json
import logging
import os
import random
import re
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests.adapters import HTTPAdapter
from config import (
    BHPHOTO_DEBUG_ROWS,
    BROWSER_WARMUP_DOMAINS,
    BROWSER_WARMUP_INTERVAL_SECONDS,
    CHROMEDRIVER_PATH,
    DISCOVERY_FAST_USABLE_THRESHOLD,
    DISCOVERY_MAX_MERGED_RESULTS,
    DISCOVERY_MIN_CONFIRMED_BEFORE_SKIP_UNCONFIRMED,
    DISCOVERY_VERIFY_WORKERS,
    ENABLE_BROWSER_WARMUP,
    ENABLE_FAST_PATH_BESTBUY,
    ENABLE_FAST_PATH_WALMART,
    MAX_RESULTS_PER_SOURCE,
    REQUEST_POOL_SIZE,
    REQUIRE_DISCOUNT,
    SCRAPER_DEBUG_DIR,
    SCRAPER_DEBUG_MAX_BYTES,
    SELENIUM_DRIVER_MAX_AGE_SECONDS,
    SELENIUM_DRIVER_MAX_PAGES,
    STRICT_FAST_CANDIDATE_THRESHOLD,
    STRICT_MAX_CANDIDATES,
    STRICT_VERIFY_WORKERS,
)
from observability import log_event
from .protected_fetch import (
    fetch_via_provider,
    is_protected_domain,
    note_fetch_failure,
    note_fetch_success,
    provider_enabled_for,
    should_bypass_direct,
    should_try_provider_after_failure,
)

from product_verifier import (
    ProductSpec,
    QueryType,
    fallback_listing_fingerprint,
    fingerprint_listing_document,
    parse_product_spec,
    product_spec_from_row,
    verification_result_to_fields,
    verify_listing,
)


# When true, prefer listings with a provable ≥5% markdown; unconfirmed (no was-price
# in SRP HTML) can still appear if we have fewer than 5 confirmed hits.


@dataclass(frozen=True)
class FetchCacheEntry:
    soup: BeautifulSoup | None
    fetch_method: str
    failure_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class SearchExecutionContext:
    """Request-scoped caches, sessions, and failure state for one search/check pass."""

    def __init__(self) -> None:
        self._fetch_cache: dict[str, FetchCacheEntry] = {}
        self._verification_cache: dict[tuple[ProductSpec, str], Any] = {}
        self._domain_failures: dict[str, str] = {}
        self._empty_result_counts: dict[str, int] = {}
        self._probe_outcomes: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._domain_escalation: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()
        self._thread_local = threading.local()

    def get_fetch_entry(self, key: str) -> FetchCacheEntry | None:
        with self._lock:
            return self._fetch_cache.get(key)

    def set_fetch_entry(self, key: str, entry: FetchCacheEntry) -> None:
        with self._lock:
            self._fetch_cache[key] = entry

    def get_verification(self, spec: ProductSpec, canonical_url: str) -> Any | None:
        with self._lock:
            return self._verification_cache.get((spec, canonical_url))

    def set_verification(self, spec: ProductSpec, canonical_url: str, verification: Any) -> None:
        with self._lock:
            self._verification_cache[(spec, canonical_url)] = verification

    def mark_domain_failure(self, domain: str, reason: str) -> None:
        if not domain or not reason:
            return
        with self._lock:
            self._domain_failures[domain] = reason

    def domain_failure_reason(self, domain: str) -> str | None:
        with self._lock:
            return self._domain_failures.get(domain)

    def should_skip_domain(self, domain: str) -> bool:
        return self.domain_failure_reason(domain) is not None

    def record_empty_result(self, domain: str) -> int:
        with self._lock:
            count = self._empty_result_counts.get(domain, 0) + 1
            self._empty_result_counts[domain] = count
            return count

    def clear_empty_results(self, domain: str) -> None:
        with self._lock:
            self._empty_result_counts.pop(domain, None)

    def record_probe_outcome(
        self,
        domain: str,
        url: str,
        stage: str,
        *,
        fetch_method: str,
        row_count: int = 0,
        usable_count: int = 0,
        failure_reason: str | None = None,
    ) -> None:
        key = (domain, url, stage)
        with self._lock:
            self._probe_outcomes[key] = {
                "fetch_method": fetch_method,
                "row_count": row_count,
                "usable_count": usable_count,
                "failure_reason": failure_reason,
            }
        log_event(
            "search.probe",
            domain=domain,
            stage=stage,
            fetch_method=fetch_method,
            row_count=row_count,
            usable_count=usable_count,
            failure_reason=failure_reason,
        )

    def get_probe_outcome(self, domain: str, url: str, stage: str) -> dict[str, Any] | None:
        with self._lock:
            return self._probe_outcomes.get((domain, url, stage))

    def set_escalation_stage(self, domain: str, url: str, stage: str) -> None:
        with self._lock:
            self._domain_escalation[(domain, url)] = stage

    def get_escalation_stage(self, domain: str, url: str) -> str | None:
        with self._lock:
            return self._domain_escalation.get((domain, url))

    def get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is not None:
            return session
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=REQUEST_POOL_SIZE,
            pool_maxsize=REQUEST_POOL_SIZE,
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self._thread_local.session = session
        return session


_PROTECTED_SESSION_POOL: dict[str, requests.Session] = {}
_PROTECTED_SESSION_LOCK = threading.Lock()


def _get_fetch_session(url: str, context: SearchExecutionContext | None = None) -> requests.Session:
    domain = urlparse(url).netloc.lower().replace("www.", "")
    if is_protected_domain(domain):
        with _PROTECTED_SESSION_LOCK:
            session = _PROTECTED_SESSION_POOL.get(domain)
            if session is None:
                session = requests.Session()
                adapter = HTTPAdapter(
                    pool_connections=max(8, REQUEST_POOL_SIZE),
                    pool_maxsize=max(8, REQUEST_POOL_SIZE),
                    max_retries=0,
                )
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                _PROTECTED_SESSION_POOL[domain] = session
            return session
    return context.get_session() if context else requests.Session()


@dataclass
class _BrowserHandle:
    driver: Any | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    pages_served: int = 0
    created_at: float = 0.0


class BrowserPool:
    """Small reusable Selenium pool: one active browser task per domain."""

    def __init__(self) -> None:
        self._handles: dict[str, _BrowserHandle] = {}
        self._lock = threading.Lock()
        self._driver_path: str | None = None
        self._driver_lock = threading.Lock()
        self._warmup_started = False
        self._warmup_lock = threading.Lock()

    def _get_handle(self, domain: str) -> _BrowserHandle:
        key = domain or "__default__"
        with self._lock:
            handle = self._handles.get(key)
            if handle is None:
                handle = _BrowserHandle()
                self._handles[key] = handle
            return handle

    def _resolve_driver_path(self) -> str:
        configured = CHROMEDRIVER_PATH
        if configured:
            return configured
        with self._driver_lock:
            if self._driver_path:
                return self._driver_path
            from webdriver_manager.chrome import ChromeDriverManager

            self._driver_path = ChromeDriverManager().install()
            return self._driver_path

    def _new_driver(self):
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        ua = random.choice(USER_AGENTS)
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-features=IsolateOrigins,site-per-process")
        opts.page_load_strategy = "eager"
        opts.add_argument(f"--user-agent={ua}")
        driver = webdriver.Chrome(
            service=Service(self._resolve_driver_path()),
            options=opts,
        )
        driver.set_page_load_timeout(25)
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                """
            })
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders",
                {"headers": {"Referer": "https://www.google.com/"}},
            )
            driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})
        except Exception:
            pass
        return driver

    def _recycle_locked(self, handle: _BrowserHandle) -> None:
        driver = handle.driver
        handle.driver = None
        handle.pages_served = 0
        handle.created_at = 0.0
        if driver is None:
            return
        try:
            driver.quit()
        except Exception:
            pass

    def _ensure_driver_locked(self, handle: _BrowserHandle):
        if (
            handle.driver is not None
            and handle.pages_served < SELENIUM_DRIVER_MAX_PAGES
            and (
                not handle.created_at
                or (time.monotonic() - handle.created_at) < SELENIUM_DRIVER_MAX_AGE_SECONDS
            )
        ):
            return handle.driver
        self._recycle_locked(handle)
        handle.driver = self._new_driver()
        handle.created_at = time.monotonic()
        return handle.driver

    def close_all(self) -> None:
        with self._lock:
            handles = list(self._handles.values())
        for handle in handles:
            with handle.lock:
                self._recycle_locked(handle)

    def warm_domain(self, domain: str, url: str | None = None) -> bool:
        target_domain = (domain or "").lower().replace("www.", "")
        if not target_domain:
            return False
        target_url = url or _default_warmup_url(target_domain)
        if not target_url:
            return False
        handle = self._get_handle(target_domain)
        with handle.lock:
            driver = self._ensure_driver_locked(handle)
            try:
                driver.get(target_url)
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.webdriver.support.ui import WebDriverWait

                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                handle.pages_served += 1
                return True
            except Exception as exc:
                logging.debug(
                    f"[{datetime.now()}] Browser warmup skipped for {target_domain}: {exc}"
                )
                self._recycle_locked(handle)
                return False

    def start_warmup_loop(self, domains: tuple[str, ...] | None = None) -> None:
        if not ENABLE_BROWSER_WARMUP or _is_test_process():
            return
        with self._warmup_lock:
            if self._warmup_started:
                return
            self._warmup_started = True

        warm_domains = tuple(
            domain.strip().lower().replace("www.", "")
            for domain in (domains or BROWSER_WARMUP_DOMAINS)
            if domain and domain.strip() and not provider_enabled_for(domain)
        )

        def loop() -> None:
            while True:
                for domain in warm_domains:
                    self.warm_domain(domain)
                time.sleep(max(60, BROWSER_WARMUP_INTERVAL_SECONDS))

        threading.Thread(target=loop, daemon=True).start()

    def fetch_soup(
        self,
        url: str,
        *,
        domain: str = "",
        debug_meta: dict | None = None,
        failure_sink: dict[str, str] | None = None,
    ) -> BeautifulSoup | None:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        effective_domain = (domain or urlparse(url).netloc or "").lower().replace("www.", "")
        mode_dbg = str((debug_meta or {}).get("mode", "selenium"))
        q_dbg = str((debug_meta or {}).get("query", ""))
        profile = _selenium_mode_profile(effective_domain, mode_dbg)
        handle = self._get_handle(effective_domain)
        with handle.lock:
            driver = self._ensure_driver_locked(handle)
            try:
                driver.set_page_load_timeout(25 if mode_dbg != "probe_light_js" else 15)
                driver.get(url)
                WebDriverWait(driver, profile["body_timeout"]).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                selectors = _selenium_wait_selectors(effective_domain, mode_dbg)
                if selectors and profile["selector_timeout"] > 0:
                    _wait_for_any_selector(
                        driver,
                        selectors,
                        timeout=profile["selector_timeout"],
                    )
                if profile["scroll_enabled"]:
                    _progressive_scroll(
                        driver,
                        effective_domain,
                        selectors[0] if selectors else None,
                        max_passes=profile["scroll_passes"],
                        target_cards=profile["target_cards"],
                    )
                page_source = driver.page_source
                body_len = len(page_source)
                handle.pages_served += 1
                if (
                    body_len < 50000
                    and effective_domain in {"walmart.com", "bestbuy.com"}
                    and not _html_has_search_result_markers(page_source, effective_domain)
                ):
                    logging.warning(
                        f"[{datetime.now()}] Possible bot wall for {effective_domain}, body_len={body_len}"
                    )
                    if failure_sink is not None:
                        failure_sink["reason"] = "bot_wall"
                    _debug_save_failure(
                        effective_domain,
                        mode_dbg,
                        q_dbg,
                        "selenium",
                        "bot_wall",
                        url=url,
                        html=page_source,
                    )
                    self._recycle_locked(handle)
                    return None
                return BeautifulSoup(page_source, "html.parser")
            except Exception as exc:
                logging.error(f"[{datetime.now()}] Selenium error for {url}: {exc}")
                if failure_sink is not None:
                    failure_sink["reason"] = "selenium_error"
                self._recycle_locked(handle)
                return None


_BROWSER_POOL = BrowserPool()


def _is_test_process() -> bool:
    return "pytest" in sys.modules or any("pytest" in arg.lower() for arg in sys.argv)


def _default_warmup_url(domain: str) -> str | None:
    urls = {
        "bestbuy.com": "https://www.bestbuy.com/",
        "walmart.com": "https://www.walmart.com/",
        "target.com": "https://www.target.com/",
        "costco.com": "https://www.costco.com/",
    }
    return urls.get((domain or "").lower().replace("www.", ""))


def start_browser_warmup() -> None:
    _BROWSER_POOL.start_warmup_loop()


def _html_has_search_result_markers(html: str, domain: str) -> bool:
    body = (html or "").lower()
    markers = {
        "bestbuy.com": ("/site/", "/product/", "sku-item", "sku-title", "product-list-item", "product-list-item-link"),
        "walmart.com": ("/ip/", "data-item-id", "product-title"),
    }
    return any(token in body for token in markers.get(domain, ()))

def _discount_confirmed(
    original_price: float | None, current_price: float | None,
) -> bool:
    """True when SRP shows a was-price at least ~5% above the current price."""
    if original_price is None or current_price is None:
        return False
    return original_price > current_price * 1.05


def _retailer_log_label(domain: str) -> str:
    return {
        "amazon.com": "Amazon",
        "bestbuy.com": "Best Buy",
        "newegg.com": "Newegg",
        "walmart.com": "Walmart",
        "ebay.com": "eBay",
        "target.com": "Target",
        "costco.com": "Costco",
        "homedepot.com": "Home Depot",
        "lowes.com": "Lowe's",
        "bhphotovideo.com": "B&H",
    }.get(domain, domain.replace(".com", "").title())


def _apply_discovery_quality_pipeline(
    rows: list[dict],
    *,
    query: str,
    max_price: float | None,
    label: str,
    price_key: str,
    name_key: str,
) -> list[dict]:
    """
    Price (<= max_price) → discount tiers → optional unconfirmed fill.

    Listing quality and identity gating run in ``product_identity`` /
    ``process_discovery_results`` / ``_filter_discover_candidates`` — not here.

    Sets ``discount_confirmed`` on each kept row. Never drops rows solely because
    ``original_price`` is missing when REQUIRE_DISCOUNT is on (they may pad results).
    """
    _ = query, name_key  # retained for logging API compatibility with extractors
    n_raw = len(rows)
    for row in rows:
        if name_key in row and row.get(name_key):
            row[name_key] = clean_listing_title(str(row.get(name_key, "")))
    after_price: list[dict] = []
    for r in rows:
        p = r.get(price_key)
        if p is None:
            continue
        if max_price is None or p <= max_price:
            after_price.append(r)
    n_price = len(after_price)

    dropped_non_sale = 0

    if not REQUIRE_DISCOUNT:
        after_rel: list[dict] = []
        for r in after_price:
            cur = r.get(price_key)
            if cur is None:
                continue
            orig = r.get("original_price")
            r["discount_confirmed"] = _discount_confirmed(orig, cur)
            after_rel.append(r)
        n_conf_rel = sum(1 for x in after_rel if x.get("discount_confirmed"))
        logging.info(
            f"[{label}] {n_raw} raw → {n_price} passed price filter → "
            f"REQUIRE_DISCOUNT off → {len(after_rel)} rows ({n_conf_rel} confirmed sale) "
            f"→ storing {len(after_rel)} results"
        )
        return after_rel

    tier1: list[dict] = []
    unconfirmed: list[dict] = []
    for r in after_price:
        cur = r.get(price_key)
        if cur is None:
            continue
        orig = r.get("original_price")
        if _discount_confirmed(orig, cur):
            tier1.append(r)
        elif orig is None:
            unconfirmed.append(r)
        else:
            dropped_non_sale += 1

    merged: list[dict] = []
    for r in tier1:
        r["discount_confirmed"] = True
        merged.append(r)

    n_pad = 0
    if len(merged) < DISCOVERY_MIN_CONFIRMED_BEFORE_SKIP_UNCONFIRMED:
        room = max(0, DISCOVERY_MAX_MERGED_RESULTS - len(merged))
        for r in unconfirmed[:room]:
            r["discount_confirmed"] = False
            merged.append(r)
            n_pad += 1

    n_rel = len(merged)
    rel1 = tier1
    rel_seg = f"{len(rel1)} confirmed tier + {n_pad} unconfirmed fill"
    logging.info(
        f"[{label}] {n_raw} raw → {n_price} passed price filter → "
        f"{len(tier1)} confirmed discount pre-relevance, "
        f"{len(unconfirmed)} unconfirmed (no was-price), "
        f"dropped_non_sale={dropped_non_sale} → "
        f"{rel_seg} → storing {n_rel} results"
    )
    return merged


# First N B&H listing rows: log name/url/card preview (0 = off).
_BHPHOTO_DEBUG_FIRST_N = BHPHOTO_DEBUG_ROWS

# Last run metrics (read by tests / monitoring); overwritten each discover_* call.
LAST_DISCOVERY_STATS: dict[str, dict] = {}

# Set when Selenium returns None after a diagnosable failure (e.g. bot wall).
_LAST_SELENIUM_FAILURE: dict | None = None

# Set on requests timeout / captcha when failure_context targets Walmart or Best Buy.
_LAST_REQUESTS_FAILURE: dict | None = None

# ---------------------------------------------------------------------------
# User-Agent pool
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]


_SELENIUM_PREFERRED = {
    "bestbuy.com",
    "target.com",
    "walmart.com",
    "costco.com",
    "homedepot.com",
    "lowes.com",
}

_DEBUG_HTML_MAX_BYTES = SCRAPER_DEBUG_MAX_BYTES


def _scraper_debug_dir() -> str:
    d = SCRAPER_DEBUG_DIR or os.path.join(tempfile.gettempdir(), "pricepulse_scraper_debug")
    os.makedirs(d, exist_ok=True)
    return d


def _debug_text_preview(html: str, limit: int = 2048) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        t = soup.get_text(separator=" ", strip=True)
        return t[:limit] if t else ""
    except Exception:
        return html[:limit]


def _debug_save_failure(
    domain: str,
    mode: str,
    query: str,
    fetch_method: str,
    failure_reason: str,
    *,
    url: str = "",
    html: str | None = None,
    note: str = "",
) -> str | None:
    """Save HTML + meta for Walmart/Best Buy failure analysis. Returns base path or None."""
    if domain not in ("walmart.com", "bestbuy.com"):
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = domain.replace(".", "_")
    base = os.path.join(_scraper_debug_dir(), f"{safe}_{mode}_{ts}")
    meta_lines = [
        f"domain={domain}",
        f"mode={mode}",
        f"query={query!r}",
        f"fetch_method={fetch_method}",
        f"failure_reason={failure_reason}",
        f"url={url}",
        f"note={note}",
        "",
        "---- text_preview (first ~2KB) ----",
    ]
    if html:
        prev = _debug_text_preview(html, 2048)
        meta_lines.append(prev or "(empty)")
    else:
        meta_lines.append("(no html captured)")
    try:
        with open(base + ".meta.txt", "w", encoding="utf-8", errors="replace") as f:
            f.write("\n".join(meta_lines))
        if html:
            raw = html.encode("utf-8", errors="replace")
            if len(raw) > _DEBUG_HTML_MAX_BYTES:
                raw = raw[:_DEBUG_HTML_MAX_BYTES]
            with open(base + ".html", "wb") as f:
                f.write(raw)
        logging.warning(
            f"[{datetime.now()}] Debug dump written: {base}.meta.txt"
            + (f" + {base}.html" if html else "")
        )
        return base
    except OSError as exc:
        logging.error(f"[{datetime.now()}] Debug dump failed: {exc}")
        return None


def _discovery_failure_reason_no_soup() -> str:
    """Best-effort label when HTML could not be loaded (Walmart/Best Buy debug signals)."""
    global _LAST_REQUESTS_FAILURE, _LAST_SELENIUM_FAILURE
    if _LAST_REQUESTS_FAILURE and _LAST_REQUESTS_FAILURE.get("reason") == "timeout":
        return "timeout"
    if _LAST_SELENIUM_FAILURE and _LAST_SELENIUM_FAILURE.get("reason") == "bot_wall":
        return "bot_wall"
    return "fetch_failed"


def _humanize_walmart_search_url(url: str) -> str:
    """Add common query params so the URL looks more like a typical browser search."""
    if "walmart.com/search" not in url.lower():
        return url
    if "sort=" in url.lower():
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sort=best_match"


def _random_headers(url: str) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    if "amazon" in url.lower():
        headers["Referer"] = "https://www.google.com/"
    elif any(
        x in url.lower()
        for x in (
            "bestbuy",
            "target",
            "walmart",
            "bhphotovideo",
            "costco",
            "homedepot",
            "lowes",
        )
    ):
        headers["Referer"] = "https://www.google.com/"
    return headers


# ---------------------------------------------------------------------------
# Price string cleaning
# ---------------------------------------------------------------------------

_PRICE_CONTEXT_BLOCKLIST = (
    "save",
    "off",
    "coupon",
    "shipping",
    "delivery",
    "/mo",
    "month",
    "plan",
    "warranty",
    "protection",
)


def _price_candidates_from_raw(raw) -> list[float]:
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []

    candidates: list[float] = []
    seen: set[float] = set()

    for match in re.finditer(r"\d[\d,]{0,6}\.\d{2}", s):
        prefix = s[max(0, match.start() - 18):match.start()].lower()
        suffix = s[match.end():match.end() + 12].lower()
        if any(token in prefix or token in suffix for token in _PRICE_CONTEXT_BLOCKLIST):
            continue
        try:
            price = round(float(match.group(0).replace(",", "")), 2)
        except ValueError:
            continue
        if 0 < price <= 100_000 and price not in seen:
            seen.add(price)
            candidates.append(price)

    if candidates:
        return candidates

    tokenized = re.sub(r"[^\d\s]", " ", s)
    chunks = re.findall(r"\d+", tokenized)
    for idx in range(len(chunks) - 1):
        dollars = chunks[idx]
        cents = chunks[idx + 1]
        if not (1 <= len(dollars) <= 4 and len(cents) == 2):
            continue
        try:
            price = round(float(f"{int(dollars)}.{cents}"), 2)
        except ValueError:
            continue
        if 0 < price <= 100_000 and price not in seen:
            seen.add(price)
            candidates.append(price)
            break

    return candidates


def clean_price(raw) -> float | None:
    if raw is None:
        return None
    candidates = _price_candidates_from_raw(raw)
    if candidates:
        return candidates[0]
    s = str(raw).strip().replace(",", "")
    text = re.sub(r"[^\d.]", "", s)
    parts = text.split(".")
    if len(parts) > 2:
        text = "".join(parts[:-1]) + "." + parts[-1]
    try:
        price = float(text)
        if 0 < price < 1_000_000:
            return round(price, 2)
    except (ValueError, TypeError):
        pass
    return None


_TITLE_LEADING_NOISE = (
    re.compile(r"^(?:overall\s+pick)\s+", re.I),
    re.compile(r"^(?:amazon'?s\s+choice)\s+", re.I),
    re.compile(r"^(?:best\s+seller)\s+", re.I),
    re.compile(r"^(?:limited\s+time\s+deal)\s+", re.I),
    re.compile(r"^(?:sponsored)\s+", re.I),
    re.compile(r"^(?:amazon\.com|walmart\.com|target\.com)\s*:\s*", re.I),
)

_TITLE_TRAILING_NOISE = (
    re.compile(r"\s+\$\s*[\d,]+(?:\.\d{2})?(?:\s+was\s+\$\s*[\d,]+(?:\.\d{2})?)?.*$", re.I),
    re.compile(r"\s+was\s+\$\s*[\d,]+(?:\.\d{2})?.*$", re.I),
    re.compile(r"\s+\d+(?:\.\d+)?\s+out\s+of\s+5\s+stars.*$", re.I),
    re.compile(r"\s+\d+\+?\s+bought\s+in\s+past\s+month.*$", re.I),
)


def clean_listing_title(raw_title: str) -> str:
    title = re.sub(r"\s+", " ", (raw_title or "").strip())
    if not title:
        return ""
    changed = True
    while changed and title:
        changed = False
        for pattern in _TITLE_LEADING_NOISE:
            cleaned = pattern.sub("", title).strip()
            if cleaned != title:
                title = cleaned
                changed = True
        for pattern in _TITLE_TRAILING_NOISE:
            cleaned = pattern.sub("", title).strip()
            if cleaned != title:
                title = cleaned
                changed = True
    if re.match(r"^(?:amazon\.com|walmart\.com|target\.com)\b", (raw_title or "").strip(), re.I):
        title = re.sub(r"\s*:\s*(?:electronics|computers(?:\s*&\s*accessories)?|home\s*&\s*kitchen|"
                       r"office\s*products|video\s*games|toys\s*&\s*games)\s*$", "", title, flags=re.I)
    title = re.sub(r"\s+[-|:]+\s*$", "", title).strip(" -|:")
    return title or re.sub(r"\s+", " ", (raw_title or "").strip())


def _first_dollar_price_in_text(text: str) -> float | None:
    """Pick first plausible $NN.nn from visible copy (fallback for sparse price nodes)."""
    if not text:
        return None
    candidates = _price_candidates_from_raw(text)
    if candidates:
        return candidates[0]
    for m in re.finditer(r"\$\s*[\d,]+\.\d{2}", text):
        p = clean_price(m.group(0))
        if p and 1.0 <= p <= 100_000:
            return p
    return None


# ---------------------------------------------------------------------------
# Extraction helpers (JSON-LD, meta, HTML)
# ---------------------------------------------------------------------------

_NON_NEW_CONDITION_PATTERN = re.compile(
    r"\b("
    r"renewed|amazonrenewed|refurb(?:ished)?|used|warehouse|"
    r"pre[- ]?owned|preowned|open[- ]?box|open box|restored|recertified|"
    r"usedaccordionrow|renewedaccordionrow|openboxaccordionrow"
    r")\b",
    re.I,
)


def _allows_non_new_condition(condition_hint_text: str | None) -> bool:
    return bool(_NON_NEW_CONDITION_PATTERN.search(condition_hint_text or ""))


def _schema_condition_allowed(obj: dict, *, allow_non_new: bool) -> bool:
    if allow_non_new:
        return True
    condition_bits = " ".join(
        str(obj.get(key, ""))
        for key in ("itemCondition", "condition", "@type", "name", "description")
    ).lower()
    if not condition_bits:
        return True
    if "newcondition" in condition_bits or re.search(r"\bnew\b", condition_bits):
        return True
    return not bool(_NON_NEW_CONDITION_PATTERN.search(condition_bits))


def _schema_prices(obj, *, allow_non_new: bool = True) -> list[float]:
    """Collect candidate prices from JSON-LD schema objects."""
    out: list[float] = []
    if isinstance(obj, list):
        for item in obj:
            out.extend(_schema_prices(item, allow_non_new=allow_non_new))
        return out
    if not isinstance(obj, dict):
        return out

    if not _schema_condition_allowed(obj, allow_non_new=allow_non_new):
        return out

    # Prefer explicit price, but keep ranges too (we'll score later).
    for key in ("price", "lowPrice", "highPrice"):
        if key in obj:
            p = clean_price(obj.get(key))
            if p:
                out.append(p)

    offers = obj.get("offers")
    if offers:
        out.extend(_schema_prices(offers, allow_non_new=allow_non_new))
    return out


def extract_price_from_json_ld(
    soup: BeautifulSoup,
    *,
    condition_hint_text: str | None = None,
) -> float | None:
    prices: list[float] = []
    allow_non_new = _allows_non_new_condition(condition_hint_text)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, AttributeError):
            continue
        prices.extend(_schema_prices(data, allow_non_new=allow_non_new))
    prices = [p for p in prices if p and 0 < p <= 100_000]
    if not prices:
        return None
    # Without a hint, the safest generic choice is the minimum (usually the actual offer price).
    return min(prices)


def extract_price_from_meta(soup: BeautifulSoup) -> float | None:
    candidates = [
        ("property", "og:price:amount"),
        ("property", "product:price:amount"),
        ("name", "price"),
        ("itemprop", "price"),
    ]
    for attr, val in candidates:
        tag = soup.find("meta", {attr: val})
        if tag:
            p = clean_price(tag.get("content"))
            if p:
                return p
    return None


def _price_element_context(el: Tag) -> str:
    parts: list[str] = []
    current: Tag | None = el
    for depth in range(7):
        if current is None:
            break
        attrs = [
            (current.get("id") or "").lower(),
            " ".join(current.get("class", [])).lower(),
            (current.get("data-cy") or "").lower(),
            (current.get("data-testid") or "").lower(),
            (current.get("aria-label") or "").lower(),
            (current.get("data-feature-name") or "").lower(),
            (current.get("data-csa-c-slot-id") or "").lower(),
            (current.get("data-csa-c-buying-option-type") or "").lower(),
        ]
        parts.extend(part for part in attrs if part)
        if depth < 2:
            text = current.get_text(" ", strip=True).lower()
            if text:
                parts.append(text)
        current = current.parent if isinstance(current.parent, Tag) else None
    return " ".join(parts)


def _is_noisy_price_element(el: Tag, *, allow_non_new: bool = True) -> bool:
    context = _price_element_context(el)
    noisy_bits = (
        "warranty", "protection", "insurance", "trade-in", "trade in",
        "monthly", "/month", "month", "store card", "subscription", "plan",
    )
    if any(bit in context for bit in noisy_bits):
        return True
    if not allow_non_new and _NON_NEW_CONDITION_PATTERN.search(context):
        return True
    return False


def _collect_html_price_candidates(
    soup: BeautifulSoup,
    *,
    condition_hint_text: str | None = None,
) -> list[float]:
    candidates: list[float] = []
    seen: set[float] = set()
    allow_non_new = _allows_non_new_condition(condition_hint_text)

    def add_price(raw_text: str) -> None:
        p = clean_price(raw_text)
        if p and p not in seen:
            seen.add(p)
            candidates.append(p)

    # Prioritize common PDP selectors before falling back to generic price scans.
    preferred_selectors = (
        "#corePrice_feature_div .a-offscreen",
        "#apex_offerDisplay_desktop .a-offscreen",
        "#tp_price_block_total_price_ww .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .a-offscreen",
        "#corePriceDisplay_mobile_feature_div .a-offscreen",
        "[data-a-color='price'] .a-offscreen",
        ".priceToPay .a-offscreen",
        '[itemprop=\"price\"]',
        "meta[property='product:price:amount']",
        "meta[property='og:price:amount']",
    )
    for sel in preferred_selectors:
        for el in soup.select(sel):
            if _is_noisy_price_element(el, allow_non_new=allow_non_new):
                continue
            add_price(el.get("content") or el.get_text(" ", strip=True))

    pattern = re.compile(r"price", re.IGNORECASE)
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", []))
        el_id = el.get("id", "")
        if not (pattern.search(classes) or pattern.search(el_id)):
            continue
        if _is_noisy_price_element(el, allow_non_new=allow_non_new):
            continue
        text = el.get_text(" ", strip=True)
        if not text:
            continue
        if "$" in text or re.search(r"\d+[.,]\d{2}", text):
            add_price(text)
    return candidates


def _collect_preferred_price_candidates(
    soup: BeautifulSoup,
    *,
    condition_hint_text: str | None = None,
) -> list[float]:
    candidates: list[float] = []
    seen: set[float] = set()
    allow_non_new = _allows_non_new_condition(condition_hint_text)

    def add_price(raw_text: str) -> None:
        p = clean_price(raw_text)
        if p and p not in seen:
            seen.add(p)
            candidates.append(p)

    preferred_selectors = (
        "#corePrice_feature_div .a-offscreen",
        "#apex_offerDisplay_desktop .a-offscreen",
        "#tp_price_block_total_price_ww .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .a-offscreen",
        "#corePriceDisplay_mobile_feature_div .a-offscreen",
        "[data-a-color='price'] .a-offscreen",
        ".priceToPay .a-offscreen",
        '[itemprop=\"price\"]',
        "meta[property='product:price:amount']",
        "meta[property='og:price:amount']",
    )
    for sel in preferred_selectors:
        for el in soup.select(sel):
            if _is_noisy_price_element(el, allow_non_new=allow_non_new):
                continue
            add_price(el.get("content") or el.get_text(" ", strip=True))
    return candidates


def extract_primary_price_from_soup(
    soup: BeautifulSoup,
    *,
    condition_hint_text: str | None = None,
) -> float | None:
    candidates = _collect_preferred_price_candidates(
        soup,
        condition_hint_text=condition_hint_text,
    )
    if not candidates:
        return None
    # The preferred selector order is curated toward the real PDP price block, so
    # use the first hit rather than the global minimum for direct-link bootstrapping.
    return candidates[0]


def extract_price_from_html(
    soup: BeautifulSoup,
    *,
    condition_hint_text: str | None = None,
) -> float | None:
    candidates = _collect_html_price_candidates(
        soup,
        condition_hint_text=condition_hint_text,
    )
    return min(candidates) if candidates else None


def _pick_price_with_hint(candidates: list[float], price_hint: float | None) -> float | None:
    if not candidates:
        return None
    candidates = [p for p in candidates if p and 0 < p <= 100_000]
    if not candidates:
        return None
    if price_hint is None:
        ordered = sorted(set(candidates))
        # Guard against obvious non-item amounts (shipping/monthly/installment) that
        # occasionally appear alongside the real PDP price, e.g. 8.99 vs 63.99.
        while len(ordered) >= 2 and ordered[0] < (ordered[1] * 0.5):
            ordered.pop(0)
        return ordered[0]

    try:
        hint = float(price_hint)
    except (TypeError, ValueError):
        return min(candidates)
    if hint <= 0:
        return min(candidates)

    def score(p: float) -> float:
        # Relative error; lower is better. This favors the discovery price when tracking.
        return abs(p - hint) / max(hint, 1.0)

    best = min(candidates, key=score)
    return best


def extract_price_from_soup(
    soup: BeautifulSoup,
    *,
    price_hint: float | None = None,
    condition_hint_text: str | None = None,
) -> float | None:
    """
    Extract a plausible item price from a PDP soup.

    When price_hint is provided (e.g. from search/discovery), choose the candidate price
    closest to that hint. This avoids accidentally selecting installment/monthly prices.
    """
    preferred_candidates = _collect_preferred_price_candidates(
        soup,
        condition_hint_text=condition_hint_text,
    )
    if preferred_candidates:
        return _pick_price_with_hint(preferred_candidates, price_hint)

    candidates: list[float] = []
    allow_non_new = _allows_non_new_condition(condition_hint_text)

    # Meta tags are often the cleanest "current price".
    meta_p = extract_price_from_meta(soup)
    if meta_p is not None:
        candidates.append(meta_p)

    # JSON-LD can include multiple prices (ranges, variations, etc).
    # We collect all of them (via extract_price_from_json_ld's internal collector) by re-parsing here.
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, AttributeError):
            continue
        candidates.extend(_schema_prices(data, allow_non_new=allow_non_new))

    candidates.extend(
        _collect_html_price_candidates(soup, condition_hint_text=condition_hint_text)
    )

    return _pick_price_with_hint(candidates, price_hint)


def _extract_price_from_soup_compat(
    soup: BeautifulSoup,
    *,
    price_hint: float | None = None,
    condition_hint_text: str | None = None,
) -> float | None:
    try:
        return extract_price_from_soup(
            soup,
            price_hint=price_hint,
            condition_hint_text=condition_hint_text,
        )
    except TypeError as exc:
        if "condition_hint_text" not in str(exc):
            raise
        return extract_price_from_soup(soup, price_hint=price_hint)


# ---------------------------------------------------------------------------
# Requests + BeautifulSoup fetch
# ---------------------------------------------------------------------------

def _fetch_soup(
    url: str,
    *,
    debug_domain: str = "",
    failure_context: dict | None = None,
    failure_sink: dict[str, str] | None = None,
    context: SearchExecutionContext | None = None,
    timeout_seconds: int | float = 25,
) -> BeautifulSoup | None:
    global _LAST_REQUESTS_FAILURE
    source_domain = urlparse(url).netloc.lower().replace("www.", "")
    log_event("source.fetch.start", domain=source_domain, strategy="direct", url=url)
    try:
        session = _get_fetch_session(url, context=context)
        resp = session.get(url, headers=_random_headers(url), timeout=timeout_seconds)
        if resp.status_code != 200:
            snippet = resp.text[:500] if resp.text else "(empty body)"
            logging.warning(
                f"[{datetime.now()}] HTTP {resp.status_code} for {url}\n"
                f"  Response preview: {snippet}"
            )
            if failure_sink is not None:
                failure_sink["reason"] = "http_error"
            note_fetch_failure(source_domain, "http_error", "requests")
            return None
        body_lower = resp.text.lower()
        if "captcha" in body_lower or "robot check" in body_lower:
            logging.warning(f"[{datetime.now()}] CAPTCHA detected for {url}")
            if failure_context and failure_context.get("domain") in (
                "walmart.com",
                "bestbuy.com",
            ):
                _LAST_REQUESTS_FAILURE = {"reason": "bot_wall", "url": url}
                if failure_sink is not None:
                    failure_sink["reason"] = "bot_wall"
                _debug_save_failure(
                    str(failure_context["domain"]),
                    str(failure_context.get("mode", "?")),
                    str(failure_context.get("query", "")),
                    "requests",
                    "bot_wall",
                    url=url,
                    html=resp.text,
                    note="captcha/robot in body",
                )
            note_fetch_failure(source_domain, "bot_wall", "requests")
            return None
        soup = BeautifulSoup(resp.content, "html.parser")
        if debug_domain:
            title = soup.title.get_text(strip=True) if soup.title else "(no title)"
            body_len = len(resp.text)
            logging.info(
                f"[{datetime.now()}] Fetched {debug_domain}: "
                f"HTTP 200, {body_len} chars, title=\"{title[:80]}\""
            )
        note_fetch_success(source_domain, "requests")
        log_event("source.fetch.finish", domain=source_domain, strategy="direct", method="requests", url=url)
        return soup
    except requests.exceptions.Timeout:
        logging.error(f"[{datetime.now()}] Timeout fetching {url}")
        if failure_sink is not None:
            failure_sink["reason"] = "timeout"
        if failure_context and failure_context.get("domain") in (
            "walmart.com",
            "bestbuy.com",
        ):
            _LAST_REQUESTS_FAILURE = {"reason": "timeout", "url": url}
            _debug_save_failure(
                str(failure_context["domain"]),
                str(failure_context.get("mode", "?")),
                str(failure_context.get("query", "")),
                "requests",
                "timeout",
                url=url,
                note="requests.Timeout",
            )
        note_fetch_failure(source_domain, "timeout", "requests")
    except requests.exceptions.RequestException as exc:
        logging.error(f"[{datetime.now()}] Request error for {url}: {exc}")
        if failure_sink is not None:
            failure_sink["reason"] = "request_error"
        note_fetch_failure(source_domain, "request_error", "requests")
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Unexpected error fetching {url}: {exc}")
        if failure_sink is not None:
            failure_sink["reason"] = "unexpected_error"
        note_fetch_failure(source_domain, "unexpected_error", "requests")
    return None


def _selenium_wait_selectors(domain: str, mode: str) -> tuple[str, ...]:
    search_selectors = {
        "bestbuy.com": (
            "li.sku-item, .sku-item, li.product-list-item, .product-list-item, "
            "a.product-list-item-link[href], [data-testid='shop-product-card'], "
            "a.sku-title[href*='/product/']",
        ),
        "walmart.com": (
            "a[href*='/ip/'], [data-testid='product-title'], [data-item-id]",
        ),
        "target.com": (
            "[data-test='product-title'], [data-test='product-card'], a[href*='/p/']",
        ),
        "costco.com": (
            "a[href*='.product.'], [automation-id='productList'] a, .product",
        ),
        "homedepot.com": (
            "div[data-testid='product-pod'], a[href*='/p/']",
        ),
        "lowes.com": (
            "[data-test='product-tile'], a[href*='/pd/']",
        ),
    }
    product_selectors = (
        "h1",
        '[data-automation="product-title"]',
        '[data-test="product-title"]',
        '[data-testid*="product-title"]',
        '[itemprop="name"]',
    )
    if mode in {"discover_deals", "strict_search", "probe_light_js"}:
        return search_selectors.get(domain, ("a[href], h1",))
    return product_selectors


def _wait_for_any_selector(driver, selectors: tuple[str, ...], timeout: int = 15) -> None:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    for selector in selectors:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return
        except Exception:
            continue


def _mode_needs_scroll(mode: str) -> bool:
    return mode in {"discover_deals", "strict_search", "probe_light_js"}


def _selenium_mode_profile(domain: str, mode: str) -> dict[str, Any]:
    search_target = 10 if domain in {"bestbuy.com", "walmart.com"} else 6
    profile = {
        "body_timeout": 15,
        "selector_timeout": 10 if mode in {"discover_deals", "strict_search"} else 12,
        "scroll_enabled": _mode_needs_scroll(mode),
        "scroll_passes": 5,
        "target_cards": search_target,
    }
    if mode == "probe_light_js":
        profile.update({
            "body_timeout": 8,
            "selector_timeout": 4,
            "scroll_passes": 2,
            "target_cards": 6 if domain in {"bestbuy.com", "walmart.com"} else 4,
        })
    elif mode == "warmup":
        profile.update({
            "body_timeout": 8,
            "selector_timeout": 0,
            "scroll_enabled": False,
            "scroll_passes": 0,
            "target_cards": 0,
        })
    return profile


def _progressive_scroll(
    driver,
    domain: str,
    selector: str | None,
    *,
    max_passes: int = 5,
    target_cards: int | None = None,
) -> None:
    from selenium.webdriver.support.ui import WebDriverWait

    target_cards = target_cards or (10 if domain in {"bestbuy.com", "walmart.com"} else 6)
    prev_height = 0
    prev_count = 0
    for _ in range(max_passes):
        try:
            current_height = int(
                driver.execute_script(
                    "return Math.max(document.body.scrollHeight || 0, document.documentElement.scrollHeight || 0, 0)"
                )
            )
        except Exception:
            break
        if selector:
            try:
                current_count = int(
                    driver.execute_script("return document.querySelectorAll(arguments[0]).length", selector)
                )
            except Exception:
                current_count = 0
        else:
            current_count = 0
        if current_count >= target_cards and current_height <= prev_height:
            break
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        try:
            WebDriverWait(driver, 4).until(
                lambda d: (
                    int(
                        d.execute_script(
                            "return Math.max(document.body.scrollHeight || 0, document.documentElement.scrollHeight || 0, 0)"
                        )
                    ) > current_height
                    or (
                        bool(selector)
                        and int(
                            d.execute_script(
                                "return document.querySelectorAll(arguments[0]).length",
                                selector,
                            )
                        ) > current_count
                    )
                )
            )
        except Exception:
            pass
        prev_height = current_height
        prev_count = current_count
        if selector and prev_count >= target_cards:
            break


def _fetch_soup_selenium(
    url: str,
    *,
    domain: str = "",
    debug_meta: dict | None = None,
) -> BeautifulSoup | None:
    global _LAST_SELENIUM_FAILURE
    _LAST_SELENIUM_FAILURE = None
    meta = debug_meta or {}
    mode_dbg = str(meta.get("mode", "selenium"))
    q_dbg = str(meta.get("query", ""))
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from webdriver_manager.chrome import ChromeDriverManager

        ua = random.choice(USER_AGENTS)
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(f"--user-agent={ua}")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=opts
        )
        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": "Object.defineProperty(navigator, 'webdriver', "
                    "{get: () => undefined})"
                },
            )
            driver.execute_cdp_cmd("Network.enable", {})
            effective_domain = (domain or urlparse(url).netloc or "").lower()
            if "walmart" in effective_domain or "bestbuy" in effective_domain:
                driver.execute_cdp_cmd(
                    "Network.setExtraHTTPHeaders",
                    {"headers": {"Referer": "https://www.google.com/"}},
                )
            driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})

            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(random.uniform(2, 3.5))
            try:
                driver.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', "
                    "{get: () => undefined})"
                )
            except Exception:
                pass

            if "walmart" in effective_domain:
                for _ in range(20):
                    if driver.execute_script("return document.readyState") == "complete":
                        break
                    time.sleep(0.5)
                time.sleep(random.uniform(5, 8))
                try:
                    h = int(
                        driver.execute_script(
                            "return Math.max("
                            "document.body.scrollHeight||0,"
                            "document.documentElement.scrollHeight||0, 8000)"
                        )
                    )
                except Exception:
                    h = 8000
                for frac in (0.2, 0.45, 0.7, 1.0):
                    driver.execute_script(
                        f"window.scrollTo(0, {max(0, int(h * frac))});"
                    )
                    time.sleep(random.uniform(0.9, 1.4))
                time.sleep(random.uniform(1.5, 2.5))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(random.uniform(1.2, 2.0))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(random.uniform(2, 3.5))

            elif "bestbuy" in effective_domain:
                time.sleep(random.uniform(2, 3))
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located(
                            (
                                By.CSS_SELECTOR,
                                "li.sku-item, .sku-item, "
                                "[data-testid='shop-product-card'], "
                                "a.sku-title[href*='/product/']",
                            )
                        )
                    )
                except Exception:
                    logging.info(
                        f"[{datetime.now()}] Best Buy: grid selectors not seen in time "
                        f"(continuing)"
                    )
                time.sleep(random.uniform(4, 6))
                driver.execute_script("window.scrollBy(0, 400)")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(random.uniform(1.5, 2.2))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(random.uniform(2.5, 4))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)

            elif any(
                s in effective_domain for s in ("target", "bhphotovideo")
            ):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1.5)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

            elif any(
                s in effective_domain
                for s in ("costco", "homedepot", "lowes")
            ):
                time.sleep(random.uniform(4, 7))
                for _ in range(5):
                    driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight)"
                    )
                    time.sleep(random.uniform(1.0, 2.0))
                time.sleep(random.uniform(2, 4))

            if "bhphotovideo" in effective_domain:
                time.sleep(2)

            page_source = driver.page_source
            body_len = len(page_source)

            if body_len < 50000:
                if "walmart" in effective_domain or "bestbuy" in effective_domain:
                    logging.warning(
                        f"[{datetime.now()}] Possible bot wall for "
                        f"{effective_domain}, body_len={body_len}"
                    )
                    _LAST_SELENIUM_FAILURE = {"reason": "bot_wall", "url": url}
                    dom_save = (
                        "walmart.com"
                        if "walmart" in effective_domain
                        else "bestbuy.com"
                    )
                    _debug_save_failure(
                        dom_save,
                        mode_dbg,
                        q_dbg,
                        "selenium",
                        "bot_wall",
                        url=url,
                        html=page_source,
                    )
                    return None
                logging.info(
                    f"[{datetime.now()}] Short HTML for {effective_domain}, "
                    f"body_len={body_len} — attempting parse anyway"
                )

            return BeautifulSoup(page_source, "html.parser")
        finally:
            driver.quit()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Selenium error for {url}: {exc}")
        _LAST_SELENIUM_FAILURE = {
            "reason": "selenium_error",
            "url": url,
            "error": str(exc),
        }
    return None


def _fetch_soup_selenium_pooled(
    url: str,
    *,
    domain: str = "",
    debug_meta: dict | None = None,
    failure_sink: dict[str, str] | None = None,
) -> BeautifulSoup | None:
    global _LAST_SELENIUM_FAILURE
    _LAST_SELENIUM_FAILURE = None
    soup = _BROWSER_POOL.fetch_soup(
        url,
        domain=domain,
        debug_meta=debug_meta,
        failure_sink=failure_sink,
    )
    if soup is None and failure_sink and failure_sink.get("reason"):
        _LAST_SELENIUM_FAILURE = {
            "reason": failure_sink["reason"],
            "url": url,
        }
    return soup


# ---------------------------------------------------------------------------
# Per-site search result extractors
# ---------------------------------------------------------------------------

def _abs(href: str, base: str) -> str:
    if href and not href.startswith("http"):
        return urljoin(base, href)
    return href or ""


def _canonical_listing_url(url: str) -> str:
    if not url:
        return ""
    raw = (url or "").strip()
    if not raw:
        return ""

    if "/ref=" in raw:
        raw = raw.split("/ref=", 1)[0]

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    if "bestbuy.com" in host or raw.startswith("/product/") or raw.startswith("/site/"):
        return _canonical_bestbuy_listing_url(raw)
    if "officedepot.com" in host or "/a/products/" in raw:
        return _canonical_officedepot_listing_url(raw)

    return raw.split("?")[0].split("#")[0].rstrip("/")


def canonicalize_listing_url(url: str) -> str:
    return _canonical_listing_url((url or "").strip())


def _canonical_bestbuy_listing_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").strip()
    if not host:
        host = "www.bestbuy.com"
    elif host == "bestbuy.com":
        host = "www.bestbuy.com"

    path = re.sub(r"/+", "/", path).rstrip("/")
    if "/ref=" in path:
        path = path.split("/ref=", 1)[0].rstrip("/")

    keep_query: list[tuple[str, str]] = []
    if path.startswith("/site/"):
        keep_query = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=False)
            if key.lower() == "skuid" and value
        ]

    return urlunparse(
        (
            "https",
            host,
            path,
            "",
            urlencode(keep_query, doseq=True),
            "",
        )
    )


def _bestbuy_extract_sku_hint(*nodes) -> str | None:
    for node in nodes:
        if not node:
            continue
        scan_nodes = [node]
        parent = getattr(node, "parent", None)
        for _ in range(6):
            if not parent or getattr(parent, "name", None) == "body":
                break
            scan_nodes.append(parent)
            parent = getattr(parent, "parent", None)

        candidates = []
        for scan_node in scan_nodes:
            candidates.extend([
                scan_node.get("data-sku-id"),
                scan_node.get("data-product-id"),
                scan_node.get("data-sku"),
                scan_node.get("data-testid"),
                scan_node.get("sku"),
            ])
        for child in node.select("[data-sku-id], [data-product-id], [data-sku], [data-testid], [sku]"):
            candidates.extend([
                child.get("data-sku-id"),
                child.get("data-product-id"),
                child.get("data-sku"),
                child.get("data-testid"),
                child.get("sku"),
            ])
        for candidate in candidates:
            if not candidate:
                continue
            match = re.search(r"\b(\d{5,12})\b", str(candidate))
            if match:
                return match.group(1)
    return None


def _bestbuy_canonicalize_extracted_url(url: str, *nodes) -> str:
    canonical = _canonical_bestbuy_listing_url(url)
    if not canonical:
        return canonical
    parsed = urlparse(canonical)
    path = parsed.path.rstrip("/")
    if not path.startswith("/product/") or "/sku/" in path:
        return canonical
    sku_hint = _bestbuy_extract_sku_hint(*nodes)
    if not sku_hint:
        return canonical
    return urlunparse(("https", "www.bestbuy.com", f"{path}/sku/{sku_hint}", "", "", ""))


def _canonical_officedepot_listing_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if not host:
        host = "www.officedepot.com"
    elif host == "officedepot.com":
        host = "www.officedepot.com"

    path = re.sub(r"/+", "/", (parsed.path or "").strip())
    path = re.sub(r"/;jsessionid=[^/?#]+", "", path, flags=re.I)
    path = path.rstrip("/")
    if not path:
        path = "/"
    return urlunparse(("https", host, path, "", "", ""))


_DIRECT_NON_PRODUCT_PATHS = (
    re.compile(r"/(?:s|search)(?:[/?]|$)", re.I),
    re.compile(r"/(?:cart|gp/cart|checkout|account|login|signin)(?:[/?]|$)", re.I),
    re.compile(r"/(?:registry|wishlist|lists?|stores?|collections?|browse|deals?)(?:[/?]|$)", re.I),
)
_DIRECT_NON_PRODUCT_TITLE = re.compile(
    r"\b(search results|shopping cart|sign in|sign-in|wishlist|your list|deals? page|category)\b",
    re.I,
)


def _looks_like_non_product_url(url: str) -> bool:
    path = urlparse(url).path or "/"
    if path in {"", "/"}:
        return True
    return any(pattern.search(path) for pattern in _DIRECT_NON_PRODUCT_PATHS)


def inspect_direct_link(
    url: str,
    *,
    source: dict | None = None,
    context: SearchExecutionContext | None = None,
) -> dict[str, Any]:
    canonical_url = canonicalize_listing_url(url)
    domain = urlparse(canonical_url).netloc.lower().replace("www.", "")
    if not canonical_url or not domain:
        return {"ok": False, "reason": "invalid_url", "url": canonical_url, "domain": domain}
    if _looks_like_non_product_url(canonical_url):
        return {"ok": False, "reason": "not_product_url", "url": canonical_url, "domain": domain}

    source_domain = (source or {}).get("domain") or domain
    soup, fetch_method = _fetch_listing_soup(canonical_url, source_domain, context=context)
    if not soup:
        return {
            "ok": False,
            "reason": "fetch_failed",
            "url": canonical_url,
            "domain": domain,
            "fetch_method": fetch_method,
        }

    bootstrap_fingerprint = fingerprint_listing_document(
        canonical_url,
        soup,
        current_price=None,
        family_hint=None,
    )
    title = clean_listing_title(bootstrap_fingerprint.title or "")
    if not title or len(title) < 6 or _DIRECT_NON_PRODUCT_TITLE.search(title):
        return {
            "ok": False,
            "reason": "weak_listing",
            "url": canonical_url,
            "domain": domain,
            "title": title,
            "price": None,
            "fetch_method": fetch_method,
        }

    spec = parse_product_spec(title)
    primary_price_hint = extract_primary_price_from_soup(soup, condition_hint_text=title)
    price = _extract_price_from_soup_compat(
        soup,
        price_hint=primary_price_hint,
        condition_hint_text=title,
    )
    if price is None:
        return {
            "ok": False,
            "reason": "price_not_found",
            "url": canonical_url,
            "domain": domain,
            "title": title,
            "fetch_method": fetch_method,
        }

    fingerprint = fingerprint_listing_document(
        canonical_url,
        soup,
        current_price=price,
        family_hint=spec.family,
    )
    verification = verify_listing(spec, fingerprint)
    return {
        "ok": verification.status in {"verified", "ambiguous"},
        "reason": verification.reason,
        "url": canonical_url,
        "domain": domain,
        "title": title,
        "price": price,
        "spec": spec,
        "verification": verification,
        "fetch_method": fetch_method,
    }


def _dedupe_rows_by_url(rows: list[dict], url_key: str) -> list[dict]:
    seen: set[str] = set()
    out = []
    for r in rows:
        u = _canonical_listing_url(r.get(url_key) or "")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(r)
    return out


def _amazon_search_url_more_results(url: str) -> str:
    if "num=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}num=50"


def _amazon_page2_soup(
    page2_url: str,
    *,
    context: SearchExecutionContext | None = None,
) -> BeautifulSoup | None:
    """Try HTTP first; Selenium fallback when Amazon throttles page-2 requests."""
    cache_key = f"search:{page2_url}"
    if context:
        cached = context.get_fetch_entry(cache_key)
        if cached is not None:
            return cached.soup
    failure_sink: dict[str, str] = {}
    soup2 = _fetch_soup(
        page2_url,
        debug_domain="amazon.com",
        failure_sink=failure_sink,
        context=context,
    )
    if soup2:
        if context:
            context.set_fetch_entry(cache_key, FetchCacheEntry(soup2, "requests"))
        return soup2
    logging.info(f"[{datetime.now()}] Amazon page 2: HTTP failed, trying Selenium")
    selenium_failure: dict[str, str] = {}
    soup2 = _fetch_soup_selenium_pooled(
        page2_url,
        domain="amazon.com",
        debug_meta={"mode": "discover_deals", "query": page2_url},
        failure_sink=selenium_failure,
    )
    if context:
        context.set_fetch_entry(
            cache_key,
            FetchCacheEntry(soup2, "selenium" if soup2 else "fetch_failed", selenium_failure.get("reason") or failure_sink.get("reason")),
        )
    return soup2


def _missing_name_count(rows: list[dict], key: str, min_chars: int = 2) -> int:
    return sum(
        1 for r in rows
        if not (r.get(key) or "").strip() or len((r.get(key) or "").strip()) <= min_chars
    )


def _missing_price_count(rows: list[dict], key: str) -> int:
    return sum(1 for r in rows if r.get(key) is None)


def _store_discovery_stats(
    domain: str,
    mode: str,
    *,
    scraped_pre_dedupe: int,
    post_dedupe: int,
    returned: int,
    missing_name: int,
    missing_price: int,
    fetch_method: str = "unknown",
    failure_reason: str = "ok",
) -> None:
    dup_removed = max(0, scraped_pre_dedupe - post_dedupe)
    stats = {
        "mode": mode,
        "domain": domain,
        "scraped_count": scraped_pre_dedupe,
        "after_dedupe_count": post_dedupe,
        "duplicates_removed": dup_removed,
        "returned_count": returned,
        "missing_name_rows": missing_name,
        "missing_price_rows": missing_price,
        "fetch_method": fetch_method,
        "failure_reason": failure_reason,
    }
    LAST_DISCOVERY_STATS[f"{domain}::{mode}"] = stats
    logging.info(
        f"[STATS] {mode} {domain} "
        f"scraped={scraped_pre_dedupe} "
        f"after_dedupe={post_dedupe} "
        f"dup_removed={dup_removed} "
        f"returned={returned} "
        f"missing_name={missing_name} "
        f"missing_price={missing_price} "
        f"fetch_method={fetch_method} "
        f"failure_reason={failure_reason}"
    )


def _amazon_item_title(item, link) -> str:
    brand = ""
    brand_el = item.select_one("h2.a-size-mini span.a-size-medium.a-color-base")
    if brand_el:
        brand = brand_el.get_text(" ", strip=True)

    for sel in (
        ".s-title-instructions-style a.a-link-normal",
        "a.a-link-normal.s-line-clamp-2",
        "a.a-link-normal.s-line-clamp-3",
        "a.a-link-normal.a-text-normal",
    ):
        el = item.select_one(sel)
        if not el:
            continue
        t = el.get_text(" ", strip=True)
        if len(t) > 8:
            if brand and not t.lower().startswith(brand.lower()):
                return f"{brand} {t}".strip()
            return t
    for sel in (
        "h2 span.a-text-normal",
        "h2 span.a-size-medium.a-color-base",
        "h2 span.a-size-base-plus",
        "h2 span.a-size-medium",
        "h2 .a-text-normal",
        "h2",
    ):
        el = item.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if len(t) > 8:
                return t
    if link:
        aria = (link.get("aria-label") or "").strip()
        if len(aria) > 8:
            return aria
        t = link.get_text(" ", strip=True)
        if len(t) > 8:
            return t
    if brand:
        return brand
    return ""


def _target_item_title(item, link) -> str:
    el = item.select_one('[data-test="product-title"]')
    if el:
        t = el.get_text(" ", strip=True)
        if len(t) > 2:
            return t
    if link:
        for sel in ("span", "div"):
            inner = link.select_one(sel)
            if inner:
                t = inner.get_text(" ", strip=True)
                if len(t) > 2:
                    return t
        img = link.select_one("img[alt]")
        if img and img.get("alt"):
            alt = img["alt"].strip()
            if len(alt) > 3 and "target" not in alt.lower():
                return alt
        aria = (link.get("aria-label") or "").strip()
        if len(aria) > 2:
            return aria
        t = link.get_text(" ", strip=True)
        if len(t) > 2:
            return t
    h = item.select_one("h3, h2")
    if h:
        t = h.get_text(strip=True)
        if len(t) > 2:
            return t
    return ""


def _find_walmart_listing_root(link_tag, max_hops: int = 28):
    """Walk ancestors until a likely product tile (data-item-id or tile root)."""
    n = link_tag
    for _ in range(max_hops):
        if n is None or getattr(n, "name", None) == "body":
            break
        if n.get("data-item-id"):
            return n
        aid = (n.get("data-automation-id") or "").lower()
        if "product" in aid and "tile" in aid:
            return n
        if n.get("data-testid") == "list-view-item":
            return n
        n = n.parent
    n = link_tag
    for _ in range(6):
        if n is None:
            break
        n = n.parent
    return n


def _walmart_extract_price(root) -> float | None:
    """
    Walmart splits the price across separate DOM elements:
      <span class="price-characteristic">168</span>   ← dollars
      <span class="price-mantissa">00</span>          ← cents (no dot!)

    Grabbing the container div with get_text() concatenates them as "16800"
    which clean_price then parses as 16800.00.  Never read the container raw.

    Strategy (in priority order):
      1. itemprop="price" content attribute — exact decimal string
      2. itemprop="price" text — complete price like "$168.00"
      3. characteristic + mantissa parsed separately and joined with a "."
      4. $X.XX regex over the container text (regex requires a dot)
    """
    if not root:
        return None

    # 1 & 2 — itemprop attribute / text (most reliable)
    for sel in ('span[itemprop="price"]', '[itemprop="price"]'):
        el = root.select_one(sel)
        if el:
            p = clean_price(el.get("content"))
            if p:
                return p
            p = clean_price(el.get_text())
            if p:
                return p

    # 3 — parse characteristic (dollars) + mantissa (cents) separately
    price_wrap = root.select_one('[data-automation-id="product-price"]')
    scope = price_wrap or root
    char_el = scope.select_one('[class*="price-characteristic"]')
    mant_el = scope.select_one('[class*="price-mantissa"]')
    if char_el:
        char_txt = re.sub(r"[^\d]", "", char_el.get_text())
        mant_txt = re.sub(r"[^\d]", "", mant_el.get_text() if mant_el else "")
        if char_txt:
            decimal_str = f"{char_txt}.{mant_txt}" if mant_txt else char_txt
            p = clean_price(decimal_str)
            if p:
                return p

    # 4 — require a "$X.XX" pattern so we never accidentally parse "16800"
    if scope:
        p = _first_dollar_price_in_text(scope.get_text(" ", strip=True))
        if p:
            return p

    return None


def _bestbuy_is_product_listing_url(url: str) -> bool:
    """PLP/PDP links: legacy /site/, /p/, or Next.js /product/.../sku/..."""
    if not url:
        return False
    u = url.lower()
    if "/site/" in u:
        return True
    if "/product/" in u and "/sku/" in u:
        return True
    if "bestbuy.com/product/" in u or u.startswith("/product/"):
        return True
    if "bestbuy.com" in u and "/p/" in u:
        return True
    return False


def _bestbuy_price_from_text_tile(text: str) -> float | None:
    """First plausible shelf price from visible text (Next.js PLP often omits old price nodes)."""
    for m in re.finditer(r"\$\s*[\d,]+\.\d{2}", text):
        p = clean_price(m.group(0))
        if p and 1.0 <= p <= 25_000:
            return p
    return None


def _bestbuy_price_from_plp_anchor(anchor) -> float | None:
    """Walk up from title link; prefer a small ancestor so we do not grab cart totals."""
    n = anchor
    for _ in range(22):
        n = n.parent if n else None
        if n is None or getattr(n, "name", None) == "body":
            break
        txt = n.get_text(" ", strip=True)
        if len(txt) > 3800:
            continue
        p = _bestbuy_price_from_text_tile(txt)
        if p:
            return p
    n = anchor
    for _ in range(30):
        n = n.parent if n else None
        if n is None:
            break
        p = _bestbuy_price_from_text_tile(n.get_text(" ", strip=True))
        if p:
            return p
    return None


def _bestbuy_title_link_from_tile(item) -> Tag | None:
    """Product title <a> inside a tile (old + new PLP)."""
    for sel in (
        "a.product-list-item-link[href]",
        ".product-title a[href]",
        "a.sku-title[href]",
        "h4.sku-title a[href]",
        ".sku-header a[href]",
        "a[class*='sku-title'][href]",
        "a[href*='/product/'][href*='/sku/']",
        "a[href*='/site/'][href*='.p']",
    ):
        a = item.select_one(sel)
        if a and a.get("href"):
            return a
    return None


def _bestbuy_price_from_item(item) -> float | None:
    sels = (
        ".priceView-customer-price span",
        ".priceView-hero-price span",
        "[data-testid='customer-price'] span",
        "[data-testid='customer-price']",
        "div[class*='priceView'] span[aria-hidden='true']",
        ".pricing-price__regular-price",
        "span[class*='priceView']",
        ".priceView-layout-large .priceView-customer-price",
        "div[data-testid='price-block'] span",
        ".customer-price",
        "span.customer-price",
        "div.customer-price",
        "[class*='customer-price']",
    )
    for sel in sels:
        el = item.select_one(sel)
        if el:
            p = clean_price(el.get_text())
            if p:
                return p
    return None


def _iter_bestbuy_product_nodes(soup):
    """Yield unique product row elements (handles class/DOM changes)."""
    selectors = (
        ".sku-item",
        "li.sku-item",
        ".product-list-item",
        "li.product-list-item",
        "[class*='sku-item']",
        "[data-testid='shop-product-card']",
        "div[data-testid='product-card']",
        "li[class*='productCard']",
    )
    seen: set[int] = set()
    for sel in selectors:
        for node in soup.select(sel):
            i = id(node)
            if i in seen:
                continue
            seen.add(i)
            yield node


def _iter_bestbuy_title_anchors(soup):
    """New PLP: title links that may sit outside legacy .sku-item tiles."""
    seen: set[int] = set()
    for sel in (
        "a.product-list-item-link[href]",
        'a.sku-title[href*="/product/"]',
        'a.sku-title[href*="/site/"]',
        'a[href*="bestbuy.com/product/"][href*="/sku/"]',
        'a[href*="/site/"][class*="product-list-item-link"]',
    ):
        for a in soup.select(sel):
            i = id(a)
            if i in seen:
                continue
            seen.add(i)
            yield a


def _amazon_listing_original_price(item) -> float | None:
    el = item.select_one(".a-price.a-text-price span.a-offscreen")
    if el:
        p = clean_price(el.get_text())
        if p:
            return p
    strike = item.select_one('[data-a-strike="true"]')
    if strike:
        so = strike.select_one("span.a-offscreen")
        if so:
            p = clean_price(so.get_text())
            if p:
                return p
        p = clean_price(strike.get_text())
        if p:
            return p
    for node in item.find_all(string=re.compile(r"List\s*Price", re.I)):
        parent = getattr(node, "parent", None)
        for _ in range(5):
            if parent is None:
                break
            p = _first_dollar_price_in_text(parent.get_text(" ", strip=True))
            if p:
                return p
            parent = getattr(parent, "parent", None)
    return None


_AMAZON_SRP_PRICE_NOISE = re.compile(
    r"(monthly|month|installment|payment|per\s+month|protection|warranty|shipping|delivery|add-?on|plan)",
    re.I,
)


def _amazon_price_node_context(node) -> str:
    parts: list[str] = []
    current = node
    for _ in range(2):
        if current is None:
            break
        if hasattr(current, "get"):
            attrs = " ".join(
                str(current.get(key, ""))
                for key in ("class", "id", "data-cy", "data-testid", "aria-label")
            )
            if attrs:
                parts.append(attrs)
        text_getter = getattr(current, "get_text", None)
        if callable(text_getter):
            text = text_getter(" ", strip=True)
            if text:
                parts.append(text)
        current = getattr(current, "parent", None)
    return " ".join(parts)


def _amazon_price_node_is_noise(node) -> bool:
    return bool(_AMAZON_SRP_PRICE_NOISE.search(_amazon_price_node_context(node)))


def _amazon_listing_price(item) -> float | None:
    preferred_selectors = (
        ".priceToPay .a-offscreen",
        ".a-price.priceToPay .a-offscreen",
        '[data-a-color="base"] .a-offscreen',
        '[data-a-size="xl"] .a-offscreen',
        '[data-a-size="l"] .a-offscreen',
    )
    seen_nodes: set[int] = set()

    for selector in preferred_selectors:
        for node in item.select(selector):
            seen_nodes.add(id(node))
            if _amazon_price_node_is_noise(node):
                continue
            price = clean_price(node.get_text())
            if price:
                return price

    for node in item.select(".a-price .a-offscreen"):
        if id(node) in seen_nodes:
            continue
        if _amazon_price_node_is_noise(node):
            continue
        price = clean_price(node.get_text())
        if price:
            return price

    return None


def _walmart_listing_original_price(root) -> float | None:
    if not root:
        return None
    for sel in (
        '[data-testid="was-price"]',
        '[class*="strike-through"]',
        '[class*="was-price"]',
        '[data-automation-id*="was"]',
        "s",
        "del",
    ):
        el = root.select_one(sel)
        if el:
            p = clean_price(el.get_text())
            if p:
                return p
    wrap = root.select_one('[data-automation-id="product-price"]') or root
    txt = wrap.get_text(" ", strip=True) if wrap else ""
    dollar_prices: list[float] = []
    for m in re.finditer(r"\$\s*[\d,]+\.\d{2}", txt):
        cp = clean_price(m.group(0))
        if cp:
            dollar_prices.append(cp)
    if len(dollar_prices) >= 2:
        hi, lo = max(dollar_prices), min(dollar_prices)
        if hi > lo * 1.05:
            return hi
    return None


def _target_listing_original_price(item) -> float | None:
    el = item.select_one(
        '[data-test="product-regular-price"], '
        '[data-testid="product-regular-price"], '
        '[data-test="comparison-price"], '
        '[data-testid="comparison-price"]'
    )
    if el:
        prices = _price_candidates_from_raw(el.get_text(" ", strip=True))
        if prices:
            current_hint = _target_current_price(item)
            if current_hint is not None:
                higher = [price for price in prices if price > current_hint * 1.02]
                if higher:
                    return min(higher)
            return max(prices)
    for span in item.select("span"):
        t = span.get_text(" ", strip=True).lower()
        if re.search(r"\breg\.?\b", t):
            prices = _price_candidates_from_raw(span.get_text(" ", strip=True))
            if prices:
                current_hint = _target_current_price(item)
                if current_hint is not None:
                    higher = [price for price in prices if price > current_hint * 1.02]
                    if higher:
                        return min(higher)
                return max(prices)
            par = span.parent
            if par:
                prices = _price_candidates_from_raw(par.get_text(" ", strip=True))
                if prices:
                    current_hint = _target_current_price(item)
                    if current_hint is not None:
                        higher = [price for price in prices if price > current_hint * 1.02]
                        if higher:
                            return min(higher)
                    return max(prices)
    return None


def _newegg_listing_original_price(item) -> float | None:
    el = item.select_one(
        ".price-was-data, li.price-was, [class*='price-was'], .price-was"
    )
    if el:
        p = clean_price(el.get_text())
        if p:
            return p
    return None


def _ebay_listing_original_price(item_scope) -> float | None:
    for sel in (".original-price", ".STRIKETHROUGH", ".s-item__price--previous"):
        el = item_scope.select_one(sel)
        if el:
            p = clean_price(el.get_text())
            if p:
                return p
    return None


def _ebay_original_from_link(link, max_hops: int = 22) -> float | None:
    p = link
    for _ in range(max_hops):
        if p is None:
            break
        o = _ebay_listing_original_price(p)
        if o:
            return o
        p = p.parent
    return None


def _bestbuy_listing_original_price(scope) -> float | None:
    if not scope:
        return None
    orig_el = scope.select_one(
        '.pricing-price__regular-price, [data-testid="regular-price"], '
        '[class*="regular-price"], .pricing-price__was-price, '
        's.regular-price, .customer-price.strike-through'
    )
    if orig_el:
        p = clean_price(orig_el.get_text())
        if p:
            return p
    t = scope.get_text(" ", strip=True)
    comp_match = re.search(
        r"(?:comp\.?\s*value|comparable value)\s*:?\s*\$?\s*([\d,]+\.\d{2})",
        t,
        re.I,
    )
    if comp_match:
        p = clean_price(comp_match.group(1))
        if p:
            return p
    if t and re.search(r"\bwas\b", t, re.I):
        p = _first_dollar_price_in_text(t)
        if p:
            return p
    return None


def _bestbuy_one_row(
    link: Tag,
    tile: Tag | None,
    base: str,
) -> tuple[str, float, str, float | None] | None:
    href = _abs(link.get("href", ""), base)
    if not _bestbuy_is_product_listing_url(href):
        return None
    href = _bestbuy_canonicalize_extracted_url(href, tile, link)
    name = link.get_text(" ", strip=True) or ""
    if len(name) < 3:
        inner = link.select_one("span.nc-product-title, span.line-clamp-3")
        if inner:
            name = inner.get_text(" ", strip=True)
    if len(name) < 3 and tile is not None:
        t_el = tile.select_one(".sku-title, .product-title, [data-testid='product-title']")
        if t_el:
            name = t_el.get_text(" ", strip=True)
    price = None
    if tile is not None:
        price = _bestbuy_price_from_item(tile)
    if not price:
        price = _bestbuy_price_from_plp_anchor(link)
    if not price or len(name.strip()) < 2:
        return None
    scope = tile if tile is not None else link.parent
    orig = _bestbuy_listing_original_price(scope) if scope else None
    return (name, price, href, orig)


def _extract_amazon_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    base = "https://www.amazon.com"
    raw_rows: list[dict] = []
    for item in soup.select('[data-component-type="s-search-result"]'):
        if len(raw_rows) >= max_items:
            break
        link = item.select_one('h2 a.a-link-normal')
        if not link:
            link = item.select_one('a.a-link-normal[href*="/dp/"]')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        href = href.split("/ref=")[0]
        name = _amazon_item_title(item, link)
        price = _amazon_listing_price(item)
        if not price or not href:
            continue
        orig = _amazon_listing_original_price(item)
        raw_rows.append({
            "url": href,
            "price": price,
            "name_found": name,
            "original_price": orig,
        })
    if not raw_rows:
        logging.warning(f"[{datetime.now()}] Amazon: 0 results extracted. "
                        f"Selectors found: s-search-result={len(soup.select('[data-component-type]'))}")
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Amazon",
        price_key="price",
        name_key="name_found",
    )


def _extract_bestbuy_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    base = "https://www.bestbuy.com"
    raw_rows: list[dict] = []
    seen: set[str] = set()

    def push(row: tuple[str, float, str, float | None] | None) -> None:
        if not row or len(raw_rows) >= max_items:
            return
        name, price, href, orig = row
        u = _canonical_listing_url(href)
        if not u or u in seen:
            return
        seen.add(u)
        raw_rows.append({
            "url": href,
            "price": price,
            "name_found": name[:500],
            "original_price": orig,
        })

    for item in _iter_bestbuy_product_nodes(soup):
        link = _bestbuy_title_link_from_tile(item)
        if link:
            push(_bestbuy_one_row(link, item, base))

    for link in _iter_bestbuy_title_anchors(soup):
        if len(raw_rows) >= max_items:
            break
        push(_bestbuy_one_row(link, None, base))

    if not raw_rows:
        for row in _iter_itemlist_jsonld_rows(soup, base, max_results=max_items):
            raw_rows.append({
                "url": row["product_url"],
                "price": row["current_price"],
                "name_found": row["product_name"],
                "original_price": row.get("original_price"),
            })

    if not raw_rows:
        logging.warning(
            f"[{datetime.now()}] Best Buy: 0 results extracted. "
            f"sku-items={len(soup.select('.sku-item'))}, "
            f"sku-title-product={len(soup.select('a.sku-title[href*=\"/product/\"]'))}, "
            f"body_len={len(soup.get_text())}"
        )
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Best Buy",
        price_key="price",
        name_key="name_found",
    )


def _extract_newegg_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    base = "https://www.newegg.com"
    raw_rows: list[dict] = []
    for item in soup.select('.item-cell, .item-container'):
        if len(raw_rows) >= max_items:
            break
        link = item.select_one('a.item-title')
        if not link:
            link = item.select_one('.item-info a')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        name = link.get_text(strip=True)
        price_el = item.select_one('li.price-current')
        price = clean_price(price_el.get_text() if price_el else None)
        if not price or not href:
            continue
        orig = _newegg_listing_original_price(item)
        raw_rows.append({
            "url": href,
            "price": price,
            "name_found": name,
            "original_price": orig,
        })
    if not raw_rows:
        logging.warning(f"[{datetime.now()}] Newegg: 0 results extracted. "
                        f"item-cells={len(soup.select('.item-cell'))}")
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Newegg",
        price_key="price",
        name_key="name_found",
    )


def _extract_walmart_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    base = "https://www.walmart.com"
    raw_rows: list[dict] = []
    seen = set()
    for link_tag in soup.select(
        'a[href*="/ip/"], a.product-title-link, '
        '[data-item-id] a[href*="/ip/"]'
    ):
        if len(raw_rows) >= max_items:
            break
        href = _abs(link_tag.get("href", ""), base)
        if href in seen:
            continue
        seen.add(href)
        name = link_tag.get_text(" ", strip=True) or ""
        root = _find_walmart_listing_root(link_tag)
        if not root:
            continue
        price = _walmart_extract_price(root)
        if not price or not href:
            continue
        orig = _walmart_listing_original_price(root)
        raw_rows.append({
            "url": href,
            "price": price,
            "name_found": name[:200],
            "original_price": orig,
        })
    if not raw_rows:
        for row in _iter_itemlist_jsonld_rows(soup, base, max_results=max_items):
            raw_rows.append({
                "url": row["product_url"],
                "price": row["current_price"],
                "name_found": row["product_name"][:200],
                "original_price": row.get("original_price"),
            })
    if not raw_rows:
        logging.warning(f"[{datetime.now()}] Walmart: 0 results extracted. "
                        f"ip-links={len(soup.select('a[href*=\"/ip/\"]'))}, "
                        f"body_len={len(soup.get_text())}")
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Walmart",
        price_key="price",
        name_key="name_found",
    )


def _ebay_normalize_itm_url(href: str) -> str:
    if not href or "/itm/" not in href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("/"):
        href = "https://www.ebay.com" + href
    if "ebay.com" not in href.lower():
        return ""
    return href.split("?")[0].split("#")[0]


def _ebay_listing_price(item_scope, link) -> float | None:
    for sel in (
        ".s-item__price",
        ".s-card__price",
        "[class*='__price']",
        "span[class*='price']",
    ):
        el = item_scope.select_one(sel)
        if el:
            p = clean_price(el.get_text())
            if p:
                return p
    p = _first_dollar_price_in_text(item_scope.get_text(" ", strip=True))
    if p:
        return p
    return _ebay_listing_price_from_ancestors(link)


def _ebay_listing_price_from_ancestors(link, max_hops: int = 26) -> float | None:
    p = link
    for _ in range(max_hops):
        p = p.parent if p else None
        if p is None:
            break
        pr = _first_dollar_price_in_text(p.get_text(" ", strip=True))
        if pr:
            return pr
    return None


def _extract_ebay_listings(soup, max_n: int) -> list[dict]:
    """SRP markup varies; combine card selectors with /itm/ link fallback."""
    seen: set[str] = set()
    out: list[dict] = []

    def push(
        name: str,
        price: float | None,
        href: str,
        *,
        item_scope=None,
        link_for_orig=None,
    ) -> None:
        if len(out) >= max_n or not price or not name or len(name.strip()) < 2:
            return
        if name.lower().startswith("shop on ebay"):
            return
        u = _ebay_normalize_itm_url(href)
        if not u or not re.search(r"/itm/\d+", u) or u in seen:
            return
        seen.add(u)
        orig = None
        if item_scope is not None:
            orig = _ebay_listing_original_price(item_scope)
        if orig is None and link_for_orig is not None:
            orig = _ebay_original_from_link(link_for_orig)
        out.append({
            "url": u,
            "price": price,
            "name_found": name[:500],
            "original_price": orig,
        })

    cards = soup.select(".s-item, li.s-card, div[class*='s-card']")
    for item in cards:
        if len(out) >= max_n:
            break
        link = item.select_one(
            "a.s-item__link, .s-item__link, a[href*='/itm/']"
        )
        if not link:
            continue
        href = link.get("href", "")
        name_el = item.select_one(
            ".s-item__title, .s-card__title, [role='heading'], "
            "h3, .s-card__subtitle"
        )
        name = (
            name_el.get_text(" ", strip=True)
            if name_el
            else link.get_text(" ", strip=True)
        )
        price = _ebay_listing_price(item, link)
        push(name, price, href, item_scope=item, link_for_orig=link)

    if len(out) < max_n:
        for link in soup.select("a[href*='/itm/']"):
            if len(out) >= max_n:
                break
            href = link.get("href", "")
            u = _ebay_normalize_itm_url(href)
            if not u or u in seen:
                continue
            if not re.search(r"/itm/\d+", u):
                continue
            name = (
                link.get_text(" ", strip=True)
                or (link.get("title") or "")
                or (link.get("aria-label") or "")
            )
            if len(name) < 4:
                continue
            price = _ebay_listing_price_from_ancestors(link)
            push(name, price, href, link_for_orig=link)

    if not out:
        logging.warning(
            f"[{datetime.now()}] eBay: 0 results; "
            f"s-item={len(soup.select('.s-item'))}, "
            f"itm_a={len(soup.select('a[href*=\"/itm/\"]'))}"
        )
    return out


def _extract_ebay_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    raw_rows = _extract_ebay_listings(soup, max_items)
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="eBay",
        price_key="price",
        name_key="name_found",
    )


def _extract_target_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
    apply_quality_pipeline: bool = True,
):
    base = "https://www.target.com"
    raw_rows: list[dict] = []
    seen = set()

    for item in soup.select(
        '[data-test="product-details"], '
        '[data-test="@web/site-top-of-funnel/ProductCardWrapper"]'
    ):
        if len(raw_rows) >= max_items:
            break
        link = item.select_one('a[data-test="product-title"], a[href*="/p/"]')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        if href in seen:
            continue
        seen.add(href)
        name = _target_item_title(item, link)
        price = _target_current_price(item)
        if not price or not href:
            continue
        orig = _target_listing_original_price(item)
        raw_rows.append({
            "url": href,
            "price": price,
            "name_found": name[:200],
            "original_price": orig,
        })

    if not raw_rows:
        for link_tag in soup.select('a[href*="/p/"]'):
            if len(raw_rows) >= max_items:
                break
            href = _abs(link_tag.get("href", ""), base)
            if href in seen:
                continue
            seen.add(href)
            parent = link_tag
            item = link_tag.parent
            for _ in range(12):
                parent = parent.parent
            if parent is None:
                break
            if parent.get("data-test") == "product-details":
                item = parent
                break
            name = _target_item_title(item, link_tag)
            price = _target_current_price(item)
            if not price or not href:
                continue
            orig = _target_listing_original_price(item)
            raw_rows.append({
                "url": href,
                "price": price,
                "name_found": name[:200],
                "original_price": orig,
            })

    if not raw_rows:
        logging.warning(f"[{datetime.now()}] Target: 0 results extracted. "
                        f"product-details={len(soup.select('[data-test=\"product-details\"]'))}, "
                        f"p-links={len(soup.select('a[href*=\"/p/\"]'))}, "
                        f"body_len={len(soup.get_text())}")
    if not apply_quality_pipeline:
        return raw_rows
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Target",
        price_key="price",
        name_key="name_found",
    )


def _extract_costco_listings(soup, max_n: int) -> list[dict]:
    base = "https://www.costco.com"
    out: list[dict] = []
    seen: set[str] = set()

    tiles = soup.select('[data-testid^="ProductTile"]')
    for tile in tiles:
        if len(out) >= max_n:
            break
        link = tile.select_one('a[href*=".product."]')
        if not link:
            continue
        href = link.get("href", "")
        u = _canonical_listing_url(_abs(href, base))
        if not u or u in seen:
            continue
        title_el = tile.select_one(
            'h3, [data-testid*="title"], [data-testid*="Title"]'
        )
        name = (
            title_el.get_text(" ", strip=True)
            if title_el
            else link.get_text(" ", strip=True)
        )
        pel = tile.select_one(
            '[data-testid^="Text_Price"], [data-testid*="price"], '
            'span[class*="price"]'
        )
        price = clean_price(pel.get_text() if pel else None)
        if not price:
            price = _first_dollar_price_in_text(tile.get_text(" ", strip=True))
        if not price or len(name.strip()) < 2:
            continue
        seen.add(u)
        out.append({
            "url": _abs(href, base).split("?")[0],
            "price": price,
            "name_found": name[:500],
            "original_price": _extract_original_price(tile),
        })

    if len(out) < max_n:
        for a in soup.select('a[href*=".product."]'):
            if len(out) >= max_n:
                break
            href = a.get("href", "")
            u = _canonical_listing_url(_abs(href, base))
            if not u or u in seen:
                continue
            name = (
                a.get_text(" ", strip=True)
                or (a.get("aria-label") or "")
                or (a.get("title") or "")
            )
            price = None
            p = a.parent
            for _ in range(14):
                if p is None:
                    break
                price = _first_dollar_price_in_text(p.get_text(" ", strip=True))
                if price:
                    break
                p = p.parent
            if not price or len(name.strip()) < 2:
                continue
            seen.add(u)
            out.append({
                "url": _abs(href, base).split("?")[0],
                "price": price,
                "name_found": name[:500],
                "original_price": _extract_original_price(p) if p else None,
            })

    if not out:
        logging.warning(
            f"[{datetime.now()}] Costco: 0 results; "
            f"ProductTile={len(soup.select('[data-testid^=\"ProductTile\"]'))}, "
            f"product_links={len(soup.select('a[href*=\".product.\"]'))}"
        )
    return out


def _extract_costco_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    raw_rows = _extract_costco_listings(soup, max_items)
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Costco",
        price_key="price",
        name_key="name_found",
    )


def _extract_homedepot_listings(soup, max_n: int) -> list[dict]:
    base = "https://www.homedepot.com"
    out: list[dict] = []
    seen: set[str] = set()
    pods = soup.select(
        '[data-testid="product-pod"], [data-testid^="product-pod"], .product-pod'
    )
    for pod in pods:
        if len(out) >= max_n:
            break
        link = pod.select_one('a[href*="/p/"]')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        if not re.search(r"/p/[^/]+/\d", href):
            continue
        u = _canonical_listing_url(href)
        if not u or u in seen:
            continue
        name = link.get_text(" ", strip=True) or ""
        pel = pod.select_one(
            '[data-testid="product-price"], [data-testid*="price"], '
            '[class*="product-price"], [class*="price__"], span.price__numbers'
        )
        price = clean_price(pel.get_text() if pel else None)
        if not price:
            price = _first_dollar_price_in_text(pod.get_text(" ", strip=True))
        if not price or len(name.strip()) < 2:
            continue
        seen.add(u)
        out.append({
            "url": href.split("?")[0],
            "price": price,
            "name_found": name[:500],
            "original_price": _extract_original_price(pod),
        })

    if len(out) < max_n:
        for a in soup.select('a[href*="/p/"]'):
            if len(out) >= max_n:
                break
            href = _abs(a.get("href", ""), base)
            if not re.search(r"/p/[^/]+/\d", href):
                continue
            u = _canonical_listing_url(href)
            if not u or u in seen:
                continue
            name = (
                a.get_text(" ", strip=True)
                or (a.get("aria-label") or "")
                or ""
            )
            price = None
            p = a.parent
            for _ in range(16):
                if p is None:
                    break
                price = _first_dollar_price_in_text(p.get_text(" ", strip=True))
                if price:
                    break
                p = p.parent
            if not price or len(name.strip()) < 2:
                continue
            seen.add(u)
            out.append({
                "url": href.split("?")[0],
                "price": price,
                "name_found": name[:500],
                "original_price": _extract_original_price(p) if p else None,
            })

    if not out:
        logging.warning(
            f"[{datetime.now()}] Home Depot: 0 results; "
            f"pods={len(soup.select('[data-testid=product-pod]'))}, "
            f"p_links={len(soup.select('a[href*=\"/p/\"]'))}"
        )
    return out


def _extract_homedepot_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    raw_rows = _extract_homedepot_listings(soup, max_items)
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Home Depot",
        price_key="price",
        name_key="name_found",
    )


def _lowes_is_product_url(href: str) -> bool:
    """Product PDPs include a numeric id segment; category /pd/ hubs do not."""
    return bool(re.search(r"/pd/[^/]+/\d+", href, re.I))


def _extract_lowes_listings(soup, max_n: int) -> list[dict]:
    base = "https://www.lowes.com"
    out: list[dict] = []
    seen: set[str] = set()
    tiles = soup.select(
        '[data-selector="productTile"], [data-qa="productTile"], '
        '[data-testid*="product-tile"], div.product-tile'
    )
    for tile in tiles:
        if len(out) >= max_n:
            break
        link = tile.select_one('a[href*="/pd/"]')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        if not _lowes_is_product_url(href):
            continue
        u = _canonical_listing_url(href)
        if not u or u in seen:
            continue
        name = link.get_text(" ", strip=True) or ""
        pel = tile.select_one(
            '[data-testid="product-price"], [data-testid*="price"], '
            'span.artful-price, div[data-testid="price"]'
        )
        price = clean_price(pel.get_text() if pel else None)
        if not price:
            price = _first_dollar_price_in_text(tile.get_text(" ", strip=True))
        if not price or len(name.strip()) < 2:
            continue
        seen.add(u)
        out.append({
            "url": href.split("?")[0],
            "price": price,
            "name_found": name[:500],
            "original_price": _extract_original_price(tile),
        })

    if len(out) < max_n:
        for a in soup.select('a[href*="/pd/"]'):
            if len(out) >= max_n:
                break
            href = _abs(a.get("href", ""), base)
            if not _lowes_is_product_url(href):
                continue
            u = _canonical_listing_url(href)
            if not u or u in seen:
                continue
            name = (
                a.get_text(" ", strip=True)
                or (a.get("aria-label") or "")
                or ""
            )
            price = None
            p = a.parent
            for _ in range(16):
                if p is None:
                    break
                price = _first_dollar_price_in_text(p.get_text(" ", strip=True))
                if price:
                    break
                p = p.parent
            if not price or len(name.strip()) < 2:
                continue
            seen.add(u)
            out.append({
                "url": href.split("?")[0],
                "price": price,
                "name_found": name[:500],
                "original_price": _extract_original_price(p) if p else None,
            })

    if not out:
        logging.warning(
            f"[{datetime.now()}] Lowe's: 0 results; "
            f"productTile={len(soup.select('[data-selector=productTile]'))}, "
            f"pd_links={len(soup.select('a[href*=\"/pd/\"]'))}"
        )
    return out


def _extract_lowes_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    raw_rows = _extract_lowes_listings(soup, max_items)
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Lowe's",
        price_key="price",
        name_key="name_found",
    )


def _officedepot_is_product_url(href: str) -> bool:
    return bool(re.search(r"/a/products/\d+/", href or "", re.I))


def _officedepot_card_title(card: Tag) -> str:
    title_node = card.select_one('span[name="skuTitleGAData"]')
    if title_node and title_node.get("data-value"):
        return BeautifulSoup(title_node.get("data-value"), "html.parser").get_text(" ", strip=True)
    for link in card.select('a[title][href]'):
        title = (link.get("title") or "").strip()
        if title and _officedepot_is_product_url(link.get("href", "")):
            return title
    for link in card.select('a[href]'):
        if not _officedepot_is_product_url(link.get("href", "")):
            continue
        title = link.get_text(" ", strip=True)
        if len(title) >= 3:
            return title
    return ""


def _officedepot_price_from_card(card: Tag) -> float | None:
    selectors = (
        ".od-graphql-price-big-price",
        '[aria-describedby="price"]',
        '[class*="price-big-price"]',
        '[class*="sale-price"]',
    )
    for sel in selectors:
        el = card.select_one(sel)
        if el:
            price = clean_price(el.get_text(" ", strip=True))
            if price:
                return price
    return _first_dollar_price_in_text(card.get_text(" ", strip=True))


def _officedepot_original_price_from_card(card: Tag) -> float | None:
    selectors = (
        ".od-graphql-price-little-price",
        '[class*="price-little-price"]',
        '[class*="regular-price"]',
    )
    for sel in selectors:
        el = card.select_one(sel)
        if el:
            price = clean_price(el.get_text(" ", strip=True))
            if price:
                return price
    return _extract_original_price(card)


def _extract_officedepot_listings(soup, max_n: int) -> list[dict]:
    base = "https://www.officedepot.com"
    out: list[dict] = []
    seen: set[str] = set()
    cards = soup.select('[data-product-id].od-product-card, [data-product-id][pagetype="search"]')
    for card in cards:
        if len(out) >= max_n:
            break
        link = None
        for candidate in card.select('a[href]'):
            href = candidate.get("href", "")
            if _officedepot_is_product_url(href):
                link = candidate
                break
        if not link:
            continue
        href = _canonical_listing_url(_abs(link.get("href", ""), base))
        if not href or href in seen:
            continue
        name = clean_listing_title(_officedepot_card_title(card))
        price = _officedepot_price_from_card(card)
        if not price or len(name.strip()) < 2:
            continue
        seen.add(href)
        out.append({
            "url": href,
            "price": price,
            "name_found": name[:500],
            "original_price": _officedepot_original_price_from_card(card),
        })

    if not out:
        logging.warning(
            f"[{datetime.now()}] Office Depot: 0 results; "
            f"cards={len(cards)}, product_links={len(soup.select('a[href*=\"/a/products/\"]'))}"
        )
    return out


def _extract_officedepot_all(
    soup,
    max_items=MAX_RESULTS_PER_SOURCE,
    *,
    query: str = "",
    target_price: float | None = None,
):
    raw_rows = _extract_officedepot_listings(soup, max_items)
    return _apply_discovery_quality_pipeline(
        raw_rows,
        query=query,
        max_price=target_price,
        label="Office Depot",
        price_key="price",
        name_key="name_found",
    )


_SITE_EXTRACTORS = {
    "amazon.com":        _extract_amazon_all,
    "bestbuy.com":       _extract_bestbuy_all,
    "newegg.com":        _extract_newegg_all,
    "walmart.com":       _extract_walmart_all,
    "ebay.com":          _extract_ebay_all,
    "target.com":        _extract_target_all,
    "costco.com":        _extract_costco_all,
    "homedepot.com":     _extract_homedepot_all,
    "lowes.com":         _extract_lowes_all,
    "officedepot.com":   _extract_officedepot_all,
}


# ---------------------------------------------------------------------------
# Public API: discover_product
# ---------------------------------------------------------------------------

def _discover_product_rank_key(c: dict) -> tuple[float, float, float]:
    """Identity match, structural fit, then price (add-product discovery)."""
    im = float(c.get("_identity_match", 0.0))
    sr = float(c.get("_structural_relevance", 0.0))
    price = float(c["price"])
    return (-im, -sr, price)


def _pick_best(candidates: list[dict], target_price: float | None) -> list[dict]:
    """
    Rank eligible rows by structural fit, then price.

    Under target: all at-or-below-target rows, best match first (not raw min-price).
    Otherwise: one row — best match among all candidates, not merely the cheapest.
    """
    if not candidates:
        return []
    ranked = sorted(candidates, key=_discover_product_rank_key)
    if target_price is not None:
        under = [
            dict(c, status_hint="watching")
            for c in ranked
            if c["price"] <= target_price
        ]
        if under:
            return under
    best = ranked[0]
    out = dict(best)
    out["status_hint"] = "above_target" if target_price is not None else "watching"
    return [out]


def _filter_discover_candidates(
    candidates: list[dict],
    product_name: str,
    *,
    name_key: str = "name_found",
    url_key: str = "url",
) -> list[dict]:
    """
    Classify → ``passes_eligibility`` (discovery_filters).

    ``name_key`` / ``url_key`` map row dicts to titles and URLs (e.g. ``name_found``
    + ``url`` for ``discover_product`` extractors).

    Defaults mirror the Discover page: new_only + primary_only + exact brand.

    If every candidate fails eligibility, returns an empty list — callers should
    treat that as "no trustworthy match" (e.g. not_found), not show unfiltered junk.
    """
    if not candidates:
        return candidates

    try:
        from discovery_filters import (
            enrich_result_metadata,
            passes_eligibility,
            resolve_family_and_intent,
        )
    except ImportError:
        return candidates

    family, accessory_intent = resolve_family_and_intent(product_name)

    kept: list[dict] = []
    for c in candidates:
        # Build a temporary row in the format enrich_result_metadata expects
        row: dict = {
            "product_name": c.get(name_key) or "",
            "product_url": c.get(url_key) or "",
        }
        enrich_result_metadata(
            row, product_name, family=family, accessory_intent=accessory_intent
        )

        ok = passes_eligibility(
            row,
            condition_filter="new_only",
            product_filter="primary_only",
            brand_filter="exact",
            family=family,
            accessory_intent=accessory_intent,
            query_for_intent=product_name,
        )
        if ok:
            # Attach metadata for downstream logging / debugging
            c["_condition_class"] = row["condition_class"]
            c["_product_kind"] = row["product_kind"]
            c["_structural_relevance"] = row["structural_relevance"]
            c["_identity_match"] = row.get("identity_match", 0.0)
            c["_confidence"] = row.get("confidence", 0.0)
            c["_listing_role"] = row.get("listing_role", "primary_product")
            c["_enrich_query"] = row.get("_enrich_query", product_name)
            kept.append(c)
        else:
            logging.info(
                f"[{datetime.now()}] discovery eligibility excluded: "
                f"name={(c.get(name_key) or '')[:80]!r} "
                f"kind={row.get('product_kind')} "
                f"condition={row.get('condition_class')} "
                f"penalty={row.get('listing_penalty', 0):.2f}"
            )

    if kept:
        best_conf = max(float(x.get("_confidence", 0.0)) for x in kept)
        if best_conf < 0.3:
            logging.warning(
                f"[{datetime.now()}] discovery eligibility: best confidence "
                f"{best_conf:.2f} < 0.3 for {product_name!r} — rejecting batch"
            )
            return []
        logging.info(
            f"[{datetime.now()}] discovery eligibility: "
            f"{len(kept)}/{len(candidates)} candidates passed "
            f"(product={product_name!r})"
        )
        return kept

    logging.warning(
        f"[{datetime.now()}] discovery eligibility: ALL {len(candidates)} "
        f"candidates removed for {product_name!r} — returning empty (no eligible rows)."
    )
    return []


def _discover_product_bhphoto(
    product_name: str,
    target_price: float | None,
    search_url: str,
    fc: dict,
) -> list[dict]:
    """B&H product search uses the same multi-row extractor as deal discovery."""
    domain = "bhphotovideo.com"
    candidates: list[dict] = []
    fetch_method_used = "unknown"
    last_soup: BeautifulSoup | None = None

    soup = _fetch_soup(search_url, debug_domain=domain)
    if soup:
        last_soup = soup
        fetch_method_used = "requests"
        raw = _extract_bhphoto_multi(soup)
        candidates = [
            {
                "url": r["product_url"],
                "price": r["current_price"],
                "name_found": (r.get("product_name") or "")[:500],
                "discount_confirmed": _discount_confirmed(
                    r.get("original_price"), r["current_price"]
                ),
            }
            for r in raw
        ]
    if not candidates:
        soup = _fetch_soup_selenium_pooled(
            search_url, domain=domain, debug_meta=fc
        )
        if soup:
            last_soup = soup
            fetch_method_used = "selenium"
            raw = _extract_bhphoto_multi(soup)
            candidates = [
                {
                    "url": r["product_url"],
                    "price": r["current_price"],
                    "name_found": (r.get("product_name") or "")[:500],
                    "discount_confirmed": _discount_confirmed(
                        r.get("original_price"), r["current_price"]
                    ),
                }
                for r in raw
            ]

    scraped_pre_dedupe = len(candidates)
    candidates = _dedupe_rows_by_url(candidates, "url")
    post_dedupe = len(candidates)

    if not candidates:
        fr = "no_rows" if last_soup else _discovery_failure_reason_no_soup()
        _store_discovery_stats(
            domain,
            "discover_product",
            scraped_pre_dedupe=scraped_pre_dedupe,
            post_dedupe=post_dedupe,
            returned=0,
            missing_name=0,
            missing_price=0,
            fetch_method=fetch_method_used,
            failure_reason=fr,
        )
        return []

    # Stage 1 — eligibility gate (condition / accessory / brand)
    product_name_bh = fc.get("query", "")
    candidates = _filter_discover_candidates(candidates, product_name_bh)

    if not candidates:
        logging.warning(
            f"[{datetime.now()}] No eligible candidates for {product_name_bh!r} "
            f"on {domain} (all listings failed discovery filters)"
        )
        _store_discovery_stats(
            domain,
            "discover_product",
            scraped_pre_dedupe=scraped_pre_dedupe,
            post_dedupe=post_dedupe,
            returned=0,
            missing_name=0,
            missing_price=0,
            fetch_method=fetch_method_used,
            failure_reason="no_eligible_candidates",
        )
        return []

    filtered = _pick_best(candidates, target_price)
    logging.info(
        f"[{datetime.now()}] {domain}: returning {len(filtered)} results "
        f"(from {post_dedupe} deduped, {scraped_pre_dedupe} scraped)"
    )
    _store_discovery_stats(
        domain,
        "discover_product",
        scraped_pre_dedupe=scraped_pre_dedupe,
        post_dedupe=post_dedupe,
        returned=len(filtered),
        missing_name=_missing_name_count(filtered, "name_found"),
        missing_price=_missing_price_count(filtered, "price"),
        fetch_method=fetch_method_used,
        failure_reason="ok",
    )
    return filtered


def discover_product(product_name: str, source: dict,
                     target_price: float | None = None) -> list[dict]:
    """
    Search a retailer for *product_name* using the source's search_url_template.
    Collects up to MAX_RESULTS_PER_SOURCE results and returns ALL eligible
    candidates at or under target_price (best structural match first, then price).
    If none are at or under target, returns the single best-matching listing
    (structural fit, then price) with status_hint='above_target'.

    Each dict has: url, price, name_found, status_hint, and usually
    discount_confirmed (whether a ≥5% markdown was visible on the SRP).
    Returns [] when the source yields nothing, or every hit fails eligibility
    (accessory / condition / brand gates).
    """
    matches = discover_product_matches(
        product_name,
        source,
        target_price=target_price,
    )
    return matches["verified"]

    time.sleep(random.uniform(2, 5))

    global _LAST_REQUESTS_FAILURE, _LAST_SELENIUM_FAILURE
    _LAST_REQUESTS_FAILURE = None
    _LAST_SELENIUM_FAILURE = None

    query = quote_plus(product_name)
    search_url = source["search_url_template"].replace("{query}", query)
    domain = source["domain"]
    if domain == "amazon.com":
        search_url = _amazon_search_url_more_results(search_url)
    if domain == "walmart.com":
        search_url = _humanize_walmart_search_url(search_url)

    logging.info(f"[{datetime.now()}] Discovering '{product_name}' on {domain}")

    fc = {
        "domain": domain,
        "mode": "discover_product",
        "query": product_name,
    }

    if domain == "bhphotovideo.com":
        return _discover_product_bhphoto(
            product_name, target_price, search_url, fc
        )

    extractor = _SITE_EXTRACTORS.get(domain)
    if not extractor:
        logging.warning(f"[{datetime.now()}] No extractor for {domain}")
        _store_discovery_stats(
            domain, "discover_product",
            scraped_pre_dedupe=0, post_dedupe=0, returned=0,
            missing_name=0, missing_price=0,
            fetch_method="none",
            failure_reason="no_extractor",
        )
        return []

    candidates: list[dict] = []
    last_soup: BeautifulSoup | None = None
    fetch_method_used = "fetch_failed"

    use_selenium_first = domain in _SELENIUM_PREFERRED
    fc_wm_bb = fc if domain in ("walmart.com", "bestbuy.com") else None
    ext_kw = {"query": product_name, "target_price": target_price}

    if use_selenium_first:
        logging.info(f"[{datetime.now()}] Using Selenium for {domain}")
        soup = _fetch_soup_selenium_pooled(
            search_url, domain=domain, debug_meta=fc
        )
        if soup:
            last_soup = soup
            fetch_method_used = "selenium"
            candidates = extractor(soup, **ext_kw)
            if candidates:
                logging.info(
                    f"[{datetime.now()}] {domain}: {len(candidates)} "
                    f"results via Selenium"
                )
    else:
        soup = _fetch_soup(
            search_url, debug_domain=domain, failure_context=fc_wm_bb
        )
        if soup:
            last_soup = soup
            fetch_method_used = "requests"
            candidates = extractor(soup, **ext_kw)
            if candidates:
                logging.info(
                    f"[{datetime.now()}] {domain}: {len(candidates)} "
                    f"results via requests"
                )
        if not candidates:
            logging.info(
                f"[{datetime.now()}] Falling back to Selenium for {domain}"
            )
            soup2 = _fetch_soup_selenium_pooled(
                search_url, domain=domain, debug_meta=fc
            )
            if soup2:
                last_soup = soup2
                fetch_method_used = "selenium"
                candidates = extractor(soup2, **ext_kw)
                if candidates:
                    logging.info(
                        f"[{datetime.now()}] {domain}: {len(candidates)} "
                        f"results via Selenium"
                    )

    if domain == "bestbuy.com" and not candidates:
        logging.info(
            f"[{datetime.now()}] Best Buy: retrying discovery via requests"
        )
        soup_bb = _fetch_soup(
            search_url, debug_domain=domain, failure_context=fc_wm_bb
        )
        if soup_bb:
            last_soup = soup_bb
            fetch_method_used = "fallback"
            c2 = extractor(soup_bb, **ext_kw)
            if c2:
                candidates = c2
                logging.info(
                    f"[{datetime.now()}] bestbuy.com: {len(candidates)} "
                    f"results via requests (fallback)"
                )

    if domain == "amazon.com" and len(candidates) < MAX_RESULTS_PER_SOURCE:
        sep = "&" if "?" in search_url else "?"
        page2_url = f"{search_url}{sep}page=2"
        time.sleep(random.uniform(2, 4))
        logging.info(f"[{datetime.now()}] Fetching Amazon page 2")
        soup2 = _amazon_page2_soup(page2_url)
        if soup2:
            page2 = extractor(soup2, **ext_kw)
            candidates.extend(page2)
            logging.info(f"[{datetime.now()}] Amazon page 2: +{len(page2)} results")

    scraped_pre_dedupe = len(candidates)
    candidates = _dedupe_rows_by_url(candidates, "url")
    post_dedupe = len(candidates)

    if not candidates:
        logging.warning(f"[{datetime.now()}] No results for "
                        f"'{product_name}' on {domain}")
        fr = "no_rows" if last_soup else _discovery_failure_reason_no_soup()
        if domain in ("walmart.com", "bestbuy.com") and last_soup is not None:
            _debug_save_failure(
                domain,
                "discover_product",
                product_name,
                fetch_method_used,
                "no_rows",
                url=search_url,
                html=str(last_soup),
            )
        _store_discovery_stats(
            domain, "discover_product",
            scraped_pre_dedupe=scraped_pre_dedupe, post_dedupe=post_dedupe,
            returned=0,
            missing_name=0, missing_price=0,
            fetch_method=fetch_method_used,
            failure_reason=fr,
        )
        return []

    # Stage 1 — eligibility gate (condition / accessory / brand)
    candidates = _filter_discover_candidates(candidates, product_name)

    if not candidates:
        logging.warning(
            f"[{datetime.now()}] No eligible candidates for {product_name!r} on {domain} "
            f"(all listings failed discovery filters)"
        )
        _store_discovery_stats(
            domain, "discover_product",
            scraped_pre_dedupe=scraped_pre_dedupe, post_dedupe=post_dedupe,
            returned=0,
            missing_name=0,
            missing_price=0,
            fetch_method=fetch_method_used,
            failure_reason="no_eligible_candidates",
        )
        return []

    # Stage 2 — rank eligible candidates (structural fit, then price)
    filtered = _pick_best(candidates, target_price)
    logging.info(f"[{datetime.now()}] {domain}: returning {len(filtered)} results "
                 f"(from {post_dedupe} deduped, {scraped_pre_dedupe} scraped)")
    _store_discovery_stats(
        domain, "discover_product",
        scraped_pre_dedupe=scraped_pre_dedupe, post_dedupe=post_dedupe,
        returned=len(filtered),
        missing_name=_missing_name_count(filtered, "name_found"),
        missing_price=_missing_price_count(filtered, "price"),
        fetch_method=fetch_method_used,
        failure_reason="ok",
    )
    return filtered


# ---------------------------------------------------------------------------
# Public API: get_price_from_url
# ---------------------------------------------------------------------------

def get_price_from_url(url: str, source_name: str = "") -> float | None:
    """
    Fetch the current price from a known product page URL.
    Returns the price as a float, or None on failure.
    """
    time.sleep(random.uniform(2, 5))
    logging.info(f"[{datetime.now()}] Checking price at: {url}")

    soup = _fetch_soup(url)
    if soup:
        price = _extract_price_from_soup_compat(
            soup,
            condition_hint_text=source_name or "",
        )
        if price is not None:
            logging.info(f"[{datetime.now()}] Price ${price} from {source_name or url}")
            return price

    logging.info(f"[{datetime.now()}] Falling back to Selenium for {url}")
    soup = _fetch_soup_selenium_pooled(url)
    if soup:
        price = _extract_price_from_soup_compat(
            soup,
            condition_hint_text=source_name or "",
        )
        if price is not None:
            logging.info(f"[{datetime.now()}] Price ${price} via Selenium "
                         f"from {source_name or url}")
            return price

    logging.warning(f"[{datetime.now()}] Could not extract price from: {url}")
    return None


# Backward-compatible alias
get_price = get_price_from_url


# ---------------------------------------------------------------------------
# Multi-result discovery for Deal Discovery feature
# ---------------------------------------------------------------------------

def _extract_original_price(el) -> float | None:
    """Try to find a strikethrough / was-price in a search result element."""
    for sel in (
        ".a-price.a-text-price span.a-offscreen",
        "span.a-text-price .a-offscreen",
        ".a-text-price",
        '[data-a-strike="true"]',
        '[data-testid="original-price"]',
        '[data-test="product-regular-price"]',
        ".price-was-data",
        ".price-was",
        "[class*='price-was']",
        ".s-item__price--previous",
        ".original-price",
        ".STRIKETHROUGH",
        "span[class*='was']",
        "span[class*='strike']",
        "span[class*='original']",
        "span[class*='list-price']",
        ".pricing-price__regular-price",
        "[class*='regular-price']",
        "del",
        "s",
        ".price-old",
        ".rrp",
    ):
        tag = el.select_one(sel)
        if tag:
            p = clean_price(tag.get_text())
            if p:
                return p
    return None


def _target_current_price(item) -> float | None:
    if not item:
        return None
    for sel in (
        '[data-test="current-price"]',
        'span[data-test="current-price"]',
        'span[class*="CurrentPrice"]',
        '[data-test="product-price"]',
        'span[class*="Price"]',
    ):
        for el in item.select(sel):
            p = clean_price(el.get_text(" ", strip=True))
            if p:
                return p
    return _first_dollar_price_in_text(item.get_text(" ", strip=True))


def _iter_itemlist_jsonld_rows(soup: BeautifulSoup, base: str, *, max_results: int) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    def push(name: str | None, href: str | None, price: Any, original_price: Any = None) -> None:
        if len(rows) >= max_results:
            return
        url = _abs(str(href or "").strip(), base)
        url = _canonical_listing_url(url)
        current_price = clean_price(str(price)) if price is not None else None
        was_price = clean_price(str(original_price)) if original_price is not None else None
        title = clean_listing_title(str(name or "").strip())
        if not url or not title or current_price is None or url in seen:
            return
        seen.add(url)
        rows.append({
            "product_name": title,
            "current_price": current_price,
            "original_price": was_price,
            "product_url": url,
        })

    def visit(node: Any) -> None:
        if len(rows) >= max_results:
            return
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return

        node_type = str(node.get("@type") or node.get("type") or "").lower()
        if node_type == "itemlist":
            visit(node.get("itemListElement") or node.get("item_list_element") or [])
        elif "listitem" in node_type:
            visit(node.get("item") or node)
        elif "product" in node_type:
            offers = node.get("offers")
            price = None
            original_price = None
            if isinstance(offers, list) and offers:
                offer = next((item for item in offers if isinstance(item, dict)), {})
            elif isinstance(offers, dict):
                offer = offers
            else:
                offer = {}
            if offer:
                price = offer.get("price") or offer.get("lowPrice") or offer.get("highPrice")
                original_price = (
                    offer.get("priceSpecification", {}).get("price")
                    if isinstance(offer.get("priceSpecification"), dict)
                    else None
                )
            push(
                node.get("name"),
                node.get("url"),
                price,
                original_price,
            )
        else:
            for value in node.values():
                if isinstance(value, (dict, list)):
                    visit(value)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            visit(json.loads(script.string or ""))
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
        if len(rows) >= max_results:
            break

    return rows


def _extract_amazon_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    base = "https://www.amazon.com"
    results = []
    for item in soup.select('[data-component-type="s-search-result"]'):
        if len(results) >= max_results:
            break
        link = item.select_one('h2 a.a-link-normal')
        if not link:
            link = item.select_one('a.a-link-normal[href*="/dp/"]')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        href = href.split("/ref=")[0]
        name = _amazon_item_title(item, link)
        price = _amazon_listing_price(item)
        if not price or not href:
            continue
        orig = _amazon_listing_original_price(item)
        results.append({
            "product_name": name, "current_price": price,
            "original_price": orig, "product_url": href,
        })
    return results


def _extract_bestbuy_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    base = "https://www.bestbuy.com"
    results = []
    seen: set[str] = set()

    def push(row: tuple[str, float, str, float | None] | None) -> None:
        if not row or len(results) >= max_results:
            return
        name, price, href, orig = row
        u = _canonical_listing_url(href)
        if not u or u in seen:
            return
        seen.add(u)
        results.append({
            "product_name": name[:500],
            "current_price": price,
            "original_price": orig,
            "product_url": href,
        })

    for item in _iter_bestbuy_product_nodes(soup):
        link = _bestbuy_title_link_from_tile(item)
        if link:
            push(_bestbuy_one_row(link, item, base))

    for link in _iter_bestbuy_title_anchors(soup):
        if len(results) >= max_results:
            break
        push(_bestbuy_one_row(link, None, base))

    if not results:
        results.extend(_iter_itemlist_jsonld_rows(soup, base, max_results=max_results))

    return results


def _extract_newegg_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    base = "https://www.newegg.com"
    results = []
    for item in soup.select('.item-cell, .item-container'):
        if len(results) >= max_results:
            break
        link = item.select_one('a.item-title')
        if not link:
            link = item.select_one('.item-info a')
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        name = link.get_text(strip=True)
        price_el = item.select_one('li.price-current')
        price = clean_price(price_el.get_text() if price_el else None)
        if not price or not href:
            continue
        orig = _newegg_listing_original_price(item)
        results.append({
            "product_name": name, "current_price": price,
            "original_price": orig, "product_url": href,
        })
    return results


def _extract_walmart_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    base = "https://www.walmart.com"
    results = []
    seen = set()
    for link_tag in soup.select('a[href*="/ip/"]'):
        if len(results) >= max_results:
            break
        href = _abs(link_tag.get("href", ""), base)
        if href in seen:
            continue
        seen.add(href)
        name = link_tag.get_text(" ", strip=True) or ""
        root = _find_walmart_listing_root(link_tag)
        if not root:
            continue
        price = _walmart_extract_price(root)
        if not price:
            continue
        orig = _walmart_listing_original_price(root)
        results.append({
            "product_name": name[:200], "current_price": price,
            "original_price": orig, "product_url": href,
        })
    if not results:
        results.extend(_iter_itemlist_jsonld_rows(soup, base, max_results=max_results))
    return results


def _extract_target_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_target_all(
        soup,
        max_items=max_results,
        apply_quality_pipeline=False,
    )
    out = []
    for r in rows:
        href = r["url"]
        name = r["name_found"]
        price = r["price"]
        if not href or not price:
            continue
        out.append({
            "product_name": name,
            "current_price": price,
            "original_price": r.get("original_price"),
            "product_url": href,
        })
    return out


def _bhphoto_normalize_price(p: float) -> float:
    """B&H sometimes exposes money as cent integers (11999 → 119.99) in markup."""
    if 500 <= p <= 99999:
        alt = round(p / 100.0, 2)
        if 4.99 <= alt <= 4999.99:
            return alt
    return p


def _bhphoto_price_from_container(root) -> float | None:
    if not root:
        return None
    sels = (
        '[data-selenium="pricingPrice"]',
        '[data-selenium="itemPrice"]',
        '[data-selenium="price"]',
        '[itemprop="price"]',
        'span[class*="price"]',
        'div[class*="price"]',
    )
    for sel in sels:
        el = root.select_one(sel)
        if el:
            p = clean_price(el.get_text())
            if p and 0 < p < 100_000:
                return p
    for el in root.select('[data-selenium*="rice"]'):
        p = clean_price(el.get_text())
        if p and 0 < p < 100_000:
            return p
    return None


def _bhphoto_product_anchor_from_card(card: Tag | None) -> Tag | None:
    """Prefer the product title link, not the first /c/product/ link in DOM order."""
    if not card:
        return None
    for sel in (
        'a[data-selenium="miniProductPageProductNameLink"][href*="/c/product/"]',
        'a[data-selenium="listingProductName"][href*="/c/product/"]',
    ):
        a = card.select_one(sel)
        if a and "/c/product/" in (a.get("href") or ""):
            return a
    for tag_name in ("h1", "h2", "h3", "h4", "h5"):
        for h in card.find_all(tag_name):
            a = h.select_one('a[href*="/c/product/"]')
            if a:
                return a
    for el in card.find_all(True):
        ds = (el.get("data-selenium") or "")
        if "roductName" in ds or "roductname" in ds.lower():
            a = el.select_one('a[href*="/c/product/"]')
            if a:
                return a
        classes = " ".join(el.get("class") or [])
        if classes and re.search(r"producttitle|product.name", classes, re.I):
            a = el.select_one('a[href*="/c/product/"]')
            if a:
                return a
    return None


def _bhphoto_name_from_title_anchor(link: Tag | None) -> str:
    """Product title comes only from the title anchor (text + title/aria)."""
    if not link:
        return ""
    name = link.get_text(" ", strip=True)
    if len(name) < 4 or re.match(
        r"^\d{1,6}\s*Reviews?$", name, re.IGNORECASE
    ):
        name = (
            (link.get("title") or link.get("aria-label") or "").strip()
        )
    return name


def _bhphoto_price_from_ancestor(link: Tag | None, max_hops: int = 22):
    """Walk up from title anchor to a node that contains a price."""
    root = link
    price = None
    for _ in range(max_hops):
        root = root.parent if root else None
        if root is None:
            break
        price = _bhphoto_price_from_container(root)
        if price is not None and 0 < price < 50_000:
            break
    return price, root


def _bhphoto_row_is_junk_name(name: str) -> bool:
    if len(name or "") < 4:
        return True
    if re.match(r"^\d{1,6}\s*Reviews?$", name, re.IGNORECASE):
        return True
    nl = name.lower()
    if "search within" in nl or nl.startswith("filter"):
        return True
    return False


def _bhphoto_log_extract_debug(
    index: int,
    name: str,
    href: str,
    card_el: Tag | None,
    title_anchor: Tag | None,
) -> None:
    if _BHPHOTO_DEBUG_FIRST_N <= 0 or index >= _BHPHOTO_DEBUG_FIRST_N:
        return
    can = _canonical_listing_url(href)
    card_snip = ""
    if card_el is not None:
        card_snip = card_el.get_text(" ", strip=True)[:120]
    anchor_txt = (
        title_anchor.get_text(" ", strip=True) if title_anchor is not None else ""
    )
    match = name.strip() == anchor_txt.strip()
    logging.info(
        f"[{datetime.now()}] B&H extract debug #{index}: "
        f"name_found={name[:100]!r} "
        f"url={href[:160]!r} "
        f"canonical={can[:140]!r} "
        f"title_anchor_match={match} "
        f"anchor_preview={anchor_txt[:100]!r} "
        f"card_preview_120={card_snip!r}"
    )


def _extract_bhphoto_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    base = "https://www.bhphotovideo.com"
    results = []
    blocks = soup.select('[data-selenium="miniProductPage"]')
    if not blocks:
        blocks = soup.select('div.listing-item, li.listing-item')

    for item in blocks:
        if len(results) >= max_results:
            break
        link = _bhphoto_product_anchor_from_card(item)
        if not link:
            continue
        href = _abs(link.get("href", ""), base)
        if "/c/product/" not in href:
            continue
        name = _bhphoto_name_from_title_anchor(link)
        price = _bhphoto_price_from_container(item)
        if price is None:
            p2, _ = _bhphoto_price_from_ancestor(link)
            price = p2
        if price is not None:
            price = _bhphoto_normalize_price(price)
        if not price or not href or _bhphoto_row_is_junk_name(name):
            continue
        orig = _extract_original_price(item)
        _bhphoto_log_extract_debug(len(results), name, href, item, link)
        results.append({
            "product_name": name,
            "current_price": price,
            "original_price": orig,
            "product_url": href,
        })

    if not results:
        seen: set[str] = set()
        # Title-first anchors document order: name and URL always from same <a>.
        ordered_links = soup.select(
            'a[data-selenium="miniProductPageProductNameLink"][href*="/c/product/"], '
            'a[data-selenium="listingProductName"][href*="/c/product/"], '
            'h1 a[href*="/c/product/"], h2 a[href*="/c/product/"], '
            'h3 a[href*="/c/product/"], h4 a[href*="/c/product/"], '
            'h5 a[href*="/c/product/"]'
        )
        for link in ordered_links:
            if len(results) >= max_results:
                break
            href = _canonical_listing_url(_abs(link.get("href", ""), base))
            if not href or "/c/product/" not in href or href in seen:
                continue
            seen.add(href)
            name = _bhphoto_name_from_title_anchor(link)
            if _bhphoto_row_is_junk_name(name):
                continue
            price, root = _bhphoto_price_from_ancestor(link)
            if not price or price >= 50_000:
                continue
            price = _bhphoto_normalize_price(price)
            if not price or price >= 50_000:
                continue
            card_for_debug = None
            p = link
            for _ in range(24):
                p = p.parent if p else None
                if p is None:
                    break
                if p.get("data-selenium") == "miniProductPage":
                    card_for_debug = p
                    break
                cl = (p.get("class") or [])
                if isinstance(cl, str):
                    cl = [cl]
                if any("listing-item" in c for c in cl):
                    card_for_debug = p
                    break
            if card_for_debug is None:
                card_for_debug = root
            orig = _extract_original_price(root) if root else None
            _bhphoto_log_extract_debug(len(results), name, href, card_for_debug, link)
            results.append({
                "product_name": name,
                "current_price": price,
                "original_price": orig,
                "product_url": href,
            })
    merged: dict[str, dict] = {}
    for r in results:
        u = _canonical_listing_url(r["product_url"])
        p = r["current_price"]
        prev = merged.get(u)
        if prev is None or p < prev["current_price"]:
            merged[u] = r
    results = list(merged.values())[:max_results]

    if not results:
        n_links = len(soup.select('a[href*="/c/product/"]'))
        ttl = soup.title.get_text(strip=True) if soup.title else ""
        logging.warning(
            f"[{datetime.now()}] B&H: extracted 0 deals; "
            f"/c/product/ links={n_links}, title={ttl[:120]!r}"
        )
    return results


def _extract_ebay_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_ebay_listings(soup, max_results)
    return [
        {
            "product_name": r["name_found"],
            "current_price": r["price"],
            "original_price": r.get("original_price"),
            "product_url": r["url"],
        }
        for r in rows
    ]


def _extract_costco_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_costco_listings(soup, max_results)
    return [
        {
            "product_name": r["name_found"],
            "current_price": r["price"],
            "original_price": r.get("original_price"),
            "product_url": r["url"],
        }
        for r in rows
    ]


def _extract_homedepot_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_homedepot_listings(soup, max_results)
    return [
        {
            "product_name": r["name_found"],
            "current_price": r["price"],
            "original_price": r.get("original_price"),
            "product_url": r["url"],
        }
        for r in rows
    ]


def _extract_lowes_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_lowes_listings(soup, max_results)
    return [
        {
            "product_name": r["name_found"],
            "current_price": r["price"],
            "original_price": r.get("original_price"),
            "product_url": r["url"],
        }
        for r in rows
    ]


def _extract_officedepot_multi(soup, max_results=MAX_RESULTS_PER_SOURCE):
    rows = _extract_officedepot_listings(soup, max_results)
    return [
        {
            "product_name": r["name_found"],
            "current_price": r["price"],
            "original_price": r.get("original_price"),
            "product_url": r["url"],
        }
        for r in rows
    ]


_MULTI_EXTRACTORS = {
    "amazon.com":       _extract_amazon_multi,
    "bestbuy.com":      _extract_bestbuy_multi,
    "newegg.com":       _extract_newegg_multi,
    "walmart.com":      _extract_walmart_multi,
    "ebay.com":         _extract_ebay_multi,
    "target.com":       _extract_target_multi,
    "bhphotovideo.com": _extract_bhphoto_multi,
    "costco.com":       _extract_costco_multi,
    "homedepot.com":    _extract_homedepot_multi,
    "lowes.com":        _extract_lowes_multi,
    "officedepot.com":  _extract_officedepot_multi,
}

STRICT_TRACKING_DOMAINS = {
    "amazon.com",
    "bestbuy.com",
    "costco.com",
    "newegg.com",
    "officedepot.com",
    "target.com",
    "walmart.com",
}
@dataclass(frozen=True)
class SourceAdapter:
    domain: str
    search_extractor: callable
    selenium_preferred: bool = False
    probe_stages: tuple[str, ...] = ("requests", "full_selenium")
    discovery_threshold: int = DISCOVERY_FAST_USABLE_THRESHOLD
    strict_threshold: int = STRICT_FAST_CANDIDATE_THRESHOLD


_SOURCE_ADAPTERS = {
    domain: SourceAdapter(
        domain=domain,
        search_extractor=extractor,
        selenium_preferred=domain in _SELENIUM_PREFERRED,
        probe_stages=(
            ("probe_html", "probe_light_js", "full_selenium")
            if (domain == "bestbuy.com" and ENABLE_FAST_PATH_BESTBUY)
            or (domain == "walmart.com" and ENABLE_FAST_PATH_WALMART)
            else ("full_selenium",)
            if domain in _SELENIUM_PREFERRED
            else ("requests", "full_selenium")
        ),
    )
    for domain, extractor in _MULTI_EXTRACTORS.items()
}


def _strict_search_url(source: dict, query: str) -> str:
    encoded = quote_plus(query)
    search_url = source["search_url_template"].replace("{query}", encoded)
    domain = source["domain"]
    if domain == "amazon.com":
        search_url = _amazon_search_url_more_results(search_url)
    if domain == "walmart.com":
        search_url = _humanize_walmart_search_url(search_url)
    return search_url


def _preview_probe_rows(
    rows: list[dict],
    *,
    mode: str,
    query: str,
    max_price: float | None,
    domain: str,
) -> tuple[list[dict], int]:
    deduped = _dedupe_rows_by_url(list(rows), "product_url")
    if mode == "discover_deals":
        usable = _apply_discovery_quality_pipeline(
            [dict(row) for row in deduped],
            query=query,
            max_price=max_price,
            label=_retailer_log_label(domain),
            price_key="current_price",
            name_key="product_name",
        )
        return deduped, len(usable)
    for row in deduped:
        if row.get("product_name"):
            row["product_name"] = clean_listing_title(str(row["product_name"]))
    return deduped, len(deduped)


def _fetch_search_probe_stage(
    search_url: str,
    domain: str,
    stage: str,
    *,
    mode: str,
    search_query: str,
    context: SearchExecutionContext | None = None,
) -> tuple[BeautifulSoup | None, str, str | None]:
    if context and context.should_skip_domain(domain):
        return None, "circuit_breaker", context.domain_failure_reason(domain)
    cache_key = f"search-stage:{stage}:{search_url}"
    if context:
        cached = context.get_fetch_entry(cache_key)
        if cached is not None:
            return cached.soup, cached.fetch_method, cached.failure_reason

    failure_context = {
        "domain": domain,
        "mode": mode,
        "query": search_query,
    }
    failure_sink: dict[str, str] = {}
    if stage == "probe_html" and should_bypass_direct(domain):
        if provider_enabled_for(domain):
            soup, fetch_method, failure_reason = fetch_via_provider(
                search_url,
                domain=domain,
                page_kind="search",
            )
            if context:
                context.set_fetch_entry(cache_key, FetchCacheEntry(soup, fetch_method, failure_reason))
            return soup, fetch_method, failure_reason
        if context:
            context.mark_domain_failure(domain, "cooldown")
            context.set_fetch_entry(cache_key, FetchCacheEntry(None, "cooldown", "cooldown"))
        return None, "cooldown", "cooldown"
    if stage == "probe_html":
        soup = _fetch_soup(
            search_url,
            debug_domain=domain,
            failure_context=failure_context if domain in {"walmart.com", "bestbuy.com"} else None,
            failure_sink=failure_sink,
            context=context,
            timeout_seconds=8,
        )
        fetch_method = "requests"
        if not soup and should_try_provider_after_failure(domain, failure_sink.get("reason")):
            log_event(
                "source.fetch.blocked",
                domain=domain,
                strategy="direct",
                reason=failure_sink.get("reason"),
                page_kind="search",
            )
            soup, fetch_method, provider_reason = fetch_via_provider(
                search_url,
                domain=domain,
                page_kind="search",
            )
            if soup is not None:
                failure_sink.pop("reason", None)
            else:
                failure_sink["reason"] = provider_reason or failure_sink.get("reason")
    elif stage == "probe_light_js":
        if provider_enabled_for(domain) and is_protected_domain(domain):
            soup, fetch_method, failure_reason = fetch_via_provider(
                search_url,
                domain=domain,
                page_kind="search",
            )
            if failure_reason:
                failure_sink["reason"] = failure_reason
        else:
            soup = _fetch_soup_selenium_pooled(
                search_url,
                domain=domain,
                debug_meta={"mode": "probe_light_js", "query": search_query},
                failure_sink=failure_sink,
            )
            fetch_method = "selenium_light"
    elif stage == "full_selenium":
        if provider_enabled_for(domain) and is_protected_domain(domain):
            soup, fetch_method, failure_reason = fetch_via_provider(
                search_url,
                domain=domain,
                page_kind="search",
            )
            if failure_reason:
                failure_sink["reason"] = failure_reason
        else:
            soup = _fetch_soup_selenium_pooled(
                search_url,
                domain=domain,
                debug_meta={"mode": mode, "query": search_query},
                failure_sink=failure_sink,
            )
            fetch_method = "selenium"
    else:
        soup = _fetch_soup(
            search_url,
            debug_domain=domain,
            failure_sink=failure_sink,
            context=context,
        )
        fetch_method = "requests"
    failure_reason = failure_sink.get("reason")
    if context:
        context.set_fetch_entry(cache_key, FetchCacheEntry(soup, fetch_method, failure_reason))
    return soup, fetch_method, failure_reason


def _search_results_probe_ladder(
    search_url: str,
    source: dict,
    *,
    mode: str,
    search_query: str,
    max_results: int,
    max_price: float | None = None,
    context: SearchExecutionContext | None = None,
) -> tuple[BeautifulSoup | None, str, list[dict]]:
    domain = source["domain"]
    adapter = _SOURCE_ADAPTERS.get(domain)
    if not adapter:
        return None, "none", []

    final_cache_key = f"search:{mode}:{search_url}"
    if context:
        cached = context.get_fetch_entry(final_cache_key)
        if cached is not None:
            rows = list((cached.metadata or {}).get("rows", []) or [])
            return cached.soup, cached.fetch_method, rows
        if context.should_skip_domain(domain):
            return None, "circuit_breaker", []

    best_soup: BeautifulSoup | None = None
    best_rows: list[dict] = []
    best_method = "fetch_failed"
    last_failure_reason: str | None = None
    chosen_stage = ""
    threshold = (
        adapter.discovery_threshold
        if mode == "discover_deals"
        else min(max_results, adapter.strict_threshold)
    )

    for stage in adapter.probe_stages:
        soup, fetch_method, failure_reason = _fetch_search_probe_stage(
            search_url,
            domain,
            stage,
            mode=mode,
            search_query=search_query,
            context=context,
        )
        last_failure_reason = failure_reason
        if not soup:
            if context:
                context.record_probe_outcome(
                    domain,
                    search_url,
                    stage,
                    fetch_method=fetch_method,
                    failure_reason=failure_reason,
                )
            if (
                not best_rows
                and failure_reason in {"bot_wall", "timeout", "selenium_error"}
                and stage == "probe_light_js"
            ):
                break
            continue

        rows = adapter.search_extractor(soup, max_results=max_results)
        deduped_rows, usable_count = _preview_probe_rows(
            rows,
            mode=mode,
            query=search_query,
            max_price=max_price,
            domain=domain,
        )
        if context:
            context.record_probe_outcome(
                domain,
                search_url,
                stage,
                fetch_method=fetch_method,
                row_count=len(deduped_rows),
                usable_count=usable_count,
                failure_reason=failure_reason,
            )
        if len(deduped_rows) > len(best_rows):
            best_soup = soup
            best_rows = deduped_rows
            best_method = fetch_method
            chosen_stage = stage
        if usable_count >= threshold or stage == adapter.probe_stages[-1]:
            if soup is not None:
                best_soup = soup
                best_rows = deduped_rows
                best_method = fetch_method
                chosen_stage = stage
            break
    if context:
        if best_soup is None and last_failure_reason in {"bot_wall", "timeout"}:
            context.mark_domain_failure(domain, last_failure_reason)
        context.set_escalation_stage(domain, search_url, chosen_stage or "none")
        context.set_fetch_entry(
            final_cache_key,
            FetchCacheEntry(
                best_soup,
                best_method,
                last_failure_reason,
                metadata={"rows": list(best_rows), "stage": chosen_stage or "none"},
            ),
        )
    return best_soup, best_method, best_rows


def _fetch_search_results_soup(
    search_url: str,
    domain: str,
    *,
    context: SearchExecutionContext | None = None,
) -> tuple[BeautifulSoup | None, str]:
    source = {"domain": domain, "search_url_template": ""}
    soup, fetch_method, _rows = _search_results_probe_ladder(
        search_url,
        source,
        mode="strict_search",
        search_query=search_url,
        max_results=max(MAX_RESULTS_PER_SOURCE, STRICT_MAX_CANDIDATES),
        context=context,
    )
    return soup, fetch_method


def _search_listing_candidates(
    search_query: str,
    source: dict,
    *,
    max_results: int = MAX_RESULTS_PER_SOURCE,
    context: SearchExecutionContext | None = None,
) -> list[dict]:
    domain = source["domain"]
    adapter = _SOURCE_ADAPTERS.get(domain)
    if not adapter:
        return []
    search_url = _strict_search_url(source, search_query)
    logging.info(f"[{datetime.now()}] Strict candidate search '{search_query}' on {domain}")
    soup, fetch_method, raw = _search_results_probe_ladder(
        search_url,
        source,
        mode="strict_search",
        search_query=search_query,
        max_results=max_results,
        context=context,
    )
    if not soup:
        logging.warning(f"[{datetime.now()}] Strict candidate search failed on {domain}")
        _store_discovery_stats(
            domain,
            "strict_search",
            scraped_pre_dedupe=0,
            post_dedupe=0,
            returned=0,
            missing_name=0,
            missing_price=0,
            fetch_method=fetch_method,
            failure_reason=_discovery_failure_reason_no_soup(),
        )
        return []
    if domain == "amazon.com" and len(raw) < min(max_results, STRICT_MAX_CANDIDATES):
        sep = "&" if "?" in search_url else "?"
        soup2 = _amazon_page2_soup(f"{search_url}{sep}page=2", context=context)
        if soup2:
            raw.extend(adapter.search_extractor(soup2, max_results=max_results))
    for row in raw:
        if row.get("product_name"):
            row["product_name"] = clean_listing_title(str(row["product_name"]))
    raw = _dedupe_rows_by_url(raw, "product_url")
    logging.info(
        f"[{datetime.now()}] Strict candidate search on {domain}: "
        f"{len(raw)} candidate URLs"
    )
    return raw


def _fetch_listing_soup(
    url: str,
    source_domain: str,
    *,
    context: SearchExecutionContext | None = None,
) -> tuple[BeautifulSoup | None, str]:
    canonical_url = canonicalize_listing_url(url) or url
    cache_key = f"listing:{canonical_url}"
    if context:
        cached = context.get_fetch_entry(cache_key)
        if cached is not None:
            return cached.soup, cached.fetch_method
        if context.should_skip_domain(source_domain):
            return None, "circuit_breaker"

    requests_failure: dict[str, str] = {}
    selenium_failure: dict[str, str] = {}
    provider_active = provider_enabled_for(source_domain) and is_protected_domain(source_domain)
    if should_bypass_direct(source_domain) and provider_enabled_for(source_domain):
        soup, fetch_method, provider_reason = fetch_via_provider(
            url,
            domain=source_domain,
            page_kind="product",
        )
        if soup:
            if context:
                context.set_fetch_entry(cache_key, FetchCacheEntry(soup, fetch_method))
            return soup, fetch_method
        requests_failure["reason"] = provider_reason or "cooldown"
    else:
        soup = _fetch_soup(
            url,
            debug_domain=source_domain,
            failure_sink=requests_failure,
            context=context,
        )
        if soup:
            if context:
                context.set_fetch_entry(cache_key, FetchCacheEntry(soup, "requests"))
            return soup, "requests"
        if should_try_provider_after_failure(source_domain, requests_failure.get("reason")):
            log_event(
                "source.fetch.blocked",
                domain=source_domain,
                strategy="direct",
                reason=requests_failure.get("reason"),
                page_kind="product",
            )
            soup, fetch_method, provider_reason = fetch_via_provider(
                url,
                domain=source_domain,
                page_kind="product",
            )
            if soup:
                if context:
                    context.set_fetch_entry(cache_key, FetchCacheEntry(soup, fetch_method))
                return soup, fetch_method
            requests_failure["reason"] = provider_reason or requests_failure.get("reason")
    if requests_failure.get("reason") == "cooldown" and not provider_enabled_for(source_domain):
        if context:
            context.set_fetch_entry(cache_key, FetchCacheEntry(None, "cooldown", "cooldown"))
            context.mark_domain_failure(source_domain, "cooldown")
        return None, "cooldown"
    if provider_active:
        failure_reason = requests_failure.get("reason") or "provider_unavailable"
        if context:
            context.set_fetch_entry(cache_key, FetchCacheEntry(None, "fetch_failed", failure_reason))
            if failure_reason in {"bot_wall", "timeout", "cooldown"}:
                context.mark_domain_failure(source_domain, failure_reason)
        return None, "fetch_failed"
    soup = _fetch_soup_selenium_pooled(
        url,
        domain=source_domain,
        debug_meta={"mode": "strict_verify", "query": url},
        failure_sink=selenium_failure,
    )
    if soup:
        if context:
            context.set_fetch_entry(cache_key, FetchCacheEntry(soup, "selenium"))
        return soup, "selenium"
    failure_reason = selenium_failure.get("reason") or requests_failure.get("reason")
    if context:
        context.set_fetch_entry(cache_key, FetchCacheEntry(None, "fetch_failed", failure_reason))
        if failure_reason in {"bot_wall", "timeout"}:
            context.mark_domain_failure(source_domain, failure_reason)
    return None, "fetch_failed"


def _fetch_outcome_from_reason(reason: str | None) -> str:
    if reason in {"bot_wall", "cooldown"}:
        return "blocked"
    if reason == "timeout":
        return "timeout"
    if reason in {None, "", "ok"}:
        return "ok"
    if reason in {"unexpected_error"}:
        return "error"
    return "unavailable"


def _listing_fetch_status(
    url: str,
    *,
    context: SearchExecutionContext | None = None,
    fetch_method: str | None = None,
    failure_reason: str | None = None,
) -> dict[str, str] | None:
    canonical_url = canonicalize_listing_url(url) or url
    if context:
        cached = context.get_fetch_entry(f"listing:{canonical_url}")
        if cached is not None:
            fetch_method = fetch_method or cached.fetch_method
            failure_reason = failure_reason or cached.failure_reason
    outcome = _fetch_outcome_from_reason(failure_reason)
    if outcome == "ok":
        return None
    return {
        "outcome": outcome,
        "method": fetch_method or "unknown",
        "reason": failure_reason or "fetch_failed",
    }


def _strict_search_fetch_status(
    domain: str,
    *,
    context: SearchExecutionContext | None = None,
) -> dict[str, str] | None:
    failure_reason = None
    fetch_method = None
    stats = LAST_DISCOVERY_STATS.get(f"{domain}::strict_search") or {}
    if stats:
        failure_reason = stats.get("failure_reason")
        fetch_method = stats.get("fetch_method")
    if context and not failure_reason:
        failure_reason = context.domain_failure_reason(domain)
    outcome = _fetch_outcome_from_reason(failure_reason)
    if outcome == "ok" or failure_reason in {None, "", "no_rows", "no_eligible_candidates"}:
        return None
    return {
        "outcome": outcome,
        "method": fetch_method or "unknown",
        "reason": failure_reason or "fetch_failed",
    }


def _verification_row(candidate: dict, verification, target_price: float | None) -> dict:
    price = verification.current_price
    status_hint = "watching"
    if verification.status == "ambiguous":
        status_hint = "pending_confirmation"
    elif verification.status == "rejected":
        status_hint = "not_found"
    elif price is None:
        status_hint = "quarantined"
    elif target_price is not None and price <= target_price:
        status_hint = "deal_found"
    row = {
        "url": candidate.get("product_url") or candidate.get("url"),
        "price": price,
        "name_found": clean_listing_title(
            verification.product_name or candidate.get("product_name") or ""
        ),
        "status_hint": status_hint,
    }
    row.update(verification_result_to_fields(verification))
    return row


def verify_candidate_listing(
    spec: ProductSpec,
    source: dict,
    candidate: dict,
    *,
    context: SearchExecutionContext | None = None,
):
    url = candidate.get("product_url") or candidate.get("url") or ""
    title_hint = candidate.get("product_name") or candidate.get("name_found") or spec.raw_query
    if not url:
        return None
    canonical_url = canonicalize_listing_url(url) or url
    if context:
        cached = context.get_verification(spec, canonical_url)
        if cached is not None:
            return cached
    soup, _fetch_method = _fetch_listing_soup(url, source["domain"], context=context)
    if not soup:
        verification = verify_listing(
            spec,
            fallback_listing_fingerprint(
                url,
                title_hint,
                current_price=None,
                family_hint=spec.family,
            ),
        )
        if context:
            context.set_verification(spec, canonical_url, verification)
        return verification
    price_hint = candidate.get("current_price")
    condition_hint_text = " ".join(
        bit for bit in (spec.raw_query, title_hint, candidate.get("product_name")) if bit
    )
    price = _extract_price_from_soup_compat(
        soup,
        price_hint=price_hint,
        condition_hint_text=condition_hint_text,
    )
    fingerprint = fingerprint_listing_document(
        url,
        soup,
        current_price=price,
        family_hint=spec.family,
    )
    verification = verify_listing(spec, fingerprint)
    if context:
        context.set_verification(spec, canonical_url, verification)
    return verification


def _verify_candidates_parallel(
    spec: ProductSpec,
    source: dict,
    candidates: list[dict],
    *,
    max_workers: int,
    context: SearchExecutionContext | None = None,
) -> list[tuple[dict, Any]]:
    if not candidates:
        return []

    ordered: list[tuple[dict, Any] | None] = [None] * len(candidates)

    def task(idx: int, candidate: dict) -> tuple[int, Any]:
        return idx, verify_candidate_listing(spec, source, candidate, context=context)

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(candidates)))) as executor:
        futures = [
            executor.submit(task, idx, candidate)
            for idx, candidate in enumerate(candidates)
        ]
        for future in as_completed(futures):
            idx, verification = future.result()
            ordered[idx] = (candidates[idx], verification)

    return [item for item in ordered if item is not None]


def _revalidate_direct_product_source(
    ps_row,
    *,
    context: SearchExecutionContext | None = None,
) -> dict[str, Any]:
    url = (ps_row.get("discovered_url") or "").strip()
    if not url:
        return {"status": "quarantined", "verified": [], "ambiguous": []}

    spec = product_spec_from_row(ps_row)
    source_domain = (
        ps_row.get("source_domain_override")
        or ps_row.get("domain")
        or urlparse(url).netloc.lower().replace("www.", "")
    )
    soup, fetch_method = _fetch_listing_soup(url, source_domain, context=context)
    if not soup:
        fetch_status = _listing_fetch_status(url, context=context, fetch_method=fetch_method)
        if fetch_status:
            return {"status": fetch_status["outcome"], "verified": [], "ambiguous": [], "fetch_status": fetch_status}
        return {"status": "quarantined", "verified": [], "ambiguous": []}

    condition_hint_text = " ".join(
        bit
        for bit in (
            ps_row.get("product_name"),
            ps_row.get("matched_product_name"),
            ps_row.get("raw_query"),
        )
        if bit
    )
    price = _extract_price_from_soup_compat(
        soup,
        price_hint=ps_row.get("current_price"),
        condition_hint_text=condition_hint_text,
    )
    fingerprint = fingerprint_listing_document(
        url,
        soup,
        current_price=price,
        family_hint=spec.family,
    )
    verification = verify_listing(spec, fingerprint)
    candidate = {
        "product_url": url,
        "product_name": ps_row.get("matched_product_name") or ps_row.get("product_name"),
        "current_price": price,
    }
    if verification.status == "verified":
        return {
            "status": "verified",
            "verified": [_verification_row(candidate, verification, ps_row.get("target_price"))],
            "ambiguous": [],
        }
    if verification.status == "ambiguous":
        return {
            "status": "pending_confirmation",
            "verified": [],
            "ambiguous": [_verification_row(candidate, verification, ps_row.get("target_price"))],
        }
    return {"status": "quarantined", "verified": [], "ambiguous": []}


def discover_product_matches(
    product_name: str,
    source: dict,
    *,
    target_price: float | None = None,
    max_candidates: int = STRICT_MAX_CANDIDATES,
    context: SearchExecutionContext | None = None,
) -> dict[str, list[dict]]:
    spec = parse_product_spec(product_name)
    search_queries = (
        spec.search_aliases
        if spec.query_type in {QueryType.EXACT_MODEL.value, QueryType.NAMED_PRODUCT.value}
        else (spec.canonical_query or product_name,)
    )
    seen_urls: set[str] = set()
    verified: list[dict] = []
    ambiguous: list[dict] = []
    rejected = 0
    remaining_budget = max_candidates
    for search_query in search_queries:
        if remaining_budget <= 0:
            break
        if context and context.should_skip_domain(source["domain"]):
            break
        rows = _search_listing_candidates(
            search_query,
            source,
            max_results=max(MAX_RESULTS_PER_SOURCE, max_candidates),
            context=context,
        )
        if rows and context:
            context.clear_empty_results(source["domain"])
        elif not rows and context and source["domain"] in _SELENIUM_PREFERRED:
            if context.record_empty_result(source["domain"]) >= 2:
                break
        new_candidates: list[dict] = []
        for candidate in rows:
            url = (candidate.get("product_url") or candidate.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            new_candidates.append(candidate)
        if not new_candidates:
            continue

        candidate_batch = new_candidates[:remaining_budget]
        remaining_budget -= len(candidate_batch)
        for candidate, verification in _verify_candidates_parallel(
            spec,
            source,
            candidate_batch,
            max_workers=STRICT_VERIFY_WORKERS,
            context=context,
        ):
            if verification is None:
                continue
            row = _verification_row(candidate, verification, target_price)
            if verification.status == "verified":
                verified.append(row)
            elif verification.status == "ambiguous":
                ambiguous.append(row)
            else:
                rejected += 1

        if verified or len(ambiguous) >= 3:
            break
    verified.sort(
        key=lambda row: (
            0 if row.get("match_label") == "verified_exact"
            else 1 if row.get("match_label") == "verified_named"
            else 2,
            row.get("price") is None,
            float(row.get("price") or 1e9),
        )
    )
    ambiguous.sort(key=lambda row: (row.get("price") is None, float(row.get("price") or 1e9)))
    logging.info(
        f"[{datetime.now()}] Strict tracking discovery on {source['domain']}: "
        f"{len(verified)} verified, {len(ambiguous)} ambiguous, {rejected} rejected "
        f"for {product_name!r}"
    )
    fetch_status = None
    if not verified and not ambiguous:
        fetch_status = _strict_search_fetch_status(source["domain"], context=context)
    payload = {"verified": verified, "ambiguous": ambiguous}
    if fetch_status:
        payload["fetch_status"] = fetch_status
    return payload


def revalidate_product_source(
    ps_row,
    *,
    context: SearchExecutionContext | None = None,
) -> dict[str, Any]:
    if ps_row.get("tracking_mode") == "direct_url":
        return _revalidate_direct_product_source(ps_row, context=context)

    spec = product_spec_from_row(ps_row)
    source = {
        "id": ps_row["source_id"],
        "name": ps_row["source_name"],
        "domain": ps_row["domain"],
        "search_url_template": ps_row["search_url_template"],
    }
    current_candidate = {
        "product_url": ps_row.get("discovered_url"),
        "product_name": ps_row.get("matched_product_name") or ps_row.get("product_name"),
        "current_price": ps_row.get("current_price"),
    }
    verification = verify_candidate_listing(spec, source, current_candidate, context=context)
    if verification and verification.status == "verified":
        return {
            "status": "verified",
            "verified": [_verification_row(current_candidate, verification, ps_row.get("target_price"))],
            "ambiguous": [],
        }
    current_fetch_status = None
    current_url = current_candidate.get("product_url")
    if current_url and verification is None:
        current_fetch_status = _listing_fetch_status(current_url, context=context)
        if current_fetch_status:
            return {
                "status": current_fetch_status["outcome"],
                "verified": [],
                "ambiguous": [],
                "fetch_status": current_fetch_status,
            }
    matches = discover_product_matches(
        spec.raw_query,
        source,
        target_price=ps_row.get("target_price"),
        context=context,
    )
    if matches["verified"]:
        return {"status": "rediscovered", **matches}
    if verification and verification.status == "ambiguous":
        matches["ambiguous"].insert(0, _verification_row(current_candidate, verification, ps_row.get("target_price")))
    if matches["ambiguous"]:
        return {"status": "pending_confirmation", **matches}
    if matches.get("fetch_status"):
        return {"status": matches["fetch_status"]["outcome"], **matches}
    if verification and verification.health_state == "quarantined":
        return {"status": "quarantined", "verified": [], "ambiguous": []}
    return {"status": "not_found", "verified": [], "ambiguous": []}


def _budget_query(query: str, max_price: float | None) -> str:
    """For budget searches (< $75), append price hint to bias retailer results."""
    if max_price is not None and max_price < 75:
        return f"{query} under ${int(max_price)}"
    return query


def discover_deals(
    query: str,
    source: dict,
    max_price: float | None = None,
    max_results: int = MAX_RESULTS_PER_SOURCE,
    *,
    context: SearchExecutionContext | None = None,
) -> list[dict]:
    """
    Search a retailer for *query*, returning up to *max_results* products.
    Each result dict has: product_name, current_price, original_price,
    discount_percent, product_url.
    """
    global _LAST_REQUESTS_FAILURE, _LAST_SELENIUM_FAILURE
    _LAST_REQUESTS_FAILURE = None
    _LAST_SELENIUM_FAILURE = None

    search_query = _budget_query(query, max_price)
    encoded = quote_plus(search_query)
    search_url = source["search_url_template"].replace("{query}", encoded)
    domain = source["domain"]
    if domain == "amazon.com":
        search_url = _amazon_search_url_more_results(search_url)
    if domain == "walmart.com":
        search_url = _humanize_walmart_search_url(search_url)

    logging.info(f"[{datetime.now()}] Deal discovery '{search_query}' on {domain}")

    extractor = _MULTI_EXTRACTORS.get(domain)
    if not extractor:
        logging.warning(f"[{datetime.now()}] No multi-extractor for {domain}")
        _store_discovery_stats(
            domain, "discover_deals",
            scraped_pre_dedupe=0, post_dedupe=0, returned=0,
            missing_name=0, missing_price=0,
            fetch_method="none",
            failure_reason="no_extractor",
        )
        return []

    fc = {
        "domain": domain,
        "mode": "discover_deals",
        "query": search_query,
    }
    fc_wm_bb = fc if domain in ("walmart.com", "bestbuy.com") else None

    soup: BeautifulSoup | None = None
    fetch_method_used = "fetch_failed"
    use_selenium_first = domain in _SELENIUM_PREFERRED
    requests_failure: dict[str, str] = {}
    selenium_failure: dict[str, str] = {}
    soup, fetch_method_used, raw = _search_results_probe_ladder(
        search_url,
        source,
        mode="discover_deals",
        search_query=search_query,
        max_results=max_results,
        max_price=max_price,
        context=context,
    )

    if not soup:
        logging.warning(
            f"[{datetime.now()}] Could not fetch {domain} for discovery"
        )
        _store_discovery_stats(
            domain, "discover_deals",
            scraped_pre_dedupe=0, post_dedupe=0, returned=0,
            missing_name=0, missing_price=0,
            fetch_method=fetch_method_used,
            failure_reason=_discovery_failure_reason_no_soup(),
        )
        return []
    page1_deduped = _dedupe_rows_by_url(list(raw), "product_url")
    page1_usable = _apply_discovery_quality_pipeline(
        [dict(row) for row in page1_deduped],
        query=search_query,
        max_price=max_price,
        label=_retailer_log_label(domain),
        price_key="current_price",
        name_key="product_name",
    )

    if domain == "amazon.com" and len(page1_usable) < DISCOVERY_MAX_MERGED_RESULTS:
        sep = "&" if "?" in search_url else "?"
        page2_url = f"{search_url}{sep}page=2"
        logging.info(f"[{datetime.now()}] Fetching Amazon page 2 for discovery")
        soup2 = _amazon_page2_soup(page2_url, context=context)
        if soup2:
            page2 = extractor(soup2, max_results=max_results)
            raw.extend(page2)
            logging.info(f"[{datetime.now()}] Amazon discovery page 2: "
                         f"+{len(page2)} results")

    if domain in ("walmart.com", "bestbuy.com") and not raw:
        _debug_save_failure(
            domain,
            "discover_deals",
            search_query,
            fetch_method_used,
            "no_rows",
            url=search_url,
            html=str(soup),
        )

    scraped_pre_dedupe = len(raw)
    raw = _dedupe_rows_by_url(raw, "product_url")
    post_dedupe = len(raw)

    raw = _apply_discovery_quality_pipeline(
        raw,
        query=search_query,
        max_price=max_price,
        label=_retailer_log_label(domain),
        price_key="current_price",
        name_key="product_name",
    )
    # Eligibility (condition / accessory / brand) runs in hf_utils.process_discovery_results
    # only, using the user's filter_* form values — avoids double gates that disagree.

    out = []
    for r in raw:
        cp = r["current_price"]
        op = r["original_price"]
        if op and op > cp:
            r["discount_percent"] = round(((op - cp) / op) * 100, 1)
        else:
            r["discount_percent"] = 0.0
        out.append(r)

    fail_reason = "no_rows" if scraped_pre_dedupe == 0 else "ok"

    logging.info(
        f"[{datetime.now()}] Discovery on {domain}: {len(out)} final results "
        f"(deduped={post_dedupe} before quality filters)"
    )
    _store_discovery_stats(
        domain, "discover_deals",
        scraped_pre_dedupe=scraped_pre_dedupe, post_dedupe=post_dedupe,
        returned=len(out),
        missing_name=_missing_name_count(out, "product_name"),
        missing_price=_missing_price_count(out, "current_price"),
        fetch_method=fetch_method_used,
        failure_reason=fail_reason,
    )
    return out


def discover_deals_for_queries(
    search_queries: tuple[str, ...] | list[str],
    source: dict,
    *,
    max_price: float | None = None,
    max_results: int = MAX_RESULTS_PER_SOURCE,
    min_usable_results: int = DISCOVERY_MAX_MERGED_RESULTS,
    context: SearchExecutionContext | None = None,
) -> list[dict]:
    source_rows: list[dict] = []
    seen_urls: set[str] = set()
    base_query = next((str(q).strip() for q in search_queries if str(q).strip()), "")
    for search_query in search_queries:
        if context and context.should_skip_domain(source["domain"]):
            break
        deals = discover_deals(
            search_query,
            source,
            max_price=max_price,
            max_results=max_results,
            context=context,
        )
        for deal in deals:
            url = (deal.get("product_url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            source_rows.append(deal)
        if deals and context:
            context.clear_empty_results(source["domain"])
        elif not deals and context and source["domain"] in _SELENIUM_PREFERRED:
            if context.record_empty_result(source["domain"]) >= 2:
                break
        usable_rows = _apply_discovery_quality_pipeline(
            [dict(row) for row in source_rows],
            query=base_query or str(search_query),
            max_price=max_price,
            label=_retailer_log_label(source.get("domain") or source.get("name") or ""),
            price_key="current_price",
            name_key="product_name",
        )
        if len(usable_rows) >= min_usable_results:
            break
    return source_rows
