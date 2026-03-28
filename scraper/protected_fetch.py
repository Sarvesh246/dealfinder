"""Protected-store transport helpers and provider fallback."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

import requests
from bs4 import BeautifulSoup

from config import (
    BRIGHTDATA_API_TOKEN,
    BRIGHTDATA_UNLOCKER_ENDPOINT,
    BRIGHTDATA_ZONE,
    PROTECTED_FETCH_DOMAINS,
    PROTECTED_FETCH_ONLY_ON_FAILURE,
    PROTECTED_FETCH_PROVIDER,
    PROTECTED_FETCH_TIMEOUT_SECONDS,
    SOURCE_BLOCK_COOLDOWN_SECONDS,
    SOURCE_BLOCK_MAX_BACKOFF_SECONDS,
)
from database import (
    get_source_access_state,
    record_source_access_failure,
    record_source_access_success,
)
from observability import log_event


@dataclass(frozen=True)
class StoreAccessStrategy:
    domain: str
    mode: str
    search_selectors: tuple[str, ...] = ()
    product_selectors: tuple[str, ...] = ()
    search_min_body_len: int = 1500
    product_min_body_len: int = 1200


class ProtectedFetchProvider(Protocol):
    name: str

    def available(self) -> bool: ...

    def fetch_html(
        self,
        url: str,
        *,
        domain: str,
        page_kind: str,
        expect_selectors: tuple[str, ...] = (),
    ) -> tuple[BeautifulSoup | None, str | None]: ...


_STRATEGIES: dict[str, StoreAccessStrategy] = {
    "bestbuy.com": StoreAccessStrategy(
        domain="bestbuy.com",
        mode="protected_optional",
        search_selectors=(
            "li.sku-item",
            ".product-list-item",
            "[data-testid='shop-product-card']",
            "a[href*='/product/']",
        ),
        product_selectors=(
            "h1",
            ".priceView-hero-price",
            "[data-testid='customer-price']",
        ),
        search_min_body_len=2500,
        product_min_body_len=1800,
    ),
    "walmart.com": StoreAccessStrategy(
        domain="walmart.com",
        mode="protected_optional",
        search_selectors=(
            "[data-item-id]",
            "[data-testid='item-stack']",
            "a[href*='/ip/']",
        ),
        product_selectors=(
            "h1",
            "[itemprop='price']",
            "[data-testid='price-wrap']",
        ),
        search_min_body_len=2500,
        product_min_body_len=1800,
    ),
}


def get_store_access_strategy(domain: str | None) -> StoreAccessStrategy:
    normalized = (domain or "").strip().lower().replace("www.", "")
    return _STRATEGIES.get(normalized, StoreAccessStrategy(domain=normalized, mode="standard_direct"))


def is_protected_domain(domain: str | None) -> bool:
    normalized = (domain or "").strip().lower().replace("www.", "")
    return normalized in PROTECTED_FETCH_DOMAINS and get_store_access_strategy(normalized).mode != "standard_direct"


def _parse_blocked_until(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def current_access_snapshot(domain: str | None) -> dict[str, object]:
    normalized = (domain or "").strip().lower().replace("www.", "")
    row = get_source_access_state(normalized)
    snapshot = {
        "domain": normalized,
        "status": "healthy",
        "failure_reason": None,
        "blocked_until": None,
        "in_cooldown": False,
        "consecutive_failures": 0,
    }
    if not row:
        return snapshot
    blocked_until = _parse_blocked_until(row["blocked_until"])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    snapshot.update(
        {
            "status": row["status"] or "healthy",
            "failure_reason": row["failure_reason"],
            "blocked_until": row["blocked_until"],
            "in_cooldown": bool(blocked_until and blocked_until > now),
            "consecutive_failures": int(row["consecutive_failures"] or 0),
            "last_fetch_method": row["last_fetch_method"],
        }
    )
    return snapshot


def should_bypass_direct(domain: str | None) -> bool:
    if not is_protected_domain(domain):
        return False
    snapshot = current_access_snapshot(domain)
    return bool(snapshot.get("in_cooldown"))


def cooldown_seconds_for_failure(domain: str | None, failure_reason: str | None) -> int:
    if failure_reason not in {"bot_wall", "timeout", "cooldown"}:
        return 0
    failures = int(current_access_snapshot(domain).get("consecutive_failures") or 0)
    base = max(30, SOURCE_BLOCK_COOLDOWN_SECONDS)
    scaled = min(SOURCE_BLOCK_MAX_BACKOFF_SECONDS, base * max(1, 2 ** min(failures, 4)))
    jitter = random.randint(0, max(5, scaled // 10))
    return min(SOURCE_BLOCK_MAX_BACKOFF_SECONDS, scaled + jitter)


def note_fetch_success(domain: str | None, fetch_method: str | None, *, via_provider: bool = False) -> None:
    if domain:
        record_source_access_success(domain, fetch_method=fetch_method, via_provider=via_provider)


def note_fetch_failure(domain: str | None, failure_reason: str | None, fetch_method: str | None) -> None:
    if domain:
        record_source_access_failure(
            domain,
            failure_reason=failure_reason,
            fetch_method=fetch_method,
            cooldown_seconds=cooldown_seconds_for_failure(domain, failure_reason),
        )


def _integrity_ok(strategy: StoreAccessStrategy, page_kind: str, html: str, soup: BeautifulSoup) -> bool:
    if page_kind == "search":
        if len(html) < strategy.search_min_body_len:
            return False
        selectors = strategy.search_selectors
    else:
        if len(html) < strategy.product_min_body_len:
            return False
        selectors = strategy.product_selectors
    return any(soup.select_one(selector) for selector in selectors) if selectors else bool(soup and soup.find("body"))


class BrightDataUnlockerProvider:
    name = "brightdata"

    def available(self) -> bool:
        return bool(BRIGHTDATA_API_TOKEN and BRIGHTDATA_ZONE and BRIGHTDATA_UNLOCKER_ENDPOINT)

    def fetch_html(
        self,
        url: str,
        *,
        domain: str,
        page_kind: str,
        expect_selectors: tuple[str, ...] = (),
    ) -> tuple[BeautifulSoup | None, str | None]:
        if not self.available():
            return None, "provider_unavailable"
        headers = {
            "Authorization": f"Bearer {BRIGHTDATA_API_TOKEN}",
            "Content-Type": "application/json",
        }
        payload: dict[str, object] = {
            "zone": BRIGHTDATA_ZONE,
            "url": url,
            "format": "raw",
        }
        if expect_selectors:
            payload["headers"] = {
                "x-unblock-expect": json.dumps({"element": expect_selectors[0]})
            }
        try:
            resp = requests.post(
                BRIGHTDATA_UNLOCKER_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=PROTECTED_FETCH_TIMEOUT_SECONDS,
            )
            if resp.status_code != 200 or not resp.text:
                return None, "provider_error"
            body_lower = resp.text.lower()
            if "captcha" in body_lower or "robot check" in body_lower:
                return None, "bot_wall"
            soup = BeautifulSoup(resp.text, "html.parser")
            strategy = get_store_access_strategy(domain)
            if not _integrity_ok(strategy, page_kind, resp.text, soup):
                return None, "provider_invalid"
            return soup, None
        except requests.exceptions.Timeout:
            return None, "timeout"
        except requests.exceptions.RequestException:
            return None, "provider_error"
        except Exception:
            return None, "provider_error"


def _provider() -> ProtectedFetchProvider | None:
    if PROTECTED_FETCH_PROVIDER == "brightdata":
        provider = BrightDataUnlockerProvider()
        return provider if provider.available() else None
    return None


def provider_enabled_for(domain: str | None) -> bool:
    return is_protected_domain(domain) and _provider() is not None


def should_try_provider_after_failure(domain: str | None, failure_reason: str | None) -> bool:
    if not provider_enabled_for(domain):
        return False
    if not PROTECTED_FETCH_ONLY_ON_FAILURE:
        return True
    return failure_reason in {"bot_wall", "timeout", "cooldown", "provider_invalid"}


def fetch_via_provider(
    url: str,
    *,
    domain: str,
    page_kind: str,
) -> tuple[BeautifulSoup | None, str, str | None]:
    provider = _provider()
    if not provider:
        return None, "provider_html", "provider_unavailable"
    strategy = get_store_access_strategy(domain)
    log_event(
        "source.fetch.provider_fallback",
        domain=domain,
        provider=provider.name,
        page_kind=page_kind,
        url=url,
    )
    soup, failure_reason = provider.fetch_html(
        url,
        domain=domain,
        page_kind=page_kind,
        expect_selectors=strategy.search_selectors if page_kind == "search" else strategy.product_selectors,
    )
    if soup is not None:
        note_fetch_success(domain, "provider_html", via_provider=True)
        return soup, "provider_html", None
    note_fetch_failure(domain, failure_reason, "provider_html")
    return None, "provider_html", failure_reason
