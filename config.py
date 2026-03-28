"""
Centralized runtime configuration for PricePulse.

All environment-backed defaults are read once here so the web app, worker,
scraper, scheduler, and HF helper share the same runtime contract.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"

load_dotenv(APP_DIR / ".env")


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


SECRET_KEY = os.getenv("SECRET_KEY", "pricepulse-change-me-in-production")
FLASK_DEBUG = _env_flag("FLASK_DEBUG", False)
PORT = _env_int("PORT", 5000)
CHECK_CRON_SECRET = os.getenv("CHECK_CRON_SECRET", "").strip()

# SQLite (web + worker share one file): wait under lock instead of failing immediately.
SQLITE_CONNECT_TIMEOUT_SECONDS = _env_int("SQLITE_CONNECT_TIMEOUT_SECONDS", 30)
SQLITE_BUSY_TIMEOUT_MS = _env_int("SQLITE_BUSY_TIMEOUT_MS", 10_000)

DISCOVERY_SOURCE_WORKERS = _env_int("DISCOVERY_SOURCE_WORKERS", 4)
STRICT_SOURCE_WORKERS = _env_int("STRICT_SOURCE_WORKERS", 4)
DISCOVERY_VERIFY_WORKERS = _env_int("DISCOVERY_VERIFY_WORKERS", 4)
STRICT_VERIFY_WORKERS = _env_int("STRICT_VERIFY_WORKERS", 3)
DISCOVERY_INTERACTIVE_WORKERS = _env_int("DISCOVERY_INTERACTIVE_WORKERS", 2)
DISCOVERY_STATUS_POLL_MS = _env_int("DISCOVERY_STATUS_POLL_MS", 1200)
DISCOVERY_SOURCE_TIMEOUT_SECONDS = _env_int("DISCOVERY_SOURCE_TIMEOUT_SECONDS", 45)
CHECK_COOLDOWN_SECONDS = _env_int("CHECK_COOLDOWN_SECONDS", 60)
TRACK_RESULT_COOLDOWN_SECONDS = _env_int("TRACK_RESULT_COOLDOWN_SECONDS", 5)

MAX_RESULTS_PER_SOURCE = _env_int("MAX_RESULTS_PER_SOURCE", 50)
SELENIUM_DRIVER_MAX_PAGES = _env_int("SELENIUM_DRIVER_MAX_PAGES", 25)
SELENIUM_DRIVER_MAX_AGE_SECONDS = _env_int("SELENIUM_DRIVER_MAX_AGE_SECONDS", 900)
REQUEST_POOL_SIZE = _env_int("REQUEST_POOL_SIZE", 16)
ENABLE_FAST_PATH_BESTBUY = _env_flag("ENABLE_FAST_PATH_BESTBUY", True)
ENABLE_FAST_PATH_WALMART = _env_flag("ENABLE_FAST_PATH_WALMART", True)
ENABLE_BROWSER_WARMUP = _env_flag("ENABLE_BROWSER_WARMUP", True)
BROWSER_WARMUP_INTERVAL_SECONDS = _env_int("BROWSER_WARMUP_INTERVAL_SECONDS", 600)
BROWSER_WARMUP_DOMAINS = tuple(
    domain.strip().lower()
    for domain in os.getenv("BROWSER_WARMUP_DOMAINS", "bestbuy.com,walmart.com").split(",")
    if domain.strip()
)
REQUIRE_DISCOUNT = _env_flag("REQUIRE_DISCOUNT", True)
DISCOVERY_MAX_MERGED_RESULTS = 8
DISCOVERY_MIN_CONFIRMED_BEFORE_SKIP_UNCONFIRMED = 5
DISCOVERY_FAST_USABLE_THRESHOLD = _env_int(
    "DISCOVERY_FAST_USABLE_THRESHOLD",
    DISCOVERY_MAX_MERGED_RESULTS,
)
STRICT_MAX_CANDIDATES = _env_int("STRICT_MAX_CANDIDATES", 8)
STRICT_FAST_CANDIDATE_THRESHOLD = _env_int(
    "STRICT_FAST_CANDIDATE_THRESHOLD",
    STRICT_MAX_CANDIDATES,
)
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "").strip()
BHPHOTO_DEBUG_ROWS = _env_int("BHPHOTO_DEBUG_ROWS", 10)
SCRAPER_DEBUG_MAX_BYTES = _env_int("SCRAPER_DEBUG_MAX_BYTES", 5 * 1024 * 1024)
SCRAPER_DEBUG_DIR = os.getenv(
    "SCRAPER_DEBUG_DIR",
    str(Path(tempfile.gettempdir()) / "pricepulse_scraper_debug"),
).strip()
BRIGHTDATA_UNLOCKER_ENDPOINT = os.getenv(
    "BRIGHTDATA_UNLOCKER_ENDPOINT",
    "https://api.brightdata.com/request",
).strip()
BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN", "").strip()
BRIGHTDATA_ZONE = os.getenv("BRIGHTDATA_ZONE", "").strip()
_protected_fetch_provider_raw = os.getenv("PROTECTED_FETCH_PROVIDER", "auto").strip().lower()
if _protected_fetch_provider_raw in {"", "auto"}:
    PROTECTED_FETCH_PROVIDER = "brightdata" if (BRIGHTDATA_API_TOKEN and BRIGHTDATA_ZONE) else "none"
else:
    PROTECTED_FETCH_PROVIDER = _protected_fetch_provider_raw
PROTECTED_FETCH_DOMAINS = tuple(
    domain.strip().lower()
    for domain in os.getenv("PROTECTED_FETCH_DOMAINS", "bestbuy.com,walmart.com").split(",")
    if domain.strip()
)
PROTECTED_FETCH_PROVIDER_DOMAINS = tuple(
    domain.strip().lower()
    for domain in os.getenv("PROTECTED_FETCH_PROVIDER_DOMAINS", "bestbuy.com").split(",")
    if domain.strip()
)
PROTECTED_FETCH_ONLY_ON_FAILURE = _env_flag("PROTECTED_FETCH_ONLY_ON_FAILURE", True)
PROTECTED_FETCH_TIMEOUT_SECONDS = _env_int("PROTECTED_FETCH_TIMEOUT_SECONDS", 30)
SOURCE_BLOCK_COOLDOWN_SECONDS = _env_int("SOURCE_BLOCK_COOLDOWN_SECONDS", 600)
SOURCE_BLOCK_MAX_BACKOFF_SECONDS = _env_int("SOURCE_BLOCK_MAX_BACKOFF_SECONDS", 3600)

WORKER_LEASE_SECONDS = _env_int("WORKER_LEASE_SECONDS", 90)
WORKER_HEARTBEAT_SECONDS = _env_int("WORKER_HEARTBEAT_SECONDS", 20)
MANUAL_CHECK_POLL_SECONDS = _env_int("MANUAL_CHECK_POLL_SECONDS", 10)
CHECK_INTERVAL_HOURS = _env_int("CHECK_INTERVAL_HOURS", 6)
ENABLE_STARTUP_BACKFILL = _env_flag("ENABLE_STARTUP_BACKFILL", True)
AUTO_START_LOCAL_WORKER = _env_flag(
    "AUTO_START_LOCAL_WORKER",
    True,
)

QUERY_ENHANCE_MODEL = os.getenv(
    "HF_QUERY_MODEL",
    "meta-llama/Llama-3.2-1B-Instruct",
).strip()
HF_TOKEN = os.getenv("HF_TOKEN", "").strip() or None
HF_RELEVANCE_CACHE_TTL_SECONDS = _env_int("HF_RELEVANCE_CACHE_TTL_SECONDS", 600)
HF_QUERY_CACHE_TTL_SECONDS = _env_int("HF_QUERY_CACHE_TTL_SECONDS", 600)
HF_QUERY_CACHE_MAX_ENTRIES = _env_int("HF_QUERY_CACHE_MAX_ENTRIES", 256)
