"""
Jinja filters and presentation helpers shared across templates.
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse

from flask import current_app, url_for

from config import CHECK_CRON_SECRET
from route_runtime import manual_check_ui_token
from scraper import canonicalize_listing_url


def coerce_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_relative_time(ts: str | None) -> str:
    if not ts:
        return "Never"
    try:
        dt = datetime.fromisoformat(ts)
        delta = datetime.now() - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return "Just now"
        if secs < 3600:
            minutes = secs // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        if secs < 86400:
            hours = secs // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        days = secs // 86400
        return f"{days} day{'s' if days != 1 else ''} ago"
    except Exception:
        return "Unknown"


def format_price(price) -> str:
    number = coerce_float(price)
    if number is None:
        return "N/A"
    return f"${number:,.2f}"


def price_status(current, target, alert_mode: str = "target_threshold") -> str:
    current_price = coerce_float(current)
    if current_price is None:
        return "not_found"
    if alert_mode == "any_drop" or target is None:
        return "watching"
    target_price = coerce_float(target)
    if target_price is None:
        return "watching"
    return "deal" if current_price <= target_price else "watching"


def price_color(current, target, alert_mode: str = "target_threshold") -> str:
    current_price = coerce_float(current)
    if current_price is None:
        return "var(--price-bad)"
    if alert_mode == "any_drop" or target is None:
        return "var(--accent-blue)"
    target_price = coerce_float(target)
    if target_price is None:
        return "var(--accent-blue)"
    if current_price <= target_price:
        return "var(--price-good)"
    if current_price <= target_price * 1.10:
        return "var(--price-warn)"
    return "var(--price-bad)"


def progress_pct(current, target, alert_mode: str = "target_threshold") -> float:
    if alert_mode == "any_drop" or target is None:
        return 100.0 if coerce_float(current) is not None else 0.0
    target_price = coerce_float(target)
    current_price = coerce_float(current)
    if current_price is None or target_price is None or target_price == 0:
        return 0.0
    return round(min(100.0, (target_price / current_price) * 100), 1)


def pct_away(current, target, alert_mode: str = "target_threshold") -> str:
    current_price = coerce_float(current)
    if current_price is None:
        return ""
    if alert_mode == "any_drop" or target is None:
        return "Watching for a new lower price"
    target_price = coerce_float(target)
    if target_price is None:
        return "Watching for a new lower price"
    if current_price <= target_price:
        return "Target reached!"
    pct = round(((current_price - target_price) / target_price) * 100, 1)
    return f"{pct}% away from target"


def canonical_external_url(url: str | None) -> str:
    cleaned = canonicalize_listing_url(url or "") or (url or "").strip()
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return cleaned


def external_domain(url: str | None, fallback: str | None = None) -> str:
    if fallback:
        domain = fallback.lower()
    else:
        domain = (urlparse(url or "").netloc or "").lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def is_bestbuy_external_url(url: str | None, fallback: str | None = None) -> bool:
    return external_domain(url, fallback=fallback) == "bestbuy.com"


def open_product_source_url(product_source) -> str:
    ps_id = None
    if isinstance(product_source, dict):
        ps_id = product_source.get("id")
    elif hasattr(product_source, "keys"):
        ps_id = product_source["id"]
    return url_for("open_product_source", product_source_id=ps_id) if ps_id else "#"


def open_discovery_result_url(result) -> str:
    result_id = None
    if isinstance(result, dict):
        result_id = result.get("id")
    elif hasattr(result, "keys"):
        result_id = result["id"]
    return url_for("open_discovery_result", result_id=result_id) if result_id else "#"


def register_template_utils(app) -> None:
    app.jinja_env.filters["relative_time"] = format_relative_time
    app.jinja_env.filters["format_price"] = format_price
    app.jinja_env.globals.update(
        price_status=price_status,
        price_color=price_color,
        progress_pct=progress_pct,
        pct_away=pct_away,
        format_relative_time=format_relative_time,
        canonical_external_url=canonical_external_url,
        external_domain=external_domain,
        is_bestbuy_external_url=is_bestbuy_external_url,
        open_product_source_url=open_product_source_url,
        open_discovery_result_url=open_discovery_result_url,
    )

    @app.context_processor
    def inject_manual_check_url():
        return {
            "manual_check_url": (
                url_for("manual_check", ui_token=manual_check_ui_token(current_app.secret_key))
                if CHECK_CRON_SECRET
                else url_for("manual_check")
            ),
        }
