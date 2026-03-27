"""
Core routes: dashboard, health endpoints, favicon, and error handlers.
"""

from __future__ import annotations

import logging
from datetime import datetime

from flask import jsonify, redirect, render_template, send_from_directory, url_for
from jinja2 import TemplateNotFound

from config import STATIC_DIR
from database import (
    get_all_products,
    get_all_sources,
    get_best_source_url,
    get_enabled_sources,
    get_last_checked_time,
    get_product_sources,
    get_runtime_diagnostics,
)
from template_utils import coerce_float

from . import main_bp


@main_bp.route("/favicon.ico", endpoint="favicon")
def favicon():
    return send_from_directory(
        STATIC_DIR,
        "favicon.svg",
        mimetype="image/svg+xml",
        max_age=86400,
    )


@main_bp.route("/healthz", endpoint="healthz")
def healthz():
    sources = get_all_sources()
    enabled_sources = len(get_enabled_sources())
    runtime = get_runtime_diagnostics()
    return jsonify(
        {
            "status": "ok",
            "service": "pricepulse",
            "checked_at": datetime.now().isoformat(),
            "sources_total": len(sources),
            "sources_enabled": enabled_sources,
            "worker_online": runtime.get("worker_online", False),
            "queue_depth": runtime.get("queue_depth", 0),
        }
    )


@main_bp.route("/readyz", endpoint="readyz")
def readyz():
    runtime = get_runtime_diagnostics()
    sources = get_all_sources()
    enabled_sources = len(get_enabled_sources())
    return jsonify(
        {
            "status": "ready",
            "service": "pricepulse",
            "checked_at": datetime.now().isoformat(),
            "database_ready": True,
            "worker_online": runtime.get("worker_online", False),
            "queue_depth": runtime.get("queue_depth", 0),
            "last_job_status": runtime.get("last_job_status"),
            "sources_enabled": enabled_sources,
            "sources_total": len(sources),
        }
    )


@main_bp.route("/diagnostics", endpoint="diagnostics")
def diagnostics():
    runtime = get_runtime_diagnostics()
    return jsonify(
        {
            "status": "ok",
            "service": "pricepulse",
            "checked_at": datetime.now().isoformat(),
            "runtime": runtime,
        }
    )


def _render_error_page(
    status_code: int,
    title: str,
    message: str,
    primary_href: str,
    primary_label: str,
    secondary_href: str | None = None,
    secondary_label: str | None = None,
):
    try:
        return (
            render_template(
                "error_page.html",
                status_code=status_code,
                title=title,
                message=message,
                primary_href=primary_href,
                primary_label=primary_label,
                secondary_href=secondary_href,
                secondary_label=secondary_label,
            ),
            status_code,
        )
    except TemplateNotFound:
        secondary = (
            f'<p><a href="{secondary_href}">{secondary_label}</a></p>'
            if secondary_href and secondary_label
            else ""
        )
        return (
            (
                "<!doctype html><html><head><meta charset='utf-8'>"
                f"<title>{status_code} · {title}</title></head><body>"
                f"<h1>{title}</h1><p>{message}</p>"
                f"<p><a href='{primary_href}'>{primary_label}</a></p>"
                f"{secondary}</body></html>"
            ),
            status_code,
        )


@main_bp.app_errorhandler(404)
def not_found(error):
    return _render_error_page(
        404,
        "Page not found",
        "That page doesn’t exist or may have moved. You can head back to the dashboard or start a new search.",
        url_for("index"),
        "Back to Dashboard",
        url_for("discover_page"),
        "Find a Deal",
    )


@main_bp.app_errorhandler(500)
def server_error(error):
    logging.exception("Unhandled application error", exc_info=error)
    return _render_error_page(
        500,
        "Something went wrong",
        "PricePulse hit an unexpected issue while loading this page. Try again in a moment, or run another search from the dashboard.",
        url_for("index"),
        "Back to Dashboard",
        url_for("discover_page"),
        "Find a Deal",
    )


@main_bp.route("/", endpoint="index")
def index():
    raw_products = get_all_products()

    products = []
    for product in raw_products:
        item = dict(product)
        sources = get_product_sources(product["id"])
        item["url"] = get_best_source_url(product["id"])
        item["sources"] = [dict(source) for source in sources]
        item["sources_found"] = sum(1 for source in sources if source["current_price"] is not None)

        last_checked = None
        for source in sources:
            checked_at = source["last_checked"]
            if checked_at and (last_checked is None or checked_at > last_checked):
                last_checked = checked_at
        item["last_checked"] = last_checked
        products.append(item)

    total_products = len(products)
    deals_found = 0
    for product in products:
        if product.get("alert_mode") == "any_drop":
            continue
        current_price = coerce_float(product.get("current_price"))
        target_price = coerce_float(product.get("target_price"))
        if current_price is not None and target_price is not None and current_price <= target_price:
            deals_found += 1

    return render_template(
        "index.html",
        products=products,
        total_products=total_products,
        deals_found=deals_found,
        last_checked_global=get_last_checked_time(),
    )
