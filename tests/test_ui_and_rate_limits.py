import importlib
import re
import sys
from pathlib import Path

from flask import render_template

from product_verifier import ListingFingerprint, VerificationResult


def _load_test_app(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    for module_name in (
        "app",
        "routes",
        "routes.core",
        "routes.discovery",
        "routes.tracking",
        "routes.settings",
        "routes.admin",
        "route_support",
        "route_runtime",
        "template_utils",
    ):
        sys.modules.pop(module_name, None)

    import app as app_module

    importlib.reload(app_module)
    app_module.app.config.update(TESTING=True)
    return database, app_module


def _verified_result(url, title, price):
    fingerprint = ListingFingerprint(
        url=url,
        domain="example.com",
        title=title,
        brand="apple",
        family="airpods",
        model_tokens=("AIRPODSPRO3",),
        normalized_model_tokens=("airpodspro3",),
        variant_tokens=(),
        current_price=price,
        accessory_signal=False,
        compatibility_signal=False,
        bundle_signal=False,
        hard_block_signal=False,
        raw_text=title,
    )
    return VerificationResult(
        status="verified",
        reason="exact_model_verified",
        health_state="healthy",
        product_name=title,
        current_price=price,
        brand="apple",
        family="airpods",
        model_token="AIRPODSPRO3",
        match_label="verified_exact",
        fingerprint=fingerprint,
    )


def test_product_detail_and_history_templates_include_mobile_table_markup(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("Apple AirPods Pro 3", 199.0)
    ps_id = database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/airpods-pro-3",
        current_price=189.0,
        status="deal_found",
        verification_state="verified",
        health_state="healthy",
    )
    database.add_price_history(ps_id, 189.0)

    client = app_module.app.test_client()
    response = client.get(f"/product/{product_id}")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'class="mobile-check-now"' in html
    assert 'id="mobile-nav-toggle"' in html
    assert 'id="mobile-nav-panel"' in html
    assert 'data-label="Date"' in html
    assert 'data-label="Price"' in html

    product = dict(database.get_product_by_id(product_id))
    price_history = database.get_price_history(product_id)
    with app_module.app.test_request_context("/history/render"):
        history_html = render_template(
            "history.html",
            product=product,
            price_history=price_history,
        )

    assert 'data-label="Date Checked"' in history_html
    assert 'data-label="vs Target"' in history_html


def test_history_template_supports_any_drop_mode(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product(
        "Apple AirPods Pro 3",
        None,
        alert_mode="any_drop",
        origin_type="direct_link",
    )
    ps_id = database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/airpods-pro-3",
        current_price=189.0,
        status="watching",
        verification_state="verified",
        health_state="healthy",
        tracking_mode="direct_url",
    )
    database.add_price_history(ps_id, 199.0)
    database.add_price_history(ps_id, 189.0)
    database.compute_best_price(product_id)

    product = dict(database.get_product_by_id(product_id))
    price_history = database.get_price_history(product_id)
    with app_module.app.test_request_context("/history/render"):
        history_html = render_template(
            "history.html",
            product=product,
            price_history=price_history,
        )

    assert "Alert: <strong" in history_html
    assert "Any drop" in history_html
    assert "Price Change" in history_html
    assert "Baseline" in history_html


def test_dashboard_cards_link_to_product_detail(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("Ninja AF101 Air Fryer", 89.0)

    client = app_module.app.test_client()
    response = client.get("/")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert f'data-detail-url="/product/{product_id}"' in html
    assert 'class="card product-card fade-in"' in html
    assert 'role="link"' in html


def test_bestbuy_links_use_redirect_routes_and_copy_fallback(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("PlayStation 5 Slim Console", 499.99)
    ps_id = database.add_product_source(
        product_id,
        2,
        discovered_url="https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC?utm_source=test",
        current_price=499.99,
        status="deal_found",
        verification_state="verified",
        health_state="healthy",
    )
    search_id = database.create_discovery_search("playstation 5", None, 550.0)
    database.add_discovery_result(
        search_id,
        2,
        "PlayStation 5 Slim Console – 1TB - PlayStation 5",
        499.99,
        None,
        0,
        "https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC?utm_source=test",
        relevance_score=88,
        deal_score=21,
        discount_confirmed=0,
        verification_label="verified_related",
    )
    result_id = database.get_discovery_results(search_id)[0]["id"]

    client = app_module.app.test_client()

    dashboard = client.get("/")
    dashboard_html = dashboard.get_data(as_text=True)
    assert f'href="/open/source/{ps_id}"' in dashboard_html
    assert 'data-copy-url="https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC"' in dashboard_html

    product_page = client.get(f"/product/{product_id}")
    product_html = product_page.get_data(as_text=True)
    assert f'href="/open/source/{ps_id}"' in product_html
    assert "try the copied link in another browser" in product_html.lower()

    discovery = client.get(f"/discover/results/{search_id}")
    discovery_html = discovery.get_data(as_text=True)
    assert f'href="/open/discovery-result/{result_id}"' in discovery_html
    assert 'data-copy-url="https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC"' in discovery_html

    open_source = client.get(f"/open/source/{ps_id}")
    assert open_source.status_code == 302
    assert (
        open_source.headers["Location"]
        == "https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC"
    )

    open_discovery = client.get(f"/open/discovery-result/{result_id}")
    assert open_discovery.status_code == 302
    assert (
        open_discovery.headers["Location"]
        == "https://www.bestbuy.com/product/playstation-5-slim-console-1tb-playstation-5/JXHQ37TYLC"
    )


def test_non_bestbuy_source_redirect_stays_direct(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("Apple AirPods Pro 3", 199.0)
    ps_id = database.add_product_source(
        product_id,
        1,
        discovered_url="https://www.amazon.com/dp/B0001?tag=test",
        current_price=199.0,
        status="deal_found",
        verification_state="verified",
        health_state="healthy",
    )

    client = app_module.app.test_client()
    response = client.get(f"/open/source/{ps_id}")

    assert response.status_code == 302
    assert response.headers["Location"] == "https://www.amazon.com/dp/B0001"


def test_settings_template_marks_pending_sources_disabled(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    microcenter = next(row for row in database.get_all_sources() if row["domain"] == "microcenter.com")

    client = app_module.app.test_client()
    response = client.get("/settings")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Office Depot" in html
    assert "Certified" in html
    assert "Micro Center" in html
    assert "Pending" in html
    assert f'value="{microcenter["id"]}"' in html
    assert re.search(
        rf'<input[^>]*value="{microcenter["id"]}"[^>]*disabled',
        html,
    )


def test_add_and_discover_only_show_live_available_sources(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()
    add_html = client.get("/add").get_data(as_text=True)
    discover_html = client.get("/discover").get_data(as_text=True)

    assert "Office Depot" in add_html
    assert "Office Depot" in discover_html
    assert "Micro Center" not in add_html
    assert "Micro Center" not in discover_html


def test_manual_check_route_has_ip_cooldown(tmp_path, monkeypatch):
    _, app_module = _load_test_app(tmp_path, monkeypatch)
    from routes import tracking as tracking_routes

    calls = {"count": 0}

    def fake_enqueue(requested_by=None):
        calls["count"] += 1
        return 123, True

    monkeypatch.setattr(tracking_routes, "enqueue_manual_check_request", fake_enqueue)
    monkeypatch.setattr(
        tracking_routes,
        "get_runtime_diagnostics",
        lambda: {
            "worker_online": True,
            "current_job_name": None,
            "running_manual_checks": 0,
            "queue_depth": 0,
        },
    )

    client = app_module.app.test_client()
    first = client.get("/check")
    second = client.get("/check", follow_redirects=True)

    assert first.status_code == 302
    assert second.status_code == 200
    assert calls["count"] == 1
    assert "Please wait a minute before starting another one." in second.get_data(as_text=True)
    assert 'aria-live="polite"' in second.get_data(as_text=True)
    assert 'class="flash-dismiss"' in second.get_data(as_text=True)


def test_app_sets_security_headers_and_healthz(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()

    home = client.get("/")
    assert home.status_code == 200
    assert home.headers["X-Content-Type-Options"] == "nosniff"
    assert home.headers["X-Frame-Options"] == "SAMEORIGIN"
    assert home.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "geolocation=()" in home.headers["Permissions-Policy"]

    health = client.get("/healthz")
    payload = health.get_json()
    assert health.status_code == 200
    assert payload["status"] == "ok"
    assert payload["service"] == "pricepulse"
    assert payload["sources_total"] >= payload["sources_enabled"] >= 0

    ready = client.get("/readyz")
    ready_payload = ready.get_json()
    assert ready.status_code == 200
    assert ready_payload["status"] == "ready"
    assert ready_payload["database_ready"] is True

    diagnostics = client.get("/diagnostics")
    diagnostics_payload = diagnostics.get_json()
    assert diagnostics.status_code == 200
    assert diagnostics_payload["status"] == "ok"
    assert "runtime" in diagnostics_payload
    assert "queue_depth" in diagnostics_payload["runtime"]


def test_missing_page_renders_friendly_error_screen(tmp_path, monkeypatch):
    _, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()
    response = client.get("/definitely-missing-page")
    html = response.get_data(as_text=True)

    assert response.status_code == 404
    assert "Page not found" in html
    assert "Back to Dashboard" in html
    assert "Find a Deal" in html


def test_discover_track_route_has_result_cooldown(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    import route_support

    search_id = database.create_discovery_search("airpods pro 3", None, 200.0)
    database.add_discovery_result(
        search_id,
        1,
        "Apple AirPods Pro 3 Wireless Earbuds",
        199.0,
        249.0,
        20.1,
        "https://example.com/airpods-pro-3",
        relevance_score=98,
        deal_score=47,
        discount_confirmed=1,
        verification_label="verified_exact",
    )
    result_id = database.get_discovery_results(search_id)[0]["id"]

    monkeypatch.setattr(
        route_support,
        "verify_candidate_listing",
        lambda spec, source, candidate: _verified_result(
            candidate["product_url"],
            "Apple AirPods Pro 3 Wireless Earbuds",
            199.0,
        ),
    )

    client = app_module.app.test_client()
    first = client.post(f"/discover/track/{result_id}")
    second = client.post(f"/discover/track/{result_id}", follow_redirects=True)

    assert first.status_code == 302
    assert second.status_code == 200
    assert len(database.get_all_products()) == 1
    assert "already being processed" in second.get_data(as_text=True)


def test_inline_source_chip_css_keeps_checkbox_accessible(tmp_path, monkeypatch):
    _load_test_app(tmp_path, monkeypatch)
    add_template = Path("C:/Projects/Cursor/Deal Finder/templates/add.html").read_text(encoding="utf-8")
    discover_template = Path("C:/Projects/Cursor/Deal Finder/templates/discover.html").read_text(encoding="utf-8")
    shared_css = Path("C:/Projects/Cursor/Deal Finder/static/css/app.css").read_text(encoding="utf-8")

    assert ".sr-only" in add_template
    assert ".sr-only" in discover_template
    assert "clip: rect(0, 0, 0, 0);" in shared_css
    assert ":focus-within" in add_template or ":focus-within" in discover_template
