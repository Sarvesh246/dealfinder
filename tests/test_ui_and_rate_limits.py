import importlib
import sys
import time
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


def test_search_flow_pages_hide_global_mobile_check_cta(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    search_id = database.create_discovery_search("standing desk", None, 300.0)
    database.update_discovery_search_state(
        search_id,
        status="completed",
        sources_total=1,
        sources_finished=1,
    )

    client = app_module.app.test_client()

    discover_html = client.get("/discover").get_data(as_text=True)
    add_html = client.get("/add").get_data(as_text=True)
    results_html = client.get(f"/discover/results/{search_id}").get_data(as_text=True)

    assert 'class="mobile-check-now"' not in discover_html
    assert 'class="mobile-check-now"' not in add_html
    assert 'class="mobile-check-now"' not in results_html


def test_base_template_exposes_app_shell_metadata_and_assets(tmp_path, monkeypatch):
    _, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()
    html = client.get("/").get_data(as_text=True)

    assert 'viewport-fit=cover' in html
    assert 'name="apple-mobile-web-app-capable" content="yes"' in html
    assert 'name="apple-mobile-web-app-title" content="PricePulse"' in html
    assert 'rel="manifest" href="/static/manifest.webmanifest"' in html
    assert 'rel="apple-touch-icon" href="/static/icons/apple-touch-icon.png"' in html

    manifest = client.get("/static/manifest.webmanifest")
    apple_icon = client.get("/static/icons/apple-touch-icon.png")
    icon_192 = client.get("/static/icons/icon-192.png")
    icon_512 = client.get("/static/icons/icon-512.png")

    assert manifest.status_code == 200
    assert manifest.is_json
    assert apple_icon.status_code == 200
    assert apple_icon.mimetype == "image/png"
    assert icon_192.status_code == 200
    assert icon_192.mimetype == "image/png"
    assert icon_512.status_code == 200
    assert icon_512.mimetype == "image/png"


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
    response = client.get("/dashboard")
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

    dashboard = client.get("/dashboard")
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


def test_settings_template_shows_only_certified_sources(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    database.record_source_access_failure(
        "amazon.com",
        failure_reason="timeout",
        fetch_method="direct",
        cooldown_seconds=60,
    )
    database.record_source_access_failure(
        "microcenter.com",
        failure_reason="timeout",
        fetch_method="direct",
        cooldown_seconds=60,
    )

    client = app_module.app.test_client()
    response = client.get("/settings")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Office Depot" in html
    assert "Certified" in html
    assert "Micro Center" not in html
    assert "Pending certification" not in html
    assert "amazon.com" in html
    assert "microcenter.com" not in html


def test_add_and_discover_show_certified_catalog_sources(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()
    add_html = client.get("/add").get_data(as_text=True)
    discover_html = client.get("/discover").get_data(as_text=True)

    assert "Office Depot" in add_html
    assert "Office Depot" in discover_html
    assert "Micro Center" not in add_html
    assert "Micro Center" not in discover_html
    assert 'id="filter-product-type"' in discover_html
    assert 'data-pp-select="field"' in discover_html
    assert 'id="filter-brand"' in discover_html


def test_add_route_rejects_only_out_of_scope_scoped_sources(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    officedepot = next(row for row in database.get_all_sources() if row["domain"] == "officedepot.com")

    client = app_module.app.test_client()
    response = client.post(
        "/add",
        data={
            "name": "Dyson V8 Cordless Vacuum",
            "target_price": "250",
            "source_ids": [str(officedepot["id"])],
        },
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/add")
    assert database.get_all_products() == []


def test_product_sources_page_filters_scoped_sources_by_product_family(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("Dyson V8 Cordless Vacuum", 300.0)
    client = app_module.app.test_client()
    response = client.get(f"/product/{product_id}/sources")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Office Depot" not in html
    assert "Amazon" in html


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


def test_discovery_progress_partial_render_uses_finalizing_state(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    from routes import discovery as discovery_routes

    search_id = database.create_discovery_search(
        "gaming mouse",
        None,
        40.0,
        status="running",
        sources_total=4,
        sources_finished=4,
    )
    assert search_id is not None
    search = dict(database.get_discovery_search(search_id))
    progress = discovery_routes._discovery_progress(search, [])

    with app_module.app.test_request_context(f"/discover/results/{search_id}"):
        html = render_template(
            "partials/discovery_progress.html",
            search=search,
            results=[],
            ai_enhanced=False,
            source_runs=[],
            coverage_summary=None,
            progress=progress,
    )

    assert "Finalizing results" in html
    assert "Finishing result ranking and dedupe." in html
    assert "Finalizing" in html

    with app_module.app.test_request_context(f"/discover/results/{search_id}"):
        body_html = render_template(
            "partials/discovery_results_body.html",
            search=search,
            results=[],
            ai_enhanced=False,
            source_runs=[],
            coverage_summary=None,
            progress=progress,
            empty_state={"title": "", "hint": ""},
        )

    assert "Finalizing verified results" in body_html


def test_app_sets_security_headers_and_healthz(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    client = app_module.app.test_client()

    home = client.get("/")
    assert home.status_code == 200
    assert home.headers["X-Content-Type-Options"] == "nosniff"
    assert home.headers["X-Frame-Options"] == "SAMEORIGIN"
    assert home.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "geolocation=()" in home.headers["Permissions-Policy"]
    assert "Find the <span class=\"gradient-word\">Best Deal</span> On Anything" in home.get_data(as_text=True)

    dashboard = client.get("/dashboard")
    assert dashboard.status_code == 200
    assert "Dashboard" in dashboard.get_data(as_text=True)

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


def test_discovery_results_show_partial_coverage_banner(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    search_id = database.create_discovery_search("airpods pro 2", None, 180.0)
    walmart = next(row for row in database.get_all_sources() if row["domain"] == "walmart.com")
    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")
    database.add_discovery_source_run(
        search_id,
        walmart["id"],
        outcome="blocked",
        fetch_strategy="provider_html",
        failure_reason="bot_wall",
        duration_ms=1200,
    )
    database.add_discovery_source_run(
        search_id,
        amazon["id"],
        outcome="no_results",
        fetch_strategy="direct",
        failure_reason=None,
        duration_ms=450,
    )

    client = app_module.app.test_client()
    response = client.get(f"/discover/results/{search_id}")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Partial coverage" in html
    assert "some stores were temporarily unavailable" in html.lower()
    assert "Walmart" in html


def test_discovery_results_render_progressively_and_status_endpoint_updates(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    walmart = next(row for row in database.get_all_sources() if row["domain"] == "walmart.com")
    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")
    target = next(row for row in database.get_all_sources() if row["domain"] == "target.com")

    search_id = database.create_discovery_search(
        "standing desk",
        None,
        400.0,
        status="running",
        sources_total=2,
        sources_finished=1,
    )
    database.add_discovery_source_run(
        search_id,
        amazon["id"],
        outcome="ok",
        fetch_strategy="direct",
        failure_reason=None,
        raw_count=3,
        eligible_count=2,
        returned_count=1,
        duration_ms=640,
    )
    database.add_discovery_source_run(
        search_id,
        walmart["id"],
        outcome="checking",
        fetch_strategy="direct",
        failure_reason=None,
        duration_ms=0,
    )
    database.add_discovery_result(
        search_id,
        amazon["id"],
        "Standing Desk Pro",
        329.0,
        399.0,
        18.0,
        "https://example.com/desk",
        relevance_score=0.93,
        deal_score=48,
        discount_confirmed=1,
        verification_label="verified_named",
    )

    client = app_module.app.test_client()

    page = client.get(f"/discover/results/{search_id}")
    html = page.get_data(as_text=True)
    assert page.status_code == 200
    assert "Checking stores for live deals" not in html
    assert 'data-discovery-shell' in html
    assert "1 / 2 stores checked" in html
    assert "checking" in html.lower()
    assert 'data-results-sort' in html
    assert 'data-results-source' in html
    assert 'data-pp-select="toolbar"' in html
    assert 'All selected sources' in html
    assert f'<option value="{amazon["id"]}">Amazon</option>' in html
    assert f'<option value="{walmart["id"]}">Walmart</option>' in html
    assert f'<option value="{target["id"]}">Target</option>' not in html
    assert "Pick a result to track." not in html

    status = client.get(f"/discover/status/{search_id}")
    payload = status.get_json()
    assert status.status_code == 200
    assert payload["status"] == "running"
    assert payload["progress"]["finished"] == 1
    assert "Standing Desk Pro" in payload["results_html"]
    assert "Walmart" in payload["progress_html"]
    assert 'data-results-sort' in payload["results_html"]
    assert 'data-results-source' in payload["results_html"]
    assert 'data-pp-select="toolbar"' in payload["results_html"]


def test_discovery_progress_uses_completed_source_runs_when_counter_lags(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    walmart = next(row for row in database.get_all_sources() if row["domain"] == "walmart.com")
    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")

    search_id = database.create_discovery_search(
        "standing desk",
        None,
        400.0,
        status="running",
        sources_total=2,
        sources_finished=1,
    )
    database.add_discovery_source_run(
        search_id,
        amazon["id"],
        outcome="ok",
        fetch_strategy="direct",
        raw_count=3,
        eligible_count=2,
        returned_count=1,
        duration_ms=640,
    )
    database.add_discovery_source_run(
        search_id,
        walmart["id"],
        outcome="blocked",
        fetch_strategy="direct",
        failure_reason="cooldown",
        duration_ms=10,
    )

    client = app_module.app.test_client()
    payload = client.get(f"/discover/status/{search_id}").get_json()

    assert payload["status"] == "running"
    assert payload["progress"]["finished"] == 2
    assert payload["progress"]["pending"] == 0


def test_discovery_job_times_out_stuck_source_and_completes_search(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    from routes import discovery as discovery_routes

    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")
    search_id = database.create_discovery_search("standing desk", None, 400.0)

    def fake_discover(_queries, _source, max_price=None, context=None):
        time.sleep(0.8)
        return []

    monkeypatch.setattr(discovery_routes, "discover_deals_for_queries", fake_discover)
    monkeypatch.setattr(discovery_routes, "DISCOVERY_SOURCE_TIMEOUT_SECONDS", 0.05)

    discovery_routes._run_discovery_search_job(
        search_id=search_id,
        query="standing desk",
        search_queries=("standing desk",),
        sources=[amazon],
        max_price=400.0,
        filter_condition="new_only",
        filter_product_type="primary_only",
        filter_brand="exact",
    )

    search = dict(database.get_discovery_search(search_id))
    runs = [dict(row) for row in database.get_discovery_source_runs(search_id)]

    assert search["status"] == "completed"
    assert search["sources_finished"] == 1
    assert runs[0]["outcome"] == "timeout"
    assert runs[0]["failure_reason"] == "timeout"


def test_discovery_job_keeps_timeout_handling_responsive_while_rendering(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    from routes import discovery as discovery_routes

    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")
    target = next(row for row in database.get_all_sources() if row["domain"] == "target.com")
    search_id = database.create_discovery_search("standing desk", None, 400.0)

    def fake_discover(_queries, source, max_price=None, context=None):
        if source["domain"] == "amazon.com":
            return [
                {
                    "product_name": "Standing Desk 48 inch",
                    "product_url": "https://example.com/standing-desk-48",
                    "current_price": 219.99,
                }
            ]
        time.sleep(1.2)
        return []

    def slow_process(**kwargs):
        time.sleep(0.8)
        return kwargs["rows"], False

    monkeypatch.setattr(discovery_routes, "discover_deals_for_queries", fake_discover)
    monkeypatch.setattr(discovery_routes, "_process_discovery_rows", slow_process)
    monkeypatch.setattr(discovery_routes, "DISCOVERY_SOURCE_TIMEOUT_SECONDS", 0.05)

    discovery_routes._run_discovery_search_job(
        search_id=search_id,
        query="standing desk",
        search_queries=("standing desk",),
        sources=[amazon, target],
        max_price=400.0,
        filter_condition="new_only",
        filter_product_type="primary_only",
        filter_brand="exact",
    )

    search = dict(database.get_discovery_search(search_id))
    runs = [dict(row) for row in database.get_discovery_source_runs(search_id)]
    outcomes = {row["source_id"]: row["outcome"] for row in runs}

    assert search["status"] == "completed"
    assert search["sources_finished"] == 2
    assert search["result_count"] == 1
    assert outcomes[amazon["id"]] == "ok"
    assert outcomes[target["id"]] == "timeout"


def test_discovery_job_finalizes_results_even_if_async_render_has_not_updated_state_yet(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    from routes import discovery as discovery_routes

    amazon = next(row for row in database.get_all_sources() if row["domain"] == "amazon.com")
    search_id = database.create_discovery_search("standing desk", None, 400.0)
    def fake_discover(_queries, _source, max_price=None, context=None):
        return [
            {
                "product_name": "Standing Desk 48 inch",
                "product_url": "https://example.com/standing-desk-48",
                "current_price": 219.99,
            }
        ]

    def fake_process(**kwargs):
        return kwargs["rows"], False

    monkeypatch.setattr(discovery_routes, "discover_deals_for_queries", fake_discover)
    monkeypatch.setattr(discovery_routes, "_process_discovery_rows", fake_process)

    discovery_routes._run_discovery_search_job(
        search_id=search_id,
        query="standing desk",
        search_queries=("standing desk",),
        sources=[amazon],
        max_price=400.0,
        filter_condition="new_only",
        filter_product_type="primary_only",
        filter_brand="exact",
    )

    search = dict(database.get_discovery_search(search_id))

    assert search["status"] == "completed"
    assert search["result_count"] == 1


def test_product_page_and_settings_show_source_access_health(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    product_id = database.add_product("Apple AirPods Pro 3", 199.0)
    database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/airpods-pro-3",
        current_price=199.0,
        status="deal_found",
        verification_state="verified",
        health_state="healthy",
        last_fetch_outcome="blocked",
        last_fetch_method="provider_html",
        last_fetch_reason="bot_wall",
        last_fetch_at="2026-03-27T10:00:00",
    )
    database.record_source_access_failure(
        "walmart.com",
        failure_reason="bot_wall",
        fetch_method="provider_html",
        cooldown_seconds=60,
    )

    client = app_module.app.test_client()
    product_html = client.get(f"/product/{product_id}").get_data(as_text=True)
    settings_html = client.get("/settings").get_data(as_text=True)

    assert "Last fetch: blocked" in product_html
    assert "Source Access Health" in settings_html
    assert "walmart.com" in settings_html

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
    discover_css = Path("C:/Projects/Cursor/Deal Finder/static/css/pages/discover.css").read_text(encoding="utf-8")
    shared_css = Path("C:/Projects/Cursor/Deal Finder/static/css/app.css").read_text(encoding="utf-8")

    assert ".sr-only" in add_template
    assert ".sr-only" in discover_css
    assert "clip: rect(0, 0, 0, 0);" in shared_css
    assert ":focus-within" in add_template or ":focus-within" in discover_css
