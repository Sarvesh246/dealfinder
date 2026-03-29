import builtins
import importlib
import sys

from pricing_status import status_for_price


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
        "routes.internal",
        "routes.tracking",
        "routes.settings",
        "routes.admin",
        "job_runner",
        "route_support",
        "route_runtime",
        "template_utils",
    ):
        sys.modules.pop(module_name, None)

    import app as app_module

    importlib.reload(app_module)
    app_module.app.config.update(TESTING=True)
    return app_module


def test_app_factory_registers_current_routes_and_endpoints(tmp_path, monkeypatch):
    app_module = _load_test_app(tmp_path, monkeypatch)
    endpoints = {rule.endpoint for rule in app_module.app.url_map.iter_rules()}

    expected = {
        "index",
        "favicon",
        "healthz",
        "readyz",
        "diagnostics",
        "discover_page",
        "discover_search",
        "discover_results",
        "discover_status",
        "discover_track",
        "internal_dispatch_jobs",
        "internal_backfill_job",
        "add_page",
        "add_product_route",
        "track_link_route",
        "product_detail",
        "product_confirmations_page",
        "confirm_product_candidate",
        "history",
        "product_sources_page",
        "product_sources_save",
        "rediscover_route",
        "delete_product_route",
        "open_product_source",
        "open_discovery_result",
        "settings_page",
        "settings_sources_save",
        "settings_test_notification",
        "admin_categories",
        "manual_check",
    }

    assert expected.issubset(endpoints)


def test_worker_import_does_not_pull_in_flask_app(monkeypatch):
    sys.modules.pop("worker", None)
    sys.modules.pop("app", None)

    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "app":
            raise AssertionError("worker import should not load app")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    worker_module = importlib.import_module("worker")

    assert worker_module is not None
    assert "app" not in sys.modules


def test_status_for_price_handles_threshold_and_any_drop_modes():
    assert status_for_price(None, 200.0, "target_threshold") == "watching"
    assert status_for_price(199.0, 200.0, "target_threshold") == "deal_found"
    assert status_for_price(219.0, 200.0, "target_threshold") == "watching"
    assert status_for_price(199.0, None, "target_threshold") == "watching"
    assert status_for_price(199.0, None, "any_drop") == "watching"


def test_local_worker_autostart_runs_by_default_for_local_and_railway(tmp_path, monkeypatch):
    app_module = _load_test_app(tmp_path, monkeypatch)

    assert app_module._should_autostart_local_worker(
        env={},
        flask_debug=False,
    ) is True
    assert app_module._should_autostart_local_worker(
        env={"WERKZEUG_RUN_MAIN": "true"},
        flask_debug=True,
    ) is True
    assert app_module._should_autostart_local_worker(
        env={},
        flask_debug=True,
    ) is False
    assert app_module._should_autostart_local_worker(
        env={"RAILWAY_ENVIRONMENT": "production"},
        flask_debug=False,
    ) is True


def test_local_worker_autostart_can_be_disabled_explicitly(tmp_path, monkeypatch):
    import config as config_module

    with monkeypatch.context() as m:
        m.setenv("JOB_RUNNER_MODE", "worker")
        m.setenv("AUTO_START_LOCAL_WORKER", "0")
        importlib.reload(config_module)

        app_module = _load_test_app(tmp_path, m)

        assert app_module._should_autostart_local_worker(
            env={"RAILWAY_ENVIRONMENT": "production"},
            flask_debug=False,
        ) is False

    importlib.reload(config_module)


def test_http_job_runner_disables_local_worker_autostart(tmp_path, monkeypatch):
    import config as config_module

    with monkeypatch.context() as m:
        m.setenv("JOB_RUNNER_MODE", "http")
        importlib.reload(config_module)
        app_module = _load_test_app(tmp_path, m)

        assert app_module._should_autostart_local_worker(env={}, flask_debug=False) is False

    importlib.reload(config_module)


def test_internal_job_routes_require_secret_and_run_when_authorized(tmp_path, monkeypatch):
    import config as config_module

    with monkeypatch.context() as m:
        m.setenv("JOB_RUNNER_MODE", "http")
        m.setenv("INTERNAL_JOB_SECRET", "secret-123")
        importlib.reload(config_module)
        app_module = _load_test_app(tmp_path, m)

        client = app_module.app.test_client()
        denied = client.get("/internal/jobs/dispatch")
        allowed = client.get(
            "/internal/jobs/dispatch?mode=manual",
            headers={"X-Internal-Job-Token": "secret-123"},
        )

        assert denied.status_code == 401
        assert allowed.status_code == 200
        assert allowed.get_json()["job_runner_mode"] == "http"

    importlib.reload(config_module)


def test_manual_check_uses_http_runner_when_configured(tmp_path, monkeypatch):
    import config as config_module

    with monkeypatch.context() as m:
        m.setenv("JOB_RUNNER_MODE", "http")
        m.setenv("APP_BASE_URL", "https://pricepulse.example")
        importlib.reload(config_module)
        app_module = _load_test_app(tmp_path, m)

        captured = {}

        def _fake_trigger(*, mode="manual", base_url=None):
            captured["mode"] = mode
            captured["base_url"] = base_url
            return True

        m.setattr("routes.tracking.trigger_internal_dispatch", _fake_trigger)
        client = app_module.app.test_client()
        response = client.get("/check", follow_redirects=True)
        html = response.get_data(as_text=True)

        assert response.status_code == 200
        assert "background runner will start it shortly" in html
        assert captured["mode"] == "manual"
        assert captured["base_url"] == "https://pricepulse.example"

    importlib.reload(config_module)


def test_protected_fetch_auto_enables_brightdata_and_scopes_to_bestbuy(monkeypatch):
    import config as config_module
    import scraper.protected_fetch as protected_fetch

    with monkeypatch.context() as m:
        m.setenv("BRIGHTDATA_API_TOKEN", "token-123")
        m.setenv("BRIGHTDATA_ZONE", "zone-123")
        m.delenv("PROTECTED_FETCH_PROVIDER", raising=False)
        m.delenv("PROTECTED_FETCH_PROVIDER_DOMAINS", raising=False)

        importlib.reload(config_module)
        importlib.reload(protected_fetch)

        assert config_module.PROTECTED_FETCH_PROVIDER == "brightdata"
        assert config_module.PROTECTED_FETCH_PROVIDER_DOMAINS == ("bestbuy.com",)
        assert protected_fetch.provider_enabled_for("bestbuy.com") is True
        assert protected_fetch.provider_enabled_for("walmart.com") is False

    importlib.reload(config_module)
    importlib.reload(protected_fetch)


def test_brightdata_provider_parses_json_html_envelope(monkeypatch):
    import config as config_module
    import scraper.protected_fetch as protected_fetch

    html_body = (
        '<html><body><ul>'
        '<li class="sku-item">Best Buy Row</li>'
        f'<div>{"x" * 3000}</div>'
        '</ul></body></html>'
    )

    class _Response:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __init__(self):
            self.text = ""

        def json(self):
            return {
                "status_code": 200,
                "body": html_body,
            }

    captured: dict[str, object] = {}

    def _fake_post(url, *, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _Response()

    with monkeypatch.context() as m:
        m.setenv("BRIGHTDATA_API_TOKEN", "token-123")
        m.setenv("BRIGHTDATA_ZONE", "zone-123")
        m.delenv("PROTECTED_FETCH_PROVIDER", raising=False)

        importlib.reload(config_module)
        importlib.reload(protected_fetch)
        m.setattr(protected_fetch.requests, "post", _fake_post)

        provider = protected_fetch.BrightDataUnlockerProvider()
        soup, failure_reason = provider.fetch_html(
            "https://www.bestbuy.com/site/searchpage.jsp?st=airpods",
            domain="bestbuy.com",
            page_kind="search",
            expect_selectors=("li.sku-item",),
        )

        assert failure_reason is None
        assert soup is not None
        assert soup.select_one("li.sku-item") is not None
        assert captured["json"]["method"] == "GET"
        assert captured["json"]["format"] == "raw"
