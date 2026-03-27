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
        "discover_track",
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
