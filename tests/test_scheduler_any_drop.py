import importlib


def _load_modules(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    import scheduler

    importlib.reload(scheduler)
    return database, scheduler


def test_any_drop_baseline_sets_price_without_alert(tmp_path, monkeypatch):
    database, scheduler = _load_modules(tmp_path, monkeypatch)

    product_id = database.add_product(
        "Acme ChefMaster Air Fryer 6qt",
        None,
        alert_mode="any_drop",
        origin_type="direct_link",
    )
    database.add_product_source(
        product_id,
        database.ensure_generic_direct_source()["id"],
        discovered_url="http://example.com/product",
        current_price=79.99,
        status="watching",
        verification_state="verified",
        health_state="healthy",
        tracking_mode="direct_url",
    )

    calls = []
    monkeypatch.setattr(scheduler, "send_alerts", lambda *args, **kwargs: calls.append((args, kwargs)) or True)

    scheduler._finalize_checked_products({product_id}, allow_alerts=True)

    product = database.get_product_by_id(product_id)
    assert product["current_price"] == 79.99
    assert calls == []


def test_any_drop_sends_alert_only_when_price_decreases(tmp_path, monkeypatch):
    database, scheduler = _load_modules(tmp_path, monkeypatch)

    product_id = database.add_product(
        "Acme ChefMaster Air Fryer 6qt",
        None,
        alert_mode="any_drop",
        origin_type="direct_link",
    )
    database.add_product_source(
        product_id,
        database.ensure_generic_direct_source()["id"],
        discovered_url="http://example.com/product",
        current_price=79.99,
        status="watching",
        verification_state="verified",
        health_state="healthy",
        tracking_mode="direct_url",
    )
    database.update_product(product_id, current_price=89.99, match_status="watching")

    calls = []
    monkeypatch.setattr(scheduler, "send_alerts", lambda *args, **kwargs: calls.append((args, kwargs)) or True)

    scheduler._finalize_checked_products({product_id}, allow_alerts=True)

    assert len(calls) == 1
    _, kwargs = calls[0]
    assert kwargs["alert_mode"] == "any_drop"
    assert kwargs["previous_price"] == 89.99

    calls.clear()
    database.update_product(product_id, current_price=79.99, match_status="watching")
    database.update_product_source(1, current_price=79.99)
    scheduler._finalize_checked_products({product_id}, allow_alerts=True)
    assert calls == []

    database.update_product(product_id, current_price=79.99, match_status="watching")
    database.update_product_source(1, current_price=94.99)
    scheduler._finalize_checked_products({product_id}, allow_alerts=True)
    assert calls == []
