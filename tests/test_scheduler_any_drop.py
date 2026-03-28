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


def test_blocked_revalidation_preserves_last_verified_price(tmp_path, monkeypatch):
    database, scheduler = _load_modules(tmp_path, monkeypatch)

    product_id = database.add_product("Apple AirPods Pro 3", 199.0)
    ps_id = database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/airpods-pro-3",
        current_price=199.0,
        status="deal_found",
        verification_state="verified",
        health_state="healthy",
    )

    changed = scheduler._persist_revalidation_result(
        {
            **dict(database.get_product_source_by_id(ps_id)),
            "product_id": product_id,
            "source_id": 1,
            "product_name": "Apple AirPods Pro 3",
            "source_name": "Amazon",
            "target_price": 199.0,
            "alert_mode": "target_threshold",
        },
        {
            "status": "blocked",
            "verified": [],
            "ambiguous": [],
            "fetch_status": {
                "outcome": "blocked",
                "method": "provider_html",
                "reason": "bot_wall",
            },
        },
        "2026-03-27T12:00:00",
    )

    source = database.get_product_source_by_id(ps_id)
    assert changed is False
    assert source["current_price"] == 199.0
    assert source["verification_state"] == "verified"
    assert source["status"] == "deal_found"
    assert source["last_fetch_outcome"] == "blocked"
    assert source["last_fetch_method"] == "provider_html"
