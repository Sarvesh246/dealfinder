import importlib


def test_compute_best_price_ignores_unverified_sources(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    product_id = database.add_product("WH-1000XM4", 200)
    verified_id = database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/verified",
        current_price=180.0,
        status="watching",
        verification_state="verified",
        health_state="healthy",
    )
    database.add_product_source(
        product_id,
        2,
        discovered_url="https://example.com/pending",
        current_price=12.99,
        status="pending_confirmation",
        verification_state="pending_confirmation",
        health_state="healthy",
    )
    best = database.compute_best_price(product_id)
    product = database.get_product_by_id(product_id)

    assert verified_id is not None
    assert best == 180.0
    assert product["current_price"] == 180.0
    assert product["match_status"] == "deal_found"


def test_candidate_confirmation_storage(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    product_id = database.add_product("WH-1000XM4", 200)
    ps_id = database.add_product_source(
        product_id,
        1,
        status="pending_confirmation",
        verification_state="pending_confirmation",
    )
    candidate_id = database.add_product_source_candidate(
        product_id,
        1,
        "https://example.com/candidate",
        product_source_id=ps_id,
        candidate_name="Sony WH-1000XM4 Headphones",
        candidate_price=199.99,
        verification_reason="model_not_proven",
        match_label="verified_related",
    )

    candidates = database.get_product_source_candidates(product_id)
    assert candidate_id is not None
    assert len(candidates) == 1
    assert candidates[0]["candidate_url"] == "https://example.com/candidate"
