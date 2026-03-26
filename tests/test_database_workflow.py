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


def test_compute_best_price_any_drop_trackers_stay_in_watching_mode(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    product_id = database.add_product(
        "Instant Pot Duo 7-in-1 Electric Pressure Cooker",
        None,
        alert_mode="any_drop",
        origin_type="direct_link",
    )
    database.add_product_source(
        product_id,
        1,
        discovered_url="https://example.com/instant-pot",
        current_price=89.99,
        status="watching",
        verification_state="verified",
        health_state="healthy",
        tracking_mode="direct_url",
    )

    best = database.compute_best_price(product_id)
    product = database.get_product_by_id(product_id)

    assert best == 89.99
    assert product["current_price"] == 89.99
    assert product["match_status"] == "watching"


def test_discovery_results_prioritize_verified_exact_matches(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    search_id = database.create_discovery_search("apple watch se", None, None)
    assert search_id is not None

    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Apple Watch Series 11",
        current_price=299.0,
        original_price=399.0,
        discount_percent=25.0,
        product_url="https://example.com/series-11",
        deal_score=80.0,
        verification_label="related",
    )
    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Apple Watch SE 3",
        current_price=219.0,
        original_price=249.0,
        discount_percent=12.0,
        product_url="https://example.com/se-3",
        deal_score=60.0,
        verification_label="verified_exact",
    )

    results = database.get_discovery_results(search_id)
    assert len(results) == 2
    assert results[0]["product_name"] == "Apple Watch SE 3"
    assert results[0]["verification_label"] == "verified_exact"


def test_discovery_results_order_exact_then_named_then_category_then_related(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

    search_id = database.create_discovery_search("standing desk", None, None)
    assert search_id is not None

    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Related Listing",
        current_price=400.0,
        original_price=500.0,
        discount_percent=20.0,
        product_url="https://example.com/related",
        deal_score=99.0,
        verification_label="related",
    )
    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Category Primary Listing",
        current_price=350.0,
        original_price=450.0,
        discount_percent=22.0,
        product_url="https://example.com/category",
        deal_score=40.0,
        verification_label="category_primary",
    )
    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Named Listing",
        current_price=320.0,
        original_price=420.0,
        discount_percent=24.0,
        product_url="https://example.com/named",
        deal_score=30.0,
        verification_label="verified_named",
    )
    database.add_discovery_result(
        search_id=search_id,
        source_id=1,
        product_name="Exact Listing",
        current_price=300.0,
        original_price=399.0,
        discount_percent=25.0,
        product_url="https://example.com/exact",
        deal_score=20.0,
        verification_label="verified_exact",
    )

    results = database.get_discovery_results(search_id)
    assert [row["verification_label"] for row in results] == [
        "verified_exact",
        "verified_named",
        "category_primary",
        "related",
    ]
