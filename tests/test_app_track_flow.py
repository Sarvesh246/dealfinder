import importlib

from product_verifier import ListingFingerprint, VerificationResult


def _load_test_app(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "price_tracker_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()

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


def test_discover_track_uses_clicked_result_without_rerunning_search(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

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
        app_module,
        "verify_candidate_listing",
        lambda spec, source, candidate: _verified_result(
            candidate["product_url"],
            "Apple AirPods Pro 3 Wireless Earbuds",
            199.0,
        ),
    )
    monkeypatch.setattr(
        app_module,
        "discover_product_matches",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("discover_product_matches should not be called by Track This")
        ),
    )

    client = app_module.app.test_client()
    response = client.post(f"/discover/track/{result_id}")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")

    products = database.get_all_products()
    assert len(products) == 1
    product = products[0]
    assert product["name"] == "airpods pro 3"
    assert product["current_price"] == 199.0
    assert product["match_status"] == "deal_found"

    sources = database.get_product_sources(product["id"])
    assert len(sources) == 1
    source = sources[0]
    assert source["discovered_url"] == "https://example.com/airpods-pro-3"
    assert source["current_price"] == 199.0
    assert source["verification_state"] == "verified"
    assert source["status"] == "deal_found"


def test_discover_track_reuses_verified_discovery_result_if_live_verify_fails(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    search_id = database.create_discovery_search("airpods pro 3", None, 200.0)
    database.add_discovery_result(
        search_id,
        1,
        "Apple AirPods Pro 3",
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

    rejected = VerificationResult(
        status="rejected",
        reason="fetch_blocked",
        health_state="healthy",
        product_name="Apple AirPods Pro 3",
        current_price=None,
        brand="apple",
        family="airpods",
        model_token="AIRPODSPRO3",
        match_label="related",
        fingerprint=ListingFingerprint(
            url="https://example.com/airpods-pro-3",
            domain="example.com",
            title="Apple AirPods Pro 3",
            brand="apple",
            family="airpods",
            model_tokens=("AIRPODSPRO3",),
            normalized_model_tokens=("airpodspro3",),
            variant_tokens=(),
            current_price=None,
            accessory_signal=False,
            compatibility_signal=False,
            bundle_signal=False,
            hard_block_signal=False,
            raw_text="Apple AirPods Pro 3",
        ),
    )
    monkeypatch.setattr(app_module, "verify_candidate_listing", lambda *args, **kwargs: rejected)

    client = app_module.app.test_client()
    response = client.post(f"/discover/track/{result_id}")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")

    products = database.get_all_products()
    assert len(products) == 1
    product = products[0]
    assert product["name"] == "airpods pro 3"
    assert product["current_price"] == 199.0

    sources = database.get_product_sources(product["id"])
    assert len(sources) == 1
    source = sources[0]
    assert source["verification_state"] == "verified"
    assert source["verification_reason"] == "reused_discovery_verification"
    assert source["discovered_url"] == "https://example.com/airpods-pro-3"
