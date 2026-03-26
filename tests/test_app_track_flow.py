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


def _verified_named_result(url, title, price, *, family="pressure_cooker", brand="instant pot"):
    fingerprint = ListingFingerprint(
        url=url,
        domain="example.com",
        title=title,
        brand=brand,
        family=family,
        model_tokens=(),
        normalized_model_tokens=(),
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
        reason="named_product_verified",
        health_state="healthy",
        product_name=title,
        current_price=price,
        brand=brand,
        family=family,
        model_token=None,
        match_label="verified_named",
        fingerprint=fingerprint,
    )


def _inspection(url, title, price, verification, *, domain="example.com"):
    return {
        "ok": verification.status in {"verified", "ambiguous"},
        "reason": verification.reason,
        "url": url,
        "domain": domain,
        "title": title,
        "price": price,
        "spec": app_spec(title),
        "verification": verification,
    }


def app_spec(title):
    from product_verifier import parse_product_spec

    return parse_product_spec(title)


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


def test_add_route_redirects_broad_category_queries_to_discovery(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    response = client.post(
        "/add",
        data={
            "name": "standing desk",
            "target_price": "300",
            "source_ids": ["1", "2"],
        },
    )

    assert response.status_code == 302
    location = response.headers["Location"]
    assert "/discover?" in location
    assert "query=standing+desk" in location
    assert "max_price=300" in location
    assert database.get_all_products() == []


def test_discover_track_promotes_category_result_to_specific_tracker(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    search_id = database.create_discovery_search("standing desk", None, 300.0)
    database.add_discovery_result(
        search_id,
        1,
        "FlexiSpot EN1 Electric Standing Desk 48 x 24",
        279.99,
        349.99,
        20.0,
        "https://example.com/flexispot-standing-desk",
        relevance_score=91,
        deal_score=52,
        discount_confirmed=1,
        verification_label="category_primary",
    )
    result_id = database.get_discovery_results(search_id)[0]["id"]

    seen_specs = []

    def _verify(spec, source, candidate):
        seen_specs.append(spec)
        return _verified_named_result(
            candidate["product_url"],
            candidate["product_name"],
            279.99,
            family="standing_desk",
            brand="flexispot",
        )

    monkeypatch.setattr(app_module, "verify_candidate_listing", _verify)

    client = app_module.app.test_client()
    response = client.post(f"/discover/track/{result_id}")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert seen_specs
    assert seen_specs[0].query_type != "category"
    assert seen_specs[0].family == "standing_desk"

    products = database.get_all_products()
    assert len(products) == 1
    product = products[0]
    assert product["name"] == "FlexiSpot EN1 Electric Standing Desk 48 x 24"
    assert product["query_type"] == "named_product"
    assert product["target_price"] == 300.0


def test_discover_track_promotes_keyboard_category_primary_to_exact_tracker(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)

    search_id = database.create_discovery_search("AULA F99 Wireless Mechanical Keyboard", None, 120.0)
    database.add_discovery_result(
        search_id,
        1,
        "AULA F99 Wireless Mechanical Keyboard",
        89.99,
        109.99,
        18.0,
        "https://example.com/aula-f99-keyboard",
        relevance_score=95,
        deal_score=58,
        discount_confirmed=1,
        verification_label="category_primary",
    )
    result_id = database.get_discovery_results(search_id)[0]["id"]

    seen_specs = []

    def _verify(spec, source, candidate):
        seen_specs.append(spec)
        fingerprint = ListingFingerprint(
            url=candidate["product_url"],
            domain="example.com",
            title=candidate["product_name"],
            brand="aula",
            family="keyboard",
            model_tokens=("F99",),
            normalized_model_tokens=("f99",),
            variant_tokens=(),
            current_price=89.99,
            accessory_signal=False,
            compatibility_signal=False,
            bundle_signal=False,
            hard_block_signal=False,
            raw_text=candidate["product_name"],
        )
        return VerificationResult(
            status="verified",
            reason="exact_model_verified",
            health_state="healthy",
            product_name=candidate["product_name"],
            current_price=89.99,
            brand="aula",
            family="keyboard",
            model_token="F99",
            match_label="verified_exact",
            fingerprint=fingerprint,
        )

    monkeypatch.setattr(app_module, "verify_candidate_listing", _verify)

    client = app_module.app.test_client()
    response = client.post(f"/discover/track/{result_id}")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert seen_specs
    assert seen_specs[0].query_type == "exact_model"
    assert seen_specs[0].family == "keyboard"
    assert seen_specs[0].model_token == "f99"

    products = database.get_all_products()
    assert len(products) == 1
    product = products[0]
    assert product["name"] == "AULA F99 Wireless Mechanical Keyboard"
    assert product["query_type"] == "exact_model"


def test_add_page_renders_search_and_link_tabs(tmp_path, monkeypatch):
    _database, app_module = _load_test_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    response = client.get("/add")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Search Stores" in body
    assert "Paste Link" in body
    assert "Track This Link" in body


def test_track_link_success_with_target_price(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    source = database.get_source_by_id(1)
    verification = _verified_result(
        "https://example.com/airpods-pro-3",
        "Apple AirPods Pro 3 Wireless Earbuds",
        189.0,
    )
    monkeypatch.setattr(app_module, "find_source_for_url", lambda url: source)
    monkeypatch.setattr(
        app_module,
        "inspect_direct_link",
        lambda url, source=None, context=None: _inspection(
            "https://example.com/airpods-pro-3",
            "Apple AirPods Pro 3 Wireless Earbuds",
            189.0,
            verification,
        ),
    )

    client = app_module.app.test_client()
    response = client.post(
        "/track/link",
        data={"product_url": "https://example.com/airpods-pro-3", "target_price": "190"},
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")

    products = database.get_all_products()
    assert len(products) == 1
    product = products[0]
    assert product["origin_type"] == "direct_link"
    assert product["alert_mode"] == "target_threshold"
    assert product["target_price"] == 190.0
    assert product["current_price"] == 189.0

    sources = database.get_product_sources(product["id"])
    assert len(sources) == 1
    row = sources[0]
    assert row["tracking_mode"] == "direct_url"
    assert row["verification_state"] == "verified"
    assert row["discovered_url"] == "https://example.com/airpods-pro-3"


def test_track_link_blank_target_uses_any_drop_and_generic_source(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    generic = database.ensure_generic_direct_source()
    verification = _verified_named_result(
        "https://shop.example.net/product/instant-pot",
        "Instant Pot Duo 7-in-1 Electric Pressure Cooker",
        89.99,
    )
    monkeypatch.setattr(app_module, "find_source_for_url", lambda url: None)
    monkeypatch.setattr(app_module, "ensure_generic_direct_source", lambda: generic)
    monkeypatch.setattr(
        app_module,
        "inspect_direct_link",
        lambda url, source=None, context=None: _inspection(
            "https://shop.example.net/product/instant-pot",
            "Instant Pot Duo 7-in-1 Electric Pressure Cooker",
            89.99,
            verification,
            domain="shop.example.net",
        ),
    )

    client = app_module.app.test_client()
    response = client.post(
        "/track/link",
        data={"product_url": "https://shop.example.net/product/instant-pot", "target_price": ""},
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")

    product = database.get_all_products()[0]
    assert product["origin_type"] == "direct_link"
    assert product["alert_mode"] == "any_drop"
    assert product["target_price"] is None
    assert product["current_price"] == 89.99

    source_row = database.get_product_sources(product["id"])[0]
    assert source_row["tracking_mode"] == "direct_url"
    assert source_row["source_name"] == "shop.example.net"
    assert source_row["domain"] == "shop.example.net"


def test_track_link_rejects_weak_pages(tmp_path, monkeypatch):
    database, app_module = _load_test_app(tmp_path, monkeypatch)
    monkeypatch.setattr(app_module, "find_source_for_url", lambda url: None)
    monkeypatch.setattr(app_module, "ensure_generic_direct_source", database.ensure_generic_direct_source)
    monkeypatch.setattr(
        app_module,
        "inspect_direct_link",
        lambda url, source=None, context=None: {
            "ok": False,
            "reason": "weak_listing",
            "url": url,
            "domain": "example.com",
        },
    )

    client = app_module.app.test_client()
    response = client.post("/track/link", data={"product_url": "https://example.com/bad", "target_price": ""})

    assert response.status_code == 302
    assert "/add?mode=link" in response.headers["Location"]
    assert database.get_all_products() == []
