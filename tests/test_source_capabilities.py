from product_verifier import parse_product_spec

from source_capabilities import filter_supported_sources, source_supports_query


def test_officedepot_supports_certified_office_queries():
    source = {"domain": "officedepot.com"}
    assert source_supports_query(source, parse_product_spec("Brother HL-L2460DW printer"))
    assert source_supports_query(source, parse_product_spec("standing desk"))


def test_officedepot_skips_out_of_scope_queries():
    source = {"domain": "officedepot.com"}
    assert not source_supports_query(source, parse_product_spec("Dyson V8 Cordless Vacuum"))


def test_filter_supported_sources_preserves_broad_sources_and_skips_scoped_mismatches():
    sources = [
        {"domain": "amazon.com", "name": "Amazon"},
        {"domain": "officedepot.com", "name": "Office Depot"},
    ]
    supported, skipped = filter_supported_sources(sources, parse_product_spec("Dyson V8 Cordless Vacuum"))

    assert [row["domain"] for row in supported] == ["amazon.com"]
    assert [row["domain"] for row in skipped] == ["officedepot.com"]
