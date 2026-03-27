import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database import DEFAULT_SOURCES, init_db
from scraper import SearchExecutionContext, discover_deals_for_queries, discover_product_matches, inspect_direct_link


SMOKE_CASES = [
    {"kind": "strict", "query": "nintendo switch", "domain": "amazon.com"},
    {"kind": "strict", "query": "steam deck", "domain": "amazon.com"},
    {"kind": "strict", "query": "meta quest 3", "domain": "amazon.com"},
    {"kind": "strict", "query": "tp-link archer ax55 router", "domain": "amazon.com"},
    {"kind": "strict", "query": "brother hl-l2460dw printer", "domain": "amazon.com"},
    {"kind": "strict", "query": "dyson v8 cordless vacuum", "domain": "amazon.com"},
    {"kind": "discovery", "query": "lego orchid", "domain": "amazon.com"},
    {"kind": "discovery", "query": "standing desk", "domain": "walmart.com"},
]

DIRECT_LINK = "https://www.amazon.com/Apple-Smartwatch-Starlight-Aluminum-Always/dp/B0FQF58VGQ"


def _source(domain: str) -> dict:
    return dict(next(source for source in DEFAULT_SOURCES if source["domain"] == domain))


def _run_case(case: dict) -> dict:
    source = _source(case["domain"])
    context = SearchExecutionContext()
    started = time.perf_counter()
    if case["kind"] == "strict":
        matches = discover_product_matches(case["query"], source, context=context)
        verified = matches.get("verified", [])
        ambiguous = matches.get("ambiguous", [])
        return {
            "query": case["query"],
            "kind": case["kind"],
            "domain": case["domain"],
            "seconds": round(time.perf_counter() - started, 3),
            "verified_count": len(verified),
            "ambiguous_count": len(ambiguous),
            "top_name": (verified or ambiguous or [{}])[0].get("name_found"),
        }
    rows = discover_deals_for_queries((case["query"],), source, context=context)
    return {
        "query": case["query"],
        "kind": case["kind"],
        "domain": case["domain"],
        "seconds": round(time.perf_counter() - started, 3),
        "results": len(rows),
        "top_name": rows[0]["product_name"] if rows else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a live smoke matrix for representative PricePulse queries.")
    parser.add_argument("--output", help="Optional JSON output path")
    args = parser.parse_args()

    init_db()
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "cwd": os.getcwd(),
        "cases": [_run_case(case) for case in SMOKE_CASES],
    }
    direct = inspect_direct_link(DIRECT_LINK)
    report["direct_link"] = {
        "url": DIRECT_LINK,
        "status": direct.get("status"),
        "title": direct.get("title"),
        "price": direct.get("price"),
        "reason": direct.get("reason"),
    }

    text = json.dumps(report, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
        print(f"Wrote live smoke report to {output_path}")
    else:
        print(text)


if __name__ == "__main__":
    main()
