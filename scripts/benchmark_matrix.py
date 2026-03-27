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
from scraper import SearchExecutionContext, discover_deals_for_queries, discover_product_matches


BENCHMARK_CASES = [
    {"kind": "strict", "query": "sony wh-1000xm4", "domains": ["amazon.com", "walmart.com"]},
    {"kind": "discovery", "query": "airpods pro 3", "domains": ["amazon.com", "bestbuy.com", "walmart.com"]},
    {"kind": "strict", "query": "instant pot duo 7-in-1", "domains": ["amazon.com", "bestbuy.com"]},
    {"kind": "discovery", "query": "standing desk", "domains": ["amazon.com", "walmart.com"]},
    {"kind": "strict", "query": "rtx 4070", "domains": ["amazon.com", "bestbuy.com"]},
]


def _source(domain: str) -> dict:
    return dict(next(source for source in DEFAULT_SOURCES if source["domain"] == domain))


def run_case(case: dict) -> dict:
    results = []
    for domain in case["domains"]:
        source = _source(domain)
        context = SearchExecutionContext()
        started = time.perf_counter()
        if case["kind"] == "strict":
            matches = discover_product_matches(case["query"], source, context=context)
            verified = len(matches.get("verified", []))
            ambiguous = len(matches.get("ambiguous", []))
            top_name = (matches.get("verified") or matches.get("ambiguous") or [{}])[0].get("name_found")
            payload = {
                "verified": verified,
                "ambiguous": ambiguous,
                "top_name": top_name,
            }
        else:
            rows = discover_deals_for_queries((case["query"],), source, context=context)
            payload = {
                "results": len(rows),
                "top_name": rows[0]["product_name"] if rows else None,
            }
        results.append(
            {
                "domain": domain,
                "seconds": round(time.perf_counter() - started, 3),
                **payload,
            }
        )
    return {"kind": case["kind"], "query": case["query"], "results": results}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a repeatable benchmark matrix for PricePulse.")
    parser.add_argument("--output", help="Optional JSON output path")
    args = parser.parse_args()

    init_db()
    started = time.time()
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "cwd": os.getcwd(),
        "cases": [run_case(case) for case in BENCHMARK_CASES],
    }
    report["total_seconds"] = round(time.time() - started, 3)

    text = json.dumps(report, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
        print(f"Wrote benchmark report to {output_path}")
    else:
        print(text)


if __name__ == "__main__":
    main()
