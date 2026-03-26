"""
app.py — Flask web application with multi-source product discovery.

New pages (add, product detail, source editor, settings) use render_template_string
so that existing templates in /templates/ remain untouched.
"""

import hashlib
import hmac
import json
import logging
import os
import secrets
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from threading import Thread
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import (
    Flask, flash, redirect, render_template, render_template_string,
    request, send_from_directory, url_for,
)

load_dotenv()

from alerts import get_notification_status, send_alerts, send_discord_alert, send_gmail_alert
from database import (
    add_category,
    add_discovery_result,
    add_price_history,
    add_product,
    add_product_source,
    add_product_source_candidate,
    clear_product_source_candidates,
    compute_best_price,
    create_discovery_search,
    delete_product,
    delete_product_sources_by_source,
    ensure_generic_direct_source,
    find_source_for_url,
    get_all_categories,
    get_all_products,
    get_all_sources,
    get_best_source_url,
    get_categories_tree,
    get_category_by_id,
    get_discovery_result_by_id,
    get_discovery_results,
    get_discovery_search,
    get_last_checked_time,
    get_parent_categories,
    get_price_history,
    get_product_by_id,
    get_product_source_candidate,
    get_product_source_candidates,
    get_product_source_by_id,
    get_product_sources,
    get_source_by_id,
    init_db,
    mark_candidate_selected,
    update_product,
    update_category,
    update_discovery_search_count,
    update_product_source,
    update_source_enabled,
)
from scheduler import check_all_products, create_scheduler, run_initial_backfill
from hf_utils import get_smart_engine
from product_verifier import QueryType, parse_product_spec, verification_result_to_fields
from scraper import (
    SearchExecutionContext,
    canonicalize_listing_url,
    discover_deals,
    discover_deals_for_queries,
    discover_product,
    discover_product_matches,
    inspect_direct_link,
    revalidate_product_source,
    verify_candidate_listing,
)

DISCOVERY_SOURCE_WORKERS = int(os.getenv("DISCOVERY_SOURCE_WORKERS", "4"))
STRICT_SOURCE_WORKERS = int(os.getenv("STRICT_SOURCE_WORKERS", "4"))
DISCOVERY_VERIFY_WORKERS = int(os.getenv("DISCOVERY_VERIFY_WORKERS", "4"))


def _first_discover_listing(rows):
    """discover_product returns a list of hits (cheapest-first); DB stores one per source."""
    if not rows:
        return None
    return rows[0] if isinstance(rows, list) else rows


def _source_status_from_price(price, target_price, alert_mode="target_threshold"):
    if price is None:
        return "watching"
    if alert_mode == "any_drop" or target_price is None:
        return "watching"
    return "deal_found" if float(price) <= float(target_price) else "watching"


def _match_row_from_verification(candidate, verification):
    row = {
        "url": candidate.get("product_url") or candidate.get("url"),
        "price": (
            verification.current_price
            if verification.current_price is not None
            else candidate.get("current_price") or candidate.get("price")
        ),
        "name_found": (
            verification.product_name
            or candidate.get("product_name")
            or candidate.get("name_found")
        ),
    }
    row.update(verification_result_to_fields(verification))
    return row


def _matches_from_clicked_discovery_result(result, spec, source):
    candidate = {
        "product_url": result.get("product_url"),
        "product_name": result.get("product_name"),
        "current_price": result.get("current_price"),
    }
    verification = verify_candidate_listing(spec, source, candidate)
    if verification and verification.status in {"verified", "ambiguous"}:
        row = _match_row_from_verification(candidate, verification)
        return {
            "verified": [row] if verification.status == "verified" else [],
            "ambiguous": [row] if verification.status == "ambiguous" else [],
        }

    label = (result.get("verification_label") or "").strip().lower()
    if label not in {"verified_exact", "verified_named", "verified_related"}:
        return {"verified": [], "ambiguous": []}

    price = result.get("current_price")
    if price is None:
        return {"verified": [], "ambiguous": []}

    fallback_row = {
        "url": result.get("product_url"),
        "price": price,
        "name_found": result.get("product_name"),
        "verification_state": "verified" if label in {"verified_exact", "verified_named"} else "ambiguous",
        "verification_reason": "reused_discovery_verification",
        "health_state": "healthy",
        "matched_product_name": result.get("product_name"),
        "fingerprint_brand": spec.brand,
        "fingerprint_family": spec.family,
        "fingerprint_model": spec.model_token,
        "fingerprint_json": None,
        "match_label": label,
    }
    return {
        "verified": [fallback_row] if label in {"verified_exact", "verified_named"} else [],
        "ambiguous": [fallback_row] if label == "verified_related" else [],
    }


def _matches_from_direct_link_inspection(inspection):
    verification = inspection.get("verification")
    if not verification:
        return {"verified": [], "ambiguous": []}
    row = {
        "url": inspection.get("url"),
        "price": inspection.get("price"),
        "name_found": inspection.get("title"),
    }
    row.update(verification_result_to_fields(verification))
    return {
        "verified": [row] if verification.status == "verified" else [],
        "ambiguous": [row] if verification.status == "ambiguous" else [],
    }


def _direct_link_error_message(reason: str) -> str:
    return {
        "invalid_url": "Paste a full product URL to start tracking from a link.",
        "not_product_url": "That link does not look like a product page. Paste the product page URL instead.",
        "fetch_failed": "We couldn't fetch that product page right now. Please try again in a moment.",
        "weak_listing": "That page didn't expose a stable product title we can track reliably.",
        "price_not_found": "That page didn't expose a reliable current price, so it can't be tracked yet.",
    }.get(reason, "That link could not be verified for reliable tracking.")


def _discover_prefill_from_request():
    source_ids = []
    for raw_sid in request.args.getlist("source_ids"):
        try:
            source_ids.append(int(raw_sid))
        except (TypeError, ValueError):
            continue
    return {
        "query": request.args.get("query", "").strip(),
        "max_price": request.args.get("max_price", "").strip(),
        "search_all_sources": request.args.get("search_all_sources", "").strip() == "1",
        "source_ids": source_ids,
    }


def _promoted_tracking_spec(search, result):
    result_title = (result.get("product_name") or "").strip()
    search_query = ((search or {}).get("query") or "").strip()
    result_spec = parse_product_spec(result_title or search_query)
    if search_query:
        search_spec = parse_product_spec(search_query)
    else:
        search_spec = result_spec
    if search_spec.query_type == QueryType.CATEGORY.value:
        if result_spec.query_type == QueryType.CATEGORY.value:
            return None, None
        return result_spec, (result_spec.raw_query or result_title)
    return search_spec, (search_query or result_title)


def _persist_source_matches(
    product,
    source,
    matches,
    *,
    existing_ps=None,
    tracking_mode="search_verified",
    source_label_override=None,
    source_domain_override=None,
):
    product_data = dict(product) if hasattr(product, "keys") and not isinstance(product, dict) else product
    product_id = product["id"]
    source_id = source["id"]
    clear_product_source_candidates(product_id, source_id)

    def ensure_row():
        nonlocal existing_ps
        if existing_ps:
            return existing_ps["id"]
        ps_id = add_product_source(
            product_id,
            source_id,
            enabled=1,
            status="not_found",
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        existing_ps = get_product_source_by_id(ps_id) if ps_id else None
        return ps_id

    verified = matches.get("verified", [])
    ambiguous = matches.get("ambiguous", [])

    if verified:
        best = verified[0]
        ps_id = ensure_row()
        if not ps_id:
            return "error"
        update_product_source(
            ps_id,
            discovered_url=best["url"],
            current_price=best.get("price"),
            status=_source_status_from_price(
                best.get("price"),
                product_data.get("target_price"),
                product_data.get("alert_mode", "target_threshold"),
            ),
            verification_state="verified",
            verification_reason=best.get("verification_reason"),
            health_state=best.get("health_state", "healthy"),
            matched_product_name=best.get("matched_product_name") or best.get("name_found"),
            fingerprint_brand=best.get("fingerprint_brand"),
            fingerprint_family=best.get("fingerprint_family"),
            fingerprint_model=best.get("fingerprint_model"),
            fingerprint_json=best.get("fingerprint_json"),
            match_label=best.get("match_label", "verified_exact"),
            last_verified=datetime.now().isoformat(),
            last_checked=datetime.now().isoformat(),
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        if best.get("price") is not None:
            add_price_history(ps_id, best["price"])
        return "verified"

    if ambiguous:
        ps_id = ensure_row()
        if not ps_id:
            return "error"
        primary = ambiguous[0]
        update_product_source(
            ps_id,
            discovered_url=None,
            current_price=None,
            status="pending_confirmation",
            verification_state="pending_confirmation",
            verification_reason=primary.get("verification_reason"),
            health_state=primary.get("health_state", "healthy"),
            matched_product_name=primary.get("matched_product_name") or primary.get("name_found"),
            fingerprint_brand=primary.get("fingerprint_brand"),
            fingerprint_family=primary.get("fingerprint_family"),
            fingerprint_model=primary.get("fingerprint_model"),
            fingerprint_json=primary.get("fingerprint_json"),
            match_label=primary.get("match_label", "verified_related"),
            last_verified=datetime.now().isoformat(),
            last_checked=datetime.now().isoformat(),
            tracking_mode=tracking_mode,
            source_label_override=source_label_override,
            source_domain_override=source_domain_override,
        )
        for candidate in ambiguous:
            add_product_source_candidate(
                product_id,
                source_id,
                candidate["url"],
                product_source_id=ps_id,
                candidate_name=candidate.get("matched_product_name") or candidate.get("name_found"),
                candidate_price=candidate.get("price"),
                verification_state="ambiguous",
                verification_reason=candidate.get("verification_reason"),
                health_state=candidate.get("health_state", "healthy"),
                fingerprint_brand=candidate.get("fingerprint_brand"),
                fingerprint_family=candidate.get("fingerprint_family"),
                fingerprint_model=candidate.get("fingerprint_model"),
                match_label=candidate.get("match_label", "verified_related"),
                fingerprint_json=candidate.get("fingerprint_json"),
            )
        return "pending_confirmation"

    ps_id = ensure_row()
    if not ps_id:
        return "error"
    update_product_source(
        ps_id,
        discovered_url=None,
        current_price=None,
        status="not_found",
        verification_state="not_found",
        verification_reason="no_verified_match",
        health_state="healthy",
        matched_product_name=None,
        fingerprint_brand=None,
        fingerprint_family=None,
        fingerprint_model=None,
        fingerprint_json=None,
        match_label="related",
        last_verified=datetime.now().isoformat(),
        last_checked=datetime.now().isoformat(),
        tracking_mode=tracking_mode,
        source_label_override=source_label_override,
        source_domain_override=source_domain_override,
    )
    return "not_found"


def _apply_source_matches_for_product(product, sources):
    outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0}
    if not sources:
        compute_best_price(product["id"])
        return outcomes
    context = SearchExecutionContext()

    def task(source_row):
        source_dict = dict(source_row)
        matches = discover_product_matches(
            product["raw_query"] or product["name"],
            source_dict,
            target_price=product["target_price"],
            context=context,
        )
        return source_dict, matches

    with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(sources) or 1))) as executor:
        futures = {executor.submit(task, source): idx for idx, source in enumerate(sources)}
        by_index = [None] * len(futures)
        for future in as_completed(futures):
            idx = futures[future]
            try:
                by_index[idx] = future.result()
            except Exception as exc:
                logging.error(
                    f"[{datetime.now()}] Strict source matching failed for {dict(sources[idx]).get('name')}: {exc}"
                )
                by_index[idx] = None

    for source_dict, matches in [row for row in by_index if row is not None]:
        outcome = _persist_source_matches(product, source_dict, matches)
        if outcome in outcomes:
            outcomes[outcome] += 1
    compute_best_price(product["id"])
    return outcomes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# App — template folder is absolute so templates always load from this project,
# regardless of current working directory when you run `python app.py`.
# ---------------------------------------------------------------------------
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_STATIC_DIR = os.path.join(_APP_DIR, "static")
app = Flask(
    __name__,
    template_folder=os.path.join(_APP_DIR, "templates"),
    static_folder=_STATIC_DIR,
    static_url_path="/static",
)
app.secret_key = os.getenv("SECRET_KEY", "pricepulse-change-me-in-production")


def _ensure_database_at_startup():
    """Run even under gunicorn (not only `python app.py`) so sources are always seeded."""
    with app.app_context():
        try:
            init_db()
        except Exception as exc:
            logging.error(f"[{datetime.now()}] init_db at startup failed: {exc}")


_ensure_database_at_startup()
Thread(target=get_smart_engine, daemon=True).start()


def _manual_check_ui_token() -> str:
    """HMAC token so Check Now links work when CHECK_CRON_SECRET is set (same calendar day)."""
    key = app.secret_key
    if isinstance(key, str):
        key = key.encode("utf-8")
    msg = f"manual_check:{date.today().isoformat()}".encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:32]


def _manual_check_authorized() -> bool:
    """
    If CHECK_CRON_SECRET is unset, /check is open (local dev).
    If set, require cron secret (query token or X-Cron-Token) or a valid ui_token
    (see _manual_check_ui_token).
    """
    cron_secret = os.getenv("CHECK_CRON_SECRET", "").strip()
    if not cron_secret:
        return True
    q = request.args.get("token", "")
    h = request.headers.get("X-Cron-Token", "")
    if secrets.compare_digest(q, cron_secret) or secrets.compare_digest(h, cron_secret):
        return True
    ui = request.args.get("ui_token", "")
    if ui and secrets.compare_digest(ui, _manual_check_ui_token()):
        return True
    return False


@app.context_processor
def _inject_manual_check_url():
    return {
        "manual_check_url": (
            url_for("manual_check", ui_token=_manual_check_ui_token())
            if os.getenv("CHECK_CRON_SECRET", "").strip()
            else url_for("manual_check")
        ),
    }


@app.route("/favicon.ico")
def favicon():
    """Served at this path so browsers' default /favicon.ico request does not 404."""
    return send_from_directory(
        _STATIC_DIR,
        "favicon.svg",
        mimetype="image/svg+xml",
        max_age=86400,
    )


# ---------------------------------------------------------------------------
# Inline template fragments (shared CSS for new pages)
# ---------------------------------------------------------------------------

_CHIP_CSS = """
  .sources-grid { display: flex; flex-wrap: wrap; gap: 10px; }

  .source-chip {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 10px 16px; border-radius: 999px; cursor: pointer;
    transition: all 200ms ease; user-select: none;
    font-size: 13px; font-weight: 500;
  }
  .source-chip input[type="checkbox"] { display: none; }
  .source-chip.off {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    color: var(--text-muted);
  }
  .source-chip.on {
    background: rgba(124,92,252,0.15);
    border: 1px solid rgba(124,92,252,0.4);
    color: var(--text-primary);
    box-shadow: 0 0 12px rgba(124,92,252,0.15);
  }
  .source-dot {
    width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
  }
"""

_CHIP_JS = """
<script>
(function(){
  document.querySelectorAll('.source-chip').forEach(function(chip){
    chip.addEventListener('click', function(e){
      if(e.target.tagName==='INPUT') return;
      var cb=chip.querySelector('input[type="checkbox"]');
      cb.checked=!cb.checked;
      chip.classList.toggle('on',cb.checked);
      chip.classList.toggle('off',!cb.checked);
    });
  });
})();
</script>
"""

_FORM_CSS = """
  .add-outer { display:flex; justify-content:center; }
  .add-card { width:100%; max-width:520px; padding:36px; border-radius:var(--radius); }
  .back-link {
    display:inline-flex; align-items:center; gap:6px; font-size:13px;
    color:var(--text-secondary); margin-bottom:20px; transition:color 180ms ease;
  }
  .back-link:hover { color:var(--text-primary); }
  .page-title {
    font-family:'Space Grotesk',sans-serif; font-size:24px; font-weight:700;
    color:var(--text-primary); margin-bottom:28px; line-height:1.2;
  }
  .form-stack { display:flex; flex-direction:column; gap:20px; }
  .field { display:flex; flex-direction:column; gap:7px; }
  .field-label {
    font-size:12px; font-weight:600; letter-spacing:1px;
    text-transform:uppercase; color:var(--text-secondary);
  }
  .field-input {
    width:100%; background:rgba(255,255,255,0.05);
    border:1px solid var(--glass-border); border-radius:var(--radius-sm);
    padding:12px 16px; color:var(--text-primary);
    font-family:'DM Sans',sans-serif; font-size:14px; outline:none;
    transition:border-color 180ms ease, box-shadow 180ms ease;
    -webkit-appearance:none; appearance:none;
  }
  .field-input::placeholder { color:var(--text-muted); }
  .field-input:focus {
    border-color:var(--accent-purple);
    box-shadow:0 0 0 3px rgba(124,92,252,0.15);
  }
  .price-field-wrap { position:relative; display:flex; align-items:center; }
  .price-prefix {
    position:absolute; left:16px; font-size:14px; font-weight:600;
    color:var(--text-secondary); pointer-events:none;
    font-family:'Space Grotesk',sans-serif; z-index:1;
  }
  .field-input.price-input { padding-left:30px; }
  input[type=number]::-webkit-inner-spin-button,
  input[type=number]::-webkit-outer-spin-button { -webkit-appearance:none; }
  input[type=number] { -moz-appearance:textfield; }
  .btn-submit { width:100%; height:48px; font-size:14px; margin-top:8px; }
  .field-hint { font-size:12px; color:var(--text-muted); margin-top:2px; }
  .loading-overlay {
    display:none; position:fixed; inset:0;
    background:rgba(7,8,15,0.85); z-index:200;
    align-items:center; justify-content:center;
    flex-direction:column; gap:16px;
  }
  .spinner {
    width:40px; height:40px;
    border:3px solid var(--glass-border);
    border-top:3px solid var(--accent-purple);
    border-radius:50%; animation:spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
  @media (max-width:639px) { .add-card { padding:24px 16px; } }
"""

# ---------------------------------------------------------------------------
# Inline templates
# ---------------------------------------------------------------------------

TEMPLATE_ADD = (
    '{% extends "base.html" %}\n'
    "{% block title %}Find Deals — PricePulse{% endblock %}\n"
    "{% block head %}<style>"
    + _FORM_CSS
    + _CHIP_CSS
    + "</style>{% endblock %}\n"
    "{% block content %}\n"
    '<div class="add-outer"><div class="card add-card fade-in">\n'
    '  <a href="{{ url_for(\'index\') }}" class="back-link">\n'
    '    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="12" x2="5" y2="12"></line><polyline points="12 19 5 12 12 5"></polyline></svg>\n'
    "    Back to Dashboard</a>\n"
    '  <h1 class="page-title">Find the Best Deals</h1>\n'
    '  <form id="add-form" method="POST" action="{{ url_for(\'add_product_route\') }}" class="form-stack">\n'
    '    <div class="field">\n'
    '      <label class="field-label" for="name">Product Name <span style="color:var(--price-bad)">*</span></label>\n'
    '      <input class="field-input" type="text" id="name" name="name"\n'
    '             placeholder="e.g. Sony WH-1000XM5 Headphones" required maxlength="200" autocomplete="off" />\n'
    '      <span class="field-hint">Be specific for best results</span>\n'
    "    </div>\n"
    '    <div class="field">\n'
    '      <label class="field-label" for="target_price">Target Price <span style="color:var(--price-bad)">*</span></label>\n'
    '      <div class="price-field-wrap"><span class="price-prefix">$</span>\n'
    '        <input class="field-input price-input" type="number" id="target_price" name="target_price"\n'
    '               placeholder="0.00" step="0.01" min="0.01" required />\n'
    "      </div>\n"
    '      <span class="field-hint">You\'ll be alerted when the price drops to or below this amount.</span>\n'
    "    </div>\n"
    '    <div class="field">\n'
    '      <label class="field-label">Sources to Search</label>\n'
    '      <div class="sources-grid">\n'
    "        {% for s in sources %}\n"
    '        <label class="source-chip {{ \'on\' if s.enabled else \'off\' }}">\n'
    '          <input type="checkbox" name="source_ids" value="{{ s.id }}" {% if s.enabled %}checked{% endif %} />\n'
    '          <span class="source-dot" style="background:{{ s.logo_color }}"></span>{{ s.name }}\n'
    "        </label>\n"
    "        {% endfor %}\n"
    "      </div>\n"
    "    </div>\n"
    '    <button type="submit" class="btn-primary btn-submit">\n'
    '      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>\n'
    "      Find Deals</button>\n"
    "  </form>\n"
    "</div></div>\n"
    '<div class="loading-overlay" id="loading-overlay">\n'
    '  <div class="spinner"></div>\n'
    '  <div style="font-family:\'Space Grotesk\',sans-serif;font-size:18px;color:var(--text-primary)">Searching for deals\u2026</div>\n'
    '  <div style="font-size:13px;color:var(--text-secondary)">This may take 15\u201330 seconds</div>\n'
    "</div>\n"
    "{% endblock %}\n"
    "{% block scripts %}" + _CHIP_JS + "\n"
    "<script>document.getElementById('add-form').addEventListener('submit',function(){"
    "document.getElementById('loading-overlay').style.display='flex';});</script>\n"
    "{% endblock %}"
)

TEMPLATE_PRODUCT = (
    '{% extends "base.html" %}\n'
    "{% block title %}{{ product.name }} — PricePulse{% endblock %}\n"
    "{% block head %}<style>\n"
    "  .back-link{display:inline-flex;align-items:center;gap:6px;font-size:13px;"
    "color:var(--text-secondary);margin-bottom:20px;transition:color 180ms ease;}"
    ".back-link:hover{color:var(--text-primary);}\n"
    ".detail-title{font-family:'Space Grotesk',sans-serif;font-size:22px;"
    "font-weight:700;color:var(--text-primary);line-height:1.3;margin-bottom:6px;}\n"
    ".detail-meta{font-size:13px;color:var(--text-secondary);margin-bottom:28px;}\n"
    ".sources-row{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));"
    "gap:12px;margin-bottom:28px;}\n"
    ".source-card{padding:16px;border-radius:var(--radius-sm);}\n"
    ".source-card-head{display:flex;align-items:center;gap:8px;margin-bottom:10px;}\n"
    ".source-card-name{font-size:13px;font-weight:600;color:var(--text-secondary);}\n"
    ".sc-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;}\n"
    ".source-price{font-family:'Space Grotesk',sans-serif;font-size:22px;"
    "font-weight:700;line-height:1;}\n"
    ".source-status{font-size:11px;font-weight:600;letter-spacing:.6px;"
    "text-transform:uppercase;margin-top:6px;}\n"
    ".source-link{font-size:11px;color:var(--accent-blue);margin-top:6px;"
    "text-decoration:underline;text-underline-offset:3px;display:inline-block;}\n"
    ".status-watching{color:var(--accent-blue);}\n"
    ".status-deal{color:var(--price-good);}\n"
    ".status-pending{color:var(--price-warn);}\n"
    ".status-quarantine{color:var(--price-bad);}\n"
    ".status-notfound{color:var(--text-muted);}\n"
    ".status-error{color:var(--price-bad);}\n"
    ".actions-row{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:28px;}\n"
    ".btn-sm{padding:10px 18px;font-size:13px;border-radius:var(--radius-sm);}\n"
    ".btn-outline{background:transparent;border:1px solid var(--glass-border);"
    "color:var(--text-secondary);cursor:pointer;font-weight:600;"
    "transition:all 180ms ease;display:inline-flex;align-items:center;gap:6px;}\n"
    ".btn-outline:hover{border-color:rgba(255,255,255,.15);color:var(--text-primary);"
    "background:var(--glass-hover);}\n"
    ".btn-danger{background:transparent;border:1px solid rgba(255,79,106,.22);"
    "color:var(--price-bad);cursor:pointer;font-weight:600;font-size:13px;"
    "padding:10px 18px;border-radius:var(--radius-sm);display:inline-flex;"
    "align-items:center;gap:6px;transition:all 180ms ease;}\n"
    ".btn-danger:hover{background:rgba(255,79,106,.08);border-color:rgba(255,79,106,.4);}\n"
    "table{width:100%;border-collapse:collapse;}\n"
    "thead tr{border-bottom:1px solid var(--glass-border);}\n"
    "thead th{padding:14px 20px;text-align:left;font-size:11px;font-weight:600;"
    "letter-spacing:1.2px;text-transform:uppercase;color:var(--text-muted);}\n"
    "tbody tr{border-bottom:1px solid rgba(255,255,255,.03);transition:background 180ms ease;}\n"
    "tbody tr:nth-child(even){background:rgba(255,255,255,.02);}\n"
    "tbody tr:hover{background:rgba(255,255,255,.04);}\n"
    "tbody tr:last-child{border-bottom:none;}\n"
    "tbody td{padding:13px 20px;font-size:14px;color:var(--text-secondary);vertical-align:middle;}\n"
    ".td-price{font-family:'Space Grotesk',sans-serif;font-size:15px;font-weight:600;}\n"
    ".price-good{color:var(--price-good);}.price-warn{color:var(--price-warn);}.price-bad{color:var(--price-bad);}\n"
    ".table-card{padding:0;overflow:hidden;}\n"
    ".history-empty{padding:48px 24px;text-align:center;color:var(--text-secondary);font-size:14px;}\n"
    ".source-badge{display:inline-flex;align-items:center;gap:4px;font-size:11px;"
    "padding:2px 8px;border-radius:999px;background:var(--glass-bg);border:1px solid var(--glass-border);}\n"
    "@media(max-width:639px){.sources-row{grid-template-columns:1fr;}}\n"
    "</style>{% endblock %}\n"
    "{% block content %}\n"
    '<a href="{{ url_for(\'index\') }}" class="back-link">\n'
    '  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="12" x2="5" y2="12"></line><polyline points="12 19 5 12 12 5"></polyline></svg>\n'
    "  Back to Dashboard</a>\n"
    '<h1 class="detail-title">{{ product.name }}</h1>\n'
    '<p class="detail-meta">Target: <strong style="color:var(--text-primary)">{{ product.target_price | format_price }}</strong></p>\n'
    '<div class="section-label">Prices by Source</div>\n'
    '<div class="sources-row">\n'
    "  {% for ps in product_sources %}\n"
    '  <div class="card source-card fade-in" style="animation-delay:{{ loop.index0 * 60 }}ms">\n'
    '    <div class="source-card-head">\n'
    '      <span class="sc-dot" style="background:{{ ps.logo_color }}"></span>\n'
    '      <span class="source-card-name">{{ ps.source_name }}</span>\n'
    "    </div>\n"
    "    {% if ps.current_price is not none %}\n"
    '      <div class="source-price" style="color:{{ price_color(ps.current_price, product.target_price) }}">\n'
    "        {{ ps.current_price | format_price }}</div>\n"
    "    {% else %}\n"
    '      <div class="source-price" style="color:var(--text-muted)">N/A</div>\n'
    "    {% endif %}\n"
    '    <div class="source-status\n'
    "      {% if ps.status=='deal_found' %}status-deal{% elif ps.status=='watching' %}status-watching{% elif ps.status=='pending_confirmation' %}status-pending{% elif ps.status=='quarantined' %}status-quarantine{% elif ps.status=='error' %}status-error{% else %}status-notfound{% endif %}\">\n"
    "      {{ ps.status | replace('_',' ') | title }}</div>\n"
    "    {% if ps.verification_reason %}<div style=\"font-size:11px;color:var(--text-muted);margin-top:4px\">{{ ps.verification_reason | replace('_',' ') | title }}</div>{% endif %}\n"
    "    {% if ps.discovered_url %}\n"
    '      <a href="{{ ps.discovered_url }}" target="_blank" rel="noopener" class="source-link">View on {{ ps.source_name }} \u2197</a>\n'
    "    {% endif %}\n"
    "  </div>\n"
    "  {% endfor %}\n"
    "</div>\n"
    '<div class="actions-row">\n'
    '  <a href="{{ url_for(\'product_sources_page\', product_id=product.id) }}" class="btn-outline btn-sm">\n'
    '    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>\n'
    "    Edit Sources</a>\n"
    '  <form method="POST" action="{{ url_for(\'rediscover_route\', product_id=product.id) }}" style="display:inline">\n'
    '    <button class="btn-outline btn-sm" type="submit">\n'
    '      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"></polyline><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"></path></svg>\n'
    "      Re-discover</button></form>\n"
    "  {% if pending_candidates_count %}\n"
    '  <a href="{{ url_for(\'product_confirmations_page\', product_id=product.id) }}" class="btn-outline btn-sm">Confirm Matches ({{ pending_candidates_count }})</a>\n'
    "  {% endif %}\n"
    '  <form method="POST" action="{{ url_for(\'delete_product_route\', product_id=product.id) }}"\n'
    "        onsubmit=\"return confirm('Remove this product from tracking?')\" style=\"display:inline\">\n"
    '    <button class="btn-danger btn-sm" type="submit">\n'
    '      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6l-1 14H6L5 6"></path><path d="M10 11v6M14 11v6"></path><path d="M9 6V4h6v2"></path></svg>\n'
    "      Delete Product</button></form>\n"
    "</div>\n"
    '<div class="section-label">Price History</div>\n'
    '<div class="card table-card fade-in" style="animation-delay:80ms">\n'
    "  {% if price_history %}\n"
    "  <table><thead><tr><th>Date</th><th>Source</th><th>Price</th><th>vs Target</th></tr></thead>\n"
    "  <tbody>\n"
    "    {% for row in price_history %}\n"
    "    {% set diff = row.price - product.target_price %}\n"
    "    <tr>\n"
    "      <td>{{ row.checked_at[:19].replace('T',' ') }}</td>\n"
    '      <td><span class="source-badge"><span class="sc-dot" style="background:{{ row.logo_color }};width:8px;height:8px"></span>{{ row.source_name }}</span></td>\n'
    '      <td><span class="td-price {% if row.price <= product.target_price %}price-good{% elif row.price <= product.target_price * 1.10 %}price-warn{% else %}price-bad{% endif %}">${{ "%.2f" | format(row.price) }}</span></td>\n'
    "      <td>{% if diff <= 0 %}<span style=\"color:var(--price-good)\">\u2193 ${{ \"%.2f\" | format(diff | abs) }} below</span>"
    '{% else %}<span style="color:var(--text-muted)">${{ "%.2f" | format(diff) }} above</span>{% endif %}</td>\n'
    "    </tr>\n"
    "    {% endfor %}\n"
    "  </tbody></table>\n"
    "  {% else %}\n"
    '  <div class="history-empty"><p>No price checks recorded yet.</p></div>\n'
    "  {% endif %}\n"
    "</div>\n"
    "{% endblock %}"
)

TEMPLATE_SOURCES = (
    '{% extends "base.html" %}\n'
    "{% block title %}Edit Sources — {{ product.name }} — PricePulse{% endblock %}\n"
    "{% block head %}<style>"
    + _FORM_CSS
    + _CHIP_CSS
    + "</style>{% endblock %}\n"
    "{% block content %}\n"
    '<div class="add-outer"><div class="card add-card fade-in">\n'
    '  <a href="{{ url_for(\'product_detail\', product_id=product.id) }}" class="back-link">\n'
    '    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="12" x2="5" y2="12"></line><polyline points="12 19 5 12 12 5"></polyline></svg>\n'
    "    Back to Product</a>\n"
    '  <h1 class="page-title">Edit Sources</h1>\n'
    '  <p style="font-size:14px;color:var(--text-secondary);margin:-20px 0 24px">{{ product.name }}</p>\n'
    '  <form id="src-form" method="POST" action="{{ url_for(\'product_sources_save\', product_id=product.id) }}" class="form-stack">\n'
    '    <div class="field">\n'
    '      <label class="field-label">Active Sources</label>\n'
    '      <div class="sources-grid">\n'
    "        {% for s in all_sources %}\n"
    '        <label class="source-chip {{ \'on\' if s.id in active_ids else \'off\' }}">\n'
    '          <input type="checkbox" name="source_ids" value="{{ s.id }}"\n'
    "                 {% if s.id in active_ids %}checked{% endif %} />\n"
    '          <span class="source-dot" style="background:{{ s.logo_color }}"></span>{{ s.name }}\n'
    "        </label>\n"
    "        {% endfor %}\n"
    "      </div>\n"
    "    </div>\n"
    '    <button type="submit" class="btn-primary btn-submit">Save Sources</button>\n'
    "  </form>\n"
    "</div></div>\n"
    '<div class="loading-overlay" id="loading-overlay">\n'
    '  <div class="spinner"></div>\n'
    '  <div style="font-family:\'Space Grotesk\',sans-serif;font-size:18px;color:var(--text-primary)">Updating sources\u2026</div>\n'
    "</div>\n"
    "{% endblock %}\n"
    "{% block scripts %}" + _CHIP_JS + "\n"
    "<script>document.getElementById('src-form').addEventListener('submit',function(){"
    "document.getElementById('loading-overlay').style.display='flex';});</script>\n"
    "{% endblock %}"
)

TEMPLATE_CONFIRMATIONS = (
    '{% extends "base.html" %}\n'
    "{% block title %}Confirm Matches — {{ product.name }} — PricePulse{% endblock %}\n"
    "{% block head %}<style>\n"
    ".back-link{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:var(--text-secondary);margin-bottom:20px;}\n"
    ".page-title{font-family:'Space Grotesk',sans-serif;font-size:24px;font-weight:700;color:var(--text-primary);margin-bottom:8px;}\n"
    ".source-dot{width:10px;height:10px;border-radius:50%;display:inline-block;}\n"
    ".confirm-wrap{display:flex;flex-direction:column;gap:18px;}\n"
    ".confirm-head{margin-bottom:10px;}\n"
    ".confirm-sub{font-size:14px;color:var(--text-secondary);}\n"
    ".confirm-group{padding:18px;border-radius:var(--radius-sm);}\n"
    ".confirm-source{font-size:13px;font-weight:600;color:var(--text-secondary);margin-bottom:12px;display:flex;align-items:center;gap:8px;}\n"
    ".confirm-grid{display:grid;gap:12px;}\n"
    ".confirm-card{padding:16px;border-radius:var(--radius-sm);background:rgba(255,255,255,0.03);border:1px solid var(--glass-border);}\n"
    ".confirm-name{font-size:15px;font-weight:600;color:var(--text-primary);line-height:1.4;margin-bottom:6px;}\n"
    ".confirm-meta{font-size:12px;color:var(--text-muted);margin-bottom:12px;}\n"
    ".confirm-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center;}\n"
    ".confirm-link{font-size:13px;color:var(--accent-blue);text-decoration:underline;text-underline-offset:3px;}\n"
    ".confirm-badge{display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;border:1px solid rgba(124,92,252,0.25);background:rgba(124,92,252,0.1);font-size:11px;color:var(--text-secondary);}\n"
    "</style>{% endblock %}\n"
    "{% block content %}\n"
    '<a href="{{ url_for(\'product_detail\', product_id=product.id) }}" class="back-link">Back to Product</a>\n'
    '<div class="confirm-wrap">\n'
    '  <div class="confirm-head">\n'
    '    <h1 class="page-title">Confirm Ambiguous Matches</h1>\n'
    '    <p class="confirm-sub">{{ product.name }} needs confirmation before these sources can count toward pricing or alerts.</p>\n'
    "  </div>\n"
    "  {% for group in groups %}\n"
    '  <div class="card confirm-group fade-in">\n'
    '    <div class="confirm-source"><span class="source-dot" style="background:{{ group.logo_color }}"></span>{{ group.source_name }}</div>\n'
    '    <div class="confirm-grid">\n'
    "      {% for candidate in group.candidates %}\n"
    '      <div class="confirm-card">\n'
    '        <div class="confirm-name">{{ candidate.candidate_name }}</div>\n'
    '        <div class="confirm-meta">\n'
    '          {% if candidate.candidate_price is not none %}{{ candidate.candidate_price | format_price }} • {% endif %}'
    '          {{ candidate.verification_reason | replace("_"," ") | title }}\n'
    "        </div>\n"
    '        <div class="confirm-actions">\n'
    '          <form method="POST" action="{{ url_for(\'confirm_product_candidate\', product_id=product.id, candidate_id=candidate.id) }}">\n'
    '            <button class="btn-primary btn-sm" type="submit">Use This Match</button>\n'
    "          </form>\n"
    '          <a class="confirm-link" href="{{ candidate.candidate_url }}" target="_blank" rel="noopener">Open Listing ↗</a>\n'
    '          <span class="confirm-badge">{{ candidate.match_label | replace("_"," ") | title }}</span>\n'
    "        </div>\n"
    "      </div>\n"
    "      {% endfor %}\n"
    "    </div>\n"
    "  </div>\n"
    "  {% endfor %}\n"
    "</div>\n"
    "{% endblock %}"
)

TEMPLATE_SETTINGS = (
    '{% extends "base.html" %}\n'
    "{% block title %}Settings — PricePulse{% endblock %}\n"
    "{% block head %}<style>"
    + _FORM_CSS
    + _CHIP_CSS
    + "</style>{% endblock %}\n"
    "{% block content %}\n"
    '<div class="add-outer"><div class="card add-card fade-in">\n'
    '  <a href="{{ url_for(\'index\') }}" class="back-link">\n'
    '    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="12" x2="5" y2="12"></line><polyline points="12 19 5 12 12 5"></polyline></svg>\n'
    "    Back to Dashboard</a>\n"
    '  <h1 class="page-title">Source Settings</h1>\n'
    '  <p style="font-size:14px;color:var(--text-secondary);margin:-20px 0 24px">'
    "Choose which sources are enabled by default on Add Product and Discover (you can override per search).</p>\n"
    '  <form method="POST" action="{{ url_for(\'settings_sources_save\') }}" class="form-stack">\n'
    '    <div class="field">\n'
    '      <label class="field-label">Default Sources</label>\n'
    '      <div class="sources-grid">\n'
    "        {% for s in sources %}\n"
    '        <label class="source-chip {{ \'on\' if s.enabled else \'off\' }}">\n'
    '          <input type="checkbox" name="source_ids" value="{{ s.id }}"\n'
    "                 {% if s.enabled %}checked{% endif %} />\n"
    '          <span class="source-dot" style="background:{{ s.logo_color }}"></span>{{ s.name }}\n'
    "        </label>\n"
    "        {% endfor %}\n"
    "      </div>\n"
    "    </div>\n"
    '    <button type="submit" class="btn-primary btn-submit">\n'
    '      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg>\n'
    "      Save Settings</button>\n"
    "  </form>\n"
    "</div></div>\n"
    "{% endblock %}\n"
    "{% block scripts %}" + _CHIP_JS + "{% endblock %}"
)

# ---------------------------------------------------------------------------
# Jinja helpers
# ---------------------------------------------------------------------------

def format_relative_time(ts: str | None) -> str:
    if not ts:
        return "Never"
    try:
        dt = datetime.fromisoformat(ts)
        delta = datetime.now() - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return "Just now"
        if secs < 3600:
            m = secs // 60
            return f"{m} minute{'s' if m != 1 else ''} ago"
        if secs < 86400:
            h = secs // 3600
            return f"{h} hour{'s' if h != 1 else ''} ago"
        d = secs // 86400
        return f"{d} day{'s' if d != 1 else ''} ago"
    except Exception:
        return "Unknown"


def format_price(price) -> str:
    if price is None:
        return "N/A"
    return f"${float(price):,.2f}"


def price_status(current, target, alert_mode="target_threshold") -> str:
    if current is None:
        return "not_found"
    if alert_mode == "any_drop" or target is None:
        return "watching"
    return "deal" if float(current) <= float(target) else "watching"


def price_color(current, target, alert_mode="target_threshold") -> str:
    if current is None:
        return "var(--price-bad)"
    if alert_mode == "any_drop" or target is None:
        return "var(--accent-blue)"
    c, t = float(current), float(target)
    if c <= t:
        return "var(--price-good)"
    if c <= t * 1.10:
        return "var(--price-warn)"
    return "var(--price-bad)"


def progress_pct(current, target, alert_mode="target_threshold") -> float:
    if alert_mode == "any_drop" or target is None:
        return 100.0 if current is not None else 0.0
    if current is None or target is None or float(target) == 0:
        return 0.0
    return round(min(100.0, (float(target) / float(current)) * 100), 1)


def pct_away(current, target, alert_mode="target_threshold") -> str:
    if current is None:
        return ""
    if alert_mode == "any_drop" or target is None:
        return "Watching for a new lower price"
    c, t = float(current), float(target)
    if c <= t:
        return "Target reached!"
    pct = round(((c - t) / t) * 100, 1)
    return f"{pct}% away from target"


app.jinja_env.filters["relative_time"] = format_relative_time
app.jinja_env.filters["format_price"] = format_price
app.jinja_env.globals.update(
    price_status=price_status,
    price_color=price_color,
    progress_pct=progress_pct,
    pct_away=pct_away,
    format_relative_time=format_relative_time,
)


# ---------------------------------------------------------------------------
# Routes — Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    raw_products = get_all_products()

    products = []
    for p in raw_products:
        d = dict(p)
        sources = get_product_sources(p["id"])
        best_url = get_best_source_url(p["id"])
        d["url"] = best_url
        d["sources"] = [dict(s) for s in sources]
        d["sources_found"] = sum(1 for s in sources if s["current_price"] is not None)

        last_checked = None
        for s in sources:
            lc = s["last_checked"]
            if lc and (last_checked is None or lc > last_checked):
                last_checked = lc
        d["last_checked"] = last_checked
        products.append(d)

    total_products = len(products)
    deals_found = sum(
        1 for p in products
        if p.get("alert_mode") != "any_drop"
        if p["current_price"] is not None
        and p["target_price"] is not None
        and float(p["current_price"]) <= float(p["target_price"])
    )
    last_checked_global = get_last_checked_time()

    return render_template(
        "index.html",
        products=products,
        total_products=total_products,
        deals_found=deals_found,
        last_checked_global=last_checked_global,
    )


# ---------------------------------------------------------------------------
# Routes — Add Product
# ---------------------------------------------------------------------------

@app.route("/add", methods=["GET"])
def add_page():
    sources = get_all_sources()
    resp = app.make_response(
        render_template(
            "add.html",
            sources=sources,
            active_mode=request.args.get("mode", "search").strip() or "search",
            link_prefill_url=request.args.get("product_url", "").strip(),
            link_prefill_target=request.args.get("target_price", "").strip(),
        )
    )
    # Lets you verify in DevTools → Network → /add → Response headers that this build is live.
    resp.headers["X-PricePulse-Add-UI"] = "search-and-link-v1"
    return resp


@app.route("/add", methods=["POST"])
def add_product_route():
    name = request.form.get("name", "").strip()
    target_str = request.form.get("target_price", "").strip()
    search_all = request.form.get("search_all_sources") == "1"
    if search_all:
        source_ids = [str(s["id"]) for s in get_all_sources()]
    else:
        source_ids = request.form.getlist("source_ids")

    if not name:
        flash("A product name is required.", "error")
        return redirect(url_for("add_page"))

    try:
        target_price = float(target_str)
        if target_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Please enter a valid target price greater than $0.", "error")
        return redirect(url_for("add_page"))

    if not source_ids:
        flash("Select at least one source to search, or choose All sources.", "error")
        return redirect(url_for("add_page"))

    spec = parse_product_spec(name)
    if spec.query_type == QueryType.CATEGORY.value:
        flash(
            "That search is still broad. Pick a specific result from Discovery before creating a tracker.",
            "info",
        )
        redirect_args = {
            "query": name,
            "max_price": target_str,
        }
        if search_all:
            redirect_args["search_all_sources"] = "1"
        else:
            redirect_args["source_ids"] = source_ids
        return redirect(url_for("discover_page", **redirect_args))

    product_id = add_product(name, target_price)
    if not product_id:
        flash("Could not save the product — please try again.", "error")
        return redirect(url_for("add_page"))

    product = dict(get_product_by_id(product_id))
    selected_sources = []
    for raw_sid in source_ids:
        try:
            sid = int(raw_sid)
        except ValueError:
            continue
        source = get_source_by_id(sid)
        if source:
            selected_sources.append(source)

    outcomes = _apply_source_matches_for_product(product, selected_sources)
    total = len(selected_sources)
    verified = outcomes["verified"]
    pending = outcomes["pending_confirmation"]
    if search_all:
        flash(
            f'Now tracking "{name}" — verified on {verified} of {total} sources'
            + (f", {pending} need confirmation" if pending else "")
            + " (all registered stores).",
            "success",
        )
    else:
        flash(
            f'Now tracking "{name}" — verified on {verified} of {total} sources'
            + (f", {pending} need confirmation" if pending else ""),
            "success",
        )
    if pending:
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    return redirect(url_for("index"))


@app.route("/track/link", methods=["POST"])
def track_link_route():
    raw_url = request.form.get("product_url", "").strip()
    target_str = request.form.get("target_price", "").strip()

    if not raw_url:
        flash("Paste a product URL to start tracking from a direct link.", "error")
        return redirect(url_for("add_page", mode="link"))

    try:
        target_price = float(target_str) if target_str else None
        if target_price is not None and target_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Enter a valid notify price, or leave it blank for any lower verified price.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    matched_source = find_source_for_url(raw_url)
    source_row = matched_source or ensure_generic_direct_source()
    if not source_row:
        flash("Could not initialize direct-link tracking right now.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    inspection = inspect_direct_link(
        raw_url,
        source=dict(matched_source) if matched_source else None,
        context=SearchExecutionContext(),
    )
    if not inspection.get("ok"):
        flash(_direct_link_error_message(str(inspection.get("reason") or "")), "error")
        return redirect(
            url_for(
                "add_page",
                mode="link",
                product_url=canonicalize_listing_url(raw_url) or raw_url,
                target_price=target_str,
            )
        )

    tracking_name = inspection.get("title") or canonicalize_listing_url(raw_url)
    alert_mode = "target_threshold" if target_price is not None else "any_drop"
    product_id = add_product(
        tracking_name,
        target_price,
        alert_mode=alert_mode,
        origin_type="direct_link",
    )
    if not product_id:
        flash("Could not save that link for tracking.", "error")
        return redirect(url_for("add_page", mode="link", product_url=raw_url, target_price=target_str))

    product = dict(get_product_by_id(product_id))
    host = (inspection.get("domain") or urlparse(raw_url).netloc or "").replace("www.", "")
    source_label_override = None if matched_source else host
    source_domain_override = None if matched_source else host
    matches = _matches_from_direct_link_inspection(inspection)
    outcome = _persist_source_matches(
        product,
        dict(source_row),
        matches,
        tracking_mode="direct_url",
        source_label_override=source_label_override,
        source_domain_override=source_domain_override,
    )
    compute_best_price(product_id)

    if outcome == "pending_confirmation":
        flash(f'Added "{tracking_name}" — confirm the match before tracking alerts start.', "success")
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    if alert_mode == "any_drop":
        flash(
            f'Now tracking "{tracking_name}" from its direct link. You’ll be alerted on any new verified lower price.',
            "success",
        )
    else:
        flash(
            f'Now tracking "{tracking_name}" from its direct link at your ${target_price:,.2f} notify price.',
            "success",
        )
    return redirect(url_for("index"))

    found_count = 0
    for sid in source_ids:
        try:
            sid = int(sid)
        except ValueError:
            continue
        source = get_source_by_id(sid)
        if not source:
            continue
        source_dict = dict(source)

        rows = discover_product(name, source_dict, target_price=target_price)
        result = _first_discover_listing(rows)
        if result:
            ps_id = add_product_source(
                product_id, sid, enabled=1,
                discovered_url=result["url"],
                current_price=result["price"],
                status="deal_found" if result["price"] <= target_price else "watching",
            )
            if ps_id and result["price"]:
                add_price_history(ps_id, result["price"])
            found_count += 1
        else:
            add_product_source(product_id, sid, enabled=1, status="not_found")

    compute_best_price(product_id)

    total = len(source_ids)
    if search_all:
        flash(
            f'Now tracking "{name}" — found on {found_count} of {total} sources (all registered stores).',
            "success",
        )
    else:
        flash(
            f'Now tracking "{name}" — found on {found_count} of {total} sources.',
            "success",
        )
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Routes — Product Detail
# ---------------------------------------------------------------------------

@app.route("/product/<int:product_id>")
def product_detail(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    product_sources = get_product_sources(product_id)
    price_history_rows = get_price_history(product_id)
    pending_candidates = get_product_source_candidates(product_id)

    return render_template(
        "product.html",
        product=dict(product),
        product_sources=product_sources,
        price_history=price_history_rows,
        pending_candidates_count=len(pending_candidates),
    )


@app.route("/product/<int:product_id>/confirmations")
def product_confirmations_page(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    candidates = get_product_source_candidates(product_id)
    if not candidates:
        flash("No pending confirmations for this product.", "success")
        return redirect(url_for("product_detail", product_id=product_id))

    grouped: dict[int, dict] = {}
    for row in candidates:
        source_id = row["source_id"]
        group = grouped.setdefault(
            source_id,
            {
                "source_id": source_id,
                "source_name": row["source_name"],
                "logo_color": row["logo_color"],
                "candidates": [],
            },
        )
        group["candidates"].append(dict(row))

    return render_template_string(
        TEMPLATE_CONFIRMATIONS,
        product=dict(product),
        groups=list(grouped.values()),
    )


@app.route("/product/<int:product_id>/confirm/<int:candidate_id>", methods=["POST"])
def confirm_product_candidate(product_id, candidate_id):
    product = get_product_by_id(product_id)
    candidate = get_product_source_candidate(candidate_id)
    if not product or not candidate or candidate["product_id"] != product_id:
        flash("Confirmation candidate not found.", "error")
        return redirect(url_for("index"))
    product_data = dict(product)

    ps_id = candidate["product_source_id"]
    if not ps_id:
        ps_id = add_product_source(
            product_id,
            candidate["source_id"],
            enabled=1,
            status="pending_confirmation",
            verification_state="pending_confirmation",
        )
    status = (
        "quarantined"
        if candidate["candidate_price"] is None
        else _source_status_from_price(
            candidate["candidate_price"],
            product_data.get("target_price"),
            product_data.get("alert_mode", "target_threshold"),
        )
    )
    update_product_source(
        ps_id,
        discovered_url=candidate["candidate_url"],
        current_price=candidate["candidate_price"],
        status=status,
        verification_state="verified",
        verification_reason="user_confirmed",
        health_state="healthy" if candidate["candidate_price"] is not None else "quarantined",
        matched_product_name=candidate["candidate_name"],
        fingerprint_brand=candidate["fingerprint_brand"],
        fingerprint_family=candidate["fingerprint_family"],
        fingerprint_model=candidate["fingerprint_model"],
        fingerprint_json=candidate["fingerprint_json"],
        match_label="verified_related",
        last_verified=datetime.now().isoformat(),
        last_checked=datetime.now().isoformat(),
    )
    if candidate["candidate_price"] is not None:
        add_price_history(ps_id, candidate["candidate_price"])
    mark_candidate_selected(candidate_id)
    clear_product_source_candidates(product_id, candidate["source_id"])
    compute_best_price(product_id)
    flash(f'Confirmed match for {candidate["source_name"]}.', "success")
    return redirect(url_for("product_detail", product_id=product_id))


@app.route("/history/<int:product_id>")
def history(product_id):
    """Backward-compatible redirect to new product detail page."""
    return redirect(url_for("product_detail", product_id=product_id))


# ---------------------------------------------------------------------------
# Routes — Product Source Management
# ---------------------------------------------------------------------------

@app.route("/product/<int:product_id>/sources", methods=["GET"])
def product_sources_page(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    all_sources = get_all_sources()
    current_ps = get_product_sources(product_id)
    active_ids = {ps["source_id"] for ps in current_ps}

    return render_template_string(
        TEMPLATE_SOURCES,
        product=dict(product),
        all_sources=all_sources,
        active_ids=active_ids,
    )


@app.route("/product/<int:product_id>/sources", methods=["POST"])
def product_sources_save(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    new_source_ids = set()
    for sid in request.form.getlist("source_ids"):
        try:
            new_source_ids.add(int(sid))
        except ValueError:
            continue

    current_ps = get_product_sources(product_id)
    current_map = {ps["source_id"]: ps for ps in current_ps}

    for sid in list(current_map.keys()):
        if sid not in new_source_ids:
            delete_product_sources_by_source(product_id, sid)

    newly_added = []
    for sid in new_source_ids:
        if sid not in current_map:
            newly_added.append(sid)
            add_product_source(product_id, sid, enabled=1, status="not_found")

    if newly_added:
        outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0}
        context = SearchExecutionContext()

        def task(sid):
            source = get_source_by_id(sid)
            if not source:
                return None
            ps_rows = get_product_sources(product_id)
            ps_row = next((r for r in ps_rows if r["source_id"] == sid), None)
            source_dict = dict(source)
            matches = discover_product_matches(
                product["raw_query"] or product["name"],
                source_dict,
                target_price=product["target_price"],
                context=context,
            )
            return source_dict, ps_row, matches

        with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(newly_added)))) as executor:
            futures = {executor.submit(task, sid): idx for idx, sid in enumerate(newly_added)}
            ordered = [None] * len(futures)
            for future in as_completed(futures):
                try:
                    ordered[futures[future]] = future.result()
                except Exception as exc:
                    logging.error(f"[{datetime.now()}] Source add verification failed: {exc}")
                    ordered[futures[future]] = None

        for item in [row for row in ordered if row is not None]:
            source_dict, ps_row, matches = item
            outcome = _persist_source_matches(dict(product), dict(source_dict), matches, existing_ps=ps_row)
            if outcome in outcomes:
                outcomes[outcome] += 1
        compute_best_price(product_id)
        flash(
            f"Sources updated — {outcomes['verified']} verified, "
            f"{outcomes['pending_confirmation']} pending confirmation.",
            "success",
        )
        if outcomes["pending_confirmation"]:
            return redirect(url_for("product_confirmations_page", product_id=product_id))
        return redirect(url_for("product_detail", product_id=product_id))

    if newly_added:
        found = 0
        for sid in newly_added:
            source = get_source_by_id(sid)
            if not source:
                continue
            rows = discover_product(
                product["name"], dict(source),
                target_price=product["target_price"],
            )
            result = _first_discover_listing(rows)
            ps_rows = get_product_sources(product_id)
            ps_row = next((r for r in ps_rows if r["source_id"] == sid), None)
            if result and ps_row:
                status = ("deal_found"
                          if result["price"] <= product["target_price"]
                          else "watching")
                update_product_source(
                    ps_row["id"],
                    discovered_url=result["url"],
                    current_price=result["price"],
                    status=status,
                    last_checked=datetime.now().isoformat(),
                )
                add_price_history(ps_row["id"], result["price"])
                found += 1
        compute_best_price(product_id)
        flash(f"Sources updated — discovered {found} new listing(s).", "success")
    else:
        compute_best_price(product_id)
        flash("Sources updated.", "success")

    return redirect(url_for("product_detail", product_id=product_id))


@app.route("/product/<int:product_id>/rediscover", methods=["POST"])
def rediscover_route(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    ps_list = get_product_sources(product_id)
    outcomes = {"verified": 0, "pending_confirmation": 0, "not_found": 0}
    context = SearchExecutionContext()

    def rediscover_task(ps_row):
        source_dict = {
            "id": ps_row["source_id"],
            "name": ps_row["source_name"],
            "domain": ps_row["domain"],
            "search_url_template": ps_row["search_url_template"],
        }
        if ps_row.get("tracking_mode") == "direct_url":
            matches = revalidate_product_source(dict(ps_row), context=context)
        else:
            matches = discover_product_matches(
                product["raw_query"] or product["name"],
                source_dict,
                target_price=product["target_price"],
                context=context,
            )
        return ps_row, source_dict, matches

    with ThreadPoolExecutor(max_workers=max(1, min(STRICT_SOURCE_WORKERS, len(ps_list) or 1))) as executor:
        futures = {executor.submit(rediscover_task, ps): idx for idx, ps in enumerate(ps_list)}
        ordered = [None] * len(futures)
        for future in as_completed(futures):
            try:
                ordered[futures[future]] = future.result()
            except Exception as exc:
                logging.error(f"[{datetime.now()}] Re-discovery failed: {exc}")
                ordered[futures[future]] = None

    for item in [row for row in ordered if row is not None]:
        ps, source_dict, matches = item
        if ps.get("tracking_mode") == "direct_url":
            outcome = _persist_source_matches(
                dict(product),
                source_dict,
                matches,
                existing_ps=ps,
                tracking_mode="direct_url",
                source_label_override=ps.get("source_label_override"),
                source_domain_override=ps.get("source_domain_override"),
            )
        else:
            outcome = _persist_source_matches(dict(product), source_dict, matches, existing_ps=ps)
        if outcome in outcomes:
            outcomes[outcome] += 1

    compute_best_price(product_id)
    flash(
        f"Re-discovery complete — {outcomes['verified']} verified, "
        f"{outcomes['pending_confirmation']} pending confirmation.",
        "success",
    )
    if outcomes["pending_confirmation"]:
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    return redirect(url_for("product_detail", product_id=product_id))

    found = 0
    for ps in ps_list:
        source_dict = {
            "domain": ps["domain"],
            "search_url_template": ps["search_url_template"],
        }
        rows = discover_product(
            product["name"], source_dict,
            target_price=product["target_price"],
        )
        result = _first_discover_listing(rows)
        if result:
            status = ("deal_found"
                      if result["price"] <= product["target_price"]
                      else "watching")
            update_product_source(
                ps["id"],
                discovered_url=result["url"],
                current_price=result["price"],
                status=status,
                last_checked=datetime.now().isoformat(),
            )
            add_price_history(ps["id"], result["price"])
            found += 1
        else:
            update_product_source(
                ps["id"], status="not_found",
                last_checked=datetime.now().isoformat(),
            )

    compute_best_price(product_id)
    flash(f"Re-discovery complete — found on {found} of {len(ps_list)} sources.", "success")
    return redirect(url_for("product_detail", product_id=product_id))


# ---------------------------------------------------------------------------
# Routes — Delete
# ---------------------------------------------------------------------------

@app.route("/delete/<int:product_id>", methods=["POST"])
def delete_product_route(product_id):
    product = get_product_by_id(product_id)
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("index"))

    display = product["name"] or "Product"
    if delete_product(product_id):
        flash(f'Removed "{display}" from tracking.', "success")
    else:
        flash("Could not delete the product — please try again.", "error")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Routes — Settings
# ---------------------------------------------------------------------------

@app.route("/settings")
def settings_page():
    sources = get_all_sources()
    return render_template(
        "settings.html",
        sources=sources,
        notification_status=get_notification_status(),
    )


@app.route("/settings/sources", methods=["POST"])
def settings_sources_save():
    enabled_ids = set()
    for sid in request.form.getlist("source_ids"):
        try:
            enabled_ids.add(int(sid))
        except ValueError:
            continue

    for source in get_all_sources():
        update_source_enabled(source["id"], 1 if source["id"] in enabled_ids else 0)

    flash("Default sources updated.", "success")
    return redirect(url_for("settings_page"))


@app.route("/settings/notifications/test/<channel>", methods=["POST"])
def settings_test_notification(channel):
    status = get_notification_status()
    channel = (channel or "").strip().lower()
    if channel not in {"discord", "gmail", "all"}:
        flash("Unknown notification channel.", "error")
        return redirect(url_for("settings_page"))

    if channel == "discord" and not status["discord_configured"]:
        flash("Discord webhook is not configured yet.", "error")
        return redirect(url_for("settings_page"))
    if channel == "gmail" and not status["gmail_configured"]:
        flash("Gmail alert settings are not configured yet.", "error")
        return redirect(url_for("settings_page"))

    sent = False
    if channel == "discord":
        sent = send_discord_alert(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )
    elif channel == "gmail":
        sent = send_gmail_alert(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )
    else:
        sent = send_alerts(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )

    flash(
        "Test notification sent." if sent else "Test notification could not be sent. Check your current channel settings.",
        "success" if sent else "error",
    )
    return redirect(url_for("settings_page"))


# ---------------------------------------------------------------------------
# Routes — Deal Discovery
# ---------------------------------------------------------------------------

def _sources_from_posted_ids(search_all: bool, posted_ids: list[str]) -> list:
    """Map discover/add-product style flags to source rows (stable order)."""
    if search_all:
        return list(get_all_sources())
    seen: set[int] = set()
    out = []
    for raw in posted_ids:
        try:
            sid = int(raw)
        except (ValueError, TypeError):
            continue
        if sid in seen:
            continue
        seen.add(sid)
        row = get_source_by_id(sid)
        if row:
            out.append(row)
    return out


@app.route("/discover")
def discover_page():
    tree = get_categories_tree()
    parents = get_parent_categories()
    sources = get_all_sources()
    if not sources:
        logging.warning(
            f"[{datetime.now()}] discover_page: no sources in DB — re-running init_db"
        )
        try:
            init_db()
            sources = get_all_sources()
        except Exception as exc:
            logging.error(f"[{datetime.now()}] discover init_db retry failed: {exc}")
    prefill = _discover_prefill_from_request()
    return render_template(
        "discover.html",
        categories=tree,
        parent_categories=parents,
        sources=sources,
        prefill=prefill,
    )


@app.route("/discover/search", methods=["POST"])
def discover_search():
    query = request.form.get("query", "").strip()
    max_price_str = request.form.get("max_price", "").strip()
    category_id = request.form.get("category_id", "").strip() or None

    if not query:
        flash("Enter a search term to find deals.", "error")
        return redirect(url_for("discover_page"))

    try:
        max_price = float(max_price_str) if max_price_str else None
        if max_price is not None and max_price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        flash("Please enter a valid maximum price.", "error")
        return redirect(url_for("discover_page"))

    cat_id_int = None
    if category_id:
        try:
            cat_id_int = int(category_id)
        except ValueError:
            pass

    # --- AI: enhance the search query --------------------------------
    engine = get_smart_engine()
    query_spec = parse_product_spec(query)
    search_terms = query
    if cat_id_int and query_spec.query_type == "category" and not query_spec.model_token:
        cat = get_category_by_id(cat_id_int)
        if cat and cat["search_keywords"]:
            search_terms = cat["search_keywords"].split(",")[0].strip()
    elif engine.available and query_spec.query_type == "category" and not query_spec.model_token:
        search_terms = engine.enhance_query(query)

    search_all = request.form.get("search_all_sources") == "1"
    posted_source_ids = request.form.getlist("source_ids")
    sources = _sources_from_posted_ids(search_all, posted_source_ids)
    if not sources:
        flash(
            "Select at least one store to search, or turn on All registered stores.",
            "error",
        )
        return redirect(url_for("discover_page"))

    filter_condition = request.form.get("filter_condition", "new_only")
    if filter_condition not in ("new_only", "include_refurb", "all"):
        filter_condition = "new_only"
    filter_product_type = request.form.get("filter_product_type", "primary_only")
    if filter_product_type not in ("primary_only", "include_accessories"):
        filter_product_type = "primary_only"
    filter_brand = request.form.get("filter_brand", "exact")
    if filter_brand not in ("exact", "similar"):
        filter_brand = "exact"

    search_id = create_discovery_search(
        query,
        cat_id_int,
        max_price,
        filter_condition=filter_condition,
        filter_product_type=filter_product_type,
        filter_brand=filter_brand,
    )
    if not search_id:
        flash("Could not start discovery — please try again.", "error")
        return redirect(url_for("discover_page"))

    # --- Scrape chosen sources, collect raw results -----------------
    if query_spec.query_type in {QueryType.EXACT_MODEL.value, QueryType.NAMED_PRODUCT.value}:
        search_queries = query_spec.search_aliases or (search_terms,)
    else:
        search_queries = (search_terms,)

    all_raw: list[dict] = []
    context = SearchExecutionContext()

    def discover_source(source_row):
        source_dict = dict(source_row)
        deals = discover_deals_for_queries(
            tuple(search_queries),
            source_dict,
            max_price=max_price,
            context=context,
        )
        for deal in deals:
            deal["source_id"] = source_row["id"]
            deal["source_name"] = source_row["name"]
        return deals

    with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_SOURCE_WORKERS, len(sources) or 1))) as executor:
        futures = {executor.submit(discover_source, source): idx for idx, source in enumerate(sources)}
        source_rows_by_index = [[] for _ in sources]
        for future in as_completed(futures):
            idx = futures[future]
            try:
                source_rows_by_index[idx] = future.result()
            except Exception as exc:
                source = sources[idx]
                logging.error(
                    f"[{datetime.now()}] Discovery error on {source['name']} for query {query!r}: {exc}"
                )
                source_rows_by_index[idx] = []

    for rows in source_rows_by_index:
        all_raw.extend(rows)

    scraped_before_rank = len(all_raw)

    # --- Classify, gate, relevance, dedupe, deal score --------------
    ai_enhanced = False
    if all_raw:
        all_raw = engine.process_discovery_results(
            query,
            all_raw,
            condition_filter=filter_condition,
            product_filter=filter_product_type,
            brand_filter=filter_brand,
        )
        ai_enhanced = engine.available
        strict_spec = parse_product_spec(query)
        source_map = {int(s["id"]): dict(s) for s in sources}
        with ThreadPoolExecutor(max_workers=max(1, min(DISCOVERY_VERIFY_WORKERS, len(all_raw) or 1))) as executor:
            futures = {}
            for idx, row in enumerate(all_raw):
                source_dict = source_map.get(int(row["source_id"]))
                if not source_dict:
                    row["verification_label"] = "related"
                    continue
                futures[
                    executor.submit(
                        verify_candidate_listing,
                        strict_spec,
                        source_dict,
                        {
                            "product_url": row["product_url"],
                            "product_name": row["product_name"],
                            "current_price": row["current_price"],
                        },
                        context=context,
                    )
                ] = idx
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    verification = future.result()
                except Exception as exc:
                    logging.error(
                        f"[{datetime.now()}] Discovery verification error for {all_raw[idx].get('product_url')}: {exc}"
                    )
                    verification = None
                all_raw[idx]["verification_label"] = (
                    verification.match_label if verification is not None else "related"
                )

    # --- Persist results ---------------------------------------------
    for r in all_raw:
        add_discovery_result(
            search_id=search_id,
            source_id=r["source_id"],
            product_name=r["product_name"],
            current_price=r["current_price"],
            original_price=r.get("original_price"),
            discount_percent=r.get("discount_percent", 0),
            product_url=r["product_url"],
            relevance_score=r.get("relevance_score", 0),
            deal_score=r.get("deal_score", 0),
            group_id=r.get("group_id"),
            also_available_at=r.get("also_available_at"),
            discount_confirmed=1 if r.get("discount_confirmed", True) else 0,
            verification_label=r.get("verification_label", "related"),
        )

    update_discovery_search_count(search_id, len(all_raw))

    if not all_raw:
        if scraped_before_rank:
            flash(
                f'No listings matched your filters or quality threshold for "{query}". '
                "Try including refurbished items, accessories, similar brands, or a different search term.",
                "error",
            )
        else:
            flash(
                f'No deals found for "{query}". Try broadening your search or raising your budget.',
                "error",
            )
    else:
        ai_note = " (AI-ranked)" if ai_enhanced else ""
        flash(f'Found {len(all_raw)} deals for "{query}"{ai_note}!', "success")

    return redirect(url_for("discover_results", search_id=search_id))


@app.route("/discover/results/<int:search_id>")
def discover_results(search_id):
    search = get_discovery_search(search_id)
    if not search:
        flash("Search not found.", "error")
        return redirect(url_for("discover_page"))

    raw_results = get_discovery_results(search_id)
    results = []
    has_ai_scores = False
    for r in raw_results:
        d = dict(r)
        if d.get("also_available_at"):
            try:
                d["also_available_at"] = json.loads(d["also_available_at"])
            except (json.JSONDecodeError, TypeError):
                d["also_available_at"] = None
        if d.get("deal_score", 0) > 0:
            has_ai_scores = True
        results.append(d)

    groups: dict[int, list[int]] = {}
    for idx, r in enumerate(results):
        gid = r.get("group_id")
        if gid is not None:
            groups.setdefault(gid, []).append(idx)
    for gid, members in groups.items():
        best_idx = min(members, key=lambda i: results[i].get("current_price", float("inf")))
        for idx in members:
            results[idx]["group_size"] = len(members)
            results[idx]["is_best_in_group"] = (idx == best_idx)

    query_spec = parse_product_spec(search["query"])

    return render_template(
        "discover_results.html",
        search=dict(search),
        results=results,
        ai_enhanced=has_ai_scores,
        query_spec=query_spec,
        is_category_search=(query_spec.query_type == QueryType.CATEGORY.value),
    )


@app.route("/discover/track/<int:result_id>", methods=["POST"])
def discover_track(result_id):
    result = get_discovery_result_by_id(result_id)
    if not result:
        flash("Deal not found.", "error")
        return redirect(url_for("discover_page"))

    result = dict(result)
    search = get_discovery_search(result["search_id"]) if result.get("search_id") else None
    search = dict(search) if search else None
    spec, tracking_name = _promoted_tracking_spec(search, result)
    if spec is None or not tracking_name:
        flash(
            "That result is still too broad to track directly. Choose a more specific listing.",
            "error",
        )
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    price = result.get("current_price")
    target_price = (
        search.get("max_price")
        if search and search.get("max_price") is not None
        else price
    )
    target_price = target_price if target_price is not None else 0
    source_id = result["source_id"]
    source = get_source_by_id(source_id) if source_id else None
    if not source or not result.get("product_url"):
        flash("That result cannot be tracked because the source listing is missing.", "error")
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    matches = _matches_from_clicked_discovery_result(result, spec, dict(source))
    if not matches["verified"] and not matches["ambiguous"]:
        flash(
            "Could not verify that listing for tracking. Please rerun discovery or choose a different result.",
            "error",
        )
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    product_id = add_product(tracking_name, target_price)
    if not product_id:
        flash("Could not save the product.", "error")
        if result.get("search_id"):
            return redirect(url_for("discover_results", search_id=result["search_id"]))
        return redirect(url_for("discover_page"))

    product = dict(get_product_by_id(product_id))
    outcome = _persist_source_matches(product, dict(source), matches)
    compute_best_price(product_id)
    if outcome == "pending_confirmation":
        flash(f'Created "{tracking_name}" with matches awaiting confirmation.', "success")
        return redirect(url_for("product_confirmations_page", product_id=product_id))
    flash(f'Now tracking "{tracking_name}".', "success")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Routes — Category Admin
# ---------------------------------------------------------------------------

@app.route("/admin/categories", methods=["GET", "POST"])
def admin_categories():
    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "add":
            name = request.form.get("name", "").strip()
            icon = request.form.get("icon", "📦").strip()
            parent_id = request.form.get("parent_id", "").strip() or None
            keywords = request.form.get("search_keywords", "").strip()
            slug = name.lower().replace(" ", "-").replace("&", "and")
            if name:
                add_category(name, slug, int(parent_id) if parent_id else None, keywords, icon)
                flash(f'Added category "{name}".', "success")

        elif action == "update":
            cat_id = request.form.get("category_id")
            if cat_id:
                fields = {}
                for f in ("name", "icon", "search_keywords"):
                    v = request.form.get(f, "").strip()
                    if v:
                        fields[f] = v
                pid = request.form.get("parent_id", "").strip()
                fields["parent_id"] = int(pid) if pid else None
                if "name" in fields:
                    fields["slug"] = fields["name"].lower().replace(" ", "-").replace("&", "and")
                update_category(int(cat_id), **fields)
                flash("Category updated.", "success")

        elif action == "toggle":
            cat_id = request.form.get("category_id")
            enabled = request.form.get("enabled", "0")
            if cat_id:
                update_category(int(cat_id), enabled=int(enabled))

        return redirect(url_for("admin_categories"))

    categories = get_all_categories()
    parents = get_parent_categories()
    return render_template(
        "admin_categories.html",
        categories=[dict(c) for c in categories],
        parent_categories=[dict(p) for p in parents],
    )


# ---------------------------------------------------------------------------
# Routes — Manual Check
# ---------------------------------------------------------------------------

@app.route("/check")
def manual_check():
    if not _manual_check_authorized():
        logging.warning(f"[{datetime.now()}] /check rejected: unauthorized")
        flash("Not authorized to run a price check.", "error")
        return redirect(url_for("index"))
    try:
        check_all_products()
        flash("Price check complete!", "success")
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Manual check error: {exc}")
        flash(f"Price check failed: {exc}", "error")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Live reload: set FLASK_DEBUG=1 in .env (or environment). Werkzeug spawns a
    # parent watchdog + child server; only start the scheduler in the child to
    # avoid duplicate APScheduler instances.
    debug_mode = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    in_reloader_child = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    scheduler = None
    if (not debug_mode) or in_reloader_child:
        scheduler = create_scheduler()
        scheduler.start()
        logging.info(f"[{datetime.now()}] APScheduler started.")
        if os.getenv("ENABLE_STARTUP_BACKFILL", "1").lower() in ("1", "true", "yes"):
            Thread(target=run_initial_backfill, daemon=True).start()
            logging.info(f"[{datetime.now()}] Initial source backfill thread started.")

    port = int(os.getenv("PORT", "5000"))
    try:
        app.run(host="0.0.0.0", port=port, debug=debug_mode, use_reloader=debug_mode)
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)
            logging.info(f"[{datetime.now()}] Scheduler stopped.")
