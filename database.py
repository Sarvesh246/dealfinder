"""
database.py — SQLite setup and all database access functions.
Supports multi-source product discovery: products → product_sources → price_history.
"""

import sqlite3
import os
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from config import SQLITE_BUSY_TIMEOUT_MS, SQLITE_CONNECT_TIMEOUT_SECONDS
from product_verifier import parse_product_spec, product_spec_to_fields

_DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "price_tracker.db")
DB_PATH = os.environ.get("DB_PATH", _DEFAULT_DB).strip() or _DEFAULT_DB
GENERIC_DIRECT_SOURCE_DOMAIN = "generic.direct.link"

DEFAULT_CATEGORIES = [
    {"name": "Computers & Components", "slug": "computers", "icon": "🖥️", "parent": None,
     "keywords": "computer,PC,desktop", "children": [
        {"name": "CPUs & Processors", "slug": "cpus", "icon": "🧠", "keywords": "CPU,processor,Ryzen,Intel Core"},
        {"name": "GPUs & Graphics Cards", "slug": "gpus", "icon": "🎮", "keywords": "GPU,graphics card,RTX,Radeon"},
        {"name": "RAM & Memory", "slug": "ram", "icon": "💾", "keywords": "RAM,memory,DDR5,DDR4"},
        {"name": "SSDs & Storage", "slug": "ssds", "icon": "💿", "keywords": "SSD,NVMe,hard drive,storage"},
        {"name": "Motherboards", "slug": "motherboards", "icon": "🔧", "keywords": "motherboard,mainboard"},
        {"name": "PC Cases", "slug": "pc-cases", "icon": "🖥️", "keywords": "PC case,computer case,tower"},
        {"name": "Power Supplies", "slug": "psus", "icon": "⚡", "keywords": "power supply,PSU"},
    ]},
    {"name": "Peripherals", "slug": "peripherals", "icon": "⌨️", "parent": None,
     "keywords": "peripheral,accessory", "children": [
        {"name": "Mechanical Keyboards", "slug": "keyboards", "icon": "⌨️", "keywords": "mechanical keyboard,gaming keyboard,TKL keyboard"},
        {"name": "Gaming Mice", "slug": "mice", "icon": "🖱️", "keywords": "gaming mouse,wireless mouse"},
        {"name": "Monitors", "slug": "monitors", "icon": "🖥️", "keywords": "monitor,display,4K monitor,ultrawide"},
        {"name": "Headsets & Headphones", "slug": "headsets", "icon": "🎧", "keywords": "headset,headphones,gaming headset"},
        {"name": "Webcams", "slug": "webcams", "icon": "📷", "keywords": "webcam,streaming camera"},
        {"name": "Controllers & Gamepads", "slug": "controllers", "icon": "🎮", "keywords": "controller,gamepad,Xbox controller,PS5 controller"},
    ]},
    {"name": "Mobile & Tablets", "slug": "mobile", "icon": "📱", "parent": None,
     "keywords": "mobile,tablet,phone", "children": [
        {"name": "Smartphones", "slug": "smartphones", "icon": "📱", "keywords": "smartphone,iPhone,Samsung Galaxy,Pixel"},
        {"name": "Tablets", "slug": "tablets", "icon": "📱", "keywords": "tablet,iPad,Android tablet"},
        {"name": "Phone Cases & Accessories", "slug": "phone-accessories", "icon": "📦", "keywords": "phone case,screen protector,phone accessory"},
        {"name": "Chargers & Cables", "slug": "chargers", "icon": "🔌", "keywords": "charger,USB-C cable,wireless charger"},
    ]},
    {"name": "Gaming", "slug": "gaming", "icon": "🎮", "parent": None,
     "keywords": "gaming", "children": [
        {"name": "Gaming Laptops", "slug": "gaming-laptops", "icon": "💻", "keywords": "gaming laptop"},
        {"name": "Consoles", "slug": "consoles", "icon": "🕹️", "keywords": "console,PS5,Xbox,Nintendo Switch"},
        {"name": "Games", "slug": "games", "icon": "🎲", "keywords": "video game,PC game,PS5 game"},
        {"name": "Gaming Chairs", "slug": "gaming-chairs", "icon": "🪑", "keywords": "gaming chair,ergonomic chair"},
    ]},
    {"name": "Audio", "slug": "audio", "icon": "🎧", "parent": None,
     "keywords": "audio,sound", "children": [
        {"name": "Earbuds & IEMs", "slug": "earbuds", "icon": "🎵", "keywords": "earbuds,IEM,wireless earbuds,AirPods"},
        {"name": "Bluetooth Speakers", "slug": "speakers", "icon": "🔊", "keywords": "bluetooth speaker,portable speaker"},
        {"name": "Studio & Recording", "slug": "studio", "icon": "🎙️", "keywords": "studio microphone,audio interface,recording"},
    ]},
    {"name": "Cameras", "slug": "cameras", "icon": "📷", "parent": None,
     "keywords": "camera,photography", "children": [
        {"name": "DSLR & Mirrorless", "slug": "dslr", "icon": "📸", "keywords": "DSLR,mirrorless camera,Sony Alpha,Canon EOS"},
        {"name": "Action Cameras", "slug": "action-cameras", "icon": "🎬", "keywords": "action camera,GoPro"},
        {"name": "Lenses & Accessories", "slug": "lenses", "icon": "🔭", "keywords": "camera lens,tripod,camera accessory"},
    ]},
    {"name": "Home & Office", "slug": "home-office", "icon": "🏠", "parent": None,
     "keywords": "home,office", "children": [
        {"name": "Desk & Office Furniture", "slug": "desks", "icon": "🪑", "keywords": "standing desk,office desk,office chair"},
        {"name": "Smart Home Devices", "slug": "smart-home", "icon": "💡", "keywords": "smart home,Alexa,Google Home"},
        {"name": "Printers & Scanners", "slug": "printers", "icon": "🖨️", "keywords": "printer,scanner,laser printer"},
        {"name": "Networking & Routers", "slug": "networking", "icon": "📡", "keywords": "router,mesh WiFi,networking"},
    ]},
    {"name": "Smart Home", "slug": "smart-home-cat", "icon": "💡", "parent": None,
     "keywords": "smart home,IoT", "children": [
        {"name": "Smart Speakers", "slug": "smart-speakers", "icon": "🔊", "keywords": "smart speaker,Echo,HomePod"},
        {"name": "Smart Lighting", "slug": "smart-lighting", "icon": "💡", "keywords": "smart bulb,Philips Hue,smart lighting"},
        {"name": "Security Cameras", "slug": "security-cameras", "icon": "📹", "keywords": "security camera,Ring,Wyze cam"},
    ]},
]

DEFAULT_SOURCES = [
    {
        "name": "Amazon",
        "domain": "amazon.com",
        "search_url_template": "https://www.amazon.com/s?k={query}",
        "enabled": 1,
        "logo_color": "#FF9900",
        "certified": 1,
        "rollout_wave": "core",
        "feature_flag": "",
        "certification_notes": "Core certified",
    },
    {
        "name": "Best Buy",
        "domain": "bestbuy.com",
        "search_url_template": "https://www.bestbuy.com/site/searchpage.jsp?st={query}",
        "enabled": 1,
        "logo_color": "#0046BE",
        "certified": 1,
        "rollout_wave": "core",
        "feature_flag": "",
        "certification_notes": "Core certified",
    },
    {
        "name": "Newegg",
        "domain": "newegg.com",
        "search_url_template": "https://www.newegg.com/p/pl?d={query}",
        "enabled": 1,
        "logo_color": "#E56708",
        "certified": 1,
        "rollout_wave": "core",
        "feature_flag": "",
        "certification_notes": "Core certified",
    },
    {
        "name": "Walmart",
        "domain": "walmart.com",
        "search_url_template": "https://www.walmart.com/search?q={query}",
        "enabled": 1,
        "logo_color": "#0071DC",
        "certified": 1,
        "rollout_wave": "core",
        "feature_flag": "",
        "certification_notes": "Core certified",
    },
    {
        "name": "eBay",
        "domain": "ebay.com",
        "search_url_template": "https://www.ebay.com/sch/i.html?_nkw={query}&LH_BIN=1",
        "enabled": 0,
        "logo_color": "#E53238",
        "certified": 0,
        "rollout_wave": "later",
        "feature_flag": "ENABLE_SOURCE_EBAY",
        "certification_notes": "Marketplace source intentionally excluded from strict rollout",
    },
    {
        "name": "B&H Photo",
        "domain": "bhphotovideo.com",
        "search_url_template": "https://www.bhphotovideo.com/c/search?Ntt={query}",
        "enabled": 0,
        "logo_color": "#000000",
        "certified": 0,
        "rollout_wave": "wave0",
        "feature_flag": "ENABLE_SOURCE_BHPHOTOVIDEO",
        "certification_notes": "Pending certification in this environment",
    },
    {
        "name": "Target",
        "domain": "target.com",
        "search_url_template": "https://www.target.com/s?searchTerm={query}",
        "enabled": 1,
        "logo_color": "#CC0000",
        "certified": 1,
        "rollout_wave": "wave0",
        "feature_flag": "ENABLE_SOURCE_TARGET",
        "certification_notes": "Certified first-wave broad retail source",
    },
    {
        "name": "Costco",
        "domain": "costco.com",
        "search_url_template":
            "https://www.costco.com/CatalogSearch?dept=All&keyword={query}",
        "enabled": 0,
        "logo_color": "#E31837",
        "certified": 0,
        "rollout_wave": "wave0",
        "feature_flag": "ENABLE_SOURCE_COSTCO",
        "certification_notes": "Pending certification in this environment",
    },
    {
        "name": "The Home Depot",
        "domain": "homedepot.com",
        "search_url_template": "https://www.homedepot.com/s/{query}",
        "enabled": 0,
        "logo_color": "#F96302",
        "certified": 0,
        "rollout_wave": "wave0",
        "feature_flag": "ENABLE_SOURCE_HOMEDEPOT",
        "certification_notes": "Pending certification in this environment",
    },
    {
        "name": "Lowe's",
        "domain": "lowes.com",
        "search_url_template":
            "https://www.lowes.com/search?searchTerm={query}",
        "enabled": 0,
        "logo_color": "#004990",
        "certified": 0,
        "rollout_wave": "wave0",
        "feature_flag": "ENABLE_SOURCE_LOWES",
        "certification_notes": "Pending certification in this environment",
    },
    {
        "name": "Office Depot",
        "domain": "officedepot.com",
        "search_url_template": "https://www.officedepot.com/a/search/?q={query}",
        "enabled": 1,
        "logo_color": "#CC0000",
        "certified": 1,
        "rollout_wave": "wave1",
        "feature_flag": "ENABLE_SOURCE_OFFICEDEPOT",
        "certification_notes": "Certified for office, furniture, printer, monitor, and networking queries",
    },
    {
        "name": "Micro Center",
        "domain": "microcenter.com",
        "search_url_template": "https://www.microcenter.com/search/search_results.aspx?Ntt={query}",
        "enabled": 0,
        "logo_color": "#E01A22",
        "certified": 0,
        "rollout_wave": "wave2",
        "feature_flag": "ENABLE_SOURCE_MICROCENTER",
        "certification_notes": "Protected-store phase; blocked in current environment",
    },
    {
        "name": "Macy's",
        "domain": "macys.com",
        "search_url_template": "https://www.macys.com/shop/featured/{query}",
        "enabled": 0,
        "logo_color": "#111111",
        "certified": 0,
        "rollout_wave": "wave2",
        "feature_flag": "ENABLE_SOURCE_MACYS",
        "certification_notes": "Protected-store phase; access denied in current environment",
    },
    {
        "name": "Sam's Club",
        "domain": "samsclub.com",
        "search_url_template": "https://www.samsclub.com/s/{query}",
        "enabled": 0,
        "logo_color": "#005DAA",
        "certified": 0,
        "rollout_wave": "wave2",
        "feature_flag": "ENABLE_SOURCE_SAMSCLUB",
        "certification_notes": "Protected-store phase; anti-bot gating observed",
    },
    {
        "name": "GameStop",
        "domain": "gamestop.com",
        "search_url_template": "https://www.gamestop.com/search/?q={query}",
        "enabled": 0,
        "logo_color": "#E2231A",
        "certified": 0,
        "rollout_wave": "wave2",
        "feature_flag": "ENABLE_SOURCE_GAMESTOP",
        "certification_notes": "Protected-store phase; anti-bot gating observed",
    },
    {
        "name": "Direct Link",
        "domain": GENERIC_DIRECT_SOURCE_DOMAIN,
        "search_url_template": "",
        "enabled": 0,
        "logo_color": "#7C5CFC",
        "certified": 1,
        "rollout_wave": "core",
        "feature_flag": "",
        "certification_notes": "Internal direct-link source",
    },
]


def _flag_is_enabled(value: str | None, default: bool) -> bool:
    if not value:
        return default
    raw = os.getenv(value)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _source_default_enabled(source: dict) -> int:
    certified = int(source.get("certified", 1))
    default_enabled = int(source.get("enabled", 0))
    if not certified:
        default_enabled = 0
    return int(_flag_is_enabled(source.get("feature_flag"), bool(default_enabled)))


def _source_runtime_allowed(row) -> bool:
    feature_flag = None
    try:
        feature_flag = row["feature_flag"]
    except Exception:
        feature_flag = None
    return _flag_is_enabled(feature_flag, True)


def _sync_default_sources(cursor):
    """Add any DEFAULT_SOURCES catalog entries not yet present (matched by domain)."""
    for s in DEFAULT_SOURCES:
        row = cursor.execute(
            "SELECT id, certified, rollout_wave, feature_flag, certification_notes FROM sources WHERE domain = ?",
            (s["domain"],),
        ).fetchone()
        enabled_default = _source_default_enabled(s)
        if row:
            cursor.execute(
                """
                UPDATE sources
                SET name = ?,
                    search_url_template = ?,
                    logo_color = ?,
                    certified = ?,
                    rollout_wave = ?,
                    feature_flag = ?,
                    certification_notes = ?
                WHERE domain = ?
                """,
                (
                    s["name"],
                    s["search_url_template"],
                    s["logo_color"],
                    int(s.get("certified", 1)),
                    s.get("rollout_wave"),
                    s.get("feature_flag"),
                    s.get("certification_notes"),
                    s["domain"],
                ),
            )
            continue
        cursor.execute(
            "INSERT INTO sources (name, domain, search_url_template, enabled, logo_color, certified, rollout_wave, feature_flag, certification_notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                s["name"],
                s["domain"],
                s["search_url_template"],
                enabled_default,
                s["logo_color"],
                int(s.get("certified", 1)),
                s.get("rollout_wave"),
                s.get("feature_flag"),
                s.get("certification_notes"),
            ),
        )
        logging.info(f"[{datetime.now()}] Registered new catalog source: {s['name']} ({s['domain']})")


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=float(SQLITE_CONNECT_TIMEOUT_SECONDS))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(f"PRAGMA busy_timeout = {int(SQLITE_BUSY_TIMEOUT_MS)}")
    return conn


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _parse_iso_datetime(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Schema & migration
# ---------------------------------------------------------------------------

def _upgrade_discovery_searches_schema(cursor):
    """Add discovery filter columns from older DBs."""
    try:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_searches'"
        )
        if not cursor.fetchone():
            return
        cursor.execute("PRAGMA table_info(discovery_searches)")
        have = {row[1] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return
    for col, decl in (
        ("filter_condition", "TEXT DEFAULT 'new_only'"),
        ("filter_product_type", "TEXT DEFAULT 'primary_only'"),
        ("filter_brand", "TEXT DEFAULT 'exact'"),
    ):
        if col not in have:
            cursor.execute(
                f"ALTER TABLE discovery_searches ADD COLUMN {col} {decl}"
            )


def _ensure_columns(cursor, table_name: str, additions: list[tuple[str, str]]) -> None:
    try:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        )
        if not cursor.fetchone():
            return
        cursor.execute(f"PRAGMA table_info({table_name})")
        have = {row[1] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return
    for col, decl in additions:
        if col not in have:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {decl}")


def _ensure_products_target_nullable(cursor) -> None:
    try:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='products'")
        if not cursor.fetchone():
            return
        cursor.execute("PRAGMA table_info(products)")
        cols = cursor.fetchall()
        target_col = next((row for row in cols if row[1] == "target_price"), None)
        if not target_col or not target_col[3]:
            return
    except sqlite3.OperationalError:
        return

    cursor.execute("PRAGMA foreign_keys = OFF")
    cursor.execute(
        """
        CREATE TABLE products__new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            target_price    REAL,
            current_price   REAL,
            alert_sent      INTEGER DEFAULT 0,
            created_at      TEXT,
            raw_query       TEXT,
            canonical_query TEXT,
            brand           TEXT,
            family          TEXT,
            model_token     TEXT,
            variant_tokens  TEXT,
            match_mode      TEXT DEFAULT 'strict',
            query_type      TEXT DEFAULT 'category',
            match_status    TEXT DEFAULT 'awaiting_match',
            alert_mode      TEXT DEFAULT 'target_threshold',
            origin_type     TEXT DEFAULT 'query_search'
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO products__new (
            id, name, target_price, current_price, alert_sent, created_at,
            raw_query, canonical_query, brand, family, model_token, variant_tokens,
            match_mode, query_type, match_status, alert_mode, origin_type
        )
        SELECT
            id, name, target_price, current_price, alert_sent, created_at,
            raw_query, canonical_query, brand, family, model_token, variant_tokens,
            match_mode, query_type, match_status,
            'target_threshold',
            'query_search'
        FROM products
        """
    )
    cursor.execute("DROP TABLE products")
    cursor.execute("ALTER TABLE products__new RENAME TO products")
    cursor.execute("PRAGMA foreign_keys = ON")


def _upgrade_products_schema(cursor) -> None:
    _ensure_products_target_nullable(cursor)
    _ensure_columns(
        cursor,
        "products",
        [
            ("raw_query", "TEXT"),
            ("canonical_query", "TEXT"),
            ("brand", "TEXT"),
            ("family", "TEXT"),
            ("model_token", "TEXT"),
            ("variant_tokens", "TEXT"),
            ("match_mode", "TEXT DEFAULT 'strict'"),
            ("query_type", "TEXT DEFAULT 'category'"),
            ("match_status", "TEXT DEFAULT 'awaiting_match'"),
            ("alert_mode", "TEXT DEFAULT 'target_threshold'"),
            ("origin_type", "TEXT DEFAULT 'query_search'"),
        ],
    )
    try:
        cursor.execute(
            """
            UPDATE products
            SET alert_mode = COALESCE(NULLIF(alert_mode, ''), 'target_threshold'),
                origin_type = COALESCE(NULLIF(origin_type, ''), 'query_search')
            """
        )
    except sqlite3.OperationalError:
        return


def _upgrade_product_sources_schema(cursor) -> None:
    _ensure_columns(
        cursor,
        "product_sources",
        [
            ("verification_state", "TEXT DEFAULT 'not_found'"),
            ("verification_reason", "TEXT"),
            ("health_state", "TEXT DEFAULT 'healthy'"),
            ("matched_product_name", "TEXT"),
            ("fingerprint_brand", "TEXT"),
            ("fingerprint_family", "TEXT"),
            ("fingerprint_model", "TEXT"),
            ("fingerprint_json", "TEXT"),
            ("match_label", "TEXT DEFAULT 'related'"),
            ("last_verified", "TEXT"),
            ("tracking_mode", "TEXT DEFAULT 'search_verified'"),
            ("source_label_override", "TEXT"),
            ("source_domain_override", "TEXT"),
            ("last_fetch_outcome", "TEXT"),
            ("last_fetch_method", "TEXT"),
            ("last_fetch_reason", "TEXT"),
            ("last_fetch_at", "TEXT"),
        ],
    )
    try:
        cursor.execute(
            """
            UPDATE product_sources
            SET current_price = NULL,
                status = 'quarantined',
                verification_state = 'quarantined',
                verification_reason = COALESCE(
                    NULLIF(verification_reason, ''),
                    'awaiting_initial_verification'
                ),
                health_state = 'quarantined',
                match_label = CASE
                    WHEN match_label IS NULL OR TRIM(match_label) = '' THEN 'related'
                    ELSE match_label
                END,
                tracking_mode = COALESCE(NULLIF(tracking_mode, ''), 'search_verified')
            WHERE discovered_url IS NOT NULL
              AND (last_verified IS NULL OR TRIM(last_verified) = '')
            """
        )
        cursor.execute(
            """
            UPDATE product_sources
            SET tracking_mode = COALESCE(NULLIF(tracking_mode, ''), 'search_verified')
            """
        )
    except sqlite3.OperationalError:
        return


def _upgrade_source_access_schema(cursor) -> None:
    _ensure_columns(
        cursor,
        "source_access_state",
        [
            ("status", "TEXT DEFAULT 'healthy'"),
            ("failure_reason", "TEXT"),
            ("blocked_until", "TEXT"),
            ("consecutive_failures", "INTEGER DEFAULT 0"),
            ("last_success_at", "TEXT"),
            ("last_attempt_at", "TEXT"),
            ("last_provider_success_at", "TEXT"),
            ("last_fetch_method", "TEXT"),
        ],
    )


def _upgrade_discovery_source_runs_schema(cursor) -> None:
    _ensure_columns(
        cursor,
        "discovery_source_runs",
        [
            ("search_id", "INTEGER NOT NULL"),
            ("source_id", "INTEGER"),
            ("outcome", "TEXT DEFAULT 'ok'"),
            ("fetch_strategy", "TEXT"),
            ("failure_reason", "TEXT"),
            ("raw_count", "INTEGER DEFAULT 0"),
            ("eligible_count", "INTEGER DEFAULT 0"),
            ("returned_count", "INTEGER DEFAULT 0"),
            ("duration_ms", "INTEGER DEFAULT 0"),
            ("created_at", "TEXT"),
        ],
    )


def _upgrade_discovery_results_schema(cursor):
    """Add columns introduced after first deploy (CREATE IF NOT EXISTS skips upgrades)."""
    try:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_results'")
        if not cursor.fetchone():
            return
        cursor.execute("PRAGMA table_info(discovery_results)")
        have = {row[1] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return
    additions = [
        ("discovered_at", "TEXT"),
        ("relevance_score", "REAL DEFAULT 0"),
        ("deal_score", "REAL DEFAULT 0"),
        ("group_id", "INTEGER"),
        ("also_available_at", "TEXT"),
        ("discount_confirmed", "INTEGER DEFAULT 1"),
        ("verification_label", "TEXT DEFAULT 'related'"),
    ]
    for col, decl in additions:
        if col not in have:
            cursor.execute(f"ALTER TABLE discovery_results ADD COLUMN {col} {decl}")


def _ensure_indexes(cursor) -> None:
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_sources_product_source "
        "ON product_sources(product_id, source_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_history_product_source_checked_at "
        "ON price_history(product_source_id, checked_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_discovery_results_search_id "
        "ON discovery_results(search_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_products_alert_sent "
        "ON products(alert_sent)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_products_current_price "
        "ON products(current_price)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_check_requests_status_requested_at "
        "ON manual_check_requests(status, requested_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_job_runs_job_name_created_at "
        "ON worker_job_runs(job_name, created_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_access_state_status_blocked_until "
        "ON source_access_state(status, blocked_until)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_discovery_source_runs_search_source "
        "ON discovery_source_runs(search_id, source_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_sources_last_fetch_outcome "
        "ON product_sources(last_fetch_outcome, last_fetch_at)"
    )


def init_db():
    """Create tables, seed sources, and migrate from v1 if needed."""
    try:
        conn = get_connection()
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                name                TEXT    NOT NULL,
                domain              TEXT    NOT NULL,
                search_url_template TEXT    NOT NULL,
                enabled             INTEGER DEFAULT 1,
                logo_color          TEXT    DEFAULT '#888888',
                certified           INTEGER DEFAULT 1,
                rollout_wave        TEXT,
                feature_flag        TEXT,
                certification_notes TEXT
            )
        """)

        _ensure_columns(c, "sources", [
            ("certified", "INTEGER DEFAULT 1"),
            ("rollout_wave", "TEXT"),
            ("feature_flag", "TEXT"),
            ("certification_notes", "TEXT"),
        ])

        needs_migration = False
        try:
            c.execute("PRAGMA table_info(products)")
            cols = [row[1] for row in c.fetchall()]
            if "url" in cols:
                needs_migration = True
        except sqlite3.OperationalError:
            pass

        if needs_migration:
            _migrate_from_v1(conn)
        else:
            c.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    name            TEXT    NOT NULL,
                    target_price    REAL,
                    current_price   REAL,
                    alert_sent      INTEGER DEFAULT 0,
                    created_at      TEXT,
                    raw_query       TEXT,
                    canonical_query TEXT,
                    brand           TEXT,
                    family          TEXT,
                    model_token     TEXT,
                    variant_tokens  TEXT,
                    match_mode      TEXT DEFAULT 'strict',
                    query_type      TEXT DEFAULT 'category',
                    match_status    TEXT DEFAULT 'awaiting_match',
                    alert_mode      TEXT DEFAULT 'target_threshold',
                    origin_type     TEXT DEFAULT 'query_search'
                )
            """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS product_sources (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id          INTEGER NOT NULL,
                source_id           INTEGER NOT NULL,
                enabled             INTEGER DEFAULT 1,
                discovered_url      TEXT,
                current_price       REAL,
                last_checked        TEXT,
                status              TEXT    DEFAULT 'not_found',
                verification_state  TEXT    DEFAULT 'not_found',
                verification_reason TEXT,
                health_state        TEXT    DEFAULT 'healthy',
                matched_product_name TEXT,
                fingerprint_brand   TEXT,
                fingerprint_family  TEXT,
                fingerprint_model   TEXT,
                fingerprint_json    TEXT,
                match_label         TEXT    DEFAULT 'related',
                last_verified       TEXT,
                tracking_mode       TEXT    DEFAULT 'search_verified',
                source_label_override TEXT,
                source_domain_override TEXT,
                last_fetch_outcome  TEXT,
                last_fetch_method   TEXT,
                last_fetch_reason   TEXT,
                last_fetch_at       TEXT,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id)  REFERENCES sources(id)  ON DELETE CASCADE
            )
        """)

        _upgrade_products_schema(c)
        _upgrade_product_sources_schema(c)

        has_new_history = False
        try:
            c.execute("PRAGMA table_info(price_history)")
            cols = [row[1] for row in c.fetchall()]
            if "product_source_id" in cols:
                has_new_history = True
        except sqlite3.OperationalError:
            pass

        if not has_new_history:
            c.execute("DROP TABLE IF EXISTS price_history")
            c.execute("""
                CREATE TABLE price_history (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_source_id INTEGER,
                    price             REAL,
                    checked_at        TEXT,
                    FOREIGN KEY (product_source_id) REFERENCES product_sources(id) ON DELETE CASCADE
                )
            """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT    NOT NULL,
                slug            TEXT    NOT NULL,
                parent_id       INTEGER,
                search_keywords TEXT,
                icon            TEXT    DEFAULT '📦',
                enabled         INTEGER DEFAULT 1,
                FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS discovery_searches (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                query               TEXT,
                category_id         INTEGER,
                max_price           REAL,
                result_count        INTEGER DEFAULT 0,
                searched_at         TEXT,
                filter_condition    TEXT DEFAULT 'new_only',
                filter_product_type TEXT DEFAULT 'primary_only',
                filter_brand        TEXT DEFAULT 'exact',
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL
            )
        """)

        _upgrade_discovery_searches_schema(c)

        c.execute("""
            CREATE TABLE IF NOT EXISTS discovery_results (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id        INTEGER NOT NULL,
                source_id        INTEGER,
                product_name     TEXT,
                current_price    REAL,
                original_price   REAL,
                discount_percent REAL    DEFAULT 0,
                discount_confirmed INTEGER DEFAULT 1,
                product_url      TEXT,
                discovered_at    TEXT,
                relevance_score  REAL    DEFAULT 0,
                deal_score       REAL    DEFAULT 0,
                group_id         INTEGER,
                also_available_at TEXT,
                verification_label TEXT DEFAULT 'related',
                FOREIGN KEY (search_id) REFERENCES discovery_searches(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL
            )
        """)

        _upgrade_discovery_results_schema(c)

        c.execute("""
            CREATE TABLE IF NOT EXISTS discovery_source_runs (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id      INTEGER NOT NULL,
                source_id      INTEGER,
                outcome        TEXT DEFAULT 'ok',
                fetch_strategy TEXT,
                failure_reason TEXT,
                raw_count      INTEGER DEFAULT 0,
                eligible_count INTEGER DEFAULT 0,
                returned_count INTEGER DEFAULT 0,
                duration_ms    INTEGER DEFAULT 0,
                created_at     TEXT,
                FOREIGN KEY (search_id) REFERENCES discovery_searches(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL
            )
        """)

        _upgrade_discovery_source_runs_schema(c)

        c.execute("""
            CREATE TABLE IF NOT EXISTS product_source_candidates (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id          INTEGER NOT NULL,
                source_id           INTEGER NOT NULL,
                product_source_id   INTEGER,
                candidate_url       TEXT    NOT NULL,
                candidate_name      TEXT,
                candidate_price     REAL,
                verification_state  TEXT    DEFAULT 'ambiguous',
                verification_reason TEXT,
                health_state        TEXT    DEFAULT 'healthy',
                fingerprint_brand   TEXT,
                fingerprint_family  TEXT,
                fingerprint_model   TEXT,
                match_label         TEXT    DEFAULT 'related',
                fingerprint_json    TEXT,
                created_at          TEXT,
                selected            INTEGER DEFAULT 0,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
                FOREIGN KEY (product_source_id) REFERENCES product_sources(id) ON DELETE CASCADE
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS worker_runtime (
                singleton_id         INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                worker_id            TEXT,
                heartbeat_at         TEXT,
                lease_until          TEXT,
                current_job_name     TEXT,
                current_job_trigger  TEXT,
                current_job_started_at TEXT,
                last_job_name        TEXT,
                last_job_status      TEXT,
                last_job_started_at  TEXT,
                last_job_finished_at TEXT,
                last_job_error       TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS worker_job_runs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name      TEXT NOT NULL,
                trigger_type  TEXT NOT NULL,
                requested_by  TEXT,
                status        TEXT NOT NULL,
                worker_id     TEXT,
                created_at    TEXT NOT NULL,
                started_at    TEXT,
                finished_at   TEXT,
                error         TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS manual_check_requests (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                requested_at  TEXT NOT NULL,
                requested_by  TEXT,
                status        TEXT NOT NULL DEFAULT 'queued',
                worker_id     TEXT,
                started_at    TEXT,
                finished_at   TEXT,
                error         TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS source_access_state (
                domain                   TEXT PRIMARY KEY,
                status                   TEXT DEFAULT 'healthy',
                failure_reason           TEXT,
                blocked_until            TEXT,
                consecutive_failures     INTEGER DEFAULT 0,
                last_success_at          TEXT,
                last_attempt_at          TEXT,
                last_provider_success_at TEXT,
                last_fetch_method        TEXT
            )
        """)

        _upgrade_source_access_schema(c)

        c.execute(
            "INSERT OR IGNORE INTO worker_runtime (singleton_id) VALUES (1)"
        )

        _sync_default_sources(c)

        cat_count = c.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        if cat_count == 0:
            _seed_categories(c)

        _sync_product_specs(c)
        _recompute_all_product_best_prices(c)
        _ensure_indexes(c)

        conn.commit()
        conn.close()
        logging.info(f"[{datetime.now()}] Database initialized at {DB_PATH}")
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Database init error: {exc}")


def _seed_categories(cursor):
    for parent in DEFAULT_CATEGORIES:
        cursor.execute(
            "INSERT INTO categories (name, slug, parent_id, search_keywords, icon) "
            "VALUES (?, ?, NULL, ?, ?)",
            (parent["name"], parent["slug"], parent["keywords"], parent["icon"]),
        )
        parent_id = cursor.lastrowid
        for child in parent.get("children", []):
            cursor.execute(
                "INSERT INTO categories (name, slug, parent_id, search_keywords, icon) "
                "VALUES (?, ?, ?, ?, ?)",
                (child["name"], child["slug"], parent_id,
                 child["keywords"], child["icon"]),
            )
    logging.info(f"[{datetime.now()}] Seeded default categories.")


def _migrate_from_v1(conn):
    """Migrate from v1 (products.url) to v2 (multi-source model)."""
    c = conn.cursor()
    logging.info(f"[{datetime.now()}] Migrating database from v1 to v2…")

    old_products = c.execute("SELECT * FROM products").fetchall()
    old_history = []
    try:
        old_history = c.execute("SELECT * FROM price_history").fetchall()
    except sqlite3.OperationalError:
        pass

    c.execute("DROP TABLE IF EXISTS price_history")
    c.execute("DROP TABLE IF EXISTS products")

    c.execute("""
        CREATE TABLE products (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            target_price    REAL    NOT NULL,
            current_price   REAL,
            alert_sent      INTEGER DEFAULT 0,
            created_at      TEXT,
            raw_query       TEXT,
            canonical_query TEXT,
            brand           TEXT,
            family          TEXT,
            model_token     TEXT,
            variant_tokens  TEXT,
            match_mode      TEXT DEFAULT 'strict',
            query_type      TEXT DEFAULT 'category',
            match_status    TEXT DEFAULT 'awaiting_match'
        )
    """)

    c.execute("""
        CREATE TABLE price_history (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            product_source_id INTEGER,
            price             REAL,
            checked_at        TEXT,
            FOREIGN KEY (product_source_id) REFERENCES product_sources(id) ON DELETE CASCADE
        )
    """)

    _sync_default_sources(c)

    for p in old_products:
        p = dict(p)
        name = p.get("name") or p.get("url", "Unknown Product")
        spec = parse_product_spec(name)
        spec_fields = product_spec_to_fields(spec)
        c.execute(
            "INSERT INTO products (id, name, target_price, current_price, alert_sent, created_at, "
            "raw_query, canonical_query, brand, family, model_token, variant_tokens, match_mode, query_type, match_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                p["id"], name, p["target_price"], None,
                p.get("alert_sent", 0), p.get("created_at"),
                spec_fields["raw_query"], spec_fields["canonical_query"],
                spec_fields["brand"], spec_fields["family"], spec_fields["model_token"],
                spec_fields["variant_tokens"], spec_fields["match_mode"],
                spec_fields["query_type"], "awaiting_match",
            ),
        )

        url = p.get("url", "")
        if url:
            source_id = _match_url_to_source(c, url)
            if source_id:
                status = "watching" if p.get("current_price") else "not_found"
                c.execute(
                    "INSERT INTO product_sources "
                    "(product_id, source_id, enabled, discovered_url, current_price, last_checked, status, "
                    "verification_state, verification_reason, health_state, match_label) "
                    "VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        p["id"],
                        source_id,
                        url,
                        None,
                        None,
                        "quarantined",
                        "quarantined",
                        "awaiting_initial_verification",
                        "quarantined",
                        "related",
                    ),
                )
                ps_id = c.lastrowid
                for h in old_history:
                    h = dict(h)
                    if h.get("product_id") == p["id"]:
                        c.execute(
                            "INSERT INTO price_history (product_source_id, price, checked_at) "
                            "VALUES (?, ?, ?)",
                            (ps_id, h["price"], h["checked_at"]),
                        )

    conn.commit()
    logging.info(f"[{datetime.now()}] Migration complete.")


def _match_url_to_source(cursor, url):
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        for s in cursor.execute("SELECT id, domain FROM sources").fetchall():
            if s["domain"] in domain:
                return s["id"]
    except Exception:
        pass
    return None


def _sync_product_specs(cursor) -> None:
    try:
        rows = cursor.execute(
            "SELECT id, name, raw_query, canonical_query, brand, family, model_token, "
            "variant_tokens, match_mode, query_type FROM products"
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for row in rows:
        row_d = dict(row)
        raw_query = (row_d.get("raw_query") or row_d.get("name") or "").strip()
        if not raw_query:
            continue
        spec = parse_product_spec(raw_query)
        fields = product_spec_to_fields(spec)
        updates = {
            "raw_query": fields["raw_query"],
            "canonical_query": row_d.get("canonical_query") or fields["canonical_query"],
            "brand": row_d.get("brand") or fields["brand"],
            "family": row_d.get("family") or fields["family"],
            "model_token": row_d.get("model_token") or fields["model_token"],
            "variant_tokens": row_d.get("variant_tokens") or fields["variant_tokens"],
            "match_mode": row_d.get("match_mode") or fields["match_mode"],
            "query_type": row_d.get("query_type") or fields["query_type"],
        }
        sets = ", ".join(f"{key} = ?" for key in updates)
        cursor.execute(
            f"UPDATE products SET {sets} WHERE id = ?",
            [*updates.values(), row_d["id"]],
        )


def _recompute_all_product_best_prices(cursor) -> None:
    try:
        products = cursor.execute(
            "SELECT id, target_price, alert_mode FROM products"
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for product in products:
        best_row = cursor.execute(
            """
            SELECT MIN(current_price) AS best
            FROM product_sources
            WHERE product_id = ?
              AND enabled = 1
              AND current_price IS NOT NULL
              AND verification_state = 'verified'
              AND health_state = 'healthy'
            """,
            (product["id"],),
        ).fetchone()
        best = best_row["best"] if best_row else None
        match_status = "awaiting_match"
        if best is not None:
            if product["alert_mode"] == "any_drop" or product["target_price"] is None:
                match_status = "watching"
            else:
                match_status = (
                    "deal_found"
                    if float(best) <= float(product["target_price"])
                    else "watching"
                )
        cursor.execute(
            "UPDATE products SET current_price = ?, match_status = ? WHERE id = ?",
            (best, match_status, product["id"]),
        )


# ---------------------------------------------------------------------------
# Products CRUD
# ---------------------------------------------------------------------------

def get_all_products():
    try:
        conn = get_connection()
        rows = conn.execute("SELECT * FROM products ORDER BY created_at DESC").fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_all_products error: {exc}")
        return []


def get_product_by_id(product_id):
    try:
        conn = get_connection()
        row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_by_id({product_id}) error: {exc}")
        return None


def add_product(
    name,
    target_price,
    *,
    alert_mode: str = "target_threshold",
    origin_type: str = "query_search",
):
    """Insert a product (name-only, no URL). Returns the new row ID or None."""
    try:
        spec = parse_product_spec(name)
        fields = product_spec_to_fields(spec)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO products (name, target_price, created_at, raw_query, canonical_query, "
            "brand, family, model_token, variant_tokens, match_mode, query_type, match_status, "
            "alert_mode, origin_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                name,
                target_price,
                datetime.now().isoformat(),
                fields["raw_query"],
                fields["canonical_query"],
                fields["brand"],
                fields["family"],
                fields["model_token"],
                fields["variant_tokens"],
                fields["match_mode"],
                fields["query_type"],
                "awaiting_match",
                alert_mode,
                origin_type,
            ),
        )
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        logging.info(f"[{datetime.now()}] Added product ID={pid}: {name}")
        return pid
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_product error: {exc}")
        return None


def update_product(product_id, **fields):
    allowed = {
        "name", "target_price", "current_price", "alert_sent",
        "raw_query", "canonical_query", "brand", "family",
        "model_token", "variant_tokens", "match_mode",
        "query_type", "match_status", "alert_mode", "origin_type",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    if not filtered:
        return
    try:
        sets = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [product_id]
        conn = get_connection()
        conn.execute(f"UPDATE products SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_product({product_id}) error: {exc}")


def delete_product(product_id):
    try:
        conn = get_connection()
        conn.execute("DELETE FROM product_source_candidates WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM price_history WHERE product_source_id IN "
                     "(SELECT id FROM product_sources WHERE product_id = ?)", (product_id,))
        conn.execute("DELETE FROM product_sources WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
        conn.commit()
        conn.close()
        logging.info(f"[{datetime.now()}] Deleted product ID={product_id}")
        return True
    except Exception as exc:
        logging.error(f"[{datetime.now()}] delete_product({product_id}) error: {exc}")
        return False


def update_product_price(product_id, best_price):
    try:
        conn = get_connection()
        conn.execute("UPDATE products SET current_price = ? WHERE id = ?",
                     (best_price, product_id))
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_product_price({product_id}) error: {exc}")


def set_alert_sent(product_id, value):
    try:
        conn = get_connection()
        conn.execute("UPDATE products SET alert_sent = ? WHERE id = ?",
                     (int(value), product_id))
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] set_alert_sent({product_id}, {value}) error: {exc}")


# ---------------------------------------------------------------------------
# Sources CRUD
# ---------------------------------------------------------------------------

def get_all_sources():
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM sources WHERE domain <> ? ORDER BY certified DESC, id",
            (GENERIC_DIRECT_SOURCE_DOMAIN,),
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_all_sources error: {exc}")
        return []


def get_available_sources():
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM sources WHERE certified = 1 AND domain <> ? ORDER BY id",
            (GENERIC_DIRECT_SOURCE_DOMAIN,),
        ).fetchall()
        conn.close()
        return [row for row in rows if _source_runtime_allowed(row)]
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_available_sources error: {exc}")
        return []


def get_source_by_id(source_id):
    try:
        conn = get_connection()
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_source_by_id({source_id}) error: {exc}")
        return None


def get_enabled_sources():
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM sources WHERE enabled = 1 AND certified = 1 AND domain <> ? ORDER BY id",
            (GENERIC_DIRECT_SOURCE_DOMAIN,),
        ).fetchall()
        conn.close()
        return [row for row in rows if _source_runtime_allowed(row)]
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_enabled_sources error: {exc}")
        return []


def update_source_enabled(source_id, enabled):
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT certified, feature_flag FROM sources WHERE id = ?",
            (source_id,),
        ).fetchone()
        if row and int(enabled) and (
            not int(row["certified"]) or not _source_runtime_allowed(row)
        ):
            enabled = 0
        conn.execute("UPDATE sources SET enabled = ? WHERE id = ?",
                     (int(enabled), source_id))
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_source_enabled({source_id}) error: {exc}")


def get_source_by_domain(domain: str):
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM sources WHERE domain = ? LIMIT 1",
            (domain.lower().replace("www.", ""),),
        ).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_source_by_domain({domain}) error: {exc}")
        return None


def ensure_generic_direct_source():
    row = get_source_by_domain(GENERIC_DIRECT_SOURCE_DOMAIN)
    if row:
        return row
    try:
        conn = get_connection()
        conn.execute(
            "INSERT INTO sources (name, domain, search_url_template, enabled, logo_color) VALUES (?, ?, ?, ?, ?)",
            ("Direct Link", GENERIC_DIRECT_SOURCE_DOMAIN, "", 0, "#7C5CFC"),
        )
        conn.commit()
        conn.close()
        return get_source_by_domain(GENERIC_DIRECT_SOURCE_DOMAIN)
    except Exception as exc:
        logging.error(f"[{datetime.now()}] ensure_generic_direct_source error: {exc}")
        return None


def find_source_for_url(url: str):
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        if not domain:
            return None
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM sources WHERE domain <> ? ORDER BY LENGTH(domain) DESC, id ASC",
            (GENERIC_DIRECT_SOURCE_DOMAIN,),
        ).fetchall()
        conn.close()
        for row in rows:
            source_domain = (row["domain"] or "").lower().replace("www.", "")
            if not source_domain:
                continue
            if domain == source_domain or domain.endswith(f".{source_domain}") or source_domain in domain:
                return row
        return None
    except Exception as exc:
        logging.error(f"[{datetime.now()}] find_source_for_url({url}) error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Product-Sources CRUD
# ---------------------------------------------------------------------------

def add_product_source(product_id, source_id, enabled=1,
                       discovered_url=None, current_price=None,
                       status="not_found",
                       verification_state="not_found",
                       verification_reason=None,
                       health_state="healthy",
                       matched_product_name=None,
                       fingerprint_brand=None,
                       fingerprint_family=None,
                       fingerprint_model=None,
                       fingerprint_json=None,
                       match_label="related",
                       last_verified=None,
                       tracking_mode="search_verified",
                       source_label_override=None,
                       source_domain_override=None,
                       last_fetch_outcome=None,
                       last_fetch_method=None,
                       last_fetch_reason=None,
                       last_fetch_at=None):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO product_sources "
            "(product_id, source_id, enabled, discovered_url, current_price, last_checked, status, "
            "verification_state, verification_reason, health_state, matched_product_name, "
            "fingerprint_brand, fingerprint_family, fingerprint_model, fingerprint_json, "
            "match_label, last_verified, tracking_mode, source_label_override, source_domain_override, "
            "last_fetch_outcome, last_fetch_method, last_fetch_reason, last_fetch_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                product_id, source_id, int(enabled), discovered_url, current_price,
                datetime.now().isoformat(), status, verification_state,
                verification_reason, health_state, matched_product_name,
                fingerprint_brand, fingerprint_family, fingerprint_model,
                fingerprint_json, match_label, last_verified,
                tracking_mode, source_label_override, source_domain_override,
                last_fetch_outcome, last_fetch_method, last_fetch_reason, last_fetch_at,
            ),
        )
        conn.commit()
        ps_id = cur.lastrowid
        conn.close()
        return ps_id
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_product_source error: {exc}")
        return None


def get_product_sources(product_id):
    """Return product_sources joined with source details, sorted by price."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ps.*, COALESCE(ps.source_label_override, s.name) AS source_name,
                   COALESCE(ps.source_domain_override, s.domain) AS domain, s.logo_color,
                   s.search_url_template, p.name AS product_name, p.target_price,
                   p.raw_query, p.canonical_query, p.brand, p.family,
                   p.model_token, p.variant_tokens, p.match_mode, p.query_type,
                   p.match_status, p.alert_mode, p.origin_type
            FROM product_sources ps
            JOIN sources s ON ps.source_id = s.id
            JOIN products p ON ps.product_id = p.id
            WHERE ps.product_id = ?
            ORDER BY
                CASE WHEN ps.verification_state = 'verified' AND ps.health_state = 'healthy' AND ps.current_price IS NOT NULL THEN 0 ELSE 1 END,
                ps.current_price ASC
        """, (product_id,)).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_sources({product_id}) error: {exc}")
        return []


def get_all_active_product_sources():
    """Return every enabled product_source with product and source info."""
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ps.*, p.name AS product_name, p.target_price, p.alert_sent,
                   p.raw_query, p.canonical_query, p.brand, p.family,
                   p.model_token, p.variant_tokens, p.match_mode, p.query_type,
                   p.alert_mode, p.origin_type,
                   COALESCE(ps.source_label_override, s.name) AS source_name,
                   COALESCE(ps.source_domain_override, s.domain) AS domain,
                   s.search_url_template
            FROM product_sources ps
            JOIN products p ON ps.product_id = p.id
            JOIN sources s  ON ps.source_id  = s.id
            WHERE ps.enabled = 1
              AND ps.discovered_url IS NOT NULL
              AND ps.verification_state = 'verified'
              AND ps.health_state = 'healthy'
        """).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_all_active_product_sources error: {exc}")
        return []


def update_product_source(ps_id, **fields):
    """Update arbitrary columns on a product_sources row."""
    if not fields:
        return
    allowed = {
        "enabled", "discovered_url", "current_price", "last_checked", "status",
        "verification_state", "verification_reason", "health_state",
        "matched_product_name", "fingerprint_brand", "fingerprint_family",
        "fingerprint_model", "fingerprint_json", "match_label", "last_verified",
        "tracking_mode", "source_label_override", "source_domain_override",
        "last_fetch_outcome", "last_fetch_method", "last_fetch_reason", "last_fetch_at",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed}
    if not filtered:
        return
    try:
        sets = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [ps_id]
        conn = get_connection()
        conn.execute(f"UPDATE product_sources SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_product_source({ps_id}) error: {exc}")


def get_product_source_by_id(ps_id):
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT ps.*, p.name AS product_name, p.target_price, p.alert_sent,
                   p.raw_query, p.canonical_query, p.brand, p.family,
                   p.model_token, p.variant_tokens, p.match_mode, p.query_type,
                   p.alert_mode, p.origin_type,
                   COALESCE(ps.source_label_override, s.name) AS source_name,
                   COALESCE(ps.source_domain_override, s.domain) AS domain,
                   s.search_url_template, s.logo_color
            FROM product_sources ps
            JOIN products p ON ps.product_id = p.id
            JOIN sources s  ON ps.source_id  = s.id
            WHERE ps.id = ?
        """, (ps_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_source_by_id({ps_id}) error: {exc}")
        return None


def delete_product_sources_by_source(product_id, source_id):
    try:
        conn = get_connection()
        conn.execute(
            "DELETE FROM product_source_candidates WHERE product_id = ? AND source_id = ?",
            (product_id, source_id),
        )
        conn.execute(
            "DELETE FROM price_history WHERE product_source_id IN "
            "(SELECT id FROM product_sources WHERE product_id = ? AND source_id = ?)",
            (product_id, source_id),
        )
        conn.execute(
            "DELETE FROM product_sources WHERE product_id = ? AND source_id = ?",
            (product_id, source_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] delete_product_sources_by_source error: {exc}")


def add_product_source_candidate(
    product_id,
    source_id,
    candidate_url,
    *,
    product_source_id=None,
    candidate_name=None,
    candidate_price=None,
    verification_state="ambiguous",
    verification_reason=None,
    health_state="healthy",
    fingerprint_brand=None,
    fingerprint_family=None,
    fingerprint_model=None,
    match_label="related",
    fingerprint_json=None,
):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO product_source_candidates "
            "(product_id, source_id, product_source_id, candidate_url, candidate_name, candidate_price, "
            "verification_state, verification_reason, health_state, fingerprint_brand, fingerprint_family, "
            "fingerprint_model, match_label, fingerprint_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                product_id, source_id, product_source_id, candidate_url, candidate_name,
                candidate_price, verification_state, verification_reason, health_state,
                fingerprint_brand, fingerprint_family, fingerprint_model, match_label,
                fingerprint_json, datetime.now().isoformat(),
            ),
        )
        conn.commit()
        cid = cur.lastrowid
        conn.close()
        return cid
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_product_source_candidate error: {exc}")
        return None


def get_product_source_candidates(product_id, source_id=None):
    try:
        conn = get_connection()
        sql = """
            SELECT c.*, s.name AS source_name, s.logo_color, s.domain
            FROM product_source_candidates c
            JOIN sources s ON c.source_id = s.id
            WHERE c.product_id = ?
        """
        params = [product_id]
        if source_id is not None:
            sql += " AND c.source_id = ?"
            params.append(source_id)
        sql += " ORDER BY c.created_at DESC, c.candidate_price ASC"
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_source_candidates({product_id}) error: {exc}")
        return []


def get_product_source_candidate(candidate_id):
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT c.*, s.name AS source_name, s.domain, s.logo_color
            FROM product_source_candidates c
            JOIN sources s ON c.source_id = s.id
            WHERE c.id = ?
        """, (candidate_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_source_candidate({candidate_id}) error: {exc}")
        return None


def clear_product_source_candidates(product_id, source_id=None):
    try:
        conn = get_connection()
        if source_id is None:
            conn.execute(
                "DELETE FROM product_source_candidates WHERE product_id = ?",
                (product_id,),
            )
        else:
            conn.execute(
                "DELETE FROM product_source_candidates WHERE product_id = ? AND source_id = ?",
                (product_id, source_id),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] clear_product_source_candidates error: {exc}")


def mark_candidate_selected(candidate_id):
    try:
        conn = get_connection()
        conn.execute(
            "UPDATE product_source_candidates SET selected = 1 WHERE id = ?",
            (candidate_id,),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] mark_candidate_selected({candidate_id}) error: {exc}")


def get_all_product_sources_for_revalidation():
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ps.*, p.name AS product_name, p.target_price, p.alert_sent,
                   p.raw_query, p.canonical_query, p.brand, p.family,
                   p.model_token, p.variant_tokens, p.match_mode, p.query_type,
                   p.alert_mode, p.origin_type,
                   COALESCE(ps.source_label_override, s.name) AS source_name,
                   COALESCE(ps.source_domain_override, s.domain) AS domain,
                   s.search_url_template
            FROM product_sources ps
            JOIN products p ON ps.product_id = p.id
            JOIN sources s  ON ps.source_id = s.id
            WHERE ps.enabled = 1 AND ps.discovered_url IS NOT NULL
        """).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_all_product_sources_for_revalidation error: {exc}")
        return []


def get_product_sources_needing_backfill():
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ps.*, p.name AS product_name, p.target_price, p.alert_sent,
                   p.raw_query, p.canonical_query, p.brand, p.family,
                   p.model_token, p.variant_tokens, p.match_mode, p.query_type,
                   p.alert_mode, p.origin_type,
                   COALESCE(ps.source_label_override, s.name) AS source_name,
                   COALESCE(ps.source_domain_override, s.domain) AS domain,
                   s.search_url_template
            FROM product_sources ps
            JOIN products p ON ps.product_id = p.id
            JOIN sources s  ON ps.source_id = s.id
            WHERE ps.enabled = 1
              AND ps.discovered_url IS NOT NULL
              AND (ps.last_verified IS NULL OR TRIM(ps.last_verified) = '')
        """).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_product_sources_needing_backfill error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

def add_price_history(product_source_id, price):
    try:
        conn = get_connection()
        conn.execute(
            "INSERT INTO price_history (product_source_id, price, checked_at) VALUES (?, ?, ?)",
            (product_source_id, price, datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_price_history({product_source_id}) error: {exc}")


def get_price_history(product_id):
    """Aggregated history across all sources for a product (newest-first).

    Returns rows with: price, checked_at, source_name, logo_color.
    """
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT ph.price, ph.checked_at,
                   COALESCE(ps.source_label_override, s.name) AS source_name,
                   s.logo_color
            FROM price_history ph
            JOIN product_sources ps ON ph.product_source_id = ps.id
            JOIN sources s          ON ps.source_id         = s.id
            WHERE ps.product_id = ?
            ORDER BY ph.checked_at DESC
        """, (product_id,)).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_price_history({product_id}) error: {exc}")
        return []


def get_price_history_for_source(product_source_id):
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM price_history WHERE product_source_id = ? ORDER BY checked_at DESC",
            (product_source_id,),
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_price_history_for_source({product_source_id}) error: {exc}")
        return []


def get_last_checked_time():
    try:
        conn = get_connection()
        row = conn.execute("SELECT MAX(checked_at) AS ts FROM price_history").fetchone()
        conn.close()
        return row["ts"] if row else None
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_last_checked_time error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Computed helpers
# ---------------------------------------------------------------------------

def compute_best_price(product_id):
    """Recalculate the best (lowest) price across all active sources and persist it."""
    try:
        conn = get_connection()
        product = conn.execute(
            "SELECT target_price, alert_mode FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        row = conn.execute("""
            SELECT MIN(current_price) AS best
            FROM product_sources
            WHERE product_id = ?
              AND enabled = 1
              AND current_price IS NOT NULL
              AND verification_state = 'verified'
              AND health_state = 'healthy'
        """, (product_id,)).fetchone()
        best = row["best"] if row else None
        match_status = "awaiting_match"
        if best is not None and product is not None:
            if product["alert_mode"] == "any_drop" or product["target_price"] is None:
                match_status = "watching"
            else:
                match_status = "deal_found" if float(best) <= float(product["target_price"]) else "watching"
        conn.execute(
            "UPDATE products SET current_price = ?, match_status = ? WHERE id = ?",
            (best, match_status, product_id),
        )
        conn.commit()
        conn.close()
        return best
    except Exception as exc:
        logging.error(f"[{datetime.now()}] compute_best_price({product_id}) error: {exc}")
        return None


def get_best_source_url(product_id):
    """Return the discovered_url of the cheapest active source, or ''."""
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT discovered_url FROM product_sources
            WHERE product_id = ?
              AND enabled = 1
              AND current_price IS NOT NULL
              AND verification_state = 'verified'
              AND health_state = 'healthy'
            ORDER BY current_price ASC LIMIT 1
        """, (product_id,)).fetchone()
        conn.close()
        return row["discovered_url"] if row else ""
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_best_source_url({product_id}) error: {exc}")
        return ""


# ---------------------------------------------------------------------------
# Source access health and cooldowns
# ---------------------------------------------------------------------------

def get_source_access_state(domain: str):
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM source_access_state WHERE domain = ?",
            ((domain or "").strip().lower(),),
        ).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_source_access_state({domain}) error: {exc}")
        return None


def record_source_access_success(
    domain: str,
    *,
    fetch_method: str | None = None,
    via_provider: bool = False,
) -> None:
    domain = (domain or "").strip().lower()
    if not domain:
        return
    now = _utcnow_iso()
    try:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO source_access_state (
                domain, status, failure_reason, blocked_until, consecutive_failures,
                last_success_at, last_attempt_at, last_provider_success_at, last_fetch_method
            ) VALUES (?, 'healthy', NULL, NULL, 0, ?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
                status = 'healthy',
                failure_reason = NULL,
                blocked_until = NULL,
                consecutive_failures = 0,
                last_success_at = excluded.last_success_at,
                last_attempt_at = excluded.last_attempt_at,
                last_provider_success_at = CASE
                    WHEN excluded.last_provider_success_at IS NOT NULL THEN excluded.last_provider_success_at
                    ELSE source_access_state.last_provider_success_at
                END,
                last_fetch_method = excluded.last_fetch_method
            """,
            (
                domain,
                now,
                now,
                now if via_provider else None,
                fetch_method,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] record_source_access_success({domain}) error: {exc}")


def record_source_access_failure(
    domain: str,
    *,
    failure_reason: str | None,
    fetch_method: str | None = None,
    cooldown_seconds: int = 0,
) -> None:
    domain = (domain or "").strip().lower()
    if not domain:
        return
    now = _utcnow_iso()
    blocked_until = None
    status = "blocked" if failure_reason in {"bot_wall", "cooldown"} else "degraded"
    if cooldown_seconds > 0:
        blocked_until = (
            datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=max(1, int(cooldown_seconds)))
        ).replace(microsecond=0).isoformat()
        status = "cooldown"
    try:
        conn = get_connection()
        existing = conn.execute(
            "SELECT consecutive_failures, last_success_at FROM source_access_state WHERE domain = ?",
            (domain,),
        ).fetchone()
        failures = int(existing["consecutive_failures"]) + 1 if existing else 1
        conn.execute(
            """
            INSERT INTO source_access_state (
                domain, status, failure_reason, blocked_until, consecutive_failures,
                last_success_at, last_attempt_at, last_fetch_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
                status = excluded.status,
                failure_reason = excluded.failure_reason,
                blocked_until = excluded.blocked_until,
                consecutive_failures = excluded.consecutive_failures,
                last_attempt_at = excluded.last_attempt_at,
                last_fetch_method = excluded.last_fetch_method
            """,
            (
                domain,
                status,
                failure_reason,
                blocked_until,
                failures,
                existing["last_success_at"] if existing else None,
                now,
                fetch_method,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] record_source_access_failure({domain}) error: {exc}")


def get_source_access_summary(limit: int = 8):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT domain, status, failure_reason, blocked_until, consecutive_failures,
                   last_success_at, last_attempt_at, last_provider_success_at, last_fetch_method
            FROM source_access_state
            ORDER BY
                CASE
                    WHEN blocked_until IS NOT NULL AND blocked_until > ? THEN 0
                    WHEN status = 'blocked' THEN 1
                    WHEN status = 'degraded' THEN 2
                    ELSE 3
                END,
                last_attempt_at DESC
            LIMIT ?
            """,
            (now.isoformat(), max(1, int(limit))),
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_source_access_summary error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Worker runtime, leases, and manual-check queue
# ---------------------------------------------------------------------------

def acquire_worker_lease(worker_id: str, lease_seconds: int = 90) -> bool:
    now = _utcnow_iso()
    lease_until = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=max(15, lease_seconds))).replace(
        microsecond=0
    ).isoformat()
    try:
        conn = get_connection()
        conn.execute("INSERT OR IGNORE INTO worker_runtime (singleton_id) VALUES (1)")
        cur = conn.execute(
            """
            UPDATE worker_runtime
            SET worker_id = ?,
                heartbeat_at = ?,
                lease_until = ?,
                current_job_name = CASE
                    WHEN worker_id IS NULL OR worker_id = ? THEN current_job_name
                    ELSE NULL
                END,
                current_job_trigger = CASE
                    WHEN worker_id IS NULL OR worker_id = ? THEN current_job_trigger
                    ELSE NULL
                END,
                current_job_started_at = CASE
                    WHEN worker_id IS NULL OR worker_id = ? THEN current_job_started_at
                    ELSE NULL
                END
            WHERE singleton_id = 1
              AND (
                    worker_id IS NULL
                    OR worker_id = ?
                    OR lease_until IS NULL
                    OR lease_until < ?
                  )
            """,
            (worker_id, now, lease_until, worker_id, worker_id, worker_id, worker_id, now),
        )
        conn.commit()
        conn.close()
        return cur.rowcount > 0
    except Exception as exc:
        logging.error(f"[{datetime.now()}] acquire_worker_lease error: {exc}")
        return False


def heartbeat_worker_lease(worker_id: str, lease_seconds: int = 90) -> bool:
    now = _utcnow_iso()
    lease_until = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=max(15, lease_seconds))).replace(
        microsecond=0
    ).isoformat()
    try:
        conn = get_connection()
        cur = conn.execute(
            """
            UPDATE worker_runtime
            SET heartbeat_at = ?, lease_until = ?
            WHERE singleton_id = 1 AND worker_id = ?
            """,
            (now, lease_until, worker_id),
        )
        conn.commit()
        conn.close()
        return cur.rowcount > 0
    except Exception as exc:
        logging.error(f"[{datetime.now()}] heartbeat_worker_lease error: {exc}")
        return False


def release_worker_lease(worker_id: str) -> None:
    try:
        conn = get_connection()
        conn.execute(
            """
            UPDATE worker_runtime
            SET worker_id = NULL,
                heartbeat_at = NULL,
                lease_until = NULL,
                current_job_name = NULL,
                current_job_trigger = NULL,
                current_job_started_at = NULL
            WHERE singleton_id = 1 AND worker_id = ?
            """,
            (worker_id,),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] release_worker_lease error: {exc}")


def begin_worker_job(
    worker_id: str,
    job_name: str,
    trigger_type: str,
    requested_by: str | None = None,
) -> int | None:
    now = _utcnow_iso()
    try:
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT OR IGNORE INTO worker_runtime (singleton_id) VALUES (1)")
        runtime = conn.execute(
            "SELECT worker_id, current_job_name FROM worker_runtime WHERE singleton_id = 1"
        ).fetchone()
        if not runtime or runtime["worker_id"] != worker_id:
            conn.rollback()
            conn.close()
            return None
        if runtime["current_job_name"]:
            conn.rollback()
            conn.close()
            return None
        conn.execute(
            """
            UPDATE worker_runtime
            SET current_job_name = ?,
                current_job_trigger = ?,
                current_job_started_at = ?
            WHERE singleton_id = 1 AND worker_id = ?
            """,
            (job_name, trigger_type, now, worker_id),
        )
        cur = conn.execute(
            """
            INSERT INTO worker_job_runs (
                job_name, trigger_type, requested_by, status, worker_id, created_at, started_at
            ) VALUES (?, ?, ?, 'running', ?, ?, ?)
            """,
            (job_name, trigger_type, requested_by, worker_id, now, now),
        )
        run_id = cur.lastrowid
        conn.commit()
        conn.close()
        return run_id
    except Exception as exc:
        logging.error(f"[{datetime.now()}] begin_worker_job error: {exc}")
        return None


def finish_worker_job(worker_id: str, run_id: int, status: str, error: str | None = None) -> None:
    finished_at = _utcnow_iso()
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT job_name, started_at FROM worker_job_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        job_name = row["job_name"] if row else None
        started_at = row["started_at"] if row else None
        conn.execute(
            """
            UPDATE worker_job_runs
            SET status = ?, finished_at = ?, error = ?
            WHERE id = ?
            """,
            (status, finished_at, error, run_id),
        )
        conn.execute(
            """
            UPDATE worker_runtime
            SET current_job_name = NULL,
                current_job_trigger = NULL,
                current_job_started_at = NULL,
                last_job_name = ?,
                last_job_status = ?,
                last_job_started_at = ?,
                last_job_finished_at = ?,
                last_job_error = ?
            WHERE singleton_id = 1 AND worker_id = ?
            """,
            (job_name, status, started_at, finished_at, error, worker_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] finish_worker_job error: {exc}")


def enqueue_manual_check_request(requested_by: str | None = None) -> tuple[int | None, bool]:
    now = _utcnow_iso()
    try:
        conn = get_connection()
        existing = conn.execute(
            """
            SELECT id
            FROM manual_check_requests
            WHERE status IN ('queued', 'running')
            ORDER BY requested_at ASC
            LIMIT 1
            """
        ).fetchone()
        if existing:
            conn.close()
            return existing["id"], False
        cur = conn.execute(
            """
            INSERT INTO manual_check_requests (requested_at, requested_by, status)
            VALUES (?, ?, 'queued')
            """,
            (now, requested_by),
        )
        request_id = cur.lastrowid
        conn.commit()
        conn.close()
        return request_id, True
    except Exception as exc:
        logging.error(f"[{datetime.now()}] enqueue_manual_check_request error: {exc}")
        return None, False


def claim_next_manual_check_request(worker_id: str) -> sqlite3.Row | None:
    now = _utcnow_iso()
    try:
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM manual_check_requests
            WHERE status = 'queued'
            ORDER BY requested_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            conn.rollback()
            conn.close()
            return None
        cur = conn.execute(
            """
            UPDATE manual_check_requests
            SET status = 'running',
                worker_id = ?,
                started_at = ?,
                error = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (worker_id, now, row["id"]),
        )
        if cur.rowcount == 0:
            conn.rollback()
            conn.close()
            return None
        claimed = conn.execute(
            "SELECT * FROM manual_check_requests WHERE id = ?",
            (row["id"],),
        ).fetchone()
        conn.commit()
        conn.close()
        return claimed
    except Exception as exc:
        logging.error(f"[{datetime.now()}] claim_next_manual_check_request error: {exc}")
        return None


def complete_manual_check_request(
    request_id: int,
    worker_id: str,
    status: str,
    error: str | None = None,
) -> None:
    finished_at = _utcnow_iso()
    try:
        conn = get_connection()
        conn.execute(
            """
            UPDATE manual_check_requests
            SET status = ?, worker_id = ?, finished_at = ?, error = ?
            WHERE id = ?
            """,
            (status, worker_id, finished_at, error, request_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] complete_manual_check_request error: {exc}")


def requeue_manual_check_request(request_id: int) -> None:
    try:
        conn = get_connection()
        conn.execute(
            """
            UPDATE manual_check_requests
            SET status = 'queued',
                worker_id = NULL,
                started_at = NULL,
                finished_at = NULL,
                error = NULL
            WHERE id = ?
            """,
            (request_id,),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] requeue_manual_check_request error: {exc}")


def get_runtime_diagnostics(stale_after_seconds: int = 120) -> dict[str, object]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    diagnostics: dict[str, object] = {
        "worker_online": False,
        "worker_id": None,
        "heartbeat_at": None,
        "lease_until": None,
        "current_job_name": None,
        "current_job_trigger": None,
        "current_job_started_at": None,
        "last_job_name": None,
        "last_job_status": None,
        "last_job_started_at": None,
        "last_job_finished_at": None,
        "last_job_error": None,
        "queue_depth": 0,
        "queued_manual_checks": 0,
        "running_manual_checks": 0,
        "last_periodic_check": None,
        "last_manual_check": None,
        "last_backfill": None,
        "recent_jobs": [],
        "source_access": [],
    }
    try:
        conn = get_connection()
        runtime = conn.execute(
            "SELECT * FROM worker_runtime WHERE singleton_id = 1"
        ).fetchone()
        if runtime:
            diagnostics.update(dict(runtime))
            heartbeat_at = runtime["heartbeat_at"]
            if heartbeat_at:
                try:
                    heartbeat_dt = datetime.fromisoformat(heartbeat_at)
                    diagnostics["worker_online"] = (
                        now - heartbeat_dt
                    ).total_seconds() <= max(30, stale_after_seconds)
                except ValueError:
                    diagnostics["worker_online"] = False
        queue_counts = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM manual_check_requests
            WHERE status IN ('queued', 'running')
            GROUP BY status
            """
        ).fetchall()
        for row in queue_counts:
            if row["status"] == "queued":
                diagnostics["queued_manual_checks"] = row["count"]
            elif row["status"] == "running":
                diagnostics["running_manual_checks"] = row["count"]
        diagnostics["queue_depth"] = (
            diagnostics["queued_manual_checks"] + diagnostics["running_manual_checks"]
        )
        for job_name, key in (
            ("periodic_price_check", "last_periodic_check"),
            ("manual_price_check", "last_manual_check"),
            ("startup_backfill", "last_backfill"),
        ):
            row = conn.execute(
                """
                SELECT id, status, started_at, finished_at, error, trigger_type
                FROM worker_job_runs
                WHERE job_name = ?
                ORDER BY COALESCE(finished_at, started_at, created_at) DESC
                LIMIT 1
                """,
                (job_name,),
            ).fetchone()
            diagnostics[key] = dict(row) if row else None
        recent = conn.execute(
            """
            SELECT id, job_name, trigger_type, status, requested_by, worker_id,
                   created_at, started_at, finished_at, error
            FROM worker_job_runs
            ORDER BY COALESCE(finished_at, started_at, created_at) DESC
            LIMIT 8
            """
        ).fetchall()
        diagnostics["recent_jobs"] = [dict(row) for row in recent]
        diagnostics["source_access"] = [dict(row) for row in get_source_access_summary(limit=8)]
        conn.close()
        return diagnostics
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_runtime_diagnostics error: {exc}")
        diagnostics["error"] = str(exc)
        return diagnostics


# ---------------------------------------------------------------------------
# Categories CRUD
# ---------------------------------------------------------------------------

def get_all_categories():
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT c.*, p.name AS parent_name
            FROM categories c
            LEFT JOIN categories p ON c.parent_id = p.id
            ORDER BY c.parent_id IS NOT NULL, c.parent_id, c.name
        """).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_all_categories error: {exc}")
        return []


def get_parent_categories():
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM categories WHERE parent_id IS NULL AND enabled = 1 ORDER BY name"
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_parent_categories error: {exc}")
        return []


def get_child_categories(parent_id):
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM categories WHERE parent_id = ? AND enabled = 1 ORDER BY name",
            (parent_id,),
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_child_categories({parent_id}) error: {exc}")
        return []


def get_category_by_id(cat_id):
    try:
        conn = get_connection()
        row = conn.execute("SELECT * FROM categories WHERE id = ?", (cat_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_category_by_id({cat_id}) error: {exc}")
        return None


def add_category(name, slug, parent_id, search_keywords, icon, enabled=1):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO categories (name, slug, parent_id, search_keywords, icon, enabled) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (name, slug, parent_id or None, search_keywords, icon or "📦", int(enabled)),
        )
        conn.commit()
        cid = cur.lastrowid
        conn.close()
        return cid
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_category error: {exc}")
        return None


def update_category(cat_id, **fields):
    allowed = {"name", "slug", "parent_id", "search_keywords", "icon", "enabled"}
    filtered = {k: v for k, v in fields.items() if k in allowed}
    if not filtered:
        return
    try:
        sets = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [cat_id]
        conn = get_connection()
        conn.execute(f"UPDATE categories SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_category({cat_id}) error: {exc}")


def get_categories_tree():
    """Return parent categories each with a 'children' list attached."""
    try:
        conn = get_connection()
        parents = conn.execute(
            "SELECT * FROM categories WHERE parent_id IS NULL AND enabled = 1 ORDER BY name"
        ).fetchall()
        result = []
        for p in parents:
            pd = dict(p)
            kids = conn.execute(
                "SELECT * FROM categories WHERE parent_id = ? AND enabled = 1 ORDER BY name",
                (p["id"],),
            ).fetchall()
            pd["children"] = [dict(k) for k in kids]
            result.append(pd)
        conn.close()
        return result
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_categories_tree error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Discovery searches + results
# ---------------------------------------------------------------------------

def create_discovery_search(
    query,
    category_id,
    max_price,
    filter_condition="new_only",
    filter_product_type="primary_only",
    filter_brand="exact",
):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO discovery_searches (query, category_id, max_price, searched_at, "
            "filter_condition, filter_product_type, filter_brand) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                query,
                category_id or None,
                max_price,
                datetime.now().isoformat(),
                filter_condition,
                filter_product_type,
                filter_brand,
            ),
        )
        conn.commit()
        sid = cur.lastrowid
        conn.close()
        return sid
    except Exception as exc:
        logging.error(f"[{datetime.now()}] create_discovery_search error: {exc}")
        return None


def add_discovery_result(search_id, source_id, product_name, current_price,
                         original_price, discount_percent, product_url,
                         relevance_score=0, deal_score=0, group_id=None,
                         also_available_at=None, discount_confirmed=1,
                         verification_label="related"):
    try:
        also_json = None
        if also_available_at:
            import json as _json
            also_json = _json.dumps(also_available_at)
        dc = 1 if discount_confirmed else 0
        conn = get_connection()
        conn.execute(
            "INSERT INTO discovery_results "
            "(search_id, source_id, product_name, current_price, original_price, "
            "discount_percent, product_url, discovered_at, relevance_score, "
            "deal_score, group_id, also_available_at, discount_confirmed, verification_label) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (search_id, source_id, product_name, current_price,
             original_price, round(discount_percent, 1), product_url,
             datetime.now().isoformat(), relevance_score, deal_score,
             group_id, also_json, dc, verification_label),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_discovery_result error: {exc}")


def update_discovery_search_count(search_id, count):
    try:
        conn = get_connection()
        conn.execute(
            "UPDATE discovery_searches SET result_count = ? WHERE id = ?",
            (count, search_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] update_discovery_search_count error: {exc}")


def add_discovery_source_run(
    search_id: int,
    source_id: int | None,
    *,
    outcome: str,
    fetch_strategy: str | None = None,
    failure_reason: str | None = None,
    raw_count: int = 0,
    eligible_count: int = 0,
    returned_count: int = 0,
    duration_ms: int = 0,
) -> None:
    try:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO discovery_source_runs (
                search_id, source_id, outcome, fetch_strategy, failure_reason,
                raw_count, eligible_count, returned_count, duration_ms, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                search_id,
                source_id,
                outcome,
                fetch_strategy,
                failure_reason,
                int(raw_count or 0),
                int(eligible_count or 0),
                int(returned_count or 0),
                int(duration_ms or 0),
                datetime.now().isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.error(f"[{datetime.now()}] add_discovery_source_run error: {exc}")


def get_discovery_source_runs(search_id: int):
    try:
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT dsr.*, s.name AS source_name, s.domain
            FROM discovery_source_runs dsr
            LEFT JOIN sources s ON dsr.source_id = s.id
            WHERE dsr.search_id = ?
            ORDER BY s.name COLLATE NOCASE ASC, dsr.id ASC
            """,
            (search_id,),
        ).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_discovery_source_runs({search_id}) error: {exc}")
        return []


def get_discovery_search(search_id):
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT ds.*, c.name AS category_name
            FROM discovery_searches ds
            LEFT JOIN categories c ON ds.category_id = c.id
            WHERE ds.id = ?
        """, (search_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_discovery_search({search_id}) error: {exc}")
        return None


def get_discovery_results(search_id):
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT dr.*, s.name AS source_name, s.logo_color AS source_logo_color
            FROM discovery_results dr
            LEFT JOIN sources s ON dr.source_id = s.id
            WHERE dr.search_id = ?
            ORDER BY
                CASE dr.verification_label
                    WHEN 'verified_exact' THEN 0
                    WHEN 'verified_named' THEN 1
                    WHEN 'category_primary' THEN 2
                    WHEN 'verified_related' THEN 3
                    ELSE 4
                END ASC,
                dr.deal_score DESC,
                dr.discount_percent DESC,
                dr.current_price ASC
        """, (search_id,)).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_discovery_results({search_id}) error: {exc}")
        return []


def get_discovery_result_by_id(result_id):
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT dr.*, s.name AS source_name, s.id AS sid, s.logo_color
            FROM discovery_results dr
            LEFT JOIN sources s ON dr.source_id = s.id
            WHERE dr.id = ?
        """, (result_id,)).fetchone()
        conn.close()
        return row
    except Exception as exc:
        logging.error(f"[{datetime.now()}] get_discovery_result_by_id({result_id}) error: {exc}")
        return None
