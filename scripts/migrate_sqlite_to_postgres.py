"""
One-time migration from the local/Railway SQLite file into Postgres.

Usage:
  DATABASE_URL=postgresql://... python scripts/migrate_sqlite_to_postgres.py --source price_tracker.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from config import DATABASE_URL
from database import init_db


TABLE_ORDER = [
    "sources",
    "categories",
    "products",
    "product_sources",
    "product_source_candidates",
    "price_history",
    "discovery_searches",
    "discovery_results",
    "discovery_source_runs",
    "worker_runtime",
    "worker_job_runs",
    "manual_check_requests",
    "source_access_state",
]


def _set_sequence(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s AND column_name = 'id')",
            (table_name,),
        )
        has_id = cur.fetchone()[0]
        if not has_id:
            return
        cur.execute(f"SELECT COALESCE(MAX(id), 1) FROM {table_name}")
        max_id = int(cur.fetchone()[0] or 1)
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table_name,))
        seq = cur.fetchone()[0]
        if seq:
            cur.execute("SELECT setval(%s, %s, true)", (seq, max_id))


def _copy_table(source_conn, target_conn, table_name: str) -> None:
    source_cur = source_conn.cursor()
    source_cur.execute(f"SELECT * FROM {table_name}")
    rows = source_cur.fetchall()
    if not rows:
        return
    columns = [desc[0] for desc in source_cur.description]
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    with target_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})",
            rows,
        )
    _set_sequence(target_conn, table_name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Path to the source SQLite database file.")
    args = parser.parse_args()

    import psycopg

    if not DATABASE_URL.startswith(("postgres://", "postgresql://")):
        raise SystemExit("DATABASE_URL must point to Postgres before running this migration.")

    source_path = Path(args.source).expanduser().resolve()
    if not source_path.exists():
        raise SystemExit(f"Source SQLite database not found: {source_path}")

    init_db()

    source_conn = sqlite3.connect(str(source_path))
    target_conn = psycopg.connect(DATABASE_URL)
    try:
        with target_conn.cursor() as cur:
            for table_name in reversed(TABLE_ORDER):
                cur.execute(f"DELETE FROM {table_name}")
        target_conn.commit()

        for table_name in TABLE_ORDER:
            _copy_table(source_conn, target_conn, table_name)
        target_conn.commit()
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
