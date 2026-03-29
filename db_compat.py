"""
Compatibility layer that exposes a sqlite3-like subset over SQLite or Postgres.

The goal is to keep the existing `database.py` API and most SQL callsites intact
while allowing Vercel to use Postgres through `DATABASE_URL`.
"""

from __future__ import annotations

import os
import re
import sqlite3 as _sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence


DATABASE_URL = (
    os.getenv("DATABASE_URL", "").strip()
    or os.getenv("POSTGRES_URL", "").strip()
    or os.getenv("POSTGRES_PRISMA_URL", "").strip()
)
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))

try:
    if USE_POSTGRES:
        import psycopg
    else:
        psycopg = None
except Exception:  # pragma: no cover - only exercised when postgres is configured
    psycopg = None


OperationalError = _sqlite3.OperationalError
Row = _sqlite3.Row if not USE_POSTGRES else None


@dataclass
class _SpecialQuery:
    kind: str | None = None
    table_name: str | None = None


class CompatRow:
    def __init__(self, columns: Sequence[str], values: Sequence[object]):
        self._columns = list(columns)
        self._values = list(values)
        self._mapping = {name: values[idx] for idx, name in enumerate(columns)}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._mapping[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def keys(self):
        return list(self._columns)

    def items(self):
        return list(self._mapping.items())

    def values(self):
        return list(self._values)

    def get(self, key, default=None):
        return self._mapping.get(key, default)

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return f"CompatRow({self._mapping!r})"


if USE_POSTGRES:
    OperationalError = getattr(psycopg, "Error", Exception) if psycopg is not None else Exception
    Row = CompatRow


def _replace_qmark_placeholders(query: str) -> str:
    out: list[str] = []
    in_single = False
    in_double = False
    idx = 0
    while idx < len(query):
        char = query[idx]
        if char == "'" and not in_double:
            if in_single and idx + 1 < len(query) and query[idx + 1] == "'":
                out.append("''")
                idx += 2
                continue
            in_single = not in_single
            out.append(char)
            idx += 1
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            out.append(char)
            idx += 1
            continue
        if char == "?" and not in_single and not in_double:
            out.append("%s")
            idx += 1
            continue
        out.append(char)
        idx += 1
    return "".join(out)


def _replace_nocase_collation(query: str) -> str:
    return re.sub(
        r"(?P<expr>[A-Za-z_][A-Za-z0-9_.]*)\s+COLLATE\s+NOCASE",
        lambda match: f"LOWER({match.group('expr')})",
        query,
        flags=re.IGNORECASE,
    )


def _prepare_postgres_query(query: str) -> tuple[str | None, _SpecialQuery]:
    stripped = query.strip()
    upper = stripped.upper()

    if upper.startswith("PRAGMA "):
        pragma = upper[7:]
        if pragma.startswith(("FOREIGN_KEYS", "JOURNAL_MODE", "BUSY_TIMEOUT")):
            return None, _SpecialQuery(kind="noop")
        table_info = re.match(r"PRAGMA\s+TABLE_INFO\(([^)]+)\)", stripped, flags=re.IGNORECASE)
        if table_info:
            table_name = table_info.group(1).strip().strip("'\"")
            rewritten = """
                SELECT
                    (cols.ordinal_position - 1) AS cid,
                    cols.column_name AS name,
                    cols.data_type AS type,
                    CASE WHEN cols.is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                    cols.column_default AS dflt_value,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu
                              ON tc.constraint_name = kcu.constraint_name
                             AND tc.table_schema = kcu.table_schema
                             AND tc.table_name = kcu.table_name
                            WHERE tc.constraint_type = 'PRIMARY KEY'
                              AND tc.table_schema = current_schema()
                              AND tc.table_name = %s
                              AND kcu.column_name = cols.column_name
                        ) THEN 1 ELSE 0
                    END AS pk
                FROM information_schema.columns cols
                WHERE cols.table_schema = current_schema()
                  AND cols.table_name = %s
                ORDER BY cols.ordinal_position
            """
            return rewritten, _SpecialQuery(kind="table_info", table_name=table_name)
        return None, _SpecialQuery(kind="noop")

    if "sqlite_master" in query:
        rewritten = """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = %s
        """
        return rewritten, _SpecialQuery(kind="table_exists")

    rewritten = query
    rewritten = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "BIGSERIAL PRIMARY KEY",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = _replace_nocase_collation(rewritten)
    rewritten = _replace_qmark_placeholders(rewritten)
    return rewritten, _SpecialQuery()


def _insert_table_name(query: str) -> str | None:
    match = re.match(
        r"^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)",
        query,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1)


class _PgCursor:
    def __init__(self, connection, cursor):
        self._connection = connection
        self._cursor = cursor
        self._last_insert_table: str | None = None
        self._lastrowid: int | None = None

    def execute(self, query: str, params: Sequence[object] | None = None):
        rewritten, special = _prepare_postgres_query(query)
        self._last_insert_table = None
        self._lastrowid = None
        params = tuple(params or ())
        if special.kind == "noop":
            self._cursor = self._connection._raw.cursor()
            self._cursor.execute("SELECT 1 WHERE 1 = 0")
            return self
        if special.kind == "table_info":
            table_name = special.table_name or ""
            self._cursor.execute(rewritten, (table_name, table_name))
            return self
        if special.kind == "table_exists":
            self._cursor.execute(rewritten, params)
            return self

        self._last_insert_table = _insert_table_name(query)
        self._cursor.execute(rewritten, params)
        return self

    def executemany(self, query: str, seq_of_params: Iterable[Sequence[object]]):
        rewritten, special = _prepare_postgres_query(query)
        if special.kind == "noop":
            return self
        self._cursor.executemany(rewritten, list(seq_of_params))
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return CompatRow([desc.name for desc in self._cursor.description], row)

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows:
            return []
        columns = [desc.name for desc in self._cursor.description]
        return [CompatRow(columns, row) for row in rows]

    @property
    def lastrowid(self):
        if self._lastrowid is not None:
            return self._lastrowid
        if not self._last_insert_table:
            return None
        seq_cursor = self._connection._raw.cursor()
        seq_cursor.execute(
            "SELECT pg_get_serial_sequence(%s, 'id')",
            (self._last_insert_table,),
        )
        row = seq_cursor.fetchone()
        if not row or not row[0]:
            return None
        seq_cursor.execute("SELECT currval(%s)", (row[0],))
        curr = seq_cursor.fetchone()
        self._lastrowid = int(curr[0]) if curr and curr[0] is not None else None
        return self._lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def close(self):
        self._cursor.close()


class _PgConnection:
    def __init__(self, raw_connection):
        self._raw = raw_connection
        self.row_factory = CompatRow

    def execute(self, query: str, params: Sequence[object] | None = None):
        cursor = self.cursor()
        return cursor.execute(query, params)

    def executemany(self, query: str, seq_of_params: Iterable[Sequence[object]]):
        cursor = self.cursor()
        return cursor.executemany(query, seq_of_params)

    def cursor(self):
        return _PgCursor(self, self._raw.cursor())

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False


def connect(database: str, timeout: float = 30.0):
    if not USE_POSTGRES:
        return _sqlite3.connect(database, timeout=timeout)
    if psycopg is None:  # pragma: no cover - exercised only in postgres runtime without dependency
        raise RuntimeError("Postgres runtime requested but psycopg is not installed.")
    raw = psycopg.connect(
        DATABASE_URL,
        connect_timeout=max(1, int(timeout)),
    )
    return _PgConnection(raw)
