#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableCoverage:
    name: str
    total_rows: int
    exact_rows: int | None
    partial_rows: int | None = None


def usage() -> None:
    print("usage: exact_money_coverage_report.py <sqlite.db>", file=sys.stderr)


def resolve_db_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_file():
        raise SystemExit(f"sqlite db not found: {raw}")
    return path


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def count_where(conn: sqlite3.Connection, table: str, predicate: str) -> int:
    query = f"SELECT COUNT(*) FROM {table} WHERE {predicate}"
    return int(conn.execute(query).fetchone()[0])


def coverage_single_nullable(
    conn: sqlite3.Connection,
    table: str,
    column: str,
) -> TableCoverage:
    if not table_exists(conn, table):
        return TableCoverage(table, 0, None, None)
    total = count_rows(conn, table)
    if not column_exists(conn, table, column):
        return TableCoverage(table, total, None, None)
    exact = count_where(conn, table, f"{column} IS NOT NULL")
    return TableCoverage(table, total, exact, 0)


def coverage_all_or_nothing(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
) -> TableCoverage:
    if not table_exists(conn, table):
        return TableCoverage(table, 0, None, None)
    total = count_rows(conn, table)
    if any(not column_exists(conn, table, column) for column in columns):
        return TableCoverage(table, total, None, None)
    all_present = " AND ".join(f"{column} IS NOT NULL" for column in columns)
    any_present = " OR ".join(f"{column} IS NOT NULL" for column in columns)
    exact = count_where(conn, table, all_present)
    partial = count_where(conn, table, f"({any_present}) AND NOT ({all_present})")
    return TableCoverage(table, total, exact, partial)


def format_ratio(exact_rows: int | None, total_rows: int) -> str:
    if exact_rows is None:
        return "n/a"
    if total_rows <= 0:
        return "n/a"
    return f"{exact_rows / total_rows:.4f}"


def mixed_state(coverage: TableCoverage) -> str:
    if coverage.exact_rows is None:
        return "n/a"
    if coverage.total_rows == 0:
        return "no"
    return "yes" if 0 < coverage.exact_rows < coverage.total_rows else "no"


def partial_state(coverage: TableCoverage) -> str:
    if coverage.partial_rows is None:
        return "n/a"
    return "yes" if coverage.partial_rows > 0 else "no"


def print_coverage(coverage: TableCoverage) -> None:
    print(f"{coverage.name}_total_rows: {coverage.total_rows}")
    print(
        f"{coverage.name}_exact_rows: "
        + (str(coverage.exact_rows) if coverage.exact_rows is not None else "n/a")
    )
    print(f"{coverage.name}_exact_ratio: {format_ratio(coverage.exact_rows, coverage.total_rows)}")
    print(f"{coverage.name}_mixed_state: {mixed_state(coverage)}")
    print(
        f"{coverage.name}_partial_exact_rows: "
        + (str(coverage.partial_rows) if coverage.partial_rows is not None else "n/a")
    )
    print(f"{coverage.name}_partial_exact_detected: {partial_state(coverage)}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        usage()
        return 1

    db_path = resolve_db_path(argv[1])
    conn = sqlite3.connect(str(db_path))
    try:
        observed_swaps = coverage_all_or_nothing(
            conn,
            "observed_swaps",
            ["qty_in_raw", "qty_in_decimals", "qty_out_raw", "qty_out_decimals"],
        )
        fills = coverage_all_or_nothing(conn, "fills", ["notional_lamports", "fee_lamports"])
        positions = coverage_single_nullable(conn, "positions", "cost_lamports")
        shadow_lots = coverage_single_nullable(conn, "shadow_lots", "cost_lamports")
        shadow_closed_trades = coverage_all_or_nothing(
            conn,
            "shadow_closed_trades",
            ["entry_cost_lamports", "exit_value_lamports", "pnl_lamports"],
        )
    finally:
        conn.close()

    print("=== Exact Money Coverage Report ===")
    print(f"db: {db_path}")
    print_coverage(observed_swaps)
    print_coverage(fills)
    print_coverage(positions)
    print_coverage(shadow_lots)
    print_coverage(shadow_closed_trades)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
