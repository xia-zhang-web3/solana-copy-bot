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
    post_cutover_rows: int | None = None
    post_cutover_exact_rows: int | None = None
    legacy_approximate_rows: int | None = None


@dataclass(frozen=True)
class CoverageQuery:
    row_source: str
    time_expr: str | None = None
    required_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class CutoverState:
    cutover_ts: str
    recorded_ts: str
    note: str


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


def count_where(conn: sqlite3.Connection, row_source: str, predicate: str) -> int:
    query = f"SELECT COUNT(*) FROM {row_source} WHERE {predicate}"
    return int(conn.execute(query).fetchone()[0])


def load_cutover_state(conn: sqlite3.Connection) -> CutoverState | None:
    if not table_exists(conn, "exact_money_cutover_state"):
        return None
    row = conn.execute(
        """
        SELECT cutover_ts, recorded_ts, COALESCE(note, '')
        FROM exact_money_cutover_state
        WHERE id = 1
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return CutoverState(
        cutover_ts=str(row[0]),
        recorded_ts=str(row[1]),
        note=str(row[2]),
    )


def has_required_tables(conn: sqlite3.Connection, query: CoverageQuery) -> bool:
    return all(table_exists(conn, table) for table in query.required_tables)


def apply_cutover_counts(
    conn: sqlite3.Connection,
    coverage: TableCoverage,
    query: CoverageQuery,
    exact_predicate: str,
    cutover: CutoverState | None,
) -> TableCoverage:
    if cutover is None or query.time_expr is None or not has_required_tables(conn, query):
        return coverage
    post_cutover_predicate = f"{query.time_expr} >= '{cutover.cutover_ts}'"
    post_cutover_rows = count_where(conn, query.row_source, post_cutover_predicate)
    post_cutover_exact_rows = count_where(
        conn,
        query.row_source,
        f"({post_cutover_predicate}) AND ({exact_predicate})",
    )
    legacy_approximate_rows = count_where(
        conn,
        query.row_source,
        f"({query.time_expr} < '{cutover.cutover_ts}') AND NOT ({exact_predicate})",
    )
    return TableCoverage(
        name=coverage.name,
        total_rows=coverage.total_rows,
        exact_rows=coverage.exact_rows,
        partial_rows=coverage.partial_rows,
        post_cutover_rows=post_cutover_rows,
        post_cutover_exact_rows=post_cutover_exact_rows,
        legacy_approximate_rows=legacy_approximate_rows,
    )


def coverage_single_nullable(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    column: str,
    query: CoverageQuery,
    cutover: CutoverState | None,
) -> TableCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return TableCoverage(name, 0, None, None)
    total = count_where(conn, query.row_source, "1 = 1")
    if not column_exists(conn, table, column):
        return TableCoverage(name, total, None, None)
    exact_predicate = f"{column} IS NOT NULL"
    exact = count_where(conn, query.row_source, exact_predicate)
    return apply_cutover_counts(
        conn,
        TableCoverage(name, total, exact, 0),
        query,
        exact_predicate,
        cutover,
    )


def coverage_all_or_nothing(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    columns: list[str],
    query: CoverageQuery,
    cutover: CutoverState | None,
) -> TableCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return TableCoverage(name, 0, None, None)
    total = count_where(conn, query.row_source, "1 = 1")
    if any(not column_exists(conn, table, column) for column in columns):
        return TableCoverage(name, total, None, None)
    all_present = " AND ".join(f"{column} IS NOT NULL" for column in columns)
    any_present = " OR ".join(f"{column} IS NOT NULL" for column in columns)
    exact = count_where(conn, query.row_source, all_present)
    partial = count_where(conn, query.row_source, f"({any_present}) AND NOT ({all_present})")
    return apply_cutover_counts(
        conn,
        TableCoverage(name, total, exact, partial),
        query,
        all_present,
        cutover,
    )


def coverage_custom_predicates(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    required_columns: list[str],
    exact_predicate: str,
    partial_predicate: str | None,
    query: CoverageQuery,
    cutover: CutoverState | None,
) -> TableCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return TableCoverage(name, 0, None, None)
    total = count_where(conn, query.row_source, "1 = 1")
    if any(not column_exists(conn, table, column) for column in required_columns):
        return TableCoverage(name, total, None, None)
    exact = count_where(conn, query.row_source, exact_predicate)
    partial = (
        count_where(conn, query.row_source, partial_predicate)
        if partial_predicate is not None
        else None
    )
    return apply_cutover_counts(
        conn,
        TableCoverage(name, total, exact, partial),
        query,
        exact_predicate,
        cutover,
    )


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
    print(
        f"{coverage.name}_post_cutover_rows: "
        + (
            str(coverage.post_cutover_rows)
            if coverage.post_cutover_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_post_cutover_exact_rows: "
        + (
            str(coverage.post_cutover_exact_rows)
            if coverage.post_cutover_exact_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_post_cutover_exact_ratio: "
        + (
            format_ratio(coverage.post_cutover_exact_rows, coverage.post_cutover_rows)
            if coverage.post_cutover_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_legacy_approximate_rows: "
        + (
            str(coverage.legacy_approximate_rows)
            if coverage.legacy_approximate_rows is not None
            else "n/a"
        )
    )


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        usage()
        return 1

    db_path = resolve_db_path(argv[1])
    conn = sqlite3.connect(str(db_path))
    try:
        cutover = load_cutover_state(conn)
        observed_swaps = coverage_all_or_nothing(
            conn,
            "observed_swaps",
            "observed_swaps",
            ["qty_in_raw", "qty_in_decimals", "qty_out_raw", "qty_out_decimals"],
            CoverageQuery("observed_swaps", "ts"),
            cutover,
        )
        copy_signals = coverage_custom_predicates(
            conn,
            "copy_signals",
            "copy_signals",
            ["notional_lamports", "notional_origin"],
            "notional_lamports IS NOT NULL AND notional_origin = 'leader_exact_lamports'",
            "((notional_origin = 'leader_exact_lamports' AND notional_lamports IS NULL) "
            "OR (notional_origin NOT IN ('leader_exact_lamports', 'leader_approximate')))",
            CoverageQuery("copy_signals", "ts"),
            cutover,
        )
        fills = coverage_all_or_nothing(
            conn,
            "fills",
            "fills",
            ["notional_lamports", "fee_lamports"],
            CoverageQuery(
                "fills JOIN orders ON orders.order_id = fills.order_id",
                "COALESCE(orders.confirm_ts, orders.submit_ts)",
                ("fills", "orders"),
            ),
            cutover,
        )
        fills_qty = coverage_all_or_nothing(
            conn,
            "fills_qty",
            "fills",
            ["qty_raw", "qty_decimals"],
            CoverageQuery(
                "fills JOIN orders ON orders.order_id = fills.order_id",
                "COALESCE(orders.confirm_ts, orders.submit_ts)",
                ("fills", "orders"),
            ),
            cutover,
        )
        positions = coverage_single_nullable(
            conn,
            "positions",
            "positions",
            "cost_lamports",
            CoverageQuery("positions", "opened_ts"),
            cutover,
        )
        positions_qty = coverage_all_or_nothing(
            conn,
            "positions_qty",
            "positions",
            ["qty_raw", "qty_decimals"],
            CoverageQuery("positions", "opened_ts"),
            cutover,
        )
        positions_pnl = coverage_single_nullable(
            conn,
            "positions_pnl",
            "positions",
            "pnl_lamports",
            CoverageQuery("positions", "COALESCE(closed_ts, opened_ts)"),
            cutover,
        )
        shadow_lots = coverage_single_nullable(
            conn,
            "shadow_lots",
            "shadow_lots",
            "cost_lamports",
            CoverageQuery("shadow_lots", "opened_ts"),
            cutover,
        )
        shadow_closed_trades = coverage_all_or_nothing(
            conn,
            "shadow_closed_trades",
            "shadow_closed_trades",
            ["entry_cost_lamports", "exit_value_lamports", "pnl_lamports"],
            CoverageQuery("shadow_closed_trades", "closed_ts"),
            cutover,
        )
    finally:
        conn.close()

    print("=== Exact Money Coverage Report ===")
    print(f"db: {db_path}")
    if cutover is None:
        print("exact_money_cutover_present: no")
        print("exact_money_cutover_ts: n/a")
        print("exact_money_cutover_recorded_ts: n/a")
        print("exact_money_cutover_note: n/a")
    else:
        print("exact_money_cutover_present: yes")
        print(f"exact_money_cutover_ts: {cutover.cutover_ts}")
        print(f"exact_money_cutover_recorded_ts: {cutover.recorded_ts}")
        print(f"exact_money_cutover_note: {cutover.note or 'n/a'}")
    print_coverage(observed_swaps)
    print_coverage(copy_signals)
    print_coverage(fills)
    print_coverage(fills_qty)
    print_coverage(positions)
    print_coverage(positions_qty)
    print_coverage(positions_pnl)
    print_coverage(shadow_lots)
    print_coverage(shadow_closed_trades)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
