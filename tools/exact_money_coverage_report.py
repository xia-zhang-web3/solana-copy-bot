#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

LEGACY_PRE_CUTOVER_BUCKET = "legacy_pre_cutover"
EXACT_POST_CUTOVER_BUCKET = "exact_post_cutover"
U64_MAX = (1 << 64) - 1


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


@dataclass(frozen=True)
class BucketCoverage:
    name: str
    total_rows: int
    legacy_bucket_rows: int | None
    exact_bucket_rows: int | None
    unknown_bucket_rows: int | None
    exact_bucket_exact_rows: int | None
    legacy_bucket_exact_rows: int | None
    exact_bucket_invalid_exact_rows: int | None
    exact_bucket_missing_exact_rows: int | None


@dataclass(frozen=True)
class InvalidExactCoverage:
    name: str
    invalid_rows: int | None
    zero_raw_rows: int | None


@dataclass(frozen=True)
class ExactMoneyCoverageReport:
    cutover: CutoverState | None
    observed_swaps: TableCoverage
    observed_swaps_invalid_exact: InvalidExactCoverage
    copy_signals: TableCoverage
    fills: TableCoverage
    fills_qty: TableCoverage
    positions: TableCoverage
    positions_qty: TableCoverage
    positions_pnl: TableCoverage
    shadow_lots: TableCoverage
    shadow_lots_qty: TableCoverage
    shadow_closed_trades: TableCoverage
    shadow_closed_trades_qty: TableCoverage
    positions_bucket: BucketCoverage
    shadow_lots_bucket: BucketCoverage
    shadow_closed_trades_bucket: BucketCoverage
    fills_invalid_exact: InvalidExactCoverage
    positions_invalid_exact: InvalidExactCoverage
    shadow_lots_invalid_exact: InvalidExactCoverage
    shadow_closed_trades_invalid_exact: InvalidExactCoverage


@dataclass(frozen=True)
class LegacyEvidenceSurface:
    name: str
    table: str
    query: CoverageQuery
    exact_predicate: str
    export_select_sql: str
    order_by_sql: str


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


def register_sqlite_functions(conn: sqlite3.Connection) -> None:
    def is_valid_u64_text(raw: object) -> int:
        if not isinstance(raw, str) or raw == "":
            return 0
        if not raw.isascii() or not raw.isdigit():
            return 0
        try:
            return 1 if int(raw) <= U64_MAX else 0
        except ValueError:
            return 0

    def is_valid_u8_value(value: object) -> int:
        if value is None:
            return 0
        if not isinstance(value, int):
            return 0
        return 1 if 0 <= value <= 255 else 0

    def is_valid_exact_qty_sidecar(raw: object, decimals: object) -> int:
        return 1 if is_valid_u64_text(raw) and is_valid_u8_value(decimals) else 0

    def is_zero_raw_text(raw: object) -> int:
        if not isinstance(raw, str) or raw == "":
            return 0
        return 1 if all(ch == "0" for ch in raw) else 0

    conn.create_function("is_valid_exact_qty_sidecar", 2, is_valid_exact_qty_sidecar)
    conn.create_function("is_zero_raw_text", 1, is_zero_raw_text)


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
    invalid_predicate: str | None = None,
) -> TableCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return TableCoverage(name, 0, None, None)
    total = count_where(conn, query.row_source, "1 = 1")
    if any(not column_exists(conn, table, column) for column in columns):
        return TableCoverage(name, total, None, None)
    all_present = " AND ".join(f"{column} IS NOT NULL" for column in columns)
    any_present = " OR ".join(f"{column} IS NOT NULL" for column in columns)
    exact_predicate = all_present
    if invalid_predicate is not None:
        exact_predicate = f"({all_present}) AND NOT ({invalid_predicate})"
    exact = count_where(conn, query.row_source, exact_predicate)
    partial = count_where(conn, query.row_source, f"({any_present}) AND NOT ({all_present})")
    return apply_cutover_counts(
        conn,
        TableCoverage(name, total, exact, partial),
        query,
        exact_predicate,
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


def valid_exact_qty_sidecar_predicate(raw_column: str, decimals_column: str) -> str:
    return f"is_valid_exact_qty_sidecar({raw_column}, {decimals_column}) = 1"


def invalid_exact_qty_sidecar_predicate(raw_column: str, decimals_column: str) -> str:
    return (
        f"{raw_column} IS NOT NULL AND {decimals_column} IS NOT NULL "
        f"AND is_valid_exact_qty_sidecar({raw_column}, {decimals_column}) = 0"
    )


def zero_raw_text_predicate(column: str) -> str:
    return f"is_zero_raw_text({column}) = 1"


def fake_exact_qty_predicate(raw_column: str, decimals_column: str, money_column: str) -> str:
    return (
        "("
        f"({invalid_exact_qty_sidecar_predicate(raw_column, decimals_column)}) "
        "OR "
        f"(({zero_raw_text_predicate(raw_column)}) AND {decimals_column} IS NOT NULL)"
        ") "
        f"AND COALESCE({money_column}, 0) > 0"
    )


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


def bucket_coverage(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    bucket_column: str,
    exact_columns: list[str],
    query: CoverageQuery,
    invalid_predicate: str | None = None,
) -> BucketCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return BucketCoverage(name, 0, None, None, None, None, None, None, None)
    total = count_where(conn, query.row_source, "1 = 1")
    if (
        not column_exists(conn, table, bucket_column)
        or any(not column_exists(conn, table, column) for column in exact_columns)
    ):
        return BucketCoverage(name, total, None, None, None, None, None, None, None)
    all_present = " AND ".join(f"{column} IS NOT NULL" for column in exact_columns)
    any_present = " OR ".join(f"{column} IS NOT NULL" for column in exact_columns)
    exact_valid_predicate = all_present
    exact_invalid_predicate = None
    if invalid_predicate is not None:
        exact_valid_predicate = f"({all_present}) AND NOT ({invalid_predicate})"
        exact_invalid_predicate = f"({all_present}) AND ({invalid_predicate})"
    legacy_bucket_rows = count_where(
        conn,
        query.row_source,
        f"{bucket_column} = '{LEGACY_PRE_CUTOVER_BUCKET}'",
    )
    exact_bucket_rows = count_where(
        conn,
        query.row_source,
        f"{bucket_column} = '{EXACT_POST_CUTOVER_BUCKET}'",
    )
    exact_bucket_exact_rows = count_where(
        conn,
        query.row_source,
        f"{bucket_column} = '{EXACT_POST_CUTOVER_BUCKET}' AND ({exact_valid_predicate})",
    )
    unknown_bucket_rows = count_where(
        conn,
        query.row_source,
        (
            f"{bucket_column} NOT IN "
            f"('{LEGACY_PRE_CUTOVER_BUCKET}', '{EXACT_POST_CUTOVER_BUCKET}')"
        ),
    )
    legacy_bucket_exact_rows = count_where(
        conn,
        query.row_source,
        f"{bucket_column} = '{LEGACY_PRE_CUTOVER_BUCKET}' AND ({any_present})",
    )
    exact_bucket_invalid_exact_rows = (
        count_where(
            conn,
            query.row_source,
            f"{bucket_column} = '{EXACT_POST_CUTOVER_BUCKET}' AND ({exact_invalid_predicate})",
        )
        if exact_invalid_predicate is not None
        else 0
    )
    exact_bucket_missing_exact_rows = count_where(
        conn,
        query.row_source,
        f"{bucket_column} = '{EXACT_POST_CUTOVER_BUCKET}' AND NOT ({all_present})",
    )
    return BucketCoverage(
        name=name,
        total_rows=total,
        legacy_bucket_rows=legacy_bucket_rows,
        exact_bucket_rows=exact_bucket_rows,
        unknown_bucket_rows=unknown_bucket_rows,
        exact_bucket_exact_rows=exact_bucket_exact_rows,
        legacy_bucket_exact_rows=legacy_bucket_exact_rows,
        exact_bucket_invalid_exact_rows=exact_bucket_invalid_exact_rows,
        exact_bucket_missing_exact_rows=exact_bucket_missing_exact_rows,
    )


def print_bucket_coverage(coverage: BucketCoverage) -> None:
    print(f"{coverage.name}_bucket_total_rows: {coverage.total_rows}")
    print(
        f"{coverage.name}_bucket_legacy_rows: "
        + (
            str(coverage.legacy_bucket_rows)
            if coverage.legacy_bucket_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_exact_rows: "
        + (
            str(coverage.exact_bucket_rows)
            if coverage.exact_bucket_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_unknown_rows: "
        + (
            str(coverage.unknown_bucket_rows)
            if coverage.unknown_bucket_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_exact_with_exact_rows: "
        + (
            str(coverage.exact_bucket_exact_rows)
            if coverage.exact_bucket_exact_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_legacy_with_exact_rows: "
        + (
            str(coverage.legacy_bucket_exact_rows)
            if coverage.legacy_bucket_exact_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_exact_invalid_exact_rows: "
        + (
            str(coverage.exact_bucket_invalid_exact_rows)
            if coverage.exact_bucket_invalid_exact_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_bucket_exact_missing_exact_rows: "
        + (
            str(coverage.exact_bucket_missing_exact_rows)
            if coverage.exact_bucket_missing_exact_rows is not None
            else "n/a"
        )
    )
    if (
        coverage.legacy_bucket_exact_rows is None
        or coverage.exact_bucket_invalid_exact_rows is None
        or coverage.exact_bucket_missing_exact_rows is None
    ):
        forbidden_merge_rows = None
    else:
        forbidden_merge_rows = (
            coverage.legacy_bucket_exact_rows
            + coverage.exact_bucket_invalid_exact_rows
            + coverage.exact_bucket_missing_exact_rows
        )
    print(
        f"{coverage.name}_bucket_forbidden_merge_rows: "
        + (str(forbidden_merge_rows) if forbidden_merge_rows is not None else "n/a")
    )
    print(
        f"{coverage.name}_bucket_forbidden_merge_detected: "
        + (
            "yes"
            if forbidden_merge_rows is not None and forbidden_merge_rows > 0
            else "no"
            if forbidden_merge_rows is not None
            else "n/a"
        )
    )


def invalid_exact_coverage(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    raw_column: str,
    decimals_column: str,
    money_column: str,
    query: CoverageQuery,
) -> InvalidExactCoverage:
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return InvalidExactCoverage(name, None, None)
    if (
        not column_exists(conn, table, raw_column)
        or not column_exists(conn, table, decimals_column)
        or not column_exists(conn, table, money_column)
    ):
        return InvalidExactCoverage(name, None, None)
    invalid_predicate = (
        fake_exact_qty_predicate(raw_column, decimals_column, money_column)
    )
    invalid_rows = count_where(
        conn,
        query.row_source,
        invalid_predicate,
    )
    zero_raw_rows = count_where(
        conn,
        query.row_source,
        (
            f"({zero_raw_text_predicate(raw_column)}) "
            f"AND {decimals_column} IS NOT NULL "
            f"AND {money_column} IS NOT NULL AND {money_column} > 0"
        ),
    )
    return InvalidExactCoverage(name, invalid_rows, zero_raw_rows)


def invalid_observed_swaps_coverage(
    conn: sqlite3.Connection,
    name: str,
    query: CoverageQuery,
) -> InvalidExactCoverage:
    table = "observed_swaps"
    required_columns = (
        "qty_in_raw",
        "qty_in_decimals",
        "qty_out_raw",
        "qty_out_decimals",
    )
    if not has_required_tables(conn, query) or not table_exists(conn, table):
        return InvalidExactCoverage(name, None, None)
    if any(not column_exists(conn, table, column) for column in required_columns):
        return InvalidExactCoverage(name, None, None)
    invalid_rows = count_where(
        conn,
        query.row_source,
        (
            "("
            f"{invalid_exact_qty_sidecar_predicate('qty_in_raw', 'qty_in_decimals')} "
            "OR "
            f"{invalid_exact_qty_sidecar_predicate('qty_out_raw', 'qty_out_decimals')}"
            ") "
            "AND qty_in_raw IS NOT NULL AND qty_in_decimals IS NOT NULL "
            "AND qty_out_raw IS NOT NULL AND qty_out_decimals IS NOT NULL"
        ),
    )
    return InvalidExactCoverage(name, invalid_rows, None)


def legacy_evidence_surfaces() -> tuple[LegacyEvidenceSurface, ...]:
    observed_swaps_invalid_predicate = (
        f"({invalid_exact_qty_sidecar_predicate('qty_in_raw', 'qty_in_decimals')}) "
        "OR "
        f"({invalid_exact_qty_sidecar_predicate('qty_out_raw', 'qty_out_decimals')})"
    )
    observed_swaps_exact_predicate = (
        "qty_in_raw IS NOT NULL AND qty_in_decimals IS NOT NULL "
        "AND qty_out_raw IS NOT NULL AND qty_out_decimals IS NOT NULL "
        f"AND NOT ({observed_swaps_invalid_predicate})"
    )
    fills_qty_invalid_predicate = fake_exact_qty_predicate(
        "qty_raw", "qty_decimals", "notional_lamports"
    )
    positions_qty_invalid_predicate = fake_exact_qty_predicate(
        "qty_raw", "qty_decimals", "cost_lamports"
    )
    shadow_lots_qty_invalid_predicate = fake_exact_qty_predicate(
        "qty_raw", "qty_decimals", "cost_lamports"
    )
    shadow_closed_trades_qty_invalid_predicate = fake_exact_qty_predicate(
        "qty_raw", "qty_decimals", "entry_cost_lamports"
    )
    return (
        LegacyEvidenceSurface(
            name="observed_swaps",
            table="observed_swaps",
            query=CoverageQuery("observed_swaps", "ts"),
            exact_predicate=observed_swaps_exact_predicate,
            export_select_sql=(
                "SELECT observed_swaps.rowid AS export_rowid, observed_swaps.* "
                "FROM observed_swaps"
            ),
            order_by_sql="observed_swaps.ts, observed_swaps.rowid",
        ),
        LegacyEvidenceSurface(
            name="copy_signals",
            table="copy_signals",
            query=CoverageQuery("copy_signals", "ts"),
            exact_predicate=(
                "notional_lamports IS NOT NULL "
                "AND notional_origin = 'leader_exact_lamports'"
            ),
            export_select_sql=(
                "SELECT copy_signals.rowid AS export_rowid, copy_signals.* "
                "FROM copy_signals"
            ),
            order_by_sql="copy_signals.ts, copy_signals.rowid",
        ),
        LegacyEvidenceSurface(
            name="fills",
            table="fills",
            query=CoverageQuery(
                "fills JOIN orders ON orders.order_id = fills.order_id",
                "COALESCE(orders.confirm_ts, orders.submit_ts)",
                ("fills", "orders"),
            ),
            exact_predicate="notional_lamports IS NOT NULL AND fee_lamports IS NOT NULL",
            export_select_sql=(
                "SELECT fills.rowid AS export_rowid, fills.*, "
                "orders.submit_ts AS order_submit_ts, "
                "orders.confirm_ts AS order_confirm_ts "
                "FROM fills JOIN orders ON orders.order_id = fills.order_id"
            ),
            order_by_sql="COALESCE(orders.confirm_ts, orders.submit_ts), fills.rowid",
        ),
        LegacyEvidenceSurface(
            name="fills_qty",
            table="fills",
            query=CoverageQuery(
                "fills JOIN orders ON orders.order_id = fills.order_id",
                "COALESCE(orders.confirm_ts, orders.submit_ts)",
                ("fills", "orders"),
            ),
            exact_predicate=(
                "qty_raw IS NOT NULL AND qty_decimals IS NOT NULL "
                f"AND NOT ({fills_qty_invalid_predicate})"
            ),
            export_select_sql=(
                "SELECT fills.rowid AS export_rowid, fills.*, "
                "orders.submit_ts AS order_submit_ts, "
                "orders.confirm_ts AS order_confirm_ts "
                "FROM fills JOIN orders ON orders.order_id = fills.order_id"
            ),
            order_by_sql="COALESCE(orders.confirm_ts, orders.submit_ts), fills.rowid",
        ),
        LegacyEvidenceSurface(
            name="positions",
            table="positions",
            query=CoverageQuery("positions", "opened_ts"),
            exact_predicate="cost_lamports IS NOT NULL",
            export_select_sql=(
                "SELECT positions.rowid AS export_rowid, positions.* FROM positions"
            ),
            order_by_sql="positions.opened_ts, positions.rowid",
        ),
        LegacyEvidenceSurface(
            name="positions_qty",
            table="positions",
            query=CoverageQuery("positions", "opened_ts"),
            exact_predicate=(
                "qty_raw IS NOT NULL AND qty_decimals IS NOT NULL "
                f"AND NOT ({positions_qty_invalid_predicate})"
            ),
            export_select_sql=(
                "SELECT positions.rowid AS export_rowid, positions.* FROM positions"
            ),
            order_by_sql="positions.opened_ts, positions.rowid",
        ),
        LegacyEvidenceSurface(
            name="positions_pnl",
            table="positions",
            query=CoverageQuery("positions", "COALESCE(closed_ts, opened_ts)"),
            exact_predicate="pnl_lamports IS NOT NULL",
            export_select_sql=(
                "SELECT positions.rowid AS export_rowid, positions.* FROM positions"
            ),
            order_by_sql="COALESCE(positions.closed_ts, positions.opened_ts), positions.rowid",
        ),
        LegacyEvidenceSurface(
            name="shadow_lots",
            table="shadow_lots",
            query=CoverageQuery("shadow_lots", "opened_ts"),
            exact_predicate="cost_lamports IS NOT NULL",
            export_select_sql=(
                "SELECT shadow_lots.rowid AS export_rowid, shadow_lots.* FROM shadow_lots"
            ),
            order_by_sql="shadow_lots.opened_ts, shadow_lots.rowid",
        ),
        LegacyEvidenceSurface(
            name="shadow_lots_qty",
            table="shadow_lots",
            query=CoverageQuery("shadow_lots", "opened_ts"),
            exact_predicate=(
                "qty_raw IS NOT NULL AND qty_decimals IS NOT NULL "
                f"AND NOT ({shadow_lots_qty_invalid_predicate})"
            ),
            export_select_sql=(
                "SELECT shadow_lots.rowid AS export_rowid, shadow_lots.* FROM shadow_lots"
            ),
            order_by_sql="shadow_lots.opened_ts, shadow_lots.rowid",
        ),
        LegacyEvidenceSurface(
            name="shadow_closed_trades",
            table="shadow_closed_trades",
            query=CoverageQuery("shadow_closed_trades", "closed_ts"),
            exact_predicate=(
                "entry_cost_lamports IS NOT NULL "
                "AND exit_value_lamports IS NOT NULL "
                "AND pnl_lamports IS NOT NULL"
            ),
            export_select_sql=(
                "SELECT shadow_closed_trades.rowid AS export_rowid, shadow_closed_trades.* "
                "FROM shadow_closed_trades"
            ),
            order_by_sql="shadow_closed_trades.closed_ts, shadow_closed_trades.rowid",
        ),
        LegacyEvidenceSurface(
            name="shadow_closed_trades_qty",
            table="shadow_closed_trades",
            query=CoverageQuery("shadow_closed_trades", "closed_ts"),
            exact_predicate=(
                "qty_raw IS NOT NULL AND qty_decimals IS NOT NULL "
                f"AND NOT ({shadow_closed_trades_qty_invalid_predicate})"
            ),
            export_select_sql=(
                "SELECT shadow_closed_trades.rowid AS export_rowid, shadow_closed_trades.* "
                "FROM shadow_closed_trades"
            ),
            order_by_sql="shadow_closed_trades.closed_ts, shadow_closed_trades.rowid",
        ),
    )


def legacy_approximate_predicate(
    surface: LegacyEvidenceSurface, cutover_ts: str
) -> str:
    return f"({surface.query.time_expr} < '{cutover_ts}') AND NOT ({surface.exact_predicate})"


def post_cutover_approximate_predicate(
    surface: LegacyEvidenceSurface, cutover_ts: str
) -> str:
    return f"({surface.query.time_expr} >= '{cutover_ts}') AND NOT ({surface.exact_predicate})"


def print_invalid_exact_coverage(coverage: InvalidExactCoverage) -> None:
    print(
        f"{coverage.name}_invalid_exact_rows: "
        + (str(coverage.invalid_rows) if coverage.invalid_rows is not None else "n/a")
    )
    print(
        f"{coverage.name}_invalid_exact_detected: "
        + (
            "yes"
            if coverage.invalid_rows is not None and coverage.invalid_rows > 0
            else "no"
            if coverage.invalid_rows is not None
            else "n/a"
        )
    )
    print(
        f"{coverage.name}_invalid_zero_raw_exact_rows: "
        + (str(coverage.zero_raw_rows) if coverage.zero_raw_rows is not None else "n/a")
    )
    print(
        f"{coverage.name}_invalid_zero_raw_exact_detected: "
        + (
            "yes"
            if coverage.zero_raw_rows is not None and coverage.zero_raw_rows > 0
            else "no"
            if coverage.zero_raw_rows is not None
            else "n/a"
        )
    )


def collect_report(
    conn: sqlite3.Connection, cutover_override: CutoverState | None = None
) -> ExactMoneyCoverageReport:
    register_sqlite_functions(conn)
    cutover = cutover_override if cutover_override is not None else load_cutover_state(conn)
    observed_swaps = coverage_all_or_nothing(
        conn,
        "observed_swaps",
        "observed_swaps",
        ["qty_in_raw", "qty_in_decimals", "qty_out_raw", "qty_out_decimals"],
        CoverageQuery("observed_swaps", "ts"),
        cutover,
        invalid_predicate=(
            f"({invalid_exact_qty_sidecar_predicate('qty_in_raw', 'qty_in_decimals')}) "
            "OR "
            f"({invalid_exact_qty_sidecar_predicate('qty_out_raw', 'qty_out_decimals')})"
        ),
    )
    observed_swaps_invalid_exact = invalid_observed_swaps_coverage(
        conn,
        "observed_swaps",
        CoverageQuery("observed_swaps", "ts"),
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
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "notional_lamports"
        ),
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
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "cost_lamports"
        ),
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
    shadow_lots_qty = coverage_all_or_nothing(
        conn,
        "shadow_lots_qty",
        "shadow_lots",
        ["qty_raw", "qty_decimals"],
        CoverageQuery("shadow_lots", "opened_ts"),
        cutover,
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "cost_lamports"
        ),
    )
    shadow_closed_trades = coverage_all_or_nothing(
        conn,
        "shadow_closed_trades",
        "shadow_closed_trades",
        ["entry_cost_lamports", "exit_value_lamports", "pnl_lamports"],
        CoverageQuery("shadow_closed_trades", "closed_ts"),
        cutover,
    )
    shadow_closed_trades_qty = coverage_all_or_nothing(
        conn,
        "shadow_closed_trades_qty",
        "shadow_closed_trades",
        ["qty_raw", "qty_decimals"],
        CoverageQuery("shadow_closed_trades", "closed_ts"),
        cutover,
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "entry_cost_lamports"
        ),
    )
    positions_bucket = bucket_coverage(
        conn,
        "positions",
        "positions",
        "accounting_bucket",
        ["qty_raw", "qty_decimals"],
        CoverageQuery("positions", "opened_ts"),
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "cost_lamports"
        ),
    )
    shadow_lots_bucket = bucket_coverage(
        conn,
        "shadow_lots",
        "shadow_lots",
        "accounting_bucket",
        ["qty_raw", "qty_decimals"],
        CoverageQuery("shadow_lots", "opened_ts"),
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "cost_lamports"
        ),
    )
    shadow_closed_trades_bucket = bucket_coverage(
        conn,
        "shadow_closed_trades",
        "shadow_closed_trades",
        "accounting_bucket",
        ["qty_raw", "qty_decimals"],
        CoverageQuery("shadow_closed_trades", "closed_ts"),
        invalid_predicate=fake_exact_qty_predicate(
            "qty_raw", "qty_decimals", "entry_cost_lamports"
        ),
    )
    fills_invalid_exact = invalid_exact_coverage(
        conn,
        "fills",
        "fills",
        "qty_raw",
        "qty_decimals",
        "notional_lamports",
        CoverageQuery(
            "fills JOIN orders ON orders.order_id = fills.order_id",
            "COALESCE(orders.confirm_ts, orders.submit_ts)",
            ("fills", "orders"),
        ),
    )
    positions_invalid_exact = invalid_exact_coverage(
        conn,
        "positions",
        "positions",
        "qty_raw",
        "qty_decimals",
        "cost_lamports",
        CoverageQuery("positions", "opened_ts"),
    )
    shadow_lots_invalid_exact = invalid_exact_coverage(
        conn,
        "shadow_lots",
        "shadow_lots",
        "qty_raw",
        "qty_decimals",
        "cost_lamports",
        CoverageQuery("shadow_lots", "opened_ts"),
    )
    shadow_closed_trades_invalid_exact = invalid_exact_coverage(
        conn,
        "shadow_closed_trades",
        "shadow_closed_trades",
        "qty_raw",
        "qty_decimals",
        "entry_cost_lamports",
        CoverageQuery("shadow_closed_trades", "closed_ts"),
    )
    return ExactMoneyCoverageReport(
        cutover=cutover,
        observed_swaps=observed_swaps,
        observed_swaps_invalid_exact=observed_swaps_invalid_exact,
        copy_signals=copy_signals,
        fills=fills,
        fills_qty=fills_qty,
        positions=positions,
        positions_qty=positions_qty,
        positions_pnl=positions_pnl,
        shadow_lots=shadow_lots,
        shadow_lots_qty=shadow_lots_qty,
        shadow_closed_trades=shadow_closed_trades,
        shadow_closed_trades_qty=shadow_closed_trades_qty,
        positions_bucket=positions_bucket,
        shadow_lots_bucket=shadow_lots_bucket,
        shadow_closed_trades_bucket=shadow_closed_trades_bucket,
        fills_invalid_exact=fills_invalid_exact,
        positions_invalid_exact=positions_invalid_exact,
        shadow_lots_invalid_exact=shadow_lots_invalid_exact,
        shadow_closed_trades_invalid_exact=shadow_closed_trades_invalid_exact,
    )


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        usage()
        return 1

    db_path = resolve_db_path(argv[1])
    conn = sqlite3.connect(str(db_path))
    try:
        report = collect_report(conn)
    finally:
        conn.close()

    print("=== Exact Money Coverage Report ===")
    print(f"db: {db_path}")
    if report.cutover is None:
        print("exact_money_cutover_present: no")
        print("exact_money_cutover_ts: n/a")
        print("exact_money_cutover_recorded_ts: n/a")
        print("exact_money_cutover_note: n/a")
    else:
        print("exact_money_cutover_present: yes")
        print(f"exact_money_cutover_ts: {report.cutover.cutover_ts}")
        print(f"exact_money_cutover_recorded_ts: {report.cutover.recorded_ts}")
        print(f"exact_money_cutover_note: {report.cutover.note or 'n/a'}")
    print_coverage(report.observed_swaps)
    print_invalid_exact_coverage(report.observed_swaps_invalid_exact)
    print_coverage(report.copy_signals)
    print_coverage(report.fills)
    print_coverage(report.fills_qty)
    print_coverage(report.positions)
    print_coverage(report.positions_qty)
    print_coverage(report.positions_pnl)
    print_coverage(report.shadow_lots)
    print_coverage(report.shadow_lots_qty)
    print_coverage(report.shadow_closed_trades)
    print_coverage(report.shadow_closed_trades_qty)
    print_bucket_coverage(report.positions_bucket)
    print_bucket_coverage(report.shadow_lots_bucket)
    print_bucket_coverage(report.shadow_closed_trades_bucket)
    print_invalid_exact_coverage(report.fills_invalid_exact)
    print_invalid_exact_coverage(report.positions_invalid_exact)
    print_invalid_exact_coverage(report.shadow_lots_invalid_exact)
    print_invalid_exact_coverage(report.shadow_closed_trades_invalid_exact)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
