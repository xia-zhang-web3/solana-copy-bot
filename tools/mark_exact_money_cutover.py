#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


CUTOVER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS exact_money_cutover_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cutover_ts TEXT NOT NULL,
    recorded_ts TEXT NOT NULL,
    note TEXT
)
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record the authoritative exact-money cutover timestamp in a SQLite DB. "
            "Used for D2-6 rollout tracking and exact coverage reporting."
        )
    )
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument(
        "--cutover-ts",
        help="RFC3339 UTC timestamp for the cutover. Defaults to current UTC time.",
    )
    parser.add_argument(
        "--note",
        default="",
        help="Optional operator note recorded with the cutover marker.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help=(
            "Bypass exact-money readiness preflight and record the cutover marker even if "
            "schema or coverage checks fail."
        ),
    )
    return parser.parse_args()


def parse_timestamp(raw: str) -> str:
    normalized = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SystemExit(f"invalid RFC3339 timestamp: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise SystemExit(f"timestamp must include explicit timezone: {raw!r}")
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def resolve_db_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_file():
        raise SystemExit(f"sqlite db not found: {raw}")
    return path


def load_exact_money_coverage_module():
    module_path = Path(__file__).with_name("exact_money_coverage_report.py")
    spec = importlib.util.spec_from_file_location(
        "exact_money_coverage_report", module_path
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed loading exact money coverage module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def collect_preflight_errors(report: object) -> list[str]:
    errors: list[str] = []
    schema_surfaces = [
        report.observed_swaps,
        report.copy_signals,
        report.fills,
        report.fills_qty,
        report.positions,
        report.positions_qty,
        report.positions_pnl,
        report.shadow_lots,
        report.shadow_lots_qty,
        report.shadow_closed_trades,
        report.shadow_closed_trades_qty,
    ]
    for coverage in schema_surfaces:
        if coverage.exact_rows is None:
            errors.append(f"{coverage.name}: exact-money schema surface missing")
        if coverage.post_cutover_rows is None or coverage.post_cutover_exact_rows is None:
            errors.append(f"{coverage.name}: post-cutover coverage unavailable")
            continue
        post_cutover_approximate_rows = (
            coverage.post_cutover_rows - coverage.post_cutover_exact_rows
        )
        if post_cutover_approximate_rows > 0:
            errors.append(
                f"{coverage.name}: post_cutover_approximate_rows="
                f"{post_cutover_approximate_rows}"
            )
    partial_surfaces = [
        report.copy_signals,
        report.fills,
        report.fills_qty,
        report.positions_qty,
        report.shadow_lots_qty,
        report.shadow_closed_trades,
        report.shadow_closed_trades_qty,
    ]
    for coverage in partial_surfaces:
        if coverage.partial_rows is not None and coverage.partial_rows > 0:
            errors.append(f"{coverage.name}: partial_exact_rows={coverage.partial_rows}")
    invalid_surfaces = [
        report.observed_swaps_invalid_exact,
        report.fills_invalid_exact,
        report.positions_invalid_exact,
        report.shadow_lots_invalid_exact,
        report.shadow_closed_trades_invalid_exact,
    ]
    for coverage in invalid_surfaces:
        if coverage.invalid_rows is None:
            errors.append(f"{coverage.name}: invalid exact coverage unavailable")
        elif coverage.invalid_rows > 0:
            errors.append(f"{coverage.name}: invalid_exact_rows={coverage.invalid_rows}")
    bucket_surfaces = [
        report.positions_bucket,
        report.shadow_lots_bucket,
        report.shadow_closed_trades_bucket,
    ]
    for coverage in bucket_surfaces:
        if coverage.total_rows == 0:
            continue
        if coverage.legacy_bucket_rows is None or coverage.exact_bucket_rows is None:
            errors.append(f"{coverage.name}: accounting bucket surface unavailable")
            continue
        if coverage.unknown_bucket_rows is not None and coverage.unknown_bucket_rows > 0:
            errors.append(f"{coverage.name}: unknown_bucket_rows={coverage.unknown_bucket_rows}")
        if (
            coverage.legacy_bucket_exact_rows is None
            or coverage.exact_bucket_invalid_exact_rows is None
            or coverage.exact_bucket_missing_exact_rows is None
        ):
            errors.append(f"{coverage.name}: forbidden bucket merge coverage unavailable")
            continue
        forbidden_merge_rows = (
            coverage.legacy_bucket_exact_rows
            + coverage.exact_bucket_invalid_exact_rows
            + coverage.exact_bucket_missing_exact_rows
        )
        if forbidden_merge_rows > 0:
            errors.append(
                f"{coverage.name}: bucket_forbidden_merge_rows={forbidden_merge_rows}"
            )
    return errors


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    cutover_ts = (
        parse_timestamp(args.cutover_ts)
        if args.cutover_ts
        else datetime.now(timezone.utc).isoformat()
    )
    recorded_ts = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(str(db_path))
    try:
        if not args.allow_dirty:
            coverage = load_exact_money_coverage_module()
            candidate_cutover = coverage.CutoverState(
                cutover_ts=cutover_ts,
                recorded_ts=recorded_ts,
                note=args.note,
            )
            report = coverage.collect_report(conn, candidate_cutover)
            preflight_errors = collect_preflight_errors(report)
            if preflight_errors:
                print("exact money cutover preflight failed:", file=sys.stderr)
                for error in preflight_errors:
                    print(f" - {error}", file=sys.stderr)
                raise SystemExit(2)
        conn.execute(CUTOVER_TABLE_SQL)
        conn.execute(
            """
            INSERT INTO exact_money_cutover_state(id, cutover_ts, recorded_ts, note)
            VALUES (1, ?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                cutover_ts = excluded.cutover_ts,
                recorded_ts = excluded.recorded_ts,
                note = excluded.note
            """,
            (cutover_ts, recorded_ts, args.note),
        )
        conn.commit()
    finally:
        conn.close()

    print(f"cutover_ts={cutover_ts}")
    print(f"recorded_ts={recorded_ts}")
    if args.note:
        print(f"note={args.note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
