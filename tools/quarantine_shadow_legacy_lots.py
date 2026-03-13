#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

OPEN_EPS = 1e-12
MARKET = "market"
QUARANTINED_LEGACY = "quarantined_legacy"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quarantine open shadow lots into risk_context=quarantined_legacy."
    )
    parser.add_argument("db_path", help="Path to sqlite database")
    parser.add_argument(
        "--older-than-hours",
        type=float,
        required=True,
        help="Only target open lots with opened_ts older than this many hours",
    )
    parser.add_argument("--wallet-id", help="Optional wallet_id filter")
    parser.add_argument("--token", help="Optional token filter")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the quarantine update; otherwise run in dry-run mode",
    )
    return parser.parse_args()


def build_where(args: argparse.Namespace, cutoff_ts: str) -> tuple[str, list[object]]:
    clauses = [
        "qty > ?",
        "opened_ts <= ?",
        "COALESCE(risk_context, 'market') = ?",
    ]
    params: list[object] = [OPEN_EPS, cutoff_ts, MARKET]
    if args.wallet_id:
        clauses.append("wallet_id = ?")
        params.append(args.wallet_id)
    if args.token:
        clauses.append("token = ?")
        params.append(args.token)
    return " AND ".join(clauses), params


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    if args.older_than_hours <= 0:
        raise SystemExit("--older-than-hours must be > 0")
    if not db_path.is_file():
        raise SystemExit(f"sqlite db not found: {db_path}")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.older_than_hours)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        columns = {
            row["name"]
            for row in conn.execute("SELECT name FROM pragma_table_info('shadow_lots')")
        }
        if "risk_context" not in columns:
            raise SystemExit(
                "shadow_lots.risk_context is missing; apply migration 0038_shadow_lot_risk_context.sql first"
            )
        notional_sol_expr = "cost_sol"
        if "cost_lamports" in columns:
            notional_sol_expr = (
                "CASE "
                "WHEN cost_lamports IS NOT NULL THEN CAST(cost_lamports AS REAL) / 1000000000.0 "
                "ELSE cost_sol "
                "END"
            )
        where_sql, params = build_where(args, cutoff_ts)
        summary_sql = f"""
            SELECT
              COUNT(*) AS matched_lots,
              COALESCE(SUM({notional_sol_expr}), 0.0) AS matched_accounting_notional_sol,
              COUNT(DISTINCT wallet_id) AS matched_wallets,
              COUNT(DISTINCT token) AS matched_tokens
            FROM shadow_lots
            WHERE {where_sql}
        """
        row = conn.execute(summary_sql, params).fetchone()
        assert row is not None

        updated_lots = 0
        if args.apply:
            update_sql = f"""
                UPDATE shadow_lots
                SET risk_context = ?
                WHERE {where_sql}
            """
            cursor = conn.execute(update_sql, [QUARANTINED_LEGACY, *params])
            updated_lots = cursor.rowcount if cursor.rowcount is not None else 0
            conn.commit()

        print(f"mode: {'apply' if args.apply else 'dry_run'}")
        print(f"db_path: {db_path}")
        print(f"cutoff_ts: {cutoff_ts}")
        print(f"target_risk_context: {QUARANTINED_LEGACY}")
        print(f"filter_wallet_id: {args.wallet_id or 'n/a'}")
        print(f"filter_token: {args.token or 'n/a'}")
        print(f"matched_lots: {row['matched_lots']}")
        print(f"matched_accounting_notional_sol: {row['matched_accounting_notional_sol']}")
        print(f"matched_wallets: {row['matched_wallets']}")
        print(f"matched_tokens: {row['matched_tokens']}")
        print(f"updated_lots: {updated_lots}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
