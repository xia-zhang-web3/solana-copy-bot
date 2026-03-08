#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
