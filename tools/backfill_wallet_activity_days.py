#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


UPSERT_SQL = """
INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
SELECT wallet_id, substr(ts, 1, 10) AS activity_day, MAX(ts) AS last_seen
FROM observed_swaps
WHERE ts >= ?1 AND ts < ?2
GROUP BY wallet_id, substr(ts, 1, 10)
ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
    last_seen = CASE
        WHEN excluded.last_seen > wallet_activity_days.last_seen
            THEN excluded.last_seen
        ELSE wallet_activity_days.last_seen
    END
"""


@dataclass(frozen=True)
class Config:
    db_path: Path
    start_day: datetime
    end_day: datetime
    sleep_ms: int


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Offline/admin backfill for wallet_activity_days from observed_swaps. "
            "Runs one UTC day per transaction so the job is resumable and bounded."
        )
    )
    parser.add_argument("db_path", help="Path to SQLite database")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--window-days",
        type=int,
        help="Backfill the trailing N UTC calendar days ending at --now or current UTC time",
    )
    mode.add_argument("--start-day", help="Inclusive UTC day in YYYY-MM-DD")
    parser.add_argument(
        "--end-day",
        help="Inclusive UTC day in YYYY-MM-DD (required with --start-day, ignored with --window-days)",
    )
    parser.add_argument(
        "--now",
        help="Override current UTC timestamp for reproducible runs (RFC3339)",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Optional sleep between day chunks",
    )
    args = parser.parse_args()

    now = parse_timestamp(args.now) if args.now else datetime.now(timezone.utc)
    if args.window_days is not None:
        if args.window_days < 1:
            raise SystemExit("--window-days must be >= 1")
        end_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_day = end_day - timedelta(days=args.window_days - 1)
    else:
        if not args.end_day:
            raise SystemExit("--end-day is required with --start-day")
        start_day = parse_day(args.start_day)
        end_day = parse_day(args.end_day)
        if end_day < start_day:
            raise SystemExit("--end-day must be >= --start-day")

    return Config(
        db_path=Path(args.db_path),
        start_day=start_day,
        end_day=end_day,
        sleep_ms=max(args.sleep_ms, 0),
    )


def parse_day(raw: str) -> datetime:
    try:
        day = datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"invalid day: {raw!r}") from exc
    return day.replace(tzinfo=timezone.utc)


def parse_timestamp(raw: str) -> datetime:
    normalized = raw.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError as exc:
        raise SystemExit(f"invalid timestamp: {raw!r}") from exc


def ensure_schema(conn: sqlite3.Connection) -> None:
    expected = {"observed_swaps", "wallet_activity_days"}
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('observed_swaps', 'wallet_activity_days')"
    ).fetchall()
    present = {row[0] for row in rows}
    missing = expected - present
    if missing:
        raise SystemExit(f"required table(s) missing: {', '.join(sorted(missing))}")


def day_bounds(day_start: datetime) -> tuple[str, str]:
    day_end = day_start + timedelta(days=1)
    return day_start.isoformat().replace("+00:00", "Z"), day_end.isoformat().replace("+00:00", "Z")


def run_chunk(conn: sqlite3.Connection, day_start: datetime) -> int:
    start_ts, end_ts = day_bounds(day_start)
    conn.execute("BEGIN IMMEDIATE TRANSACTION")
    try:
        conn.execute(UPSERT_SQL, (start_ts, end_ts))
        changed = int(conn.execute("SELECT changes()").fetchone()[0])
        conn.execute("COMMIT")
        return changed
    except Exception:
        conn.execute("ROLLBACK")
        raise


def iter_days(start_day: datetime, end_day: datetime):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def main() -> int:
    config = parse_args()
    conn = sqlite3.connect(config.db_path)
    try:
        ensure_schema(conn)
        total_changed = 0
        processed_days = 0
        for day_start in iter_days(config.start_day, config.end_day):
            changed = run_chunk(conn, day_start)
            processed_days += 1
            total_changed += changed
            print(
                f"backfilled_day={day_start.date().isoformat()} changed_rows={changed}",
                flush=True,
            )
            if config.sleep_ms > 0 and day_start < config.end_day:
                time.sleep(config.sleep_ms / 1000.0)
        print(
            f"summary processed_days={processed_days} total_changed_rows={total_changed}",
            flush=True,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
