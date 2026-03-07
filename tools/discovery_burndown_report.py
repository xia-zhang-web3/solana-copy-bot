#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MAX_TIME_BUDGET_EXHAUSTED_RATIO_FOR_PASS = 0.05


def resolve_summary_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_dir():
        candidate = path / "computed_summary.json"
        if candidate.is_file():
            return candidate
    if path.is_file():
        return path
    raise SystemExit(f"summary path not found: {raw}")


def parse_timestamp(raw: str) -> datetime:
    if raw.strip().lower() == "n/a":
        raise SystemExit("timestamp field is unavailable (got 'n/a')")
    normalized = raw.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError as exc:
        raise SystemExit(f"invalid timestamp: {raw!r}") from exc


def parse_float(raw: Any, field: str) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid {field}: {raw!r}") from exc
    if not math.isfinite(value):
        raise SystemExit(f"non-finite {field}: {raw!r}")
    return value


@dataclass
class Snapshot:
    path: Path
    snapshot_utc: datetime
    cursor_ts: datetime
    head_ts: datetime | None
    gap_seconds: float
    fetch_limit_ratio: float | None
    page_budget_ratio: float | None
    time_budget_ratio: float | None


def load_snapshot(raw_path: str) -> Snapshot:
    path = resolve_summary_path(raw_path)
    payload = json.loads(path.read_text(encoding="utf-8"))

    snapshot_raw = payload.get("snapshot_utc")
    cursor_raw = payload.get("discovery_cursor_ts")
    if not snapshot_raw or not cursor_raw:
        raise SystemExit(
            f"{path}: missing required fields snapshot_utc/discovery_cursor_ts"
        )

    head_raw = payload.get("observed_swaps_head_ts") or payload.get("observed_swaps_max_ts")
    gap_raw = payload.get("discovery_cursor_head_gap_seconds")
    if gap_raw is None:
        gap_raw = payload.get("discovery_cursor_ts_gap_seconds")
    if gap_raw is None:
        raise SystemExit(
            f"{path}: missing discovery_cursor_head_gap_seconds/discovery_cursor_ts_gap_seconds"
        )

    return Snapshot(
        path=path,
        snapshot_utc=parse_timestamp(snapshot_raw),
        cursor_ts=parse_timestamp(cursor_raw),
        head_ts=parse_timestamp(head_raw) if head_raw else None,
        gap_seconds=parse_float(gap_raw, "gap_seconds"),
        fetch_limit_ratio=(
            parse_float(payload["swaps_fetch_limit_reached_ratio"], "swaps_fetch_limit_reached_ratio")
            if payload.get("swaps_fetch_limit_reached_ratio") not in (None, "n/a")
            else None
        ),
        page_budget_ratio=(
            parse_float(
                payload["swaps_fetch_page_budget_exhausted_ratio"],
                "swaps_fetch_page_budget_exhausted_ratio",
            )
            if payload.get("swaps_fetch_page_budget_exhausted_ratio") not in (None, "n/a")
            else None
        ),
        time_budget_ratio=(
            parse_float(
                payload["swaps_fetch_time_budget_exhausted_ratio"],
                "swaps_fetch_time_budget_exhausted_ratio",
            )
            if payload.get("swaps_fetch_time_budget_exhausted_ratio") not in (None, "n/a")
            else None
        ),
    )


def format_seconds(value: float) -> str:
    sign = "-" if value < 0 else ""
    value = abs(int(round(value)))
    hours, rem = divmod(value, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{sign}{hours}h {minutes}m {seconds}s"


def verdict(
    cursor_advance_ratio: float,
    gap_delta_seconds: float,
    fetch_limit_ratio: float | None,
    page_budget_ratio: float | None,
    time_budget_ratio: float | None,
) -> tuple[str, str]:
    if (
        time_budget_ratio is not None
        and time_budget_ratio >= MAX_TIME_BUDGET_EXHAUSTED_RATIO_FOR_PASS
    ):
        return (
            "HOLD",
            "time-budget exhaustion remains elevated, so discovery is not yet stable under the higher-throughput fetch path",
        )
    if cursor_advance_ratio > 1.0 and gap_delta_seconds < 0:
        return (
            "PASS",
            "cursor advances faster than wall time and head gap is shrinking",
        )
    if (
        page_budget_ratio is not None
        and page_budget_ratio >= 0.99
        and gap_delta_seconds >= 0
    ):
        return (
            "HOLD",
            "page budget is still saturated and head gap is flat or worsening",
        )
    if fetch_limit_ratio is not None and fetch_limit_ratio >= 0.99 and gap_delta_seconds >= 0:
        return (
            "HOLD",
            "fetch cap is saturated and head gap is flat or worsening",
        )
    if cursor_advance_ratio <= 1.0:
        return (
            "HOLD",
            "cursor advance ratio is <= 1.0 so backlog is not burning down",
        )
    return (
        "MIXED",
        "signals are mixed; review head-gap and budget ratios directly",
    )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: discovery_burndown_report.py <older_snapshot.json|dir> <newer_snapshot.json|dir>",
            file=sys.stderr,
        )
        return 1

    older = load_snapshot(argv[1])
    newer = load_snapshot(argv[2])
    if newer.snapshot_utc <= older.snapshot_utc:
        raise SystemExit("newer snapshot must be later than older snapshot")

    wall_time_delta = (newer.snapshot_utc - older.snapshot_utc).total_seconds()
    cursor_delta = (newer.cursor_ts - older.cursor_ts).total_seconds()
    cursor_advance_ratio = cursor_delta / wall_time_delta if wall_time_delta > 0 else 0.0
    gap_delta_seconds = newer.gap_seconds - older.gap_seconds
    status, reason = verdict(
        cursor_advance_ratio,
        gap_delta_seconds,
        newer.fetch_limit_ratio,
        newer.page_budget_ratio,
        newer.time_budget_ratio,
    )

    print("=== Discovery Burndown Report ===")
    print(f"older_snapshot: {older.path}")
    print(f"newer_snapshot: {newer.path}")
    print(f"older_snapshot_utc: {older.snapshot_utc.isoformat()}")
    print(f"newer_snapshot_utc: {newer.snapshot_utc.isoformat()}")
    print(f"wall_time_delta_seconds: {int(round(wall_time_delta))}")
    print(f"wall_time_delta_human: {format_seconds(wall_time_delta)}")
    print(f"older_cursor_ts: {older.cursor_ts.isoformat()}")
    print(f"newer_cursor_ts: {newer.cursor_ts.isoformat()}")
    print(f"cursor_advance_seconds: {int(round(cursor_delta))}")
    print(f"cursor_advance_human: {format_seconds(cursor_delta)}")
    print(f"cursor_advance_ratio: {cursor_advance_ratio:.4f}")
    print(f"older_head_gap_seconds: {int(round(older.gap_seconds))}")
    print(f"newer_head_gap_seconds: {int(round(newer.gap_seconds))}")
    print(f"head_gap_delta_seconds: {int(round(gap_delta_seconds))}")
    print(f"head_gap_delta_human: {format_seconds(gap_delta_seconds)}")
    if older.head_ts:
        print(f"older_head_ts: {older.head_ts.isoformat()}")
    else:
        print("older_head_ts: n/a")
    if newer.head_ts:
        print(f"newer_head_ts: {newer.head_ts.isoformat()}")
    else:
        print("newer_head_ts: n/a")
    print(
        "older_swaps_fetch_limit_reached_ratio: "
        + (
            f"{older.fetch_limit_ratio:.4f}"
            if older.fetch_limit_ratio is not None
            else "n/a"
        )
    )
    print(
        "newer_swaps_fetch_limit_reached_ratio: "
        + (
            f"{newer.fetch_limit_ratio:.4f}"
            if newer.fetch_limit_ratio is not None
            else "n/a"
        )
    )
    print(
        "newer_swaps_fetch_page_budget_exhausted_ratio: "
        + (
            f"{newer.page_budget_ratio:.4f}"
            if newer.page_budget_ratio is not None
            else "n/a"
        )
    )
    print(
        "newer_swaps_fetch_time_budget_exhausted_ratio: "
        + (
            f"{newer.time_budget_ratio:.4f}"
            if newer.time_budget_ratio is not None
            else "n/a"
        )
    )
    print(f"burndown_verdict: {status}")
    print(f"burndown_reason: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
