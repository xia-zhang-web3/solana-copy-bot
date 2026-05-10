from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SOL_MINT = "So11111111111111111111111111111111111111112"
TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")
JSON_RE = re.compile(r"(\{.*\})\s*$")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(text: str) -> datetime | None:
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def pct_delta(base: float, candidate: float) -> float:
    if base <= 0:
        return 100.0 if candidate > 0 else 0.0
    return ((candidate - base) / base) * 100.0


def load_db_metrics(db_path: str, window_minutes: int) -> dict[str, Any]:
    window_expr = f"-{window_minutes} minutes"
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    def one(sql: str, params: tuple[Any, ...] = ()) -> float:
        row = cur.execute(sql, params).fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    observed_total = one(
        "SELECT COUNT(*) FROM observed_swaps WHERE datetime(ts) >= datetime('now', ?)",
        (window_expr,),
    )
    observed_buy = one(
        """
        SELECT COUNT(*)
        FROM observed_swaps
        WHERE datetime(ts) >= datetime('now', ?)
          AND token_in = ?
          AND token_out <> ?
        """,
        (window_expr, SOL_MINT, SOL_MINT),
    )
    observed_sell = one(
        """
        SELECT COUNT(*)
        FROM observed_swaps
        WHERE datetime(ts) >= datetime('now', ?)
          AND token_out = ?
          AND token_in <> ?
        """,
        (window_expr, SOL_MINT, SOL_MINT),
    )
    signals_total = one(
        "SELECT COUNT(*) FROM copy_signals WHERE datetime(ts) >= datetime('now', ?)",
        (window_expr,),
    )
    signals_buy = one(
        """
        SELECT COUNT(*)
        FROM copy_signals
        WHERE datetime(ts) >= datetime('now', ?)
          AND side = 'buy'
        """,
        (window_expr,),
    )
    signals_sell = one(
        """
        SELECT COUNT(*)
        FROM copy_signals
        WHERE datetime(ts) >= datetime('now', ?)
          AND side = 'sell'
        """,
        (window_expr,),
    )
    status_rows = cur.execute(
        """
        SELECT status, COUNT(*)
        FROM copy_signals
        WHERE datetime(ts) >= datetime('now', ?)
        GROUP BY status
        ORDER BY COUNT(*) DESC
        """,
        (window_expr,),
    ).fetchall()
    risk_rows = cur.execute(
        """
        SELECT type, COUNT(*)
        FROM risk_events
        WHERE datetime(ts) >= datetime('now', ?)
        GROUP BY type
        ORDER BY COUNT(*) DESC
        LIMIT 10
        """,
        (window_expr,),
    ).fetchall()
    con.close()
    return {
        "observed_total": int(observed_total),
        "observed_buy": int(observed_buy),
        "observed_sell": int(observed_sell),
        "signals_total": int(signals_total),
        "signals_buy": int(signals_buy),
        "signals_sell": int(signals_sell),
        "signal_status_counts": {str(status): int(count) for status, count in status_rows},
        "risk_event_counts": {str(event_type): int(count) for event_type, count in risk_rows},
    }


@dataclass
class MetricRow:
    ts: datetime
    payload: dict[str, Any]


def load_metric_rows(log_path: str) -> tuple[list[MetricRow], list[MetricRow]]:
    rows: list[MetricRow] = []
    sqlite_rows: list[MetricRow] = []
    if not Path(log_path).exists():
        return rows, sqlite_rows
    with open(log_path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            m_json = JSON_RE.search(line)
            if not m_json:
                continue
            try:
                payload = json.loads(m_json.group(1))
            except json.JSONDecodeError:
                continue
            ts = None
            m_ts = TIMESTAMP_RE.search(line)
            if m_ts:
                ts = parse_ts(m_ts.group(1))
            if ts is None and isinstance(payload.get("ts_utc"), str):
                ts = parse_ts(payload["ts_utc"])
            if ts is None:
                continue
            if "ingestion pipeline metrics" in line:
                rows.append(MetricRow(ts=ts, payload=payload))
            elif "sqlite contention counters" in line:
                sqlite_rows.append(MetricRow(ts=ts, payload=payload))
    rows.sort(key=lambda row: row.ts)
    sqlite_rows.sort(key=lambda row: row.ts)
    return rows, sqlite_rows


def counter_delta(start: dict[str, Any], end: dict[str, Any], key: str) -> float | None:
    a = as_number(start.get(key))
    b = as_number(end.get(key))
    if a is None or b is None:
        return None
    delta = b - a
    return None if delta < 0 else delta


def find_window_start(rows: list[MetricRow], idx: int, window_seconds: int) -> int:
    target = rows[idx].ts - timedelta(seconds=window_seconds)
    cursor = idx
    while cursor > 0 and rows[cursor - 1].ts >= target:
        cursor -= 1
    return cursor


def evaluate_telemetry(log_path: str) -> dict[str, Any]:
    rows, sqlite_rows = load_metric_rows(log_path)
    if not rows:
        return {
            "available": False,
            "sample_count": 0,
            "minute_buckets": 0,
            "notes": "no ingestion pipeline metrics rows parsed",
        }
    bucket_map: dict[datetime, MetricRow] = {}
    for row in rows:
        bucket_map[row.ts.replace(second=0, microsecond=0)] = row
    bucket_rows = [bucket_map[key] for key in sorted(bucket_map.keys())]
    p95_values = [
        value
        for value in (as_number(row.payload.get("ingestion_lag_ms_p95")) for row in bucket_rows)
        if value is not None
    ]
    p99_values = [
        value
        for value in (as_number(row.payload.get("ingestion_lag_ms_p99")) for row in bucket_rows)
        if value is not None
    ]
    p95_pass_ratio = (
        sum(1 for value in p95_values if value <= 3000.0) / len(p95_values)
        if p95_values
        else None
    )
    p99_pass_ratio = (
        sum(1 for value in p99_values if value <= 6000.0) / len(p99_values)
        if p99_values
        else None
    )

    replaced_ratios: list[float] = []
    reconnect_storm_breaches = 0
    reject_rate_breaches = 0
    no_processed_breaches = 0
    for i in range(1, len(rows)):
        d_enq = counter_delta(rows[i - 1].payload, rows[i].payload, "ws_notifications_enqueued")
        d_replaced = counter_delta(
            rows[i - 1].payload, rows[i].payload, "ws_notifications_replaced_oldest"
        )
        if d_enq is not None and d_replaced is not None and d_enq >= 500:
            replaced_ratios.append(d_replaced / max(d_enq, 1.0))
    for i in range(len(rows)):
        i_5m = find_window_start(rows, i, 300)
        reconnect_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "reconnect_count")
        if reconnect_delta is not None and reconnect_delta >= 6:
            reconnect_storm_breaches += 1
        parse_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "parse_rejected_total")
        decode_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "grpc_decode_errors")
        inbound_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "grpc_message_total")
        if None not in (parse_delta, decode_delta, inbound_delta) and inbound_delta >= 500:
            reject_rate = (parse_delta + decode_delta) / max(inbound_delta, 1.0)
            if reject_rate > 0.20:
                reject_rate_breaches += 1
        i_120s = find_window_start(rows, i, 120)
        inbound_delta_120 = counter_delta(
            rows[i_120s].payload, rows[i].payload, "grpc_message_total"
        )
        processed_delta_120 = counter_delta(
            rows[i_120s].payload, rows[i].payload, "ws_notifications_enqueued"
        )
        if (
            inbound_delta_120 is not None
            and processed_delta_120 is not None
            and inbound_delta_120 >= 200
            and processed_delta_120 == 0
        ):
            no_processed_breaches += 1

    sqlite_write_retry_delta = None
    sqlite_busy_error_delta = None
    if len(sqlite_rows) >= 2:
        sqlite_write_retry_delta = counter_delta(
            sqlite_rows[0].payload, sqlite_rows[-1].payload, "sqlite_write_retry_total"
        )
        sqlite_busy_error_delta = counter_delta(
            sqlite_rows[0].payload, sqlite_rows[-1].payload, "sqlite_busy_error_total"
        )
    latest_payload = rows[-1].payload
    replaced_ratio_pass_ratio = (
        sum(1 for ratio in replaced_ratios if ratio < 0.05) / len(replaced_ratios)
        if replaced_ratios
        else None
    )
    return {
        "available": True,
        "sample_count": len(rows),
        "minute_buckets": len(bucket_rows),
        "lag_p95_last_ms": as_number(latest_payload.get("ingestion_lag_ms_p95")),
        "lag_p99_last_ms": as_number(latest_payload.get("ingestion_lag_ms_p99")),
        "lag_p95_pass_ratio": p95_pass_ratio,
        "lag_p99_pass_ratio": p99_pass_ratio,
        "replaced_ratio_eval_windows": len(replaced_ratios),
        "replaced_ratio_pass_ratio": replaced_ratio_pass_ratio,
        "reconnect_storm_breach_count": reconnect_storm_breaches,
        "reject_rate_breach_count": reject_rate_breaches,
        "no_processed_breach_count": no_processed_breaches,
        "sqlite_write_retry_delta": sqlite_write_retry_delta,
        "sqlite_busy_error_delta": sqlite_busy_error_delta,
    }
