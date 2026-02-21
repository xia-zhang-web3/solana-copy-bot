#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  tools/ingestion_ab_report.sh
    --control-config <path>
    --candidate-config <path>
    [--control-db <path>]
    [--candidate-db <path>]
    [--control-service <name>]
    [--candidate-service <name>]
    [--window-minutes <int>]
    [--mode replay|live]
    [--fixture-id <id>]
    [--fixture-sha256 <sha256>]
    [--output-json <path>]
    [--buy-target-pct <float>]
    [--sell-max-degrade-pct <float>]

Defaults:
  --window-minutes 360
  --mode live
  --buy-target-pct 15
  --sell-max-degrade-pct 3

Notes:
  - DB metrics are read from observed_swaps/copy_signals/risk_events.
  - Telemetry gates are evaluated from journald ingestion/app logs when service names are provided.
  - Exit code is 0 on pass, 2 on gate failure.
USAGE
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
mkdir -p "${REPO_ROOT}/state"

CONTROL_CONFIG=""
CANDIDATE_CONFIG=""
CONTROL_DB=""
CANDIDATE_DB=""
CONTROL_SERVICE=""
CANDIDATE_SERVICE=""
WINDOW_MINUTES=360
MODE="live"
FIXTURE_ID=""
FIXTURE_SHA256=""
OUTPUT_JSON=""
BUY_TARGET_PCT="15"
SELL_MAX_DEGRADE_PCT="3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --control-config)
      CONTROL_CONFIG="$2"
      shift 2
      ;;
    --candidate-config)
      CANDIDATE_CONFIG="$2"
      shift 2
      ;;
    --control-db)
      CONTROL_DB="$2"
      shift 2
      ;;
    --candidate-db)
      CANDIDATE_DB="$2"
      shift 2
      ;;
    --control-service)
      CONTROL_SERVICE="$2"
      shift 2
      ;;
    --candidate-service)
      CANDIDATE_SERVICE="$2"
      shift 2
      ;;
    --window-minutes)
      WINDOW_MINUTES="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --fixture-id)
      FIXTURE_ID="$2"
      shift 2
      ;;
    --fixture-sha256)
      FIXTURE_SHA256="$2"
      shift 2
      ;;
    --output-json)
      OUTPUT_JSON="$2"
      shift 2
      ;;
    --buy-target-pct)
      BUY_TARGET_PCT="$2"
      shift 2
      ;;
    --sell-max-degrade-pct)
      SELL_MAX_DEGRADE_PCT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${CONTROL_CONFIG}" || -z "${CANDIDATE_CONFIG}" ]]; then
  echo "--control-config and --candidate-config are required" >&2
  usage >&2
  exit 1
fi

if ! [[ "${WINDOW_MINUTES}" =~ ^[0-9]+$ ]] || (( WINDOW_MINUTES < 1 )); then
  echo "--window-minutes must be integer >= 1" >&2
  exit 1
fi

case "${MODE}" in
  replay|live)
    ;;
  *)
    echo "--mode must be replay or live (got: ${MODE})" >&2
    exit 1
    ;;
esac

cfg_value() {
  local config_path="$1"
  local section="$2"
  local key="$3"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = $2
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$config_path"
}

resolve_db_path() {
  local config_path="$1"
  local explicit_db="$2"
  if [[ -n "${explicit_db}" ]]; then
    printf '%s\n' "${explicit_db}"
    return 0
  fi
  cfg_value "${config_path}" sqlite path
}

sha256_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
  else
    shasum -a 256 "$file_path" | awk '{print $1}'
  fi
}

for cfg in "${CONTROL_CONFIG}" "${CANDIDATE_CONFIG}"; do
  if [[ ! -f "${cfg}" ]]; then
    echo "config file not found: ${cfg}" >&2
    exit 1
  fi
done

CONTROL_DB="$(resolve_db_path "${CONTROL_CONFIG}" "${CONTROL_DB}")"
CANDIDATE_DB="$(resolve_db_path "${CANDIDATE_CONFIG}" "${CANDIDATE_DB}")"

for db in "${CONTROL_DB}" "${CANDIDATE_DB}"; do
  if [[ ! -f "${db}" ]]; then
    echo "sqlite db not found: ${db}" >&2
    exit 1
  fi
done

CONTROL_CONFIG_SHA="$(sha256_file "${CONTROL_CONFIG}")"
CANDIDATE_CONFIG_SHA="$(sha256_file "${CANDIDATE_CONFIG}")"
GIT_COMMIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"

collect_journal() {
  local service_name="$1"
  local out_file="$2"
  if [[ -z "${service_name}" ]]; then
    : >"${out_file}"
    return 0
  fi
  if journalctl -u "${service_name}" -n 5 --no-pager >/dev/null 2>&1; then
    journalctl -u "${service_name}" --since "${WINDOW_MINUTES} min ago" --no-pager -o cat >"${out_file}" || true
    return 0
  fi
  if sudo -n journalctl -u "${service_name}" -n 5 --no-pager >/dev/null 2>&1; then
    sudo -n journalctl -u "${service_name}" --since "${WINDOW_MINUTES} min ago" --no-pager -o cat >"${out_file}" || true
    return 0
  fi
  : >"${out_file}"
  return 0
}

CONTROL_LOG_FILE="$(mktemp "${REPO_ROOT}/state/control_ingestion_log.XXXXXX")"
CANDIDATE_LOG_FILE="$(mktemp "${REPO_ROOT}/state/candidate_ingestion_log.XXXXXX")"
trap 'rm -f "${CONTROL_LOG_FILE}" "${CANDIDATE_LOG_FILE}"' EXIT

collect_journal "${CONTROL_SERVICE}" "${CONTROL_LOG_FILE}"
collect_journal "${CANDIDATE_SERVICE}" "${CANDIDATE_LOG_FILE}"

python3 - "${CONTROL_DB}" "${CANDIDATE_DB}" "${WINDOW_MINUTES}" "${MODE}" "${FIXTURE_ID}" "${FIXTURE_SHA256}" "${CONTROL_LOG_FILE}" "${CANDIDATE_LOG_FILE}" "${CONTROL_SERVICE}" "${CANDIDATE_SERVICE}" "${OUTPUT_JSON}" "${BUY_TARGET_PCT}" "${SELL_MAX_DEGRADE_PCT}" "${CONTROL_CONFIG}" "${CANDIDATE_CONFIG}" "${CONTROL_CONFIG_SHA}" "${CANDIDATE_CONFIG_SHA}" "${GIT_COMMIT_SHA}" <<'PY'
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

SOL_MINT = "So11111111111111111111111111111111111111112"

TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")
JSON_RE = re.compile(r"(\{.*\})\s*$")

(
    control_db,
    candidate_db,
    window_minutes_raw,
    mode,
    fixture_id,
    fixture_sha256,
    control_log_path,
    candidate_log_path,
    control_service,
    candidate_service,
    output_json,
    buy_target_pct_raw,
    sell_max_degrade_pct_raw,
    control_config,
    candidate_config,
    control_config_sha,
    candidate_config_sha,
    git_commit_sha,
) = sys.argv[1:]

window_minutes = int(window_minutes_raw)
buy_target_pct = float(buy_target_pct_raw)
sell_max_degrade_pct = float(sell_max_degrade_pct_raw)
window_expr = f"-{window_minutes} minutes"


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


def load_db_metrics(db_path: str) -> dict[str, Any]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    def one(sql: str, params: tuple[Any, ...] = ()) -> float:
        row = cur.execute(sql, params).fetchone()
        if row is None:
            return 0.0
        value = row[0]
        if value is None:
            return 0.0
        return float(value)

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
    status_counts = {str(status): int(count) for status, count in status_rows}

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
    risk_counts = {str(event_type): int(count) for event_type, count in risk_rows}

    con.close()
    return {
        "observed_total": int(observed_total),
        "observed_buy": int(observed_buy),
        "observed_sell": int(observed_sell),
        "signals_total": int(signals_total),
        "signals_buy": int(signals_buy),
        "signals_sell": int(signals_sell),
        "signal_status_counts": status_counts,
        "risk_event_counts": risk_counts,
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

            m_ts = TIMESTAMP_RE.search(line)
            ts = None
            if m_ts:
                ts = parse_ts(m_ts.group(1))
            if ts is None:
                ts_value = payload.get("ts_utc")
                if isinstance(ts_value, str):
                    ts = parse_ts(ts_value)
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
    d = b - a
    if d < 0:
        return None
    return d



def find_window_start(rows: list[MetricRow], idx: int, window_seconds: int) -> int:
    target = rows[idx].ts - timedelta(seconds=window_seconds)
    j = idx
    while j > 0 and rows[j - 1].ts >= target:
        j -= 1
    return j



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
        minute = row.ts.replace(second=0, microsecond=0)
        bucket_map[minute] = row
    bucket_rows = [bucket_map[key] for key in sorted(bucket_map.keys())]

    p95_values = [as_number(row.payload.get("ingestion_lag_ms_p95")) for row in bucket_rows]
    p95_values = [value for value in p95_values if value is not None]
    p99_values = [as_number(row.payload.get("ingestion_lag_ms_p99")) for row in bucket_rows]
    p99_values = [value for value in p99_values if value is not None]

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
    for i in range(1, len(rows)):
        d_enq = counter_delta(rows[i - 1].payload, rows[i].payload, "ws_notifications_enqueued")
        d_replaced = counter_delta(
            rows[i - 1].payload,
            rows[i].payload,
            "ws_notifications_replaced_oldest",
        )
        if d_enq is None or d_replaced is None:
            continue
        if d_enq < 500:
            continue
        replaced_ratios.append(d_replaced / max(d_enq, 1.0))

    replaced_ratio_pass_ratio = (
        sum(1 for ratio in replaced_ratios if ratio < 0.05) / len(replaced_ratios)
        if replaced_ratios
        else None
    )

    reconnect_storm_breaches = 0
    reject_rate_breaches = 0
    no_processed_breaches = 0

    for i in range(len(rows)):
        i_5m = find_window_start(rows, i, 300)
        reconnect_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "reconnect_count")
        if reconnect_delta is not None and reconnect_delta >= 6:
            reconnect_storm_breaches += 1

        parse_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "parse_rejected_total")
        decode_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "grpc_decode_errors")
        inbound_delta = counter_delta(rows[i_5m].payload, rows[i].payload, "grpc_message_total")
        if (
            parse_delta is not None
            and decode_delta is not None
            and inbound_delta is not None
            and inbound_delta >= 500
        ):
            reject_rate = (parse_delta + decode_delta) / max(inbound_delta, 1.0)
            if reject_rate > 0.20:
                reject_rate_breaches += 1

        i_120s = find_window_start(rows, i, 120)
        inbound_delta_120 = counter_delta(rows[i_120s].payload, rows[i].payload, "grpc_message_total")
        processed_delta_120 = counter_delta(
            rows[i_120s].payload,
            rows[i].payload,
            "ws_notifications_enqueued",
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
            sqlite_rows[0].payload,
            sqlite_rows[-1].payload,
            "sqlite_write_retry_total",
        )
        sqlite_busy_error_delta = counter_delta(
            sqlite_rows[0].payload,
            sqlite_rows[-1].payload,
            "sqlite_busy_error_total",
        )

    latest_payload = rows[-1].payload
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


control_db_metrics = load_db_metrics(control_db)
candidate_db_metrics = load_db_metrics(candidate_db)
control_telemetry = evaluate_telemetry(control_log_path)
candidate_telemetry = evaluate_telemetry(candidate_log_path)

buy_delta_pct = pct_delta(
    float(control_db_metrics["observed_buy"]),
    float(candidate_db_metrics["observed_buy"]),
)
sell_delta_pct = pct_delta(
    float(control_db_metrics["observed_sell"]),
    float(candidate_db_metrics["observed_sell"]),
)
signal_buy_delta_pct = pct_delta(
    float(control_db_metrics["signals_buy"]),
    float(candidate_db_metrics["signals_buy"]),
)

checks: list[tuple[str, bool, str]] = []
checks.append(
    (
        "buy_capture_delta",
        buy_delta_pct >= buy_target_pct,
        f"candidate_observed_buy_delta_pct={buy_delta_pct:.2f} target>={buy_target_pct:.2f}",
    )
)
checks.append(
    (
        "sell_capture_non_regression",
        sell_delta_pct >= -sell_max_degrade_pct,
        f"candidate_observed_sell_delta_pct={sell_delta_pct:.2f} allowed>=-{sell_max_degrade_pct:.2f}",
    )
)

if mode == "live":
    if candidate_telemetry.get("available"):
        lag_p95_pass_ratio = candidate_telemetry.get("lag_p95_pass_ratio")
        lag_p99_pass_ratio = candidate_telemetry.get("lag_p99_pass_ratio")
        checks.append(
            (
                "lag_p95_gate",
                lag_p95_pass_ratio is not None and lag_p95_pass_ratio >= 0.95,
                f"lag_p95_pass_ratio={lag_p95_pass_ratio}",
            )
        )
        checks.append(
            (
                "lag_p99_gate",
                lag_p99_pass_ratio is not None and lag_p99_pass_ratio >= 0.90,
                f"lag_p99_pass_ratio={lag_p99_pass_ratio}",
            )
        )

        replaced_eval_windows = candidate_telemetry.get("replaced_ratio_eval_windows") or 0
        replaced_pass_ratio = candidate_telemetry.get("replaced_ratio_pass_ratio")
        if replaced_eval_windows > 0:
            checks.append(
                (
                    "replaced_ratio_gate",
                    replaced_pass_ratio is not None and replaced_pass_ratio >= 0.95,
                    f"replaced_ratio_pass_ratio={replaced_pass_ratio}",
                )
            )

        checks.append(
            (
                "reconnect_storm_gate",
                int(candidate_telemetry.get("reconnect_storm_breach_count") or 0) == 0,
                f"reconnect_storm_breach_count={candidate_telemetry.get('reconnect_storm_breach_count')}",
            )
        )
        checks.append(
            (
                "reject_rate_gate",
                int(candidate_telemetry.get("reject_rate_breach_count") or 0) == 0,
                f"reject_rate_breach_count={candidate_telemetry.get('reject_rate_breach_count')}",
            )
        )
        checks.append(
            (
                "no_processed_gate",
                int(candidate_telemetry.get("no_processed_breach_count") or 0) == 0,
                f"no_processed_breach_count={candidate_telemetry.get('no_processed_breach_count')}",
            )
        )

        control_retry = control_telemetry.get("sqlite_write_retry_delta")
        candidate_retry = candidate_telemetry.get("sqlite_write_retry_delta")
        control_busy = control_telemetry.get("sqlite_busy_error_delta")
        candidate_busy = candidate_telemetry.get("sqlite_busy_error_delta")

        if control_retry is not None and candidate_retry is not None:
            if control_retry == 0:
                sqlite_retry_ok = candidate_retry == 0
            else:
                sqlite_retry_ok = candidate_retry <= control_retry * 1.2
            checks.append(
                (
                    "sqlite_retry_amplification",
                    sqlite_retry_ok,
                    f"control={control_retry}, candidate={candidate_retry}",
                )
            )

        if control_busy is not None and candidate_busy is not None:
            if control_busy == 0:
                sqlite_busy_ok = candidate_busy == 0
            else:
                sqlite_busy_ok = candidate_busy <= control_busy * 1.2
            checks.append(
                (
                    "sqlite_busy_amplification",
                    sqlite_busy_ok,
                    f"control={control_busy}, candidate={candidate_busy}",
                )
            )
    else:
        checks.append(("candidate_telemetry_available", False, "candidate telemetry unavailable"))

failed_checks = [name for name, ok, _ in checks if not ok]
pass_all = not failed_checks

report = {
    "generated_at_utc": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "mode": mode,
    "window_minutes": window_minutes,
    "fixture": {
        "id": fixture_id or None,
        "sha256": fixture_sha256 or None,
    },
    "build": {
        "git_commit_sha": git_commit_sha,
        "control_config": control_config,
        "candidate_config": candidate_config,
        "control_config_sha256": control_config_sha,
        "candidate_config_sha256": candidate_config_sha,
    },
    "control": {
        "db_path": control_db,
        "service": control_service or None,
        "db": control_db_metrics,
        "telemetry": control_telemetry,
    },
    "candidate": {
        "db_path": candidate_db,
        "service": candidate_service or None,
        "db": candidate_db_metrics,
        "telemetry": candidate_telemetry,
    },
    "ab_metrics": {
        "observed_buy_delta_pct": buy_delta_pct,
        "observed_sell_delta_pct": sell_delta_pct,
        "signals_buy_delta_pct": signal_buy_delta_pct,
        "control_observed_buy": control_db_metrics["observed_buy"],
        "candidate_observed_buy": candidate_db_metrics["observed_buy"],
        "control_observed_sell": control_db_metrics["observed_sell"],
        "candidate_observed_sell": candidate_db_metrics["observed_sell"],
    },
    "checks": [
        {"name": name, "ok": ok, "detail": detail}
        for name, ok, detail in checks
    ],
    "pass": pass_all,
    "failed_checks": failed_checks,
}

print("=== Ingestion A/B Report ===")
print(f"generated_at_utc: {report['generated_at_utc']}")
print(f"mode: {mode}")
print(f"window_minutes: {window_minutes}")
if fixture_id:
    print(f"fixture_id: {fixture_id}")
if fixture_sha256:
    print(f"fixture_sha256: {fixture_sha256}")
print(f"git_commit_sha: {git_commit_sha}")
print(f"control_config_sha256: {control_config_sha}")
print(f"candidate_config_sha256: {candidate_config_sha}")
print()
print("-- DB capture --")
print(
    f"control observed_buy={control_db_metrics['observed_buy']} observed_sell={control_db_metrics['observed_sell']} signals_buy={control_db_metrics['signals_buy']}"
)
print(
    f"candidate observed_buy={candidate_db_metrics['observed_buy']} observed_sell={candidate_db_metrics['observed_sell']} signals_buy={candidate_db_metrics['signals_buy']}"
)
print(f"delta observed_buy_pct={buy_delta_pct:.2f}")
print(f"delta observed_sell_pct={sell_delta_pct:.2f}")
print(f"delta signals_buy_pct={signal_buy_delta_pct:.2f}")
print()
print("-- Checks --")
for name, ok, detail in checks:
    print(f"[{ 'PASS' if ok else 'FAIL' }] {name}: {detail}")
print()
print(f"overall: {'PASS' if pass_all else 'FAIL'}")

if output_json:
    out_path = Path(output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"json_report: {out_path}")

raise SystemExit(0 if pass_all else 2)
PY
