#!/usr/bin/env bash
set -euo pipefail

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-120}"
SERVICE="${SERVICE:-solana-copy-bot}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  echo "window hours must be an integer (got: $WINDOW_HOURS)" >&2
  exit 1
fi

if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  echo "risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

cfg_value() {
  local section="$1"
  local key="$2"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = $2
        gsub(/[ "]/, "", value)
        print value
        exit
      }
    }
  ' "$CONFIG_PATH"
}

DB_PATH="${DB_PATH:-$(cfg_value sqlite path)}"
if [[ -z "$DB_PATH" ]]; then
  echo "failed to read sqlite.path from $CONFIG_PATH" >&2
  exit 1
fi
if [[ ! -f "$DB_PATH" ]]; then
  echo "sqlite db not found: $DB_PATH" >&2
  exit 1
fi

order_column_exists() {
  local column="$1"
  [[ "$(sqlite3 -noheader "$DB_PATH" "SELECT 1 FROM pragma_table_info('orders') WHERE name = '$column' LIMIT 1;")" == "1" ]]
}

order_column_expr_or_zero() {
  local column="$1"
  if order_column_exists "$column"; then
    printf "COALESCE(o.%s, 0)" "$column"
  else
    printf "0"
  fi
}

APPLIED_TIP_EXPR="$(order_column_expr_or_zero applied_tip_lamports)"
ATA_RENT_EXPR="$(order_column_expr_or_zero ata_create_rent_lamports)"
NETWORK_FEE_HINT_EXPR="$(order_column_expr_or_zero network_fee_lamports_hint)"
BASE_FEE_HINT_EXPR="$(order_column_expr_or_zero base_fee_lamports_hint)"
PRIORITY_FEE_HINT_EXPR="$(order_column_expr_or_zero priority_fee_lamports_hint)"

MAX_POSITION_SOL="$(cfg_value risk max_position_sol)"
MAX_TOTAL_EXPOSURE_SOL="$(cfg_value risk max_total_exposure_sol)"
MAX_HOLD_HOURS="$(cfg_value risk max_hold_hours)"
SOFT_CAP_SOL="$(cfg_value risk shadow_soft_exposure_cap_sol)"
HARD_CAP_SOL="$(cfg_value risk shadow_hard_exposure_cap_sol)"
KILLSWITCH_ENABLED="$(cfg_value risk shadow_killswitch_enabled)"

sql_row() {
  sqlite3 -noheader -separator '|' "$DB_PATH" "$1"
}

open_row="$(sql_row "
SELECT
  COUNT(*) AS open_lots,
  COALESCE(SUM(cost_sol), 0.0) AS open_notional_sol,
  COUNT(DISTINCT wallet_id) AS open_wallets,
  COUNT(DISTINCT token) AS open_tokens
FROM shadow_lots;
")"
IFS='|' read -r OPEN_LOTS OPEN_NOTIONAL_SOL OPEN_WALLETS OPEN_TOKENS <<< "$open_row"

closed_24h_row="$(sql_row "
SELECT
  COUNT(*) AS closed_trades_24h,
  COALESCE(SUM(pnl_sol), 0.0) AS pnl_24h,
  COALESCE(SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END), 0) AS wins_24h,
  COALESCE(SUM(CASE WHEN pnl_sol <= 0 THEN 1 ELSE 0 END), 0) AS losses_24h
FROM shadow_closed_trades
WHERE datetime(closed_ts) >= datetime('now', '-24 hours');
")"
IFS='|' read -r CLOSED_24H PNL_24H WINS_24H LOSSES_24H <<< "$closed_24h_row"

closed_window_row="$(sql_row "
SELECT
  COUNT(*) AS closed_trades_window,
  COALESCE(SUM(pnl_sol), 0.0) AS pnl_window
FROM shadow_closed_trades
WHERE datetime(closed_ts) >= datetime('now', '-${WINDOW_HOURS} hours');
")"
IFS='|' read -r CLOSED_WINDOW PNL_WINDOW <<< "$closed_window_row"

signal_window_row="$(sql_row "
SELECT
  COUNT(*) AS signals_total,
  COALESCE(SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END), 0) AS signals_buy,
  COALESCE(SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END), 0) AS signals_sell
FROM copy_signals
WHERE datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours');
")"
IFS='|' read -r SIGNALS_TOTAL SIGNALS_BUY SIGNALS_SELL <<< "$signal_window_row"

opened_window_row="$(sql_row "
SELECT
  COUNT(*) AS opened_lots_window,
  COALESCE(SUM(cost_sol), 0.0) AS opened_notional_window
FROM shadow_lots
WHERE datetime(opened_ts) >= datetime('now', '-${WINDOW_HOURS} hours');
")"
IFS='|' read -r OPENED_WINDOW OPENED_NOTIONAL_WINDOW <<< "$opened_window_row"

oldest_open_hours="$(sql_row "
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN 0
    ELSE (julianday('now') - julianday(MIN(opened_ts))) * 24.0
  END
FROM shadow_lots;
")"

if [[ "${MAX_HOLD_HOURS:-0}" =~ ^[0-9]+$ ]] && (( MAX_HOLD_HOURS > 0 )); then
  stale_row="$(sql_row "
  SELECT
    COUNT(*) AS stale_lots,
    COALESCE(SUM(cost_sol), 0.0) AS stale_notional_sol
  FROM shadow_lots
  WHERE datetime(opened_ts) <= datetime('now', '-${MAX_HOLD_HOURS} hours');
  ")"
  IFS='|' read -r STALE_LOTS STALE_NOTIONAL_SOL <<< "$stale_row"
else
  STALE_LOTS=0
  STALE_NOTIONAL_SOL=0
fi

format_pct() {
  awk -v n="${1:-0}" -v d="${2:-0}" 'BEGIN { if (d > 0) printf "%.2f%%", (n/d)*100; else printf "n/a"; }'
}

WIN_RATE_24H="$(format_pct "$WINS_24H" "$CLOSED_24H")"
USAGE_SOFT="$(format_pct "$OPEN_NOTIONAL_SOL" "${SOFT_CAP_SOL:-0}")"
USAGE_HARD="$(format_pct "$OPEN_NOTIONAL_SOL" "${HARD_CAP_SOL:-0}")"
USAGE_TOTAL="$(format_pct "$OPEN_NOTIONAL_SOL" "${MAX_TOTAL_EXPOSURE_SOL:-0}")"

echo "=== CopyBot Runtime Snapshot ==="
echo "utc_now: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "config: $CONFIG_PATH"
echo "db: $DB_PATH"
echo
echo "=== Exposure ==="
echo "open_lots: $OPEN_LOTS"
echo "open_notional_sol: $OPEN_NOTIONAL_SOL"
echo "open_wallets: $OPEN_WALLETS"
echo "open_tokens: $OPEN_TOKENS"
echo "oldest_open_lot_hours: $(printf "%.2f" "$oldest_open_hours")"
echo
echo "=== Trade Activity ==="
echo "signals_${WINDOW_HOURS}h_total: $SIGNALS_TOTAL"
echo "signals_${WINDOW_HOURS}h_buy: $SIGNALS_BUY"
echo "signals_${WINDOW_HOURS}h_sell: $SIGNALS_SELL"
echo "opened_lots_${WINDOW_HOURS}h: $OPENED_WINDOW"
echo "opened_notional_${WINDOW_HOURS}h_sol: $OPENED_NOTIONAL_WINDOW"
echo "closed_trades_window: $CLOSED_WINDOW"
echo "realized_pnl_window_sol: $PNL_WINDOW"
echo "closed_trades_24h: $CLOSED_24H"
echo "realized_pnl_24h_sol: $PNL_24H"
echo "winrate_24h: $WIN_RATE_24H"
echo
echo "=== Risk Limits ==="
echo "killswitch_enabled: ${KILLSWITCH_ENABLED:-unknown}"
echo "max_position_sol: ${MAX_POSITION_SOL:-n/a}"
echo "max_total_exposure_sol: ${MAX_TOTAL_EXPOSURE_SOL:-n/a} (usage $USAGE_TOTAL)"
echo "shadow_soft_exposure_cap_sol: ${SOFT_CAP_SOL:-n/a} (usage $USAGE_SOFT)"
echo "shadow_hard_exposure_cap_sol: ${HARD_CAP_SOL:-n/a} (usage $USAGE_HARD)"
echo "max_hold_hours: ${MAX_HOLD_HOURS:-n/a}"
echo "stale_open_lots_now: $STALE_LOTS"
echo "stale_open_notional_sol_now: $STALE_NOTIONAL_SOL"
echo
echo "=== Signals Status (${WINDOW_HOURS}h) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT status, COUNT(*) AS cnt
FROM copy_signals
WHERE datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
GROUP BY status
ORDER BY cnt DESC;
SQL

echo
echo "=== Execution Fee Breakdown by Route (${WINDOW_HOURS}h) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH confirmed_orders AS (
  SELECT
    o.order_id,
    o.route,
    ${APPLIED_TIP_EXPR} AS applied_tip_lamports,
    ${ATA_RENT_EXPR} AS ata_create_rent_lamports,
    ${NETWORK_FEE_HINT_EXPR} AS network_fee_lamports_hint,
    ${BASE_FEE_HINT_EXPR} AS base_fee_lamports_hint,
    ${PRIORITY_FEE_HINT_EXPR} AS priority_fee_lamports_hint
  FROM orders o
  WHERE o.status = 'execution_confirmed'
    AND o.confirm_ts IS NOT NULL
    AND datetime(o.confirm_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
)
SELECT
  route,
  COUNT(*) AS confirmed_orders,
  COALESCE(SUM(COALESCE(f.fee, 0.0)), 0.0) AS fee_sol_sum,
  SUM(applied_tip_lamports) AS tip_lamports_sum,
  SUM(ata_create_rent_lamports) AS ata_rent_lamports_sum,
  SUM(network_fee_lamports_hint) AS network_fee_hint_lamports_sum,
  SUM(base_fee_lamports_hint) AS base_fee_hint_lamports_sum,
  SUM(priority_fee_lamports_hint) AS priority_fee_hint_lamports_sum
FROM confirmed_orders o
LEFT JOIN fills f ON f.order_id = o.order_id
GROUP BY route
ORDER BY confirmed_orders DESC, route ASC;
SQL

echo
echo "=== Recent Risk Events (${RISK_EVENTS_MINUTES}m) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT ts, type, severity, COALESCE(details_json, '') AS details_json
FROM risk_events
WHERE datetime(ts) >= datetime('now', '-${RISK_EVENTS_MINUTES} minutes')
ORDER BY ts DESC;
SQL

collect_journal() {
  if journalctl -u "$SERVICE" -n 5 --no-pager >/dev/null 2>&1; then
    journalctl -u "$SERVICE" -n 250 --no-pager -o cat
    return 0
  fi
  if sudo -n journalctl -u "$SERVICE" -n 5 --no-pager >/dev/null 2>&1; then
    sudo -n journalctl -u "$SERVICE" -n 250 --no-pager -o cat
    return 0
  fi
  return 1
}

echo
echo "=== Ingestion Runtime (latest samples) ==="
if journal_text="$(collect_journal)"; then
  python3 - <<'PY' "$journal_text"
import json
import re
import sys

text = sys.argv[1]
rows = []
sqlite_rows = []
for line in text.splitlines():
    m = re.search(r'(\{.*\})\s*$', line)
    if not m:
        continue
    try:
        payload = json.loads(m.group(1))
    except json.JSONDecodeError:
        continue
    if "ingestion pipeline metrics" in line:
        rows.append(payload)
    elif "sqlite contention counters" in line:
        sqlite_rows.append(payload)

if not rows:
    print("no ingestion metric samples found")
    raise SystemExit(0)

last = rows[-1]
keys = [
    "ingestion_lag_ms_p95",
    "ingestion_lag_ms_p99",
    "ws_to_fetch_queue_depth",
    "fetch_to_output_queue_depth",
    "fetch_concurrency_inflight",
    "ws_notifications_enqueued",
    "ws_notifications_replaced_oldest",
    "reconnect_count",
    "stream_gap_detected",
    "parse_rejected_total",
    "grpc_message_total",
    "grpc_decode_errors",
    "rpc_429",
    "rpc_5xx",
]
for key in keys:
    print(f"{key}: {last.get(key)}")

for map_key in ("parse_rejected_by_reason", "parse_fallback_by_reason"):
    breakdown = last.get(map_key)
    if isinstance(breakdown, dict) and breakdown:
        ordered = {key: breakdown[key] for key in sorted(breakdown)}
        print(f"{map_key}: {json.dumps(ordered, sort_keys=True)}")
    else:
        print(f"{map_key}: {{}}")

if len(rows) >= 2:
    prev = rows[-2]
    delta_enqueued = (last.get("ws_notifications_enqueued") or 0) - (prev.get("ws_notifications_enqueued") or 0)
    delta_replaced = (last.get("ws_notifications_replaced_oldest") or 0) - (prev.get("ws_notifications_replaced_oldest") or 0)
    if delta_enqueued > 0:
        print(f"replaced_ratio_last_interval: {delta_replaced / delta_enqueued:.4f}")
    else:
        print("replaced_ratio_last_interval: n/a")

if sqlite_rows:
    sqlite_last = sqlite_rows[-1]
    print(f"sqlite_write_retry_total: {sqlite_last.get('sqlite_write_retry_total')}")
    print(f"sqlite_busy_error_total: {sqlite_last.get('sqlite_busy_error_total')}")
PY
else
  echo "journal access unavailable for service '$SERVICE' (try running with sudo)"
fi
