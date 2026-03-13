#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

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

file_size_bytes() {
  local path="$1"
  if [[ -f "$path" ]]; then
    wc -c <"$path" | tr -d '[:space:]'
  else
    printf "0"
  fi
}

DB_WAL_PATH="${DB_PATH}-wal"
DB_SHM_PATH="${DB_PATH}-shm"
DB_FILE_BYTES="$(file_size_bytes "$DB_PATH")"
DB_WAL_BYTES="$(file_size_bytes "$DB_WAL_PATH")"
DB_SHM_BYTES="$(file_size_bytes "$DB_SHM_PATH")"
DB_MOUNT_SOURCE="n/a"
DB_MOUNT_TOTAL_KB="n/a"
DB_MOUNT_USED_KB="n/a"
DB_MOUNT_AVAIL_KB="n/a"
DB_MOUNT_USE_PCT="n/a"
DB_MOUNT_POINT="n/a"
mount_row="$(df -P "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $1 "|" $2 "|" $3 "|" $4 "|" $5 "|" $6}')"
if [[ -n "$mount_row" ]]; then
  IFS='|' read -r DB_MOUNT_SOURCE DB_MOUNT_TOTAL_KB DB_MOUNT_USED_KB DB_MOUNT_AVAIL_KB DB_MOUNT_USE_PCT DB_MOUNT_POINT <<<"$mount_row"
fi

table_exists() {
  local table="$1"
  [[ "$(sqlite3 -noheader "$DB_PATH" "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '$table' LIMIT 1;")" == "1" ]]
}

order_column_exists() {
  local column="$1"
  [[ "$(sqlite3 -noheader "$DB_PATH" "PRAGMA table_info('orders');" | awk -F'|' -v column="$column" '$2 == column { print 1; exit }')" == "1" ]]
}

shadow_lot_column_exists() {
  local column="$1"
  [[ "$(sqlite3 -noheader "$DB_PATH" "PRAGMA table_info('shadow_lots');" | awk -F'|' -v column="$column" '$2 == column { print 1; exit }')" == "1" ]]
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

SHADOW_LOT_RISK_CONTEXT_EXPR="'market'"
if shadow_lot_column_exists risk_context; then
  SHADOW_LOT_RISK_CONTEXT_EXPR="COALESCE(risk_context, 'market')"
fi

SHADOW_LOT_OPEN_FILTER_EXPR="1"
if shadow_lot_column_exists qty; then
  SHADOW_LOT_OPEN_FILTER_EXPR="qty > 0.000000000001"
fi

MAX_POSITION_SOL="$(cfg_value risk max_position_sol)"
MAX_TOTAL_EXPOSURE_SOL="$(cfg_value risk max_total_exposure_sol)"
MAX_HOLD_HOURS="$(cfg_value risk max_hold_hours)"
STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED="$(cfg_value risk shadow_stale_close_recovery_zero_price_enabled)"
if [[ -z "${STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED:-}" ]]; then
  STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED="false"
fi
SOFT_CAP_SOL="$(cfg_value risk shadow_soft_exposure_cap_sol)"
HARD_CAP_SOL="$(cfg_value risk shadow_hard_exposure_cap_sol)"
KILLSWITCH_ENABLED="$(cfg_value risk shadow_killswitch_enabled)"
PRETRADE_MIN_SOL_RESERVE="$(cfg_or_env_string execution pretrade_min_sol_reserve SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE "")"
PRETRADE_MAX_FEE_OVERHEAD_BPS="$(cfg_or_env_string execution pretrade_max_fee_overhead_bps SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_FEE_OVERHEAD_BPS "")"
PRETRADE_MIN_SOL_RESERVE_LAMPORTS="n/a"
PRETRADE_FEE_RESERVE_GUARD_ENABLED="unknown"
PRETRADE_FEE_OVERHEAD_GUARD_ENABLED="unknown"

pretrade_min_sol_reserve_trimmed="$(trim_string "${PRETRADE_MIN_SOL_RESERVE:-}")"
if [[ -n "$pretrade_min_sol_reserve_trimmed" ]]; then
  if pretrade_min_sol_reserve_lamports_parsed="$(sol_to_lamports_ceil_strict "$pretrade_min_sol_reserve_trimmed" 2>/dev/null)"; then
    PRETRADE_MIN_SOL_RESERVE_LAMPORTS="$pretrade_min_sol_reserve_lamports_parsed"
    if [[ "$pretrade_min_sol_reserve_lamports_parsed" == "0" ]]; then
      PRETRADE_FEE_RESERVE_GUARD_ENABLED="false"
    else
      PRETRADE_FEE_RESERVE_GUARD_ENABLED="true"
    fi
  fi
fi

pretrade_max_fee_overhead_bps_trimmed="$(trim_string "${PRETRADE_MAX_FEE_OVERHEAD_BPS:-}")"
if [[ -n "$pretrade_max_fee_overhead_bps_trimmed" ]]; then
  if pretrade_max_fee_overhead_bps_parsed="$(parse_u64_token_strict "$pretrade_max_fee_overhead_bps_trimmed" 2>/dev/null)"; then
    if [[ "$pretrade_max_fee_overhead_bps_parsed" == "0" ]]; then
      PRETRADE_FEE_OVERHEAD_GUARD_ENABLED="false"
    else
      PRETRADE_FEE_OVERHEAD_GUARD_ENABLED="true"
    fi
  fi
fi

sql_row() {
  sqlite3 -noheader -separator '|' "$DB_PATH" "$1"
}

DISCOVERY_CURSOR_TS="n/a"
DISCOVERY_HEAD_TS="n/a"
DISCOVERY_CURSOR_HEAD_GAP_SECONDS="n/a"
if table_exists "discovery_runtime_state"; then
  DISCOVERY_CURSOR_TS="$(sql_row "
  SELECT COALESCE(
    (SELECT cursor_ts FROM discovery_runtime_state WHERE id = 1),
    'n/a'
  );
  ")"
fi
if table_exists "observed_swaps"; then
  DISCOVERY_HEAD_TS="$(sql_row "
  SELECT COALESCE(MAX(ts), 'n/a')
  FROM observed_swaps;
  ")"
fi
if [[ "$DISCOVERY_CURSOR_TS" != "n/a" && "$DISCOVERY_HEAD_TS" != "n/a" ]]; then
  DISCOVERY_CURSOR_HEAD_GAP_SECONDS="$(sql_row "
  SELECT CAST(
    MAX(
      0,
      ROUND((julianday('$DISCOVERY_HEAD_TS') - julianday('$DISCOVERY_CURSOR_TS')) * 86400.0)
    ) AS INTEGER
  );
  ")"
fi

open_row="$(sql_row "
SELECT
  COUNT(*) AS open_lots,
  COALESCE(SUM(cost_sol), 0.0) AS open_accounting_notional_sol,
  COALESCE(SUM(CASE WHEN ${SHADOW_LOT_RISK_CONTEXT_EXPR} = 'market' THEN cost_sol ELSE 0.0 END), 0.0) AS open_risk_notional_sol,
  COUNT(DISTINCT wallet_id) AS open_wallets,
  COUNT(DISTINCT token) AS open_tokens
FROM shadow_lots
WHERE ${SHADOW_LOT_OPEN_FILTER_EXPR};
")"
IFS='|' read -r OPEN_LOTS OPEN_ACCOUNTING_NOTIONAL_SOL OPEN_RISK_NOTIONAL_SOL OPEN_WALLETS OPEN_TOKENS <<< "$open_row"

risk_context_breakdown_row="$(sql_row "
SELECT
  COALESCE(SUM(CASE WHEN ${SHADOW_LOT_RISK_CONTEXT_EXPR} = 'market' THEN 1 ELSE 0 END), 0) AS market_lots,
  COALESCE(SUM(CASE WHEN ${SHADOW_LOT_RISK_CONTEXT_EXPR} = 'market' THEN cost_sol ELSE 0.0 END), 0.0) AS market_notional_sol,
  COALESCE(SUM(CASE WHEN ${SHADOW_LOT_RISK_CONTEXT_EXPR} = 'quarantined_legacy' THEN 1 ELSE 0 END), 0) AS quarantined_legacy_lots,
  COALESCE(SUM(CASE WHEN ${SHADOW_LOT_RISK_CONTEXT_EXPR} = 'quarantined_legacy' THEN cost_sol ELSE 0.0 END), 0.0) AS quarantined_legacy_notional_sol
FROM shadow_lots
WHERE ${SHADOW_LOT_OPEN_FILTER_EXPR};
")"
IFS='|' read -r OPEN_MARKET_LOTS OPEN_MARKET_NOTIONAL_SOL OPEN_QUARANTINED_LEGACY_LOTS OPEN_QUARANTINED_LEGACY_NOTIONAL_SOL <<< "$risk_context_breakdown_row"

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
USAGE_SOFT="$(format_pct "$OPEN_RISK_NOTIONAL_SOL" "${SOFT_CAP_SOL:-0}")"
USAGE_HARD="$(format_pct "$OPEN_RISK_NOTIONAL_SOL" "${HARD_CAP_SOL:-0}")"
USAGE_TOTAL="$(format_pct "$OPEN_ACCOUNTING_NOTIONAL_SOL" "${MAX_TOTAL_EXPOSURE_SOL:-0}")"

SHADOW_RISK_PAUSE_STATE_OUTPUT="$(
  python3 - <<'PY' "$DB_PATH"
import datetime
import json
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

def latest(event_type: str):
    return conn.execute(
        """
        SELECT rowid, ts, COALESCE(details_json, '') AS details_json
        FROM risk_events
        WHERE type = ?
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (event_type,),
    ).fetchone()

def parse_ts(value: str):
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)

pause = latest("shadow_risk_pause")
cleared = latest("shadow_risk_pause_cleared")
state = "none"
until = "n/a"
reason = "n/a"
pause_ts = "n/a"
clear_ts = "n/a"

if cleared:
    clear_ts = cleared["ts"] or "n/a"

if pause:
    pause_ts = pause["ts"] or "n/a"
    details = {}
    raw_details = pause["details_json"] or ""
    if raw_details:
        try:
            details = json.loads(raw_details)
        except json.JSONDecodeError:
            details = {}
    pause_type = details.get("pause_type")
    detail = details.get("detail")
    until = details.get("until") or "n/a"
    if pause_type and detail:
        reason = f"{pause_type}: {detail}"
    elif detail:
        reason = str(detail)
    elif raw_details:
        reason = raw_details

    if cleared and cleared["rowid"] > pause["rowid"]:
        cleared_details = {}
        cleared_raw_details = cleared["details_json"] or ""
        if cleared_raw_details:
            try:
                cleared_details = json.loads(cleared_raw_details)
            except json.JSONDecodeError:
                cleared_details = {}
        reason = cleared_details.get("previous_reason") or reason
        state = "cleared"
    else:
        until_ts = parse_ts(until)
        if until_ts is not None and until_ts > datetime.datetime.now(datetime.timezone.utc):
            state = "active"
        else:
            state = "expired"

print(f"shadow_risk_pause_state: {state}")
print(f"shadow_risk_pause_until: {until}")
print(f"shadow_risk_pause_reason: {reason}")
print(f"shadow_risk_pause_last_event_ts: {pause_ts}")
print(f"shadow_risk_pause_last_clear_ts: {clear_ts}")
PY
)"

echo "=== CopyBot Runtime Snapshot ==="
echo "utc_now: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "config: $CONFIG_PATH"
echo "db: $DB_PATH"
echo
echo "=== Storage Layout ==="
echo "db_mount_source: $DB_MOUNT_SOURCE"
echo "db_mount_total_kb: $DB_MOUNT_TOTAL_KB"
echo "db_mount_used_kb: $DB_MOUNT_USED_KB"
echo "db_mount_avail_kb: $DB_MOUNT_AVAIL_KB"
echo "db_mount_use_pct: $DB_MOUNT_USE_PCT"
echo "db_mount_point: $DB_MOUNT_POINT"
echo "db_file_bytes: $DB_FILE_BYTES"
echo "db_wal_path: $DB_WAL_PATH"
echo "db_wal_bytes: $DB_WAL_BYTES"
echo "db_shm_path: $DB_SHM_PATH"
echo "db_shm_bytes: $DB_SHM_BYTES"
echo
echo "=== Exposure ==="
echo "open_lots: $OPEN_LOTS"
echo "open_notional_sol: $OPEN_ACCOUNTING_NOTIONAL_SOL"
echo "open_accounting_notional_sol: $OPEN_ACCOUNTING_NOTIONAL_SOL"
echo "open_risk_notional_sol: $OPEN_RISK_NOTIONAL_SOL"
echo "open_wallets: $OPEN_WALLETS"
echo "open_tokens: $OPEN_TOKENS"
echo "open_market_lots: $OPEN_MARKET_LOTS"
echo "open_market_notional_sol: $OPEN_MARKET_NOTIONAL_SOL"
echo "open_quarantined_legacy_lots: $OPEN_QUARANTINED_LEGACY_LOTS"
echo "open_quarantined_legacy_notional_sol: $OPEN_QUARANTINED_LEGACY_NOTIONAL_SOL"
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
echo "shadow_stale_close_recovery_zero_price_enabled: ${STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED:-false}"
echo "stale_open_lots_now: $STALE_LOTS"
echo "stale_open_notional_sol_now: $STALE_NOTIONAL_SOL"
echo "execution_pretrade_min_sol_reserve: ${PRETRADE_MIN_SOL_RESERVE:-n/a}"
echo "execution_pretrade_min_sol_reserve_lamports: ${PRETRADE_MIN_SOL_RESERVE_LAMPORTS:-n/a}"
echo "execution_pretrade_fee_reserve_guard_enabled: ${PRETRADE_FEE_RESERVE_GUARD_ENABLED:-unknown}"
echo "execution_pretrade_max_fee_overhead_bps: ${PRETRADE_MAX_FEE_OVERHEAD_BPS:-n/a}"
echo "execution_pretrade_fee_overhead_guard_enabled: ${PRETRADE_FEE_OVERHEAD_GUARD_ENABLED:-unknown}"
echo
echo "=== Shadow Risk State ==="
while IFS= read -r line; do
  echo "$line"
done <<< "$SHADOW_RISK_PAUSE_STATE_OUTPUT"
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
execution_rows = []
discovery_rows = []
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
    elif "execution batch processed" in line:
        execution_rows.append(payload)
    elif "discovery cycle completed" in line:
        discovery_rows.append(payload)

def emit_execution_sample(rows):
    if rows:
        print("execution_batch_sample_available: true")
        execution_last = rows[-1]
        keys = [
            "attempted",
            "confirmed",
            "dropped",
            "failed",
            "skipped",
        ]
        for key in keys:
            print(f"{key}: {execution_last.get(key)}")
        map_keys = [
            "submit_attempted_by_route",
            "submit_retry_scheduled_by_route",
            "submit_failed_by_route",
            "submit_dynamic_cu_policy_enabled_by_route",
            "submit_dynamic_cu_hint_used_by_route",
            "submit_dynamic_cu_hint_api_by_route",
            "submit_dynamic_cu_hint_rpc_by_route",
            "submit_dynamic_cu_price_applied_by_route",
            "submit_dynamic_cu_static_fallback_by_route",
            "submit_dynamic_tip_policy_enabled_by_route",
            "submit_dynamic_tip_applied_by_route",
            "submit_dynamic_tip_static_floor_by_route",
        ]
        for map_key in map_keys:
            value = execution_last.get(map_key)
            if isinstance(value, dict) and value:
                ordered = {key: value[key] for key in sorted(value)}
                print(f"{map_key}: {json.dumps(ordered, sort_keys=True)}")
            else:
                print(f"{map_key}: {{}}")
    else:
        print("execution_batch_sample_available: false")

def emit_discovery_sample(rows):
    if not rows:
        print("discovery_cycle_sample_available: false")
        print("discovery_cycle_duration_ms_last: n/a")
        print("discovery_published_last: n/a")
        print("eligible_wallets_last: n/a")
        print("active_follow_wallets_last: n/a")
        print("swaps_query_rows_last: n/a")
        print("swaps_query_rows_last_page_last: n/a")
        print("swaps_delta_fetched_last: n/a")
        print("swaps_warm_loaded_last: n/a")
        print("swaps_evicted_due_cap_last: n/a")
        print("swaps_fetch_pages_last: n/a")
        print("swaps_fetch_page_limit_last: n/a")
        print("swaps_fetch_time_budget_ms_last: n/a")
        print("swaps_fetch_limit_reached_last: n/a")
        print("swaps_fetch_page_budget_exhausted_last: n/a")
        print("swaps_fetch_time_budget_exhausted_last: n/a")
        print("discovery_cycle_samples_in_journal: 0")
        print("swaps_fetch_limit_reached_ratio: n/a")
        print("swaps_fetch_page_budget_exhausted_ratio: n/a")
        print("swaps_fetch_time_budget_exhausted_ratio: n/a")
        return

    last = rows[-1]
    published_rows = [
        row
        for row in rows
        if row.get("discovery_published") is True or "discovery_published" not in row
    ]
    last_published = published_rows[-1] if published_rows else None
    print("discovery_cycle_sample_available: true")
    fetch_keys = [
        "discovery_cycle_duration_ms",
        "swaps_query_rows",
        "swaps_query_rows_last_page",
        "swaps_delta_fetched",
        "swaps_warm_loaded",
        "swaps_evicted_due_cap",
        "swaps_fetch_pages",
        "swaps_fetch_page_limit",
        "swaps_fetch_time_budget_ms",
        "swaps_fetch_limit_reached",
        "swaps_fetch_page_budget_exhausted",
        "swaps_fetch_time_budget_exhausted",
    ]
    for key in fetch_keys:
        value = last.get(key)
        if isinstance(value, bool):
            value = str(value).lower()
        print(f"{key}_last: {value}")
    published_value = last.get("discovery_published")
    if isinstance(published_value, bool):
        published_value = str(published_value).lower()
    elif published_value is None:
        published_value = "n/a"
    print(f"discovery_published_last: {published_value}")
    for key in ["eligible_wallets", "active_follow_wallets"]:
        value = last_published.get(key) if last_published is not None else "n/a"
        if isinstance(value, bool):
            value = str(value).lower()
        print(f"{key}_last: {value}")

    samples = len(rows)
    print(f"discovery_cycle_samples_in_journal: {samples}")
    bool_keys = [
        "swaps_fetch_limit_reached",
        "swaps_fetch_page_budget_exhausted",
        "swaps_fetch_time_budget_exhausted",
    ]
    for key in bool_keys:
        samples_with_key = sum(1 for row in rows if isinstance(row.get(key), bool))
        if samples_with_key == 0:
            print(f"{key}_ratio: n/a")
            continue
        seen = sum(1 for row in rows if row.get(key) is True)
        print(f"{key}_ratio: {seen / samples_with_key:.4f}")

if not rows:
    print("no ingestion metric samples found")
    emit_discovery_sample(discovery_rows)
    emit_execution_sample(execution_rows)
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

emit_discovery_sample(discovery_rows)
emit_execution_sample(execution_rows)
PY
else
  echo "journal access unavailable for service '$SERVICE' (try running with sudo)"
fi

echo
echo "=== Discovery Cursor State ==="
echo "discovery_cursor_ts: $DISCOVERY_CURSOR_TS"
echo "observed_swaps_head_ts: $DISCOVERY_HEAD_TS"
echo "discovery_cursor_head_gap_seconds: $DISCOVERY_CURSOR_HEAD_GAP_SECONDS"
