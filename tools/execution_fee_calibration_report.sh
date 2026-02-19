#!/usr/bin/env bash
set -euo pipefail

WINDOW_HOURS="${1:-24}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  echo "window hours must be an integer (got: $WINDOW_HOURS)" >&2
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

echo "=== execution fee calibration (${WINDOW_HOURS}h) ==="
echo "config: $CONFIG_PATH"
echo "db: $DB_PATH"
echo

echo "=== confirmed fee breakdown by route ==="
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
  COALESCE(AVG(COALESCE(f.fee, 0.0)), 0.0) AS fee_sol_avg,
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
echo "=== network fee source fallback events by route ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT
  COALESCE(json_extract(details_json, '$.route'), '') AS route,
  type,
  COUNT(*) AS cnt
FROM risk_events
WHERE type IN (
  'execution_network_fee_unavailable_submit_hint_used',
  'execution_network_fee_unavailable_fallback_used',
  'execution_network_fee_hint_mismatch'
)
  AND datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
GROUP BY route, type
ORDER BY cnt DESC, route ASC, type ASC;
SQL

echo
echo "=== strict policy rejects (submit_adapter_policy_echo_missing) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT
  COALESCE(json_extract(details_json, '$.route'), '') AS route,
  COUNT(*) AS cnt
FROM risk_events
WHERE type = 'execution_submit_failed'
  AND json_extract(details_json, '$.error_code') = 'submit_adapter_policy_echo_missing'
  AND datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
GROUP BY route
ORDER BY cnt DESC, route ASC;
SQL
