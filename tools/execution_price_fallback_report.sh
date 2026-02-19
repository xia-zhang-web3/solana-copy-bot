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

echo "=== execution_price_unavailable_fallback_used (${WINDOW_HOURS}h) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH fallback_events AS (
  SELECT
    ts,
    COALESCE(json_extract(details_json, '$.signal_id'), '') AS signal_id,
    COALESCE(json_extract(details_json, '$.order_id'), '') AS order_id,
    COALESCE(json_extract(details_json, '$.token'), '') AS token,
    COALESCE(json_extract(details_json, '$.route'), '') AS route,
    COALESCE(json_extract(details_json, '$.fallback_source'), '') AS fallback_source,
    COALESCE(json_extract(details_json, '$.fallback_avg_price_sol'), 0.0) AS fallback_avg_price_sol,
    COALESCE(json_extract(details_json, '$.manual_reconcile_recommended'), 0) AS manual_reconcile_recommended
  FROM risk_events
  WHERE type = 'execution_price_unavailable_fallback_used'
    AND datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
)
SELECT
  e.ts,
  e.signal_id,
  e.order_id,
  e.token,
  e.route,
  e.fallback_source,
  e.fallback_avg_price_sol,
  e.manual_reconcile_recommended,
  COALESCE(o.status, '') AS order_status,
  COALESCE(o.err_code, '') AS order_err_code,
  COALESCE(o.tx_signature, '') AS tx_signature,
  COALESCE(f.qty, 0.0) AS fill_qty,
  COALESCE(f.avg_price, 0.0) AS fill_avg_price
FROM fallback_events e
LEFT JOIN orders o ON o.order_id = e.order_id
LEFT JOIN fills f ON f.order_id = e.order_id
ORDER BY e.ts DESC;
SQL

echo
echo "=== fallback_source breakdown (${WINDOW_HOURS}h) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT
  COALESCE(json_extract(details_json, '$.fallback_source'), 'unknown') AS fallback_source,
  COUNT(*) AS cnt
FROM risk_events
WHERE type = 'execution_price_unavailable_fallback_used'
  AND datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
GROUP BY fallback_source
ORDER BY cnt DESC;
SQL
