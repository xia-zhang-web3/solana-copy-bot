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

cfg_list_csv() {
  local section="$1"
  local key="$2"
  awk -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
      if ($0 != section) {
        collecting = 0
        value = ""
      }
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      if (!collecting) {
        left = line
        sub(/=.*/, "", left)
        gsub(/[[:space:]]/, "", left)
        if (left != key) {
          next
        }
        collecting = 1
        value = line
      } else {
        value = value " " line
      }
      if (index(value, "]") > 0) {
        start = index(value, "[")
        if (start == 0) {
          print ""
          exit
        }
        body = substr(value, start + 1)
        end = index(body, "]")
        if (end == 0) {
          next
        }
        body = substr(body, 1, end - 1)
        gsub(/"/, "", body)
        gsub(/'\''/, "", body)
        gsub(/[[:space:]]/, "", body)
        gsub(/,+/, ",", body)
        sub(/^,/, "", body)
        sub(/,$/, "", body)
        print body
        exit
      }
    }
  ' "$CONFIG_PATH"
}

normalize_route_token() {
  local route="$1"
  route="${route#"${route%%[![:space:]]*}"}"
  route="${route%"${route##*[![:space:]]}"}"
  printf '%s' "$route" | tr '[:upper:]' '[:lower:]'
}

csv_contains_route() {
  local csv="$1"
  local needle="$2"
  local -a values=()
  local raw value
  if [[ -z "$csv" || -z "$needle" ]]; then
    return 1
  fi
  IFS=',' read -r -a values <<< "$csv"
  for raw in "${values[@]}"; do
    value="$(normalize_route_token "$raw")"
    if [[ "$value" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

build_allowed_routes_values() {
  local csv="$1"
  local values=""
  local -a raw_routes=()
  local seen_routes=""
  local route raw_route normalized escaped_route
  if [[ -z "${csv//[[:space:]]/}" ]]; then
    printf "%s" "$values"
    return
  fi
  IFS=',' read -r -a raw_routes <<< "$csv"
  for raw_route in "${raw_routes[@]}"; do
    normalized="$(normalize_route_token "$raw_route")"
    if [[ -z "$normalized" ]]; then
      continue
    fi
    if printf '%s\n' "$seen_routes" | grep -Fqx -- "$normalized"; then
      continue
    fi
    seen_routes+="${normalized}"$'\n'
    escaped_route="${normalized//\'/\'\'}"
    if [[ -n "$values" ]]; then
      values+=", "
    fi
    values+="('${escaped_route}')"
  done
  printf "%s" "$values"
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

order_column_expr_or_null() {
  local column="$1"
  if order_column_exists "$column"; then
    printf "o.%s" "$column"
  else
    printf "NULL"
  fi
}

APPLIED_TIP_EXPR="$(order_column_expr_or_zero applied_tip_lamports)"
ATA_RENT_EXPR="$(order_column_expr_or_zero ata_create_rent_lamports)"
NETWORK_FEE_HINT_EXPR="$(order_column_expr_or_zero network_fee_lamports_hint)"
BASE_FEE_HINT_EXPR="$(order_column_expr_or_zero base_fee_lamports_hint)"
PRIORITY_FEE_HINT_EXPR="$(order_column_expr_or_zero priority_fee_lamports_hint)"
APPLIED_TIP_RAW_EXPR="$(order_column_expr_or_null applied_tip_lamports)"
ATA_RENT_RAW_EXPR="$(order_column_expr_or_null ata_create_rent_lamports)"
NETWORK_FEE_HINT_RAW_EXPR="$(order_column_expr_or_null network_fee_lamports_hint)"
BASE_FEE_HINT_RAW_EXPR="$(order_column_expr_or_null base_fee_lamports_hint)"
PRIORITY_FEE_HINT_RAW_EXPR="$(order_column_expr_or_null priority_fee_lamports_hint)"

SUBMIT_ALLOWED_ROUTES_CSV="$(cfg_list_csv execution submit_allowed_routes)"
ALLOWED_ROUTES_VALUES="$(build_allowed_routes_values "$SUBMIT_ALLOWED_ROUTES_CSV")"
DEFAULT_ROUTE="$(normalize_route_token "$(cfg_value execution default_route)")"
if [[ -z "$DEFAULT_ROUTE" ]]; then
  DEFAULT_ROUTE="paper"
fi

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
echo "=== fee hint coverage by route (confirmed orders) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH confirmed_orders AS (
  SELECT
    o.order_id,
    o.route,
    ${APPLIED_TIP_RAW_EXPR} AS applied_tip_lamports_raw,
    ${ATA_RENT_RAW_EXPR} AS ata_create_rent_lamports_raw,
    ${NETWORK_FEE_HINT_RAW_EXPR} AS network_fee_lamports_hint_raw,
    ${BASE_FEE_HINT_RAW_EXPR} AS base_fee_lamports_hint_raw,
    ${PRIORITY_FEE_HINT_RAW_EXPR} AS priority_fee_lamports_hint_raw
  FROM orders o
  WHERE o.status = 'execution_confirmed'
    AND o.confirm_ts IS NOT NULL
    AND datetime(o.confirm_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
)
SELECT
  route,
  COUNT(*) AS confirmed_orders,
  SUM(CASE WHEN network_fee_lamports_hint_raw IS NOT NULL THEN 1 ELSE 0 END) AS network_fee_hint_rows,
  SUM(CASE WHEN base_fee_lamports_hint_raw IS NOT NULL THEN 1 ELSE 0 END) AS base_fee_hint_rows,
  SUM(CASE WHEN priority_fee_lamports_hint_raw IS NOT NULL THEN 1 ELSE 0 END) AS priority_fee_hint_rows,
  SUM(CASE WHEN applied_tip_lamports_raw IS NOT NULL THEN 1 ELSE 0 END) AS applied_tip_rows,
  SUM(CASE WHEN ata_create_rent_lamports_raw IS NOT NULL THEN 1 ELSE 0 END) AS ata_rent_rows
FROM confirmed_orders
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
WITH strict_reject_events AS (
  SELECT
    rowid AS source_row_id,
    'event' AS source_kind,
    COALESCE(json_extract(details_json, '$.order_id'), '') AS order_id,
    COALESCE(json_extract(details_json, '$.route'), '') AS route
  FROM risk_events
  WHERE type = 'execution_submit_failed'
    AND json_extract(details_json, '$.error_code') = 'submit_adapter_policy_echo_missing'
    AND datetime(ts) >= datetime('now', '-${WINDOW_HOURS} hours')
),
legacy_strict_reject_orders AS (
  SELECT
    rowid AS source_row_id,
    'legacy' AS source_kind,
    COALESCE(order_id, '') AS order_id,
    COALESCE(route, '') AS route
  FROM orders
  WHERE status = 'execution_failed'
    AND err_code = 'submit_terminal_rejected'
    AND simulation_error LIKE '%submit_adapter_policy_echo_missing%'
    AND datetime(submit_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
),
strict_rejects_raw AS (
  SELECT
    source_row_id,
    source_kind,
    order_id,
    route,
    CASE
      WHEN order_id <> '' THEN order_id
      ELSE source_kind || ':' || CAST(source_row_id AS TEXT)
    END AS dedupe_key
  FROM strict_reject_events
  UNION ALL
  SELECT
    source_row_id,
    source_kind,
    order_id,
    route,
    CASE
      WHEN order_id <> '' THEN order_id
      ELSE source_kind || ':' || CAST(source_row_id AS TEXT)
    END AS dedupe_key
  FROM legacy_strict_reject_orders
),
strict_rejects AS (
  SELECT
    dedupe_key,
    COALESCE(
      MAX(CASE WHEN source_kind = 'event' AND route <> '' THEN route END),
      MAX(CASE WHEN source_kind = 'legacy' AND route <> '' THEN route END),
      ''
    ) AS route
  FROM strict_rejects_raw
  GROUP BY dedupe_key
)
SELECT
  route,
  COUNT(*) AS cnt
FROM strict_rejects
GROUP BY route
ORDER BY cnt DESC, route ASC;
SQL

echo
echo "=== route outcome KPI (${WINDOW_HOURS}h submit window) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH window_orders AS (
  SELECT
    COALESCE(route, '') AS route,
    COALESCE(status, '') AS status,
    COALESCE(err_code, '') AS err_code
  FROM orders
  WHERE datetime(submit_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
)
SELECT
  route,
  COUNT(*) AS attempted_orders,
  SUM(CASE WHEN status = 'execution_confirmed' THEN 1 ELSE 0 END) AS confirmed_orders,
  SUM(CASE WHEN status = 'execution_failed' THEN 1 ELSE 0 END) AS failed_orders,
  SUM(CASE WHEN status = 'execution_dropped' THEN 1 ELSE 0 END) AS dropped_orders,
  SUM(CASE WHEN status IN ('execution_pending', 'execution_simulated', 'execution_submitted') THEN 1 ELSE 0 END) AS inflight_orders,
  SUM(CASE WHEN err_code IN ('confirm_timeout', 'confirm_timeout_manual_reconcile_required') THEN 1 ELSE 0 END) AS confirm_timeout_orders,
  SUM(CASE WHEN err_code IN ('confirm_error', 'confirm_error_manual_reconcile_required') THEN 1 ELSE 0 END) AS confirm_error_orders,
  ROUND(
    100.0 * SUM(CASE WHEN status = 'execution_confirmed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    2
  ) AS success_rate_pct
FROM window_orders
GROUP BY route
ORDER BY attempted_orders DESC, route ASC;
SQL

echo
echo "=== confirm latency by route (${WINDOW_HOURS}h submit window, ms) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH confirmed_orders AS (
  SELECT
    COALESCE(route, '') AS route,
    CASE
      WHEN submit_ts IS NULL OR confirm_ts IS NULL THEN NULL
      WHEN julianday(confirm_ts) < julianday(submit_ts) THEN 0
      ELSE CAST((julianday(confirm_ts) - julianday(submit_ts)) * 86400000 AS INTEGER)
    END AS latency_ms
  FROM orders
  WHERE status = 'execution_confirmed'
    AND confirm_ts IS NOT NULL
    AND datetime(submit_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
),
ranked AS (
  SELECT
    route,
    latency_ms,
    ROW_NUMBER() OVER (PARTITION BY route ORDER BY latency_ms) AS row_num,
    COUNT(*) OVER (PARTITION BY route) AS row_count
  FROM confirmed_orders
  WHERE latency_ms IS NOT NULL
),
p95 AS (
  SELECT
    route,
    MIN(latency_ms) AS p95_ms
  FROM ranked
  WHERE row_num >= ((row_count * 95 + 99) / 100)
  GROUP BY route
)
SELECT
  c.route,
  COUNT(*) AS confirmed_orders,
  MIN(c.latency_ms) AS min_ms,
  ROUND(AVG(c.latency_ms), 2) AS avg_ms,
  MAX(c.latency_ms) AS max_ms,
  p95.p95_ms
FROM confirmed_orders c
LEFT JOIN p95 ON p95.route = c.route
WHERE c.latency_ms IS NOT NULL
GROUP BY c.route
ORDER BY confirmed_orders DESC, c.route ASC;
SQL

echo
echo "=== route calibration scorecard (${WINDOW_HOURS}h submit window) ==="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
WITH window_orders AS (
  SELECT
    COALESCE(route, '') AS route,
    COALESCE(status, '') AS status,
    COALESCE(err_code, '') AS err_code,
    submit_ts,
    confirm_ts
  FROM orders
  WHERE datetime(submit_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
),
route_kpi AS (
  SELECT
    route,
    COUNT(*) AS attempted_orders,
    SUM(CASE WHEN status = 'execution_confirmed' THEN 1 ELSE 0 END) AS confirmed_orders,
    SUM(CASE WHEN status = 'execution_failed' THEN 1 ELSE 0 END) AS failed_orders,
    SUM(CASE WHEN err_code IN ('confirm_timeout', 'confirm_timeout_manual_reconcile_required') THEN 1 ELSE 0 END) AS confirm_timeout_orders,
    SUM(CASE WHEN err_code IN ('confirm_error', 'confirm_error_manual_reconcile_required') THEN 1 ELSE 0 END) AS confirm_error_orders,
    ROUND(
      100.0 * SUM(CASE WHEN status = 'execution_confirmed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
      2
    ) AS success_rate_pct,
    ROUND(
      100.0 * SUM(CASE WHEN status = 'execution_failed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
      2
    ) AS failure_rate_pct,
    ROUND(
      100.0 * SUM(CASE WHEN err_code IN ('confirm_timeout', 'confirm_timeout_manual_reconcile_required') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
      2
    ) AS timeout_rate_pct
  FROM window_orders
  GROUP BY route
),
confirmed_latencies AS (
  SELECT
    COALESCE(route, '') AS route,
    CASE
      WHEN submit_ts IS NULL OR confirm_ts IS NULL THEN NULL
      WHEN julianday(confirm_ts) < julianday(submit_ts) THEN 0
      ELSE CAST((julianday(confirm_ts) - julianday(submit_ts)) * 86400000 AS INTEGER)
    END AS latency_ms
  FROM window_orders
  WHERE status = 'execution_confirmed'
    AND confirm_ts IS NOT NULL
),
ranked_latencies AS (
  SELECT
    route,
    latency_ms,
    ROW_NUMBER() OVER (PARTITION BY route ORDER BY latency_ms) AS row_num,
    COUNT(*) OVER (PARTITION BY route) AS row_count
  FROM confirmed_latencies
  WHERE latency_ms IS NOT NULL
),
latency_by_route AS (
  SELECT
    route,
    COUNT(*) AS latency_samples,
    ROUND(AVG(latency_ms), 2) AS avg_confirm_latency_ms,
    MAX(latency_ms) AS max_confirm_latency_ms
  FROM confirmed_latencies
  WHERE latency_ms IS NOT NULL
  GROUP BY route
),
p95 AS (
  SELECT
    route,
    MIN(latency_ms) AS p95_confirm_latency_ms
  FROM ranked_latencies
  WHERE row_num >= ((row_count * 95 + 99) / 100)
  GROUP BY route
),
scorecard AS (
  SELECT
    k.route,
    k.attempted_orders,
    k.confirmed_orders,
    k.failed_orders,
    k.confirm_timeout_orders,
    k.confirm_error_orders,
    k.success_rate_pct,
    k.failure_rate_pct,
    k.timeout_rate_pct,
    COALESCE(l.latency_samples, 0) AS latency_samples,
    l.avg_confirm_latency_ms,
    p.p95_confirm_latency_ms,
    l.max_confirm_latency_ms
  FROM route_kpi k
  LEFT JOIN latency_by_route l ON l.route = k.route
  LEFT JOIN p95 p ON p.route = k.route
)
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      success_rate_pct DESC,
      timeout_rate_pct ASC,
      CASE WHEN p95_confirm_latency_ms IS NULL THEN 1 ELSE 0 END ASC,
      p95_confirm_latency_ms ASC,
      attempted_orders DESC,
      route ASC
  ) AS recommended_rank,
  route,
  attempted_orders,
  confirmed_orders,
  failed_orders,
  confirm_timeout_orders,
  confirm_error_orders,
  success_rate_pct,
  failure_rate_pct,
  timeout_rate_pct,
  latency_samples,
  avg_confirm_latency_ms,
  p95_confirm_latency_ms,
  max_confirm_latency_ms
FROM scorecard
ORDER BY recommended_rank ASC;
SQL

echo
echo "=== recommended submit_route_order (${WINDOW_HOURS}h submit window) ==="
default_route_injected=0
if [[ -z "$ALLOWED_ROUTES_VALUES" ]]; then
  RECOMMENDED_ROUTE_ORDER_CSV=""
else
  RECOMMENDED_ROUTE_ORDER_CSV="$(
  sqlite3 -noheader "$DB_PATH" <<SQL
WITH window_orders AS (
  SELECT
    COALESCE(route, '') AS route,
    COALESCE(status, '') AS status,
    COALESCE(err_code, '') AS err_code,
    submit_ts,
    confirm_ts
  FROM orders
  WHERE datetime(submit_ts) >= datetime('now', '-${WINDOW_HOURS} hours')
),
route_kpi AS (
  SELECT
    route,
    COUNT(*) AS attempted_orders,
    ROUND(
      100.0 * SUM(CASE WHEN status = 'execution_confirmed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
      2
    ) AS success_rate_pct,
    ROUND(
      100.0 * SUM(CASE WHEN err_code IN ('confirm_timeout', 'confirm_timeout_manual_reconcile_required') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
      2
    ) AS timeout_rate_pct
  FROM window_orders
  GROUP BY route
),
confirmed_latencies AS (
  SELECT
    COALESCE(route, '') AS route,
    CASE
      WHEN submit_ts IS NULL OR confirm_ts IS NULL THEN NULL
      WHEN julianday(confirm_ts) < julianday(submit_ts) THEN 0
      ELSE CAST((julianday(confirm_ts) - julianday(submit_ts)) * 86400000 AS INTEGER)
    END AS latency_ms
  FROM window_orders
  WHERE status = 'execution_confirmed'
    AND confirm_ts IS NOT NULL
),
ranked_latencies AS (
  SELECT
    route,
    latency_ms,
    ROW_NUMBER() OVER (PARTITION BY route ORDER BY latency_ms) AS row_num,
    COUNT(*) OVER (PARTITION BY route) AS row_count
  FROM confirmed_latencies
  WHERE latency_ms IS NOT NULL
),
p95 AS (
  SELECT
    route,
    MIN(latency_ms) AS p95_confirm_latency_ms
  FROM ranked_latencies
  WHERE row_num >= ((row_count * 95 + 99) / 100)
  GROUP BY route
),
allowed_routes(route) AS (
  VALUES ${ALLOWED_ROUTES_VALUES}
),
ranked_routes AS (
  SELECT
    k.route
  FROM route_kpi k
  LEFT JOIN p95 p ON p.route = k.route
  INNER JOIN allowed_routes a ON LOWER(TRIM(k.route)) = a.route
  WHERE TRIM(k.route) <> ''
  ORDER BY
    k.success_rate_pct DESC,
    k.timeout_rate_pct ASC,
    CASE WHEN p.p95_confirm_latency_ms IS NULL THEN 1 ELSE 0 END ASC,
    p.p95_confirm_latency_ms ASC,
    k.attempted_orders DESC,
    k.route ASC
)
SELECT COALESCE(group_concat(route, ','), '')
FROM ranked_routes;
SQL
)"
fi

if [[ -n "$DEFAULT_ROUTE" ]] \
  && csv_contains_route "$SUBMIT_ALLOWED_ROUTES_CSV" "$DEFAULT_ROUTE" \
  && ! csv_contains_route "$RECOMMENDED_ROUTE_ORDER_CSV" "$DEFAULT_ROUTE"; then
  if [[ -n "$RECOMMENDED_ROUTE_ORDER_CSV" ]]; then
    RECOMMENDED_ROUTE_ORDER_CSV="${DEFAULT_ROUTE},${RECOMMENDED_ROUTE_ORDER_CSV}"
  else
    RECOMMENDED_ROUTE_ORDER_CSV="$DEFAULT_ROUTE"
  fi
  default_route_injected=1
fi

if [[ -n "$RECOMMENDED_ROUTE_ORDER_CSV" ]]; then
  echo "recommended_route_order_csv: $RECOMMENDED_ROUTE_ORDER_CSV"
  echo "env_override: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER=$RECOMMENDED_ROUTE_ORDER_CSV"
  if [[ "$default_route_injected" -eq 1 ]]; then
    echo "note: default_route '$DEFAULT_ROUTE' added to recommendation for runtime contract compatibility"
  fi
else
  echo "recommended_route_order_csv: <empty>"
  if [[ -z "$ALLOWED_ROUTES_VALUES" ]]; then
    echo "note: execution.submit_allowed_routes is empty or missing in config; cannot compute filtered recommendation"
  else
    echo "note: no allowlisted route data in submit window; keep current route order"
  fi
fi
