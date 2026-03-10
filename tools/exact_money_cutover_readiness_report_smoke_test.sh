#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_SKIP="$(mktemp -t exact-money-cutover-readiness-skip.XXXXXX.db)"
DB_WARN="$(mktemp -t exact-money-cutover-readiness-warn.XXXXXX.db)"
DB_PASS="$(mktemp -t exact-money-cutover-readiness-pass.XXXXXX.db)"
cleanup() {
  rm -f "$DB_SKIP" "$DB_WARN" "$DB_PASS"
}
trap cleanup EXIT

assert_field_equals() {
  local text="$1"
  local key="$2"
  local expected="$3"
  local actual=""
  actual="$(printf '%s\n' "$text" | awk -F': ' -v key="$key" '$1 == key {print substr($0, index($0, ": ") + 2); exit}')"
  if [[ "$actual" != "$expected" ]]; then
    echo "expected $key=$expected, got ${actual:-<empty>}" >&2
    exit 1
  fi
}

create_schema() {
  local db_path="$1"
  sqlite3 "$db_path" <<'SQL'
CREATE TABLE observed_swaps(
  signature TEXT PRIMARY KEY,
  ts TEXT,
  qty_in_raw TEXT,
  qty_in_decimals INTEGER,
  qty_out_raw TEXT,
  qty_out_decimals INTEGER
);
CREATE TABLE copy_signals(
  signal_id TEXT PRIMARY KEY,
  ts TEXT,
  notional_lamports INTEGER,
  notional_origin TEXT NOT NULL
);
CREATE TABLE fills(
  id INTEGER PRIMARY KEY,
  order_id TEXT,
  qty_raw TEXT,
  qty_decimals INTEGER,
  notional_lamports INTEGER,
  fee_lamports INTEGER
);
CREATE TABLE orders(
  order_id TEXT PRIMARY KEY,
  submit_ts TEXT NOT NULL,
  confirm_ts TEXT
);
CREATE TABLE positions(
  id INTEGER PRIMARY KEY,
  opened_ts TEXT,
  closed_ts TEXT,
  accounting_bucket TEXT NOT NULL,
  qty_raw TEXT,
  qty_decimals INTEGER,
  cost_lamports INTEGER,
  pnl_lamports INTEGER
);
CREATE TABLE shadow_lots(
  id INTEGER PRIMARY KEY,
  opened_ts TEXT,
  accounting_bucket TEXT NOT NULL,
  qty_raw TEXT,
  qty_decimals INTEGER,
  cost_lamports INTEGER
);
CREATE TABLE shadow_closed_trades(
  id INTEGER PRIMARY KEY,
  closed_ts TEXT,
  accounting_bucket TEXT NOT NULL,
  qty_raw TEXT,
  qty_decimals INTEGER,
  entry_cost_lamports INTEGER,
  exit_value_lamports INTEGER,
  pnl_lamports INTEGER
);
CREATE TABLE exact_money_cutover_state(
  id INTEGER PRIMARY KEY,
  cutover_ts TEXT NOT NULL,
  recorded_ts TEXT NOT NULL,
  note TEXT
);
SQL
}

create_schema "$DB_SKIP"
skip_output="$(bash "$ROOT_DIR/tools/exact_money_cutover_readiness_report.sh" "$DB_SKIP")"
assert_field_equals "$skip_output" "exact_money_cutover_guard_verdict" "SKIP"
assert_field_equals "$skip_output" "exact_money_cutover_guard_reason_code" "cutover_not_marked"

create_schema "$DB_WARN"
sqlite3 "$DB_WARN" <<'SQL'
INSERT INTO exact_money_cutover_state VALUES
  (1, '2026-03-08T00:30:00+00:00', '2026-03-08T00:35:00+00:00', 'warn-case');
INSERT INTO observed_swaps VALUES
  ('sig-warn', '2026-03-08T00:45:00+00:00', '', 6, '200', 6);
INSERT INTO orders VALUES
  ('ord-1', '2026-03-08T01:00:00+00:00', '2026-03-08T01:01:00+00:00');
INSERT INTO fills VALUES
  (1, 'ord-1', '500', 6, NULL, NULL);
INSERT INTO positions VALUES
  (1, '2026-03-08T02:00:00+00:00', NULL, 'legacy_pre_cutover', '500', 6, NULL, NULL);
SQL
warn_output="$(bash "$ROOT_DIR/tools/exact_money_cutover_readiness_report.sh" "$DB_WARN")"
assert_field_equals "$warn_output" "exact_money_cutover_guard_verdict" "WARN"
assert_field_equals "$warn_output" "exact_money_cutover_guard_reason_code" "post_cutover_exact_violations_detected"
assert_field_equals "$warn_output" "exact_money_post_cutover_surface_failures" "4"
assert_field_equals "$warn_output" "exact_money_invalid_exact_rows_total" "1"
assert_field_equals "$warn_output" "exact_money_forbidden_merge_rows_total" "1"
assert_field_equals "$warn_output" "observed_swaps_post_cutover_rows" "1"
assert_field_equals "$warn_output" "observed_swaps_post_cutover_exact_rows" "0"
assert_field_equals "$warn_output" "observed_swaps_invalid_exact_rows" "1"
assert_field_equals "$warn_output" "positions_pnl_post_cutover_rows" "1"
assert_field_equals "$warn_output" "positions_pnl_post_cutover_exact_rows" "0"

create_schema "$DB_PASS"
sqlite3 "$DB_PASS" <<'SQL'
INSERT INTO exact_money_cutover_state VALUES
  (1, '2026-03-08T00:30:00+00:00', '2026-03-08T00:35:00+00:00', 'pass-case');
INSERT INTO observed_swaps VALUES
  ('sig-1', '2026-03-08T01:00:00+00:00', '100', 6, '200', 6);
INSERT INTO copy_signals VALUES
  ('copy-1', '2026-03-08T01:05:00+00:00', 1000, 'leader_exact_lamports');
INSERT INTO orders VALUES
  ('ord-1', '2026-03-08T01:10:00+00:00', '2026-03-08T01:11:00+00:00');
INSERT INTO fills VALUES
  (1, 'ord-1', '500', 6, 1000, 50);
INSERT INTO positions VALUES
  (1, '2026-03-08T01:12:00+00:00', NULL, 'exact_post_cutover', '500', 6, 1050, 0);
INSERT INTO shadow_lots VALUES
  (1, '2026-03-08T01:13:00+00:00', 'exact_post_cutover', '500', 6, 1000);
INSERT INTO shadow_closed_trades VALUES
  (1, '2026-03-08T01:14:00+00:00', 'exact_post_cutover', '500', 6, 1000, 1100, 100);
SQL
pass_output="$(bash "$ROOT_DIR/tools/exact_money_cutover_readiness_report.sh" "$DB_PASS")"
assert_field_equals "$pass_output" "exact_money_cutover_guard_verdict" "PASS"
assert_field_equals "$pass_output" "exact_money_cutover_guard_reason_code" "post_cutover_exact_ready"
assert_field_equals "$pass_output" "exact_money_post_cutover_surface_failures" "0"
assert_field_equals "$pass_output" "exact_money_invalid_exact_rows_total" "0"
assert_field_equals "$pass_output" "exact_money_forbidden_merge_rows_total" "0"
assert_field_equals "$pass_output" "observed_swaps_post_cutover_exact_rows" "1"
assert_field_equals "$pass_output" "positions_pnl_post_cutover_exact_rows" "1"

echo "[ok] exact money cutover readiness report smoke"
