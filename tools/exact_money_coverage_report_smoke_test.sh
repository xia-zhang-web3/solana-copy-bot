#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$(mktemp -t exact-money-coverage.XXXXXX.db)"
cleanup() {
  rm -f "$DB_PATH"
}
trap cleanup EXIT

sqlite3 "$DB_PATH" <<'SQL'
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

INSERT INTO observed_swaps VALUES
  ('sig-1', '2026-03-08T00:00:00+00:00', '100', 6, '200', 6),
  ('sig-2', '2026-03-08T00:01:00+00:00', NULL, NULL, NULL, NULL),
  ('sig-3', '2026-03-08T00:02:00+00:00', '300', 6, NULL, NULL),
  ('sig-4', '2026-03-08T00:03:00+00:00', '', 6, '400', 6);

INSERT INTO copy_signals VALUES
  ('copy-1', '2026-03-08T01:00:00+00:00', 1000, 'leader_exact_lamports'),
  ('copy-2', '2026-03-08T00:10:00+00:00', 2000, 'leader_approximate'),
  ('copy-3', '2026-03-08T01:10:00+00:00', NULL, 'leader_approximate');

INSERT INTO orders VALUES
  ('ord-1', '2026-03-08T01:00:00+00:00', '2026-03-08T01:01:00+00:00'),
  ('ord-2', '2026-03-07T23:30:00+00:00', NULL),
  ('ord-3', '2026-03-08T02:00:00+00:00', '2026-03-08T02:01:00+00:00'),
  ('ord-4', '2026-03-08T02:10:00+00:00', '2026-03-08T02:11:00+00:00');

INSERT INTO fills VALUES
  (1, 'ord-1', '500', 6, 1000, 10),
  (2, 'ord-2', NULL, NULL, NULL, NULL),
  (3, 'ord-3', '600', NULL, 2000, NULL),
  (4, 'ord-4', '000', 6, 4000, 40);

INSERT INTO positions VALUES
  (1, '2026-03-08T03:00:00+00:00', NULL, 'exact_post_cutover', '700', 6, 5000, 50),
  (2, '2026-03-07T20:00:00+00:00', '2026-03-07T21:00:00+00:00', 'legacy_pre_cutover', NULL, NULL, NULL, NULL),
  (3, '2026-03-08T03:30:00+00:00', NULL, 'exact_post_cutover', NULL, NULL, NULL, NULL),
  (4, '2026-03-08T03:45:00+00:00', NULL, 'exact_post_cutover', '000', 6, 9000, NULL),
  (5, '2026-03-08T03:50:00+00:00', NULL, 'exact_post_cutover', '', 6, 7000, NULL);

INSERT INTO shadow_lots VALUES
  (1, '2026-03-08T04:00:00+00:00', 'exact_post_cutover', '700', 6, 7000),
  (2, '2026-03-07T18:00:00+00:00', 'legacy_pre_cutover', NULL, NULL, NULL),
  (3, '2026-03-08T04:30:00+00:00', 'legacy_pre_cutover', '0', 6, 9000);

INSERT INTO shadow_closed_trades VALUES
  (1, '2026-03-08T05:00:00+00:00', 'exact_post_cutover', '500', 6, 1000, 1200, 200),
  (2, '2026-03-07T19:00:00+00:00', 'legacy_pre_cutover', NULL, NULL, NULL, NULL, NULL),
  (3, '2026-03-08T06:00:00+00:00', 'exact_post_cutover', '600', NULL, 3000, NULL, NULL),
  (4, '2026-03-08T06:10:00+00:00', 'exact_post_cutover', 'abc', 6, 4000, 4500, 500);

INSERT INTO exact_money_cutover_state VALUES
  (1, '2026-03-08T00:30:00+00:00', '2026-03-08T07:00:00+00:00', 'smoke');
SQL

output="$(python3 "$ROOT_DIR/tools/exact_money_coverage_report.py" "$DB_PATH")"

assert_contains() {
  local needle="$1"
  if [[ "$output" != *"$needle"* ]]; then
    echo "expected output to contain: $needle" >&2
    echo "$output" >&2
    exit 1
  fi
}

assert_contains "observed_swaps_total_rows: 4"
assert_contains "observed_swaps_exact_rows: 1"
assert_contains "observed_swaps_partial_exact_rows: 1"
assert_contains "observed_swaps_mixed_state: yes"
assert_contains "observed_swaps_invalid_exact_rows: 1"
assert_contains "observed_swaps_invalid_exact_detected: yes"
assert_contains "observed_swaps_post_cutover_rows: 0"
assert_contains "observed_swaps_post_cutover_exact_rows: 0"
assert_contains "copy_signals_total_rows: 3"
assert_contains "copy_signals_exact_rows: 1"
assert_contains "copy_signals_partial_exact_rows: 0"
assert_contains "copy_signals_post_cutover_rows: 2"
assert_contains "copy_signals_post_cutover_exact_rows: 1"
assert_contains "copy_signals_legacy_approximate_rows: 1"
assert_contains "fills_total_rows: 4"
assert_contains "fills_exact_rows: 2"
assert_contains "fills_partial_exact_rows: 1"
assert_contains "fills_post_cutover_rows: 3"
assert_contains "fills_post_cutover_exact_rows: 2"
assert_contains "fills_qty_total_rows: 4"
assert_contains "fills_qty_exact_rows: 1"
assert_contains "fills_qty_partial_exact_rows: 1"
assert_contains "fills_invalid_zero_raw_exact_rows: 1"
assert_contains "fills_invalid_zero_raw_exact_detected: yes"
assert_contains "positions_total_rows: 5"
assert_contains "positions_exact_rows: 3"
assert_contains "positions_qty_total_rows: 5"
assert_contains "positions_qty_exact_rows: 1"
assert_contains "positions_pnl_total_rows: 5"
assert_contains "positions_pnl_exact_rows: 1"
assert_contains "positions_mixed_state: yes"
assert_contains "shadow_lots_total_rows: 3"
assert_contains "shadow_lots_exact_rows: 2"
assert_contains "shadow_lots_qty_total_rows: 3"
assert_contains "shadow_lots_qty_exact_rows: 1"
assert_contains "shadow_closed_trades_total_rows: 4"
assert_contains "shadow_closed_trades_exact_rows: 2"
assert_contains "shadow_closed_trades_partial_exact_rows: 1"
assert_contains "shadow_closed_trades_qty_total_rows: 4"
assert_contains "shadow_closed_trades_qty_exact_rows: 1"
assert_contains "shadow_closed_trades_qty_partial_exact_rows: 1"
assert_contains "exact_money_cutover_present: yes"
assert_contains "exact_money_cutover_ts: 2026-03-08T00:30:00+00:00"
assert_contains "positions_legacy_approximate_rows: 1"
assert_contains "shadow_lots_legacy_approximate_rows: 1"
assert_contains "positions_bucket_legacy_rows: 1"
assert_contains "positions_bucket_exact_rows: 4"
assert_contains "positions_bucket_exact_with_exact_rows: 1"
assert_contains "positions_bucket_exact_invalid_exact_rows: 2"
assert_contains "positions_bucket_exact_missing_exact_rows: 1"
assert_contains "positions_bucket_forbidden_merge_rows: 3"
assert_contains "shadow_lots_bucket_legacy_rows: 2"
assert_contains "shadow_lots_bucket_exact_rows: 1"
assert_contains "shadow_lots_bucket_exact_with_exact_rows: 1"
assert_contains "shadow_lots_bucket_legacy_with_exact_rows: 1"
assert_contains "shadow_lots_bucket_forbidden_merge_rows: 1"
assert_contains "shadow_closed_trades_bucket_legacy_rows: 1"
assert_contains "shadow_closed_trades_bucket_exact_rows: 3"
assert_contains "shadow_closed_trades_bucket_exact_with_exact_rows: 1"
assert_contains "shadow_closed_trades_bucket_exact_invalid_exact_rows: 1"
assert_contains "shadow_closed_trades_bucket_exact_missing_exact_rows: 1"
assert_contains "shadow_closed_trades_bucket_forbidden_merge_rows: 2"
assert_contains "fills_invalid_exact_rows: 1"
assert_contains "positions_invalid_exact_rows: 2"
assert_contains "shadow_lots_invalid_exact_rows: 1"
assert_contains "shadow_closed_trades_invalid_exact_rows: 1"
assert_contains "positions_invalid_zero_raw_exact_rows: 1"
assert_contains "positions_invalid_zero_raw_exact_detected: yes"
assert_contains "shadow_lots_invalid_zero_raw_exact_rows: 1"
assert_contains "shadow_lots_invalid_zero_raw_exact_detected: yes"
assert_contains "shadow_closed_trades_invalid_zero_raw_exact_rows: 0"

echo "[ok] exact money coverage report smoke"
