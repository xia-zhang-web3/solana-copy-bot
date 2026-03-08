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
  qty_raw TEXT,
  qty_decimals INTEGER,
  cost_lamports INTEGER,
  pnl_lamports INTEGER
);
CREATE TABLE shadow_lots(
  id INTEGER PRIMARY KEY,
  opened_ts TEXT,
  cost_lamports INTEGER
);
CREATE TABLE shadow_closed_trades(
  id INTEGER PRIMARY KEY,
  closed_ts TEXT,
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
  ('sig-3', '2026-03-08T00:02:00+00:00', '300', 6, NULL, NULL);

INSERT INTO orders VALUES
  ('ord-1', '2026-03-08T01:00:00+00:00', '2026-03-08T01:01:00+00:00'),
  ('ord-2', '2026-03-07T23:30:00+00:00', NULL),
  ('ord-3', '2026-03-08T02:00:00+00:00', '2026-03-08T02:01:00+00:00');

INSERT INTO fills VALUES
  (1, 'ord-1', '500', 6, 1000, 10),
  (2, 'ord-2', NULL, NULL, NULL, NULL),
  (3, 'ord-3', '600', NULL, 2000, NULL);

INSERT INTO positions VALUES
  (1, '2026-03-08T03:00:00+00:00', NULL, '700', 6, 5000, 50),
  (2, '2026-03-07T20:00:00+00:00', '2026-03-07T21:00:00+00:00', NULL, NULL, NULL, NULL);

INSERT INTO shadow_lots VALUES
  (1, '2026-03-08T04:00:00+00:00', 7000),
  (2, '2026-03-07T18:00:00+00:00', NULL);

INSERT INTO shadow_closed_trades VALUES
  (1, '2026-03-08T05:00:00+00:00', 1000, 1200, 200),
  (2, '2026-03-07T19:00:00+00:00', NULL, NULL, NULL),
  (3, '2026-03-08T06:00:00+00:00', 3000, NULL, NULL);

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

assert_contains "observed_swaps_total_rows: 3"
assert_contains "observed_swaps_exact_rows: 1"
assert_contains "observed_swaps_partial_exact_rows: 1"
assert_contains "observed_swaps_mixed_state: yes"
assert_contains "observed_swaps_post_cutover_rows: 0"
assert_contains "observed_swaps_post_cutover_exact_rows: 0"
assert_contains "fills_exact_rows: 1"
assert_contains "fills_partial_exact_rows: 1"
assert_contains "fills_post_cutover_rows: 2"
assert_contains "fills_post_cutover_exact_rows: 1"
assert_contains "fills_qty_total_rows: 3"
assert_contains "fills_qty_exact_rows: 1"
assert_contains "fills_qty_partial_exact_rows: 1"
assert_contains "positions_exact_rows: 1"
assert_contains "positions_qty_total_rows: 2"
assert_contains "positions_qty_exact_rows: 1"
assert_contains "positions_pnl_total_rows: 2"
assert_contains "positions_pnl_exact_rows: 1"
assert_contains "positions_mixed_state: yes"
assert_contains "shadow_lots_exact_rows: 1"
assert_contains "shadow_closed_trades_exact_rows: 1"
assert_contains "shadow_closed_trades_partial_exact_rows: 1"
assert_contains "exact_money_cutover_present: yes"
assert_contains "exact_money_cutover_ts: 2026-03-08T00:30:00+00:00"
assert_contains "positions_legacy_approximate_rows: 1"
assert_contains "shadow_lots_legacy_approximate_rows: 1"

echo "[ok] exact money coverage report smoke"
