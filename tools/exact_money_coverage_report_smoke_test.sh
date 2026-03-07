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
  notional_lamports INTEGER,
  fee_lamports INTEGER
);
CREATE TABLE positions(
  id INTEGER PRIMARY KEY,
  cost_lamports INTEGER
);
CREATE TABLE shadow_lots(
  id INTEGER PRIMARY KEY,
  cost_lamports INTEGER
);
CREATE TABLE shadow_closed_trades(
  id INTEGER PRIMARY KEY,
  entry_cost_lamports INTEGER,
  exit_value_lamports INTEGER,
  pnl_lamports INTEGER
);

INSERT INTO observed_swaps VALUES
  ('sig-1', '2026-03-08T00:00:00Z', '100', 6, '200', 6),
  ('sig-2', '2026-03-08T00:01:00Z', NULL, NULL, NULL, NULL),
  ('sig-3', '2026-03-08T00:02:00Z', '300', 6, NULL, NULL);

INSERT INTO fills VALUES
  (1, 1000, 10),
  (2, NULL, NULL),
  (3, 2000, NULL);

INSERT INTO positions VALUES
  (1, 5000),
  (2, NULL);

INSERT INTO shadow_lots VALUES
  (1, 7000),
  (2, NULL);

INSERT INTO shadow_closed_trades VALUES
  (1, 1000, 1200, 200),
  (2, NULL, NULL, NULL),
  (3, 3000, NULL, NULL);
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
assert_contains "fills_exact_rows: 1"
assert_contains "fills_partial_exact_rows: 1"
assert_contains "positions_exact_rows: 1"
assert_contains "positions_mixed_state: yes"
assert_contains "shadow_lots_exact_rows: 1"
assert_contains "shadow_closed_trades_exact_rows: 1"
assert_contains "shadow_closed_trades_partial_exact_rows: 1"

echo "[ok] exact money coverage report smoke"
