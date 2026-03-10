#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$(mktemp -t exact-money-cutover.XXXXXX.db)"
DIRTY_DB_PATH="$(mktemp -t exact-money-cutover-dirty.XXXXXX.db)"
cleanup() {
  rm -f "$DB_PATH" "$DIRTY_DB_PATH"
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
SQL

python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DB_PATH" \
  --cutover-ts "2026-03-08T12:34:56+00:00" \
  --note "test-cutover" >/dev/null

rows="$(sqlite3 "$DB_PATH" "SELECT cutover_ts || '|' || note FROM exact_money_cutover_state WHERE id = 1;")"
if [[ "$rows" != "2026-03-08T12:34:56+00:00|test-cutover" ]]; then
  echo "unexpected cutover state row: $rows" >&2
  exit 1
fi

if python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DB_PATH" \
  --cutover-ts "2026-03-08T12:34:56" >/dev/null 2>/tmp/exact-money-cutover.err; then
  echo "expected naive timestamp to be rejected" >&2
  exit 1
fi

if ! grep -q "timestamp must include explicit timezone" /tmp/exact-money-cutover.err; then
  echo "unexpected error for naive timestamp" >&2
  cat /tmp/exact-money-cutover.err >&2
  exit 1
fi

rm -f /tmp/exact-money-cutover.err

sqlite3 "$DIRTY_DB_PATH" <<'SQL'
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

INSERT INTO orders(order_id, submit_ts, confirm_ts) VALUES
  ('ord-dirty-fill', '2026-03-08T12:35:00+00:00', '2026-03-08T12:36:00+00:00');

INSERT INTO fills(
  id, order_id, qty_raw, qty_decimals, notional_lamports, fee_lamports
) VALUES (
  1, 'ord-dirty-fill', '500', 6, NULL, NULL
);

INSERT INTO positions(
  id, opened_ts, closed_ts, accounting_bucket, qty_raw, qty_decimals, cost_lamports, pnl_lamports
) VALUES (
  1, '2026-03-08T12:37:00+00:00', NULL, 'legacy_pre_cutover', '500', 6, NULL, NULL
);
SQL

if python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DIRTY_DB_PATH" \
  --cutover-ts "2026-03-08T12:34:56+00:00" >/dev/null 2>/tmp/exact-money-cutover.err; then
  echo "expected first dirty cutover mark to fail preflight" >&2
  exit 1
fi

if ! grep -q "fills: post_cutover_approximate_rows=1" /tmp/exact-money-cutover.err; then
  echo "missing fills post-cutover approximate preflight error" >&2
  cat /tmp/exact-money-cutover.err >&2
  exit 1
fi

if ! grep -q "positions: post_cutover_approximate_rows=1" /tmp/exact-money-cutover.err; then
  echo "missing positions post-cutover approximate preflight error" >&2
  cat /tmp/exact-money-cutover.err >&2
  exit 1
fi

if python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DIRTY_DB_PATH" \
  --allow-dirty \
  --cutover-ts "2026-03-08T12:34:56+00:00" >/dev/null; then
  :
else
  echo "expected --allow-dirty to bypass first-mark dirty preflight" >&2
  exit 1
fi

rm -f /tmp/exact-money-cutover.err

sqlite3 "$DB_PATH" <<'SQL'
INSERT INTO positions(
  id, opened_ts, accounting_bucket, qty_raw, qty_decimals, cost_lamports, pnl_lamports
) VALUES (
  1, '2026-03-08T13:00:00+00:00', 'exact_post_cutover', NULL, NULL, 1000, NULL
);
SQL

if python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DB_PATH" \
  --cutover-ts "2026-03-08T12:35:56+00:00" >/dev/null 2>/tmp/exact-money-cutover.err; then
  echo "expected dirty exact-money state to fail preflight" >&2
  exit 1
fi

if ! grep -q "positions: bucket_forbidden_merge_rows=1" /tmp/exact-money-cutover.err; then
  echo "unexpected error for dirty exact-money state" >&2
  cat /tmp/exact-money-cutover.err >&2
  exit 1
fi

python3 "$ROOT_DIR/tools/mark_exact_money_cutover.py" \
  "$DB_PATH" \
  --allow-dirty \
  --cutover-ts "2026-03-08T12:35:56+00:00" >/dev/null

rm -f /tmp/exact-money-cutover.err

echo "[ok] exact money cutover marker smoke"
