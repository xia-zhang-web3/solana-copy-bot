#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/tools/backfill_wallet_activity_days.py"

require_bin() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required binary: $1" >&2
    exit 1
  }
}

require_bin python3
require_bin sqlite3

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

db_path="$tmpdir/backfill.db"
sqlite3 "$db_path" <<'SQL'
CREATE TABLE observed_swaps (
  signature TEXT PRIMARY KEY,
  wallet_id TEXT NOT NULL,
  dex TEXT NOT NULL,
  token_in TEXT NOT NULL,
  token_out TEXT NOT NULL,
  qty_in REAL NOT NULL,
  qty_out REAL NOT NULL,
  slot INTEGER NOT NULL,
  ts TEXT NOT NULL
);
CREATE TABLE wallet_activity_days (
  wallet_id TEXT NOT NULL,
  activity_day TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  PRIMARY KEY (wallet_id, activity_day)
);
INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts) VALUES
('sig-1', 'wallet-a', 'raydium', 'So111', 'TokenA', 1, 10, 1, '2026-03-06T08:00:00Z'),
('sig-2', 'wallet-a', 'raydium', 'So111', 'TokenA', 1, 10, 2, '2026-03-06T12:00:00Z'),
('sig-3', 'wallet-a', 'raydium', 'So111', 'TokenA', 1, 10, 3, '2026-03-07T00:00:00+00:00'),
('sig-4', 'wallet-a', 'raydium', 'So111', 'TokenA', 1, 10, 4, '2026-03-07T09:00:00Z'),
('sig-5', 'wallet-b', 'raydium', 'So111', 'TokenB', 1, 10, 5, '2026-03-07T13:00:00Z');
SQL

output="$(python3 "$SCRIPT_PATH" "$db_path" --start-day 2026-03-06 --end-day 2026-03-07)"
[[ "$output" == *"backfilled_day=2026-03-06 changed_rows=1"* ]]
[[ "$output" == *"backfilled_day=2026-03-07 changed_rows=2"* ]]
[[ "$output" == *"summary processed_days=2 total_changed_rows=3"* ]]

rows="$(sqlite3 "$db_path" "SELECT wallet_id || '|' || activity_day || '|' || last_seen FROM wallet_activity_days ORDER BY wallet_id, activity_day;")"
[[ "$rows" == *"wallet-a|2026-03-06|2026-03-06T12:00:00Z"* ]]
[[ "$rows" == *"wallet-a|2026-03-07|2026-03-07T09:00:00Z"* ]]
[[ "$rows" == *"wallet-b|2026-03-07|2026-03-07T13:00:00Z"* ]]

echo "[ok] wallet activity day backfill smoke"
