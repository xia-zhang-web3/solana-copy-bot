#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$(mktemp -t exact-money-cutover.XXXXXX.db)"
cleanup() {
  rm -f "$DB_PATH"
}
trap cleanup EXIT

sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE orders(
  order_id TEXT PRIMARY KEY,
  submit_ts TEXT NOT NULL,
  confirm_ts TEXT
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

echo "[ok] exact money cutover marker smoke"
