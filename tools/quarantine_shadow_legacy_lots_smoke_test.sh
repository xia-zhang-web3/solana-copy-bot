#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
DB_PATH="$TMP_DIR/quarantine-shadow-lots.db"
LEGACY_DB_PATH="$TMP_DIR/quarantine-shadow-lots-legacy.db"
trap 'rm -rf "$TMP_DIR"' EXIT

sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE shadow_lots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  wallet_id TEXT NOT NULL,
  token TEXT NOT NULL,
  accounting_bucket TEXT NOT NULL DEFAULT 'legacy_pre_cutover',
  risk_context TEXT NOT NULL DEFAULT 'market',
  qty REAL NOT NULL,
  qty_raw TEXT,
  qty_decimals INTEGER,
  cost_sol REAL NOT NULL,
  cost_lamports INTEGER,
  opened_ts TEXT NOT NULL
);

INSERT INTO shadow_lots(wallet_id, token, risk_context, qty, cost_sol, cost_lamports, opened_ts) VALUES
  ('wallet-a', 'token-old', 'market', 10.0, 0.20, 250000000, '2026-03-01T10:00:00Z'),
  ('wallet-a', 'token-fresh', 'market', 10.0, 0.30, NULL, '2100-01-01T10:00:00Z'),
  ('wallet-b', 'token-old', 'quarantined_legacy', 10.0, 0.40, 400000000, '2026-03-01T10:00:00Z');
SQL

dry_run_output="$(
  python3 "$ROOT_DIR/tools/quarantine_shadow_legacy_lots.py" "$DB_PATH" --older-than-hours 1
)"
[[ "$dry_run_output" == *"mode: dry_run"* ]]
[[ "$dry_run_output" == *"matched_lots: 1"* ]]
[[ "$dry_run_output" == *"matched_accounting_notional_sol: 0.25"* ]]
[[ "$dry_run_output" == *"updated_lots: 0"* ]]

apply_output="$(
  python3 "$ROOT_DIR/tools/quarantine_shadow_legacy_lots.py" "$DB_PATH" --older-than-hours 1 --apply
)"
[[ "$apply_output" == *"mode: apply"* ]]
[[ "$apply_output" == *"matched_lots: 1"* ]]
[[ "$apply_output" == *"updated_lots: 1"* ]]

updated_context="$(sqlite3 -noheader "$DB_PATH" "SELECT risk_context FROM shadow_lots WHERE wallet_id='wallet-a' AND token='token-old';")"
[[ "$updated_context" == "quarantined_legacy" ]]

fresh_context="$(sqlite3 -noheader "$DB_PATH" "SELECT risk_context FROM shadow_lots WHERE wallet_id='wallet-a' AND token='token-fresh';")"
[[ "$fresh_context" == "market" ]]

sqlite3 "$LEGACY_DB_PATH" <<'SQL'
CREATE TABLE shadow_lots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  wallet_id TEXT NOT NULL,
  token TEXT NOT NULL,
  qty REAL NOT NULL,
  cost_sol REAL NOT NULL,
  opened_ts TEXT NOT NULL
);
SQL

missing_column_output=""
if missing_column_output="$(
  python3 "$ROOT_DIR/tools/quarantine_shadow_legacy_lots.py" "$LEGACY_DB_PATH" --older-than-hours 1 2>&1
)"; then
  echo "expected quarantine tool to fail when shadow_lots.risk_context is missing" >&2
  exit 1
fi
[[ "$missing_column_output" == *"shadow_lots.risk_context is missing"* ]]

echo "quarantine_shadow_legacy_lots smoke: PASS"
