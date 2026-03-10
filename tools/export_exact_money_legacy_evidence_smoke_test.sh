#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d -t exact-money-legacy-export.XXXXXX)"
DB_NO_CUTOVER="$TMP_DIR/no-cutover.db"
DB_PASS="$TMP_DIR/pass.db"
DB_WARN="$TMP_DIR/warn.db"
OUT_PASS="$TMP_DIR/out-pass"
OUT_WARN="$TMP_DIR/out-warn"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

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
CREATE TABLE orders(
  order_id TEXT PRIMARY KEY,
  submit_ts TEXT NOT NULL,
  confirm_ts TEXT
);
CREATE TABLE fills(
  id INTEGER PRIMARY KEY,
  order_id TEXT,
  qty_raw TEXT,
  qty_decimals INTEGER,
  notional_lamports INTEGER,
  fee_lamports INTEGER
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

seed_pass_fixture() {
  local db_path="$1"
  sqlite3 "$db_path" <<'SQL'
INSERT INTO observed_swaps VALUES
  ('obs-legacy', '2026-03-08T00:00:00+00:00', '100', 6, NULL, NULL),
  ('obs-exact', '2026-03-08T01:00:00+00:00', '100', 6, '200', 6);

INSERT INTO copy_signals VALUES
  ('sig-legacy', '2026-03-08T00:01:00+00:00', NULL, 'leader_approximate'),
  ('sig-exact', '2026-03-08T01:01:00+00:00', 1000, 'leader_exact_lamports');

INSERT INTO orders VALUES
  ('ord-legacy', '2026-03-08T00:02:00+00:00', '2026-03-08T00:03:00+00:00'),
  ('ord-exact', '2026-03-08T01:02:00+00:00', '2026-03-08T01:03:00+00:00');

INSERT INTO fills VALUES
  (1, 'ord-legacy', NULL, NULL, NULL, NULL),
  (2, 'ord-exact', '500', 6, 1000, 10);

INSERT INTO positions VALUES
  (1, '2026-03-08T00:04:00+00:00', NULL, 'legacy_pre_cutover', NULL, NULL, NULL, NULL),
  (2, '2026-03-08T01:04:00+00:00', NULL, 'exact_post_cutover', '700', 6, 5000, NULL);

INSERT INTO shadow_lots VALUES
  (1, '2026-03-08T00:05:00+00:00', 'legacy_pre_cutover', NULL, NULL, NULL),
  (2, '2026-03-08T01:05:00+00:00', 'exact_post_cutover', '800', 6, 8000);

INSERT INTO shadow_closed_trades VALUES
  (1, '2026-03-08T00:06:00+00:00', 'legacy_pre_cutover', NULL, NULL, NULL, NULL, NULL),
  (2, '2026-03-08T01:06:00+00:00', 'exact_post_cutover', '900', 6, 4000, 4500, 500);

INSERT INTO exact_money_cutover_state VALUES
  (1, '2026-03-08T00:30:00+00:00', '2026-03-08T02:00:00+00:00', 'smoke-export');
SQL
}

assert_contains() {
  local output="$1"
  local needle="$2"
  if [[ "$output" != *"$needle"* ]]; then
    echo "expected output to contain: $needle" >&2
    echo "$output" >&2
    exit 1
  fi
}

create_schema "$DB_NO_CUTOVER"
if python3 "$ROOT_DIR/tools/export_exact_money_legacy_evidence.py" "$DB_NO_CUTOVER" "$TMP_DIR/no-cutover-out" >/tmp/exact-money-legacy-export.err 2>&1; then
  echo "expected legacy evidence export to fail without cutover marker" >&2
  exit 1
fi
if ! grep -q "exact money cutover marker is missing" /tmp/exact-money-legacy-export.err; then
  echo "missing no-cutover failure detail" >&2
  cat /tmp/exact-money-legacy-export.err >&2
  exit 1
fi
rm -f /tmp/exact-money-legacy-export.err

create_schema "$DB_PASS"
seed_pass_fixture "$DB_PASS"
pass_output="$(python3 "$ROOT_DIR/tools/export_exact_money_legacy_evidence.py" "$DB_PASS" "$OUT_PASS")"

assert_contains "$pass_output" "exact_money_legacy_export_verdict: PASS"
assert_contains "$pass_output" "exact_money_legacy_export_reason_code: legacy_evidence_exported"
assert_contains "$pass_output" "legacy_approximate_rows_total: 10"
assert_contains "$pass_output" "post_cutover_approximate_rows_total: 0"
assert_contains "$pass_output" "observed_swaps_legacy_approximate_rows: 1"
assert_contains "$pass_output" "copy_signals_legacy_approximate_rows: 1"
assert_contains "$pass_output" "fills_legacy_approximate_rows: 1"
assert_contains "$pass_output" "fills_qty_legacy_approximate_rows: 1"
assert_contains "$pass_output" "positions_legacy_approximate_rows: 1"
assert_contains "$pass_output" "positions_qty_legacy_approximate_rows: 1"
assert_contains "$pass_output" "shadow_lots_legacy_approximate_rows: 1"
assert_contains "$pass_output" "shadow_lots_qty_legacy_approximate_rows: 1"
assert_contains "$pass_output" "shadow_closed_trades_legacy_approximate_rows: 1"
assert_contains "$pass_output" "shadow_closed_trades_qty_legacy_approximate_rows: 1"
assert_contains "$pass_output" "artifact_summary: $OUT_PASS/exact_money_legacy_export_summary.txt"
assert_contains "$pass_output" "artifact_manifest: $OUT_PASS/exact_money_legacy_export_manifest.txt"

if ! [[ -f "$OUT_PASS/exact_money_legacy_observed_swaps.csv" ]]; then
  echo "missing observed_swaps legacy csv" >&2
  exit 1
fi
if ! [[ -f "$OUT_PASS/exact_money_legacy_export_summary.txt" ]]; then
  echo "missing summary artifact" >&2
  exit 1
fi
if ! [[ -f "$OUT_PASS/exact_money_legacy_export_manifest.txt" ]]; then
  echo "missing manifest artifact" >&2
  exit 1
fi
if [[ "$(wc -l <"$OUT_PASS/exact_money_legacy_observed_swaps.csv")" -ne 2 ]]; then
  echo "expected observed_swaps legacy csv to contain one data row plus header" >&2
  cat "$OUT_PASS/exact_money_legacy_observed_swaps.csv" >&2
  exit 1
fi
if ! grep -q "obs-legacy" "$OUT_PASS/exact_money_legacy_observed_swaps.csv"; then
  echo "expected observed_swaps legacy csv to include obs-legacy row" >&2
  cat "$OUT_PASS/exact_money_legacy_observed_swaps.csv" >&2
  exit 1
fi

cp "$DB_PASS" "$DB_WARN"
sqlite3 "$DB_WARN" <<'SQL'
INSERT INTO orders VALUES ('ord-post-approx', '2026-03-08T01:10:00+00:00', '2026-03-08T01:11:00+00:00');
INSERT INTO fills VALUES (3, 'ord-post-approx', NULL, NULL, NULL, NULL);
SQL
warn_output="$(python3 "$ROOT_DIR/tools/export_exact_money_legacy_evidence.py" "$DB_WARN" "$OUT_WARN")"
assert_contains "$warn_output" "exact_money_legacy_export_verdict: WARN"
assert_contains "$warn_output" "exact_money_legacy_export_reason_code: post_cutover_approximate_detected"
assert_contains "$warn_output" "post_cutover_approximate_rows_total: 2"

echo "[ok] exact money legacy evidence export smoke"
