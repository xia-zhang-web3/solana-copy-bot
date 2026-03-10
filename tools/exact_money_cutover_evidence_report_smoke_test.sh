#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d -t exact-money-cutover-evidence.XXXXXX)"
DB_SKIP="$TMP_DIR/skip.db"
DB_PASS="$TMP_DIR/pass.db"
DB_WARN="$TMP_DIR/warn.db"
OUT_SKIP="$TMP_DIR/out-skip"
OUT_PASS="$TMP_DIR/out-pass"
OUT_WARN="$TMP_DIR/out-warn"
ORIGINAL_READINESS_SCRIPT="$ROOT_DIR/tools/exact_money_cutover_readiness_report.sh"
READINESS_SCRIPT_BACKUP="$TMP_DIR/exact_money_cutover_readiness_report.sh.original"

cleanup() {
  if [[ -f "$READINESS_SCRIPT_BACKUP" ]]; then
    mv "$READINESS_SCRIPT_BACKUP" "$ORIGINAL_READINESS_SCRIPT"
  fi
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
  (2, '2026-03-08T01:04:00+00:00', NULL, 'exact_post_cutover', '700', 6, 5000, 0);

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

create_schema "$DB_SKIP"
skip_output="$(OUTPUT_DIR="$OUT_SKIP" bash "$ROOT_DIR/tools/exact_money_cutover_evidence_report.sh" "$DB_SKIP")"
assert_contains "$skip_output" "exact_money_cutover_evidence_verdict: SKIP"
assert_contains "$skip_output" "exact_money_cutover_evidence_reason_code: cutover_not_marked"
assert_contains "$skip_output" "readiness_guard_verdict: SKIP"
assert_contains "$skip_output" "legacy_export_verdict: SKIP"
assert_contains "$skip_output" "legacy_export_reason_code: cutover_not_marked"
assert_contains "$skip_output" "artifact_readiness_capture: $OUT_SKIP/"
assert_contains "$skip_output" "artifact_legacy_export_capture: $OUT_SKIP/"

create_schema "$DB_PASS"
seed_pass_fixture "$DB_PASS"
pass_output="$(OUTPUT_DIR="$OUT_PASS" bash "$ROOT_DIR/tools/exact_money_cutover_evidence_report.sh" "$DB_PASS")"
assert_contains "$pass_output" "exact_money_cutover_evidence_verdict: PASS"
assert_contains "$pass_output" "exact_money_cutover_evidence_reason_code: cutover_evidence_ready"
assert_contains "$pass_output" "readiness_guard_verdict: PASS"
assert_contains "$pass_output" "legacy_export_verdict: PASS"
assert_contains "$pass_output" "legacy_approximate_rows_total: 11"
assert_contains "$pass_output" "post_cutover_approximate_rows_total: 0"
assert_contains "$pass_output" "artifact_legacy_export_summary: $OUT_PASS/legacy_export/exact_money_legacy_export_summary.txt"
assert_contains "$pass_output" "artifact_legacy_export_manifest: $OUT_PASS/legacy_export/exact_money_legacy_export_manifest.txt"
if ! [[ -f "$OUT_PASS/legacy_export/exact_money_legacy_observed_swaps.csv" ]]; then
  echo "missing nested legacy export csv artifact" >&2
  exit 1
fi
if ! ls "$OUT_PASS"/exact_money_cutover_evidence_summary_*.txt >/dev/null 2>&1; then
  echo "missing cutover evidence summary artifact" >&2
  exit 1
fi
if ! ls "$OUT_PASS"/exact_money_cutover_evidence_manifest_*.txt >/dev/null 2>&1; then
  echo "missing cutover evidence manifest artifact" >&2
  exit 1
fi

cp "$ORIGINAL_READINESS_SCRIPT" "$READINESS_SCRIPT_BACKUP"
cat >"$ORIGINAL_READINESS_SCRIPT" <<'EOF_STUB_READINESS'
#!/usr/bin/env bash
cat <<'EOF_STUB_READINESS_OUTPUT'
exact_money_cutover_present: yes
exact_money_cutover_ts: 2026-03-08T00:30:00+00:00
exact_money_post_cutover_surface_failures: 0
exact_money_invalid_exact_rows_total: 0
exact_money_forbidden_merge_rows_total: 0
exact_money_cutover_guard_verdict: PASS
exact_money_cutover_guard_reason_code: post_cutover_exact_ready
EOF_STUB_READINESS_OUTPUT
exit 7
EOF_STUB_READINESS
chmod +x "$ORIGINAL_READINESS_SCRIPT"

readiness_fail_output="$(OUTPUT_DIR="$TMP_DIR/out-readiness-fail" bash "$ROOT_DIR/tools/exact_money_cutover_evidence_report.sh" "$DB_PASS")"
assert_contains "$readiness_fail_output" "exact_money_cutover_evidence_verdict: UNKNOWN"
assert_contains "$readiness_fail_output" "exact_money_cutover_evidence_reason_code: readiness_failed"
assert_contains "$readiness_fail_output" "readiness_exit_code: 7"
assert_contains "$readiness_fail_output" "readiness_guard_verdict: PASS"
assert_contains "$readiness_fail_output" "legacy_export_verdict: SKIP"
assert_contains "$readiness_fail_output" "legacy_export_reason_code: readiness_failed"

mv "$READINESS_SCRIPT_BACKUP" "$ORIGINAL_READINESS_SCRIPT"

cp "$DB_PASS" "$DB_WARN"
sqlite3 "$DB_WARN" <<'SQL'
INSERT INTO orders VALUES ('ord-post-approx', '2026-03-08T01:10:00+00:00', '2026-03-08T01:11:00+00:00');
INSERT INTO fills VALUES (3, 'ord-post-approx', NULL, NULL, NULL, NULL);
SQL
warn_output="$(OUTPUT_DIR="$OUT_WARN" bash "$ROOT_DIR/tools/exact_money_cutover_evidence_report.sh" "$DB_WARN")"
assert_contains "$warn_output" "exact_money_cutover_evidence_verdict: WARN"
assert_contains "$warn_output" "readiness_guard_verdict: WARN"
assert_contains "$warn_output" "legacy_export_verdict: WARN"
assert_contains "$warn_output" "post_cutover_approximate_rows_total: 2"

echo "[ok] exact money cutover evidence report smoke"
