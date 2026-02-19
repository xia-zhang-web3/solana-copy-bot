#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

require_bin() {
  local bin="$1"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "required binary not found: $bin" >&2
    exit 1
  fi
}

require_bin sqlite3
require_bin python3
require_bin bash

write_config() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[sqlite]
path = "$db_path"

[risk]
max_position_sol = 0.5
max_total_exposure_sol = 3.0
max_hold_hours = 8
shadow_soft_exposure_cap_sol = 10.0
shadow_hard_exposure_cap_sol = 12.0
shadow_killswitch_enabled = true
EOF
}

init_common_tables() {
  local db_path="$1"
  sqlite3 "$db_path" <<'SQL'
PRAGMA journal_mode = WAL;

CREATE TABLE shadow_lots (
  wallet_id TEXT,
  token TEXT,
  cost_sol REAL,
  opened_ts TEXT
);

CREATE TABLE shadow_closed_trades (
  pnl_sol REAL,
  closed_ts TEXT
);

CREATE TABLE copy_signals (
  ts TEXT,
  status TEXT,
  side TEXT
);

CREATE TABLE fills (
  order_id TEXT,
  fee REAL
);

CREATE TABLE risk_events (
  ts TEXT,
  type TEXT,
  severity TEXT,
  details_json TEXT
);

INSERT INTO shadow_lots(wallet_id, token, cost_sol, opened_ts)
VALUES ('wallet-a', 'token-a', 0.25, datetime('now', '-1 hour'));

INSERT INTO shadow_closed_trades(pnl_sol, closed_ts)
VALUES (0.02, datetime('now', '-30 minutes'));

INSERT INTO copy_signals(ts, status, side)
VALUES
  (datetime('now', '-10 minutes'), 'shadow_recorded', 'buy'),
  (datetime('now', '-8 minutes'), 'execution_confirmed', 'sell');

INSERT INTO risk_events(ts, type, severity, details_json)
VALUES
  (datetime('now', '-5 minutes'), 'execution_submit_failed', 'error', '{"order_id":"order-strict","route":"paper","error_code":"submit_adapter_policy_echo_missing"}'),
  (datetime('now', '-4 minutes'), 'execution_network_fee_unavailable_submit_hint_used', 'warn', '{"route":"paper"}'),
  (datetime('now', '-3 minutes'), 'execution_network_fee_unavailable_fallback_used', 'warn', '{"route":"paper"}'),
  (datetime('now', '-2 minutes'), 'execution_network_fee_hint_mismatch', 'warn', '{"route":"paper"}');
SQL
}

create_legacy_db() {
  local db_path="$1"
  init_common_tables "$db_path"
  sqlite3 "$db_path" <<'SQL'
CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  route TEXT,
  status TEXT,
  err_code TEXT,
  simulation_error TEXT,
  submit_ts TEXT,
  confirm_ts TEXT
);

INSERT INTO orders(order_id, route, status, err_code, simulation_error, submit_ts, confirm_ts)
VALUES
  ('order-confirmed-legacy', 'paper', 'execution_confirmed', NULL, NULL, datetime('now', '-20 minutes'), datetime('now', '-15 minutes')),
  ('order-strict', 'paper', 'execution_failed', 'submit_terminal_rejected', 'submit_adapter_policy_echo_missing', datetime('now', '-10 minutes'), NULL);

INSERT INTO fills(order_id, fee)
VALUES ('order-confirmed-legacy', 0.000012);
SQL
}

create_modern_db() {
  local db_path="$1"
  init_common_tables "$db_path"
  sqlite3 "$db_path" <<'SQL'
CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  route TEXT,
  status TEXT,
  err_code TEXT,
  simulation_error TEXT,
  submit_ts TEXT,
  confirm_ts TEXT,
  applied_tip_lamports INTEGER,
  ata_create_rent_lamports INTEGER,
  network_fee_lamports_hint INTEGER,
  base_fee_lamports_hint INTEGER,
  priority_fee_lamports_hint INTEGER
);

INSERT INTO orders(
  order_id, route, status, err_code, simulation_error, submit_ts, confirm_ts,
  applied_tip_lamports, ata_create_rent_lamports, network_fee_lamports_hint,
  base_fee_lamports_hint, priority_fee_lamports_hint
)
VALUES
  ('order-confirmed-modern', 'paper', 'execution_confirmed', NULL, NULL, datetime('now', '-20 minutes'), datetime('now', '-15 minutes'), 3000, 2039280, 7000, 5000, 2000),
  ('order-strict', 'paper', 'execution_failed', 'submit_terminal_rejected', 'submit_adapter_policy_echo_missing', datetime('now', '-10 minutes'), NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO fills(order_id, fee)
VALUES ('order-confirmed-modern', 0.00204928);
SQL
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "expected output to contain: $needle" >&2
    exit 1
  fi
}

run_ops_scripts_for_db() {
  local label="$1"
  local db_path="$2"
  local config_path="$3"

  local calibration_output
  calibration_output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$calibration_output" "=== confirmed fee breakdown by route ==="
  assert_contains "$calibration_output" "=== fee hint coverage by route (confirmed orders) ==="
  assert_contains "$calibration_output" "=== strict policy rejects (submit_adapter_policy_echo_missing) ==="

  local snapshot_output
  snapshot_output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-missing-service" \
      bash "$ROOT_DIR/tools/runtime_snapshot.sh" 24 60
  )"
  assert_contains "$snapshot_output" "=== CopyBot Runtime Snapshot ==="
  assert_contains "$snapshot_output" "=== Execution Fee Breakdown by Route (24h) ==="
  assert_contains "$snapshot_output" "=== Recent Risk Events (60m) ==="

  echo "[ok] ${label}"
}

main() {
  local legacy_db="$TMP_DIR/legacy.db"
  local legacy_cfg="$TMP_DIR/legacy.toml"
  create_legacy_db "$legacy_db"
  write_config "$legacy_cfg" "$legacy_db"
  run_ops_scripts_for_db "legacy schema" "$legacy_db" "$legacy_cfg"

  local modern_db="$TMP_DIR/modern.db"
  local modern_cfg="$TMP_DIR/modern.toml"
  create_modern_db "$modern_db"
  write_config "$modern_cfg" "$modern_db"
  run_ops_scripts_for_db "modern schema" "$modern_db" "$modern_cfg"

  echo "ops scripts smoke: PASS"
}

main "$@"
