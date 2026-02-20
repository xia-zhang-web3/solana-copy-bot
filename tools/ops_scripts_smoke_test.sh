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

FAKE_BIN_DIR="$TMP_DIR/fake-bin"
mkdir -p "$FAKE_BIN_DIR"

write_fake_journalctl() {
  local script_path="$FAKE_BIN_DIR/journalctl"
  cat >"$script_path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"-n 250"* ]]; then
  mode="${COPYBOT_SMOKE_JOURNAL_MODE:-normal}"
  if [[ "$mode" == "no_ingestion" ]]; then
    cat <<'LOGS'
2026-02-19T12:00:00Z INFO unrelated runtime line without json payload
2026-02-19T12:00:00Z INFO ingestion pipeline metrics {not-valid-json
2026-02-19T12:00:01Z INFO sqlite contention counters {"sqlite_write_retry_total":0,"sqlite_busy_error_total":0}
LOGS
    exit 0
  fi
  cat <<'LOGS'
2026-02-19T12:00:00Z INFO unrelated runtime line without json payload
2026-02-19T12:00:00Z INFO ingestion pipeline metrics {not-valid-json
2026-02-19T12:00:00Z INFO some other metrics {"foo":"bar"}
2026-02-19T12:00:00Z INFO ingestion pipeline metrics {"ingestion_lag_ms_p95":1400,"ingestion_lag_ms_p99":2100,"ws_to_fetch_queue_depth":1,"fetch_to_output_queue_depth":0,"fetch_concurrency_inflight":2,"ws_notifications_enqueued":111,"ws_notifications_replaced_oldest":0,"reconnect_count":0,"stream_gap_detected":0,"parse_rejected_total":3,"parse_rejected_by_reason":{"other":1,"missing_slot":2},"parse_fallback_by_reason":{"missing_program_ids_fallback":4},"grpc_message_total":12345,"grpc_decode_errors":0,"rpc_429":0,"rpc_5xx":0}
2026-02-19T12:00:00Z INFO ingestion pipeline metrics {"ingestion_lag_ms_p95":1700,"ingestion_lag_ms_p99":2600,"ws_to_fetch_queue_depth":2,"fetch_to_output_queue_depth":1,"fetch_concurrency_inflight":3,"ws_notifications_enqueued":222,"ws_notifications_replaced_oldest":1,"reconnect_count":1,"stream_gap_detected":0,"parse_rejected_total":5,"parse_rejected_by_reason":{"missing_signer":3,"other":2},"parse_fallback_by_reason":{"missing_program_ids_fallback":1,"missing_slot_fallback":2},"grpc_message_total":22345,"grpc_decode_errors":1,"rpc_429":1,"rpc_5xx":0}
2026-02-19T12:00:01Z INFO sqlite contention counters {"sqlite_write_retry_total":0,"sqlite_busy_error_total":0}
LOGS
fi
exit 0
EOF
  chmod +x "$script_path"
}

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

[execution]
default_route = "paper"
submit_allowed_routes = ["paper"]
EOF
}

write_config_empty_allowlist() {
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

[execution]
default_route = "paper"
submit_allowed_routes = []
EOF
}

write_config_multiline_allowlist() {
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

[execution]
default_route = "paper"
submit_allowed_routes = [
  "paper",
  "rpc"
]
EOF
}

write_config_default_route_with_rpc_allowlist() {
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

[execution]
default_route = "paper"
submit_allowed_routes = ["paper", "rpc"]
EOF
}

write_config_missing_default_route_with_rpc_allowlist() {
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

[execution]
submit_allowed_routes = ["paper", "rpc"]
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

create_rpc_only_db() {
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
  ('order-confirmed-rpc', 'rpc', 'execution_confirmed', NULL, NULL, datetime('now', '-20 minutes'), datetime('now', '-15 minutes'), 3000, 2039280, 7000, 5000, 2000);

INSERT INTO fills(order_id, fee)
VALUES ('order-confirmed-rpc', 0.00204928);
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
  assert_contains "$calibration_output" "=== fee decomposition readiness by route (confirmed orders) ==="
  assert_contains "$calibration_output" "=== strict policy rejects (submit_adapter_policy_echo_missing) ==="
  assert_contains "$calibration_output" "=== fee decomposition readiness verdict (24h confirmed window) ==="
  assert_contains "$calibration_output" "fee_decomposition_verdict: SKIP"
  assert_contains "$calibration_output" "=== route outcome KPI (24h submit window) ==="
  assert_contains "$calibration_output" "=== confirm latency by route (24h submit window, ms) ==="
  assert_contains "$calibration_output" "=== route calibration scorecard (24h submit window) ==="
  assert_contains "$calibration_output" "=== recommended submit_route_order (24h submit window) ==="
  assert_contains "$calibration_output" "recommended_route_order_csv:"
  assert_contains "$calibration_output" "recommended_route_order_csv: paper"

  local snapshot_output
  snapshot_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/runtime_snapshot.sh" 24 60
  )"
  assert_contains "$snapshot_output" "=== CopyBot Runtime Snapshot ==="
  assert_contains "$snapshot_output" "=== Execution Fee Breakdown by Route (24h) ==="
  assert_contains "$snapshot_output" "=== Recent Risk Events (60m) ==="
  assert_contains "$snapshot_output" "ingestion_lag_ms_p95: 1700"
  assert_contains "$snapshot_output" "parse_rejected_total: 5"
  assert_contains "$snapshot_output" "parse_rejected_by_reason: {\"missing_signer\": 3, \"other\": 2}"
  assert_contains "$snapshot_output" "parse_fallback_by_reason: {\"missing_program_ids_fallback\": 1, \"missing_slot_fallback\": 2}"

  echo "[ok] ${label}"
}

run_calibration_empty_allowlist_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$output" "recommended_route_order_csv: <empty>"
  assert_contains "$output" "execution.submit_allowed_routes is empty or missing in config"
  echo "[ok] calibration empty allowlist branch"
}

run_calibration_multiline_allowlist_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$output" "recommended_route_order_csv: paper"
  echo "[ok] calibration multiline allowlist parse"
}

run_calibration_default_route_injection_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$output" "recommended_route_order_csv: paper,rpc"
  assert_contains "$output" "default_route 'paper' added to recommendation"
  echo "[ok] calibration default-route injection"
}

run_calibration_default_route_runtime_fallback_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$output" "recommended_route_order_csv: paper,rpc"
  assert_contains "$output" "default_route 'paper' added to recommendation"
  echo "[ok] calibration runtime default-route fallback"
}

run_runtime_snapshot_no_ingestion_case() {
  local db_path="$1"
  local config_path="$2"
  local snapshot_output
  snapshot_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" COPYBOT_SMOKE_JOURNAL_MODE="no_ingestion" \
      DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/runtime_snapshot.sh" 24 60
  )"
  assert_contains "$snapshot_output" "=== Ingestion Runtime (latest samples) ==="
  assert_contains "$snapshot_output" "no ingestion metric samples found"
  echo "[ok] runtime snapshot no-ingestion branch"
}

main() {
  write_fake_journalctl

  local legacy_db="$TMP_DIR/legacy.db"
  local legacy_cfg="$TMP_DIR/legacy.toml"
  create_legacy_db "$legacy_db"
  write_config "$legacy_cfg" "$legacy_db"
  run_ops_scripts_for_db "legacy schema" "$legacy_db" "$legacy_cfg"
  run_runtime_snapshot_no_ingestion_case "$legacy_db" "$legacy_cfg"

  local modern_db="$TMP_DIR/modern.db"
  local modern_cfg="$TMP_DIR/modern.toml"
  create_modern_db "$modern_db"
  write_config "$modern_cfg" "$modern_db"
  run_ops_scripts_for_db "modern schema" "$modern_db" "$modern_cfg"

  local empty_allowlist_cfg="$TMP_DIR/empty-allowlist.toml"
  write_config_empty_allowlist "$empty_allowlist_cfg" "$modern_db"
  run_calibration_empty_allowlist_case "$modern_db" "$empty_allowlist_cfg"

  local multiline_allowlist_cfg="$TMP_DIR/multiline-allowlist.toml"
  write_config_multiline_allowlist "$multiline_allowlist_cfg" "$modern_db"
  run_calibration_multiline_allowlist_case "$modern_db" "$multiline_allowlist_cfg"

  local rpc_only_db="$TMP_DIR/rpc-only.db"
  local default_injection_cfg="$TMP_DIR/default-injection.toml"
  create_rpc_only_db "$rpc_only_db"
  write_config_default_route_with_rpc_allowlist "$default_injection_cfg" "$rpc_only_db"
  run_calibration_default_route_injection_case "$rpc_only_db" "$default_injection_cfg"

  local runtime_default_fallback_cfg="$TMP_DIR/default-fallback.toml"
  write_config_missing_default_route_with_rpc_allowlist "$runtime_default_fallback_cfg" "$rpc_only_db"
  run_calibration_default_route_runtime_fallback_case "$rpc_only_db" "$runtime_default_fallback_cfg"

  echo "ops scripts smoke: PASS"
}

main "$@"
