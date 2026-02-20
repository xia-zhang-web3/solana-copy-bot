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

write_config_adapter_mode() {
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
mode = "adapter_submit_confirm"
default_route = "paper"
submit_allowed_routes = ["paper", "rpc"]
submit_adapter_require_policy_echo = true
EOF
}

write_config_devnet_rehearsal() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "dev"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
rpc_http_url = "https://rpc.mainnet.local"
rpc_devnet_http_url = "https://api.devnet.solana.com"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1000 }
submit_adapter_auth_token = "token-inline"
EOF
}

write_config_adapter_preflight_pass() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
rpc_http_url = "https://rpc.primary.local"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_fallback_http_url = "https://adapter.fallback.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper", "rpc"]
submit_route_order = ["paper", "rpc"]
submit_route_max_slippage_bps = { paper = 50.0, rpc = 40.0 }
submit_route_tip_lamports = { paper = 0, rpc = 1000 }
submit_route_compute_unit_limit = { paper = 250000, rpc = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1, rpc = 2000 }
submit_adapter_auth_token_file = "secrets/auth.token"
submit_adapter_hmac_key_id = "key-123"
submit_adapter_hmac_secret_file = "secrets/hmac.secret"
submit_adapter_hmac_ttl_sec = 30
EOF
}

write_config_adapter_preflight_fail() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = false
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 250000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1 }
EOF
}

write_config_adapter_preflight_missing_route_policy_map() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper", "rpc"]
submit_route_order = ["paper", "rpc"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0, rpc = 1000 }
submit_route_compute_unit_limit = { paper = 250000, rpc = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1, rpc = 2000 }
submit_adapter_auth_token = "token-inline"
EOF
}

write_config_adapter_preflight_invalid_route_order() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper", "rpc"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 250000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1 }
submit_adapter_auth_token = "token-inline"
EOF
}

write_config_adapter_preflight_missing_secret_file() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 250000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1 }
submit_adapter_auth_token_file = "secrets/missing.token"
EOF
}

write_config_adapter_preflight_tip_above_max() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 100000001 }
submit_route_compute_unit_limit = { paper = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1000 }
submit_adapter_auth_token = "token-inline"
EOF
}

write_config_adapter_preflight_default_cu_limit_too_low() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 90000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1000 }
submit_adapter_auth_token = "token-inline"
EOF
}

write_config_adapter_preflight_route_price_exceeds_pretrade_cap() {
  local config_path="$1"
  local db_path="$2"
  cat >"$config_path" <<EOF
[system]
env = "prod-eu"

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
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper", "rpc"]
submit_route_order = ["paper", "rpc"]
submit_route_max_slippage_bps = { paper = 50.0, rpc = 40.0 }
submit_route_tip_lamports = { paper = 0, rpc = 1000 }
submit_route_compute_unit_limit = { paper = 300000, rpc = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1000, rpc = 2000 }
pretrade_max_priority_fee_lamports = 1500
submit_adapter_auth_token = "token-inline"
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

write_adapter_env_rotation_report() {
  local env_path="$1"
  cat >"$env_path" <<'EOF'
COPYBOT_ADAPTER_BEARER_TOKEN_FILE="secrets/adapter_bearer.token"
COPYBOT_ADAPTER_HMAC_KEY_ID="key-rotation"
COPYBOT_ADAPTER_HMAC_SECRET_FILE="secrets/adapter_hmac.secret"
COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE="secrets/upstream_auth.token"
COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE="secrets/upstream_fallback_auth.token"
COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE="secrets/send_rpc_auth.token"
COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE="secrets/send_rpc_fallback_auth.token"
COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN_FILE="secrets/route_rpc_auth.token"
COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_AUTH_TOKEN_FILE="secrets/route_rpc_send_rpc_auth.token"
COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=false
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
  assert_contains "$calibration_output" "=== fee accounting consistency vs hints (confirmed orders) ==="
  assert_contains "$calibration_output" "=== strict policy rejects (submit_adapter_policy_echo_missing) ==="
  assert_contains "$calibration_output" "=== fee decomposition readiness verdict (24h confirmed window) ==="
  assert_contains "$calibration_output" "fee_decomposition_verdict: SKIP"
  assert_contains "$calibration_output" "=== route outcome KPI (24h submit window) ==="
  assert_contains "$calibration_output" "=== confirm latency by route (24h submit window, ms) ==="
  assert_contains "$calibration_output" "=== route calibration scorecard (24h submit window) ==="
  assert_contains "$calibration_output" "=== recommended submit_route_order (24h submit window) ==="
  assert_contains "$calibration_output" "recommended_route_order_csv:"
  assert_contains "$calibration_output" "recommended_route_order_csv: paper"
  assert_contains "$calibration_output" "=== route profile readiness verdict (24h submit window) ==="
  assert_contains "$calibration_output" "route_profile_verdict: SKIP"

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

  local go_nogo_output
  go_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$go_nogo_output" "=== Execution Go/No-Go Summary ==="
  assert_contains "$go_nogo_output" "fee_decomposition_verdict: SKIP"
  assert_contains "$go_nogo_output" "route_profile_verdict: SKIP"
  assert_contains "$go_nogo_output" "preflight_verdict: SKIP"
  assert_contains "$go_nogo_output" "overall_go_nogo_verdict: HOLD"

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

run_calibration_adapter_mode_route_profile_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    DB_PATH="$db_path" CONFIG_PATH="$config_path" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24
  )"
  assert_contains "$output" "=== route profile readiness verdict (24h submit window) ==="
  assert_contains "$output" "calibration_knobs: submit_route_order + submit_route_max_slippage_bps + submit_route_tip_lamports + submit_route_compute_unit_limit + submit_route_compute_unit_price_micro_lamports"
  assert_contains "$output" "ata_rows_total: 1"
  assert_contains "$output" "fee_consistency_missing_coverage_rows: 0"
  assert_contains "$output" "primary_route: paper"
  assert_contains "$output" "route_profile_verdict: WARN"

  local go_nogo_output
  go_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$go_nogo_output" "fee_decomposition_verdict: WARN"
  assert_contains "$go_nogo_output" "route_profile_verdict: WARN"
  assert_contains "$go_nogo_output" "preflight_verdict: SKIP"
  assert_contains "$go_nogo_output" "overall_go_nogo_verdict: NO_GO"
  echo "[ok] calibration adapter-mode route profile verdict"
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

run_go_nogo_artifact_export_case() {
  local db_path="$1"
  local config_path="$2"
  local artifacts_dir="$TMP_DIR/go-nogo-artifacts"
  local output
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" OUTPUT_DIR="$artifacts_dir" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$output" "artifacts_written: true"
  assert_contains "$output" "artifact_calibration:"
  assert_contains "$output" "artifact_snapshot:"
  assert_contains "$output" "artifact_preflight:"
  assert_contains "$output" "artifact_summary:"
  if ! ls "$artifacts_dir"/execution_go_nogo_summary_*.txt >/dev/null 2>&1; then
    echo "expected go/no-go summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_fee_calibration_*.txt >/dev/null 2>&1; then
    echo "expected calibration artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/runtime_snapshot_*.txt >/dev/null 2>&1; then
    echo "expected runtime snapshot artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_adapter_preflight_*.txt >/dev/null 2>&1; then
    echo "expected adapter preflight artifact in $artifacts_dir" >&2
    exit 1
  fi
  echo "[ok] go-no-go artifact export"
}

run_go_nogo_unknown_precedence_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="unknown-value" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="skip" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$output" "fee_decomposition_verdict: UNKNOWN"
  assert_contains "$output" "route_profile_verdict: SKIP"
  assert_contains "$output" "overall_go_nogo_verdict: NO_GO"
  assert_contains "$output" "overall_go_nogo_reason: unable to classify readiness gate verdicts from tool output"
  echo "[ok] go-no-go UNKNOWN precedence"
}

run_go_nogo_preflight_fail_case() {
  local db_path="$1"
  local fail_cfg="$TMP_DIR/go-nogo-preflight-fail.toml"
  local output
  write_config_adapter_preflight_fail "$fail_cfg" "$db_path"
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$fail_cfg" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$output" "preflight_verdict: FAIL"
  assert_contains "$output" "overall_go_nogo_verdict: NO_GO"
  assert_contains "$output" "overall_go_nogo_reason: adapter preflight failed:"
  echo "[ok] go-no-go preflight fail gate"
}

run_adapter_preflight_case() {
  local db_path="$1"
  local pass_cfg="$TMP_DIR/adapter-preflight-pass.toml"
  local fail_cfg="$TMP_DIR/adapter-preflight-fail.toml"
  local missing_map_cfg="$TMP_DIR/adapter-preflight-missing-map.toml"
  local invalid_route_order_cfg="$TMP_DIR/adapter-preflight-invalid-route-order.toml"
  local missing_secret_cfg="$TMP_DIR/adapter-preflight-missing-secret.toml"
  local tip_above_max_cfg="$TMP_DIR/adapter-preflight-tip-above-max.toml"
  local default_cu_limit_too_low_cfg="$TMP_DIR/adapter-preflight-default-cu-limit-too-low.toml"
  local route_price_exceeds_pretrade_cfg="$TMP_DIR/adapter-preflight-route-price-exceeds-pretrade.toml"
  local secrets_dir="$TMP_DIR/secrets"
  local missing_map_output
  local invalid_route_order_output
  local missing_secret_output
  local env_override_output
  local tip_above_max_output
  local default_cu_limit_too_low_output
  local route_price_exceeds_pretrade_output
  local env_underscore_numeric_output
  mkdir -p "$secrets_dir"
  printf 'token-pass\n' >"$secrets_dir/auth.token"
  printf 'hmac-pass\n' >"$secrets_dir/hmac.secret"

  write_config_adapter_preflight_pass "$pass_cfg" "$db_path"
  local pass_output
  pass_output="$(
    CONFIG_PATH="$pass_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh"
  )"
  assert_contains "$pass_output" "=== Execution Adapter Preflight ==="
  assert_contains "$pass_output" "preflight_verdict: PASS"

  if env_override_output="$(
    CONFIG_PATH="$pass_cfg" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO="false" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for strict echo env override in prod-like profile" >&2
    exit 1
  fi
  assert_contains "$env_override_output" "strict_policy_echo: false"
  assert_contains "$env_override_output" "preflight_verdict: FAIL"
  assert_contains "$env_override_output" "submit_adapter_require_policy_echo must be true in production-like env profiles"

  write_config_adapter_preflight_fail "$fail_cfg" "$db_path"
  local fail_output
  if fail_output="$(
    CONFIG_PATH="$fail_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for invalid config" >&2
    exit 1
  fi
  assert_contains "$fail_output" "preflight_verdict: FAIL"
  assert_contains "$fail_output" "submit_adapter_require_policy_echo must be true in production-like env profiles"

  write_config_adapter_preflight_missing_route_policy_map "$missing_map_cfg" "$db_path"
  if missing_map_output="$(
    CONFIG_PATH="$missing_map_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for missing route policy map coverage" >&2
    exit 1
  fi
  assert_contains "$missing_map_output" "preflight_verdict: FAIL"
  assert_contains "$missing_map_output" "execution.submit_route_max_slippage_bps is missing entry for allowed route=rpc"

  write_config_adapter_preflight_invalid_route_order "$invalid_route_order_cfg" "$db_path"
  if invalid_route_order_output="$(
    CONFIG_PATH="$invalid_route_order_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for invalid submit_route_order" >&2
    exit 1
  fi
  assert_contains "$invalid_route_order_output" "preflight_verdict: FAIL"
  assert_contains "$invalid_route_order_output" "execution.submit_route_order route=rpc must be present in execution.submit_allowed_routes"

  write_config_adapter_preflight_missing_secret_file "$missing_secret_cfg" "$db_path"
  if missing_secret_output="$(
    CONFIG_PATH="$missing_secret_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for missing auth token secret file" >&2
    exit 1
  fi
  assert_contains "$missing_secret_output" "preflight_verdict: FAIL"
  assert_contains "$missing_secret_output" "execution.submit_adapter_auth_token_file invalid: secret file not found:"

  write_config_adapter_preflight_tip_above_max "$tip_above_max_cfg" "$db_path"
  if tip_above_max_output="$(
    CONFIG_PATH="$tip_above_max_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for tip above max guardrail" >&2
    exit 1
  fi
  assert_contains "$tip_above_max_output" "preflight_verdict: FAIL"
  assert_contains "$tip_above_max_output" "execution.submit_route_tip_lamports route=paper must be in 0..=100000000, got 100000001"

  write_config_adapter_preflight_default_cu_limit_too_low "$default_cu_limit_too_low_cfg" "$db_path"
  if default_cu_limit_too_low_output="$(
    CONFIG_PATH="$default_cu_limit_too_low_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for default route compute unit limit lower bound" >&2
    exit 1
  fi
  assert_contains "$default_cu_limit_too_low_output" "preflight_verdict: FAIL"
  assert_contains "$default_cu_limit_too_low_output" "execution.submit_route_compute_unit_limit default route paper limit (90000) is too low for reliable swaps; expected >= 100000"

  write_config_adapter_preflight_route_price_exceeds_pretrade_cap "$route_price_exceeds_pretrade_cfg" "$db_path"
  if route_price_exceeds_pretrade_output="$(
    CONFIG_PATH="$route_price_exceeds_pretrade_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for route compute unit price above pretrade max priority fee" >&2
    exit 1
  fi
  assert_contains "$route_price_exceeds_pretrade_output" "preflight_verdict: FAIL"
  assert_contains "$route_price_exceeds_pretrade_output" "execution.submit_route_compute_unit_price_micro_lamports route rpc price (2000) cannot exceed execution.pretrade_max_priority_fee_lamports (1500) (unit: micro-lamports per CU for both fields)"

  if env_underscore_numeric_output="$(
    CONFIG_PATH="$pass_cfg" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS="paper:50,rpc:4_0" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for underscore numeric in env route-map value" >&2
    exit 1
  fi
  assert_contains "$env_underscore_numeric_output" "preflight_verdict: FAIL"
  assert_contains "$env_underscore_numeric_output" "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS contains invalid numeric value for route=rpc: 4_0"

  if env_malformed_route_map_output="$(
    CONFIG_PATH="$pass_cfg" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS="paper:50,rpc" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for malformed env route-map token" >&2
    exit 1
  fi
  assert_contains "$env_malformed_route_map_output" "preflight_verdict: FAIL"
  assert_contains "$env_malformed_route_map_output" "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS contains malformed token (expected route:value): rpc"

  echo "[ok] adapter preflight pass/fail + route-policy + route-order + secret diagnostics + numeric parity guards"
}

run_adapter_secret_rotation_report_case() {
  local env_path="$TMP_DIR/adapter-rotation.env"
  local secrets_dir="$TMP_DIR/secrets"
  local artifacts_dir="$TMP_DIR/adapter-rotation-artifacts"
  mkdir -p "$secrets_dir"
  write_adapter_env_rotation_report "$env_path"

  printf 'bearer-pass\n' >"$secrets_dir/adapter_bearer.token"
  printf 'hmac-pass\n' >"$secrets_dir/adapter_hmac.secret"
  printf 'upstream-pass\n' >"$secrets_dir/upstream_auth.token"
  printf 'upstream-fallback-pass\n' >"$secrets_dir/upstream_fallback_auth.token"
  printf 'send-rpc-pass\n' >"$secrets_dir/send_rpc_auth.token"
  printf 'send-rpc-fallback-pass\n' >"$secrets_dir/send_rpc_fallback_auth.token"
  printf 'route-pass\n' >"$secrets_dir/route_rpc_auth.token"
  printf 'route-send-rpc-pass\n' >"$secrets_dir/route_rpc_send_rpc_auth.token"
  printf 'route-fast-lane-pass\n' >"$secrets_dir/route_fast_lane_auth.token"
  chmod 600 "$secrets_dir"/*.token "$secrets_dir"/*.secret

  local pass_output
  pass_output="$(
    ADAPTER_ENV_PATH="$env_path" OUTPUT_DIR="$artifacts_dir" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh"
  )"
  assert_contains "$pass_output" "=== Adapter Secret Rotation Report ==="
  assert_contains "$pass_output" "rotation_readiness_verdict: PASS"
  assert_contains "$pass_output" "artifact_report:"
  if ! ls "$artifacts_dir"/adapter_secret_rotation_report_*.txt >/dev/null 2>&1; then
    echo "expected adapter secret rotation artifact in $artifacts_dir" >&2
    exit 1
  fi

  local duplicate_key_env_path="$TMP_DIR/adapter-rotation-duplicate.env"
  cp "$env_path" "$duplicate_key_env_path"
  {
    echo 'COPYBOT_ADAPTER_BEARER_TOKEN_FILE="secrets/missing-first.token"'
    echo 'COPYBOT_ADAPTER_BEARER_TOKEN_FILE="secrets/adapter_bearer.token"'
  } >>"$duplicate_key_env_path"
  local duplicate_key_output
  duplicate_key_output="$(
    ADAPTER_ENV_PATH="$duplicate_key_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh"
  )"
  assert_contains "$duplicate_key_output" "rotation_readiness_verdict: PASS"

  local quoted_hash_env_path="$TMP_DIR/adapter-rotation-quoted-hash.env"
  cp "$env_path" "$quoted_hash_env_path"
  printf 'bearer-hash-pass\n' >"$secrets_dir/adapter_bearer#quoted.token"
  chmod 600 "$secrets_dir/adapter_bearer#quoted.token"
  echo 'COPYBOT_ADAPTER_BEARER_TOKEN_FILE="secrets/adapter_bearer#quoted.token"' >>"$quoted_hash_env_path"
  local quoted_hash_output
  quoted_hash_output="$(
    ADAPTER_ENV_PATH="$quoted_hash_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh"
  )"
  assert_contains "$quoted_hash_output" "rotation_readiness_verdict: PASS"
  assert_contains "$quoted_hash_output" "adapter_bearer#quoted.token"

  local conflict_env_path="$TMP_DIR/adapter-rotation-conflict.env"
  cp "$env_path" "$conflict_env_path"
  echo 'COPYBOT_ADAPTER_BEARER_TOKEN="inline-conflict-token"' >>"$conflict_env_path"
  local conflict_output=""
  if conflict_output="$(
    ADAPTER_ENV_PATH="$conflict_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for inline+file secret conflict" >&2
    exit 1
  else
    local conflict_exit_code=$?
    if [[ "$conflict_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for conflict, got $conflict_exit_code" >&2
      echo "$conflict_output" >&2
      exit 1
    fi
  fi
  assert_contains "$conflict_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$conflict_output" "COPYBOT_ADAPTER_BEARER_TOKEN and COPYBOT_ADAPTER_BEARER_TOKEN_FILE cannot both be set"

  local upstream_fallback_conflict_env_path="$TMP_DIR/adapter-rotation-upstream-fallback-conflict.env"
  cp "$env_path" "$upstream_fallback_conflict_env_path"
  echo 'COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN="inline-upstream-fallback-conflict"' >>"$upstream_fallback_conflict_env_path"
  local upstream_fallback_conflict_output=""
  if upstream_fallback_conflict_output="$(
    ADAPTER_ENV_PATH="$upstream_fallback_conflict_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for upstream fallback auth inline+file conflict" >&2
    exit 1
  else
    local upstream_fallback_conflict_exit_code=$?
    if [[ "$upstream_fallback_conflict_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for upstream fallback auth conflict, got $upstream_fallback_conflict_exit_code" >&2
      echo "$upstream_fallback_conflict_output" >&2
      exit 1
    fi
  fi
  assert_contains "$upstream_fallback_conflict_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$upstream_fallback_conflict_output" "COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN and COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE cannot both be set"

  local send_rpc_fallback_conflict_env_path="$TMP_DIR/adapter-rotation-send-rpc-fallback-conflict.env"
  cp "$env_path" "$send_rpc_fallback_conflict_env_path"
  echo 'COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN="inline-send-rpc-fallback-conflict"' >>"$send_rpc_fallback_conflict_env_path"
  local send_rpc_fallback_conflict_output=""
  if send_rpc_fallback_conflict_output="$(
    ADAPTER_ENV_PATH="$send_rpc_fallback_conflict_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for send RPC fallback auth inline+file conflict" >&2
    exit 1
  else
    local send_rpc_fallback_conflict_exit_code=$?
    if [[ "$send_rpc_fallback_conflict_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for send RPC fallback auth conflict, got $send_rpc_fallback_conflict_exit_code" >&2
      echo "$send_rpc_fallback_conflict_output" >&2
      exit 1
    fi
  fi
  assert_contains "$send_rpc_fallback_conflict_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$send_rpc_fallback_conflict_output" "COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN and COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE cannot both be set"

  local route_conflict_env_path="$TMP_DIR/adapter-rotation-route-conflict.env"
  cp "$env_path" "$route_conflict_env_path"
  {
    echo 'COPYBOT_ADAPTER_ROUTE_FAST_LANE_AUTH_TOKEN_FILE="secrets/route_fast_lane_auth.token"'
    echo 'COPYBOT_ADAPTER_ROUTE_FAST_LANE_AUTH_TOKEN="inline-fast-lane-conflict"'
  } >>"$route_conflict_env_path"
  local route_conflict_output=""
  if route_conflict_output="$(
    ADAPTER_ENV_PATH="$route_conflict_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for FAST_LANE route inline+file conflict" >&2
    exit 1
  else
    local route_conflict_exit_code=$?
    if [[ "$route_conflict_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for FAST_LANE route conflict, got $route_conflict_exit_code" >&2
      echo "$route_conflict_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_conflict_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$route_conflict_output" "COPYBOT_ADAPTER_ROUTE_FAST_LANE_AUTH_TOKEN and COPYBOT_ADAPTER_ROUTE_FAST_LANE_AUTH_TOKEN_FILE cannot both be set"

  chmod 644 "$secrets_dir/adapter_bearer.token"
  local warn_output=""
  if warn_output="$(
    ADAPTER_ENV_PATH="$env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected WARN exit for broad secret file permissions" >&2
    exit 1
  else
    local warn_exit_code=$?
    if [[ "$warn_exit_code" -ne 2 ]]; then
      echo "expected WARN exit code 2, got $warn_exit_code" >&2
      echo "$warn_output" >&2
      exit 1
    fi
  fi
  assert_contains "$warn_output" "rotation_readiness_verdict: WARN"
  assert_contains "$warn_output" "broad permissions"

  rm -f "$secrets_dir/route_rpc_auth.token"
  local fail_output=""
  if fail_output="$(
    ADAPTER_ENV_PATH="$env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for missing secret file" >&2
    exit 1
  else
    local fail_exit_code=$?
    if [[ "$fail_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1, got $fail_exit_code" >&2
      echo "$fail_output" >&2
      exit 1
    fi
  fi
  assert_contains "$fail_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$fail_output" "COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN_FILE missing file"
  echo "[ok] adapter secret rotation report pass/warn/fail + conflict + duplicate-key precedence + quoted-hash + underscore route conflict + fallback auth conflict"
}

run_devnet_rehearsal_case() {
  local db_path="$1"
  local config_path="$2"
  local artifacts_dir="$TMP_DIR/devnet-rehearsal-artifacts"
  local output
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" OUTPUT_DIR="$artifacts_dir" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60
  )"
  assert_contains "$output" "=== Execution Devnet Rehearsal ==="
  assert_contains "$output" "preflight_verdict: PASS"
  assert_contains "$output" "overall_go_nogo_verdict: GO"
  assert_contains "$output" "tests_run: false"
  assert_contains "$output" "devnet_rehearsal_verdict: GO"
  assert_contains "$output" "artifact_summary:"
  assert_contains "$output" "artifact_preflight:"
  assert_contains "$output" "artifact_go_nogo:"
  assert_contains "$output" "artifact_tests:"
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_summary_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_preflight_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal preflight artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_go_nogo_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal go/no-go artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_tests_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal tests artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/go_nogo/execution_go_nogo_captured_*.txt >/dev/null 2>&1; then
    echo "expected nested go/no-go capture artifact in $artifacts_dir/go_nogo" >&2
    exit 1
  fi
  echo "[ok] execution devnet rehearsal helper"
}

main() {
  write_fake_journalctl

  local legacy_db="$TMP_DIR/legacy.db"
  local legacy_cfg="$TMP_DIR/legacy.toml"
  create_legacy_db "$legacy_db"
  write_config "$legacy_cfg" "$legacy_db"
  run_ops_scripts_for_db "legacy schema" "$legacy_db" "$legacy_cfg"
  run_runtime_snapshot_no_ingestion_case "$legacy_db" "$legacy_cfg"
  run_go_nogo_artifact_export_case "$legacy_db" "$legacy_cfg"
  run_go_nogo_unknown_precedence_case "$legacy_db" "$legacy_cfg"
  run_adapter_preflight_case "$legacy_db"
  run_adapter_secret_rotation_report_case
  run_go_nogo_preflight_fail_case "$legacy_db"
  local devnet_rehearsal_cfg="$TMP_DIR/devnet-rehearsal.toml"
  write_config_devnet_rehearsal "$devnet_rehearsal_cfg" "$legacy_db"
  run_devnet_rehearsal_case "$legacy_db" "$devnet_rehearsal_cfg"

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

  local adapter_mode_cfg="$TMP_DIR/adapter-mode.toml"
  write_config_adapter_mode "$adapter_mode_cfg" "$modern_db"
  run_calibration_adapter_mode_route_profile_case "$modern_db" "$adapter_mode_cfg"

  echo "ops scripts smoke: PASS"
}

main "$@"
