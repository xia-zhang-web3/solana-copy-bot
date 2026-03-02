#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
source "$ROOT_DIR/tools/lib/common.sh"

OPS_SMOKE_TARGET_CASES="${OPS_SMOKE_TARGET_CASES:-}"

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
2026-02-19T12:00:02Z INFO execution batch processed {"attempted":3,"confirmed":2,"dropped":0,"failed":1,"skipped":0,"submit_attempted_by_route":{"rpc":3},"submit_retry_scheduled_by_route":{"rpc":1},"submit_failed_by_route":{"rpc":1},"submit_dynamic_cu_policy_enabled_by_route":{"rpc":2},"submit_dynamic_cu_hint_used_by_route":{"rpc":2},"submit_dynamic_cu_hint_api_by_route":{"rpc":1},"submit_dynamic_cu_hint_rpc_by_route":{"rpc":1},"submit_dynamic_cu_price_applied_by_route":{"rpc":1},"submit_dynamic_cu_static_fallback_by_route":{"rpc":1},"submit_dynamic_tip_policy_enabled_by_route":{"rpc":2},"submit_dynamic_tip_applied_by_route":{"rpc":1},"submit_dynamic_tip_static_floor_by_route":{"rpc":1}}
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

write_config_adapter_preflight_fastlane_routes() {
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
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "fastlane"
submit_allowed_routes = ["fastlane", "rpc"]
submit_route_order = ["fastlane", "rpc"]
submit_route_max_slippage_bps = { fastlane = 50.0, rpc = 40.0 }
submit_route_tip_lamports = { fastlane = 10000, rpc = 0 }
submit_route_compute_unit_limit = { fastlane = 300000, rpc = 300000 }
submit_route_compute_unit_price_micro_lamports = { fastlane = 1500, rpc = 1000 }
pretrade_max_priority_fee_lamports = 2000
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

write_executor_env_preflight() {
  local env_path="$1"
  local port="$2"
  local token="$3"
  local allow_unauth="${4:-false}"
  cat >"$env_path" <<EOF
COPYBOT_EXECUTOR_BIND_ADDR="127.0.0.1:${port}"
COPYBOT_EXECUTOR_CONTRACT_VERSION="v1"
COPYBOT_EXECUTOR_SIGNER_PUBKEY="Signer1111111111111111111111111111111111"
COPYBOT_EXECUTOR_SIGNER_SOURCE="kms"
COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID="kms-key-1"
COPYBOT_EXECUTOR_ROUTE_ALLOWLIST="paper,rpc,jito"
COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=false
COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=${allow_unauth}
COPYBOT_EXECUTOR_BEARER_TOKEN="${token}"
COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL="https://executor.upstream.local/submit"
COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL="https://executor.upstream.local/simulate"
COPYBOT_EXECUTOR_ROUTE_RPC_SEND_RPC_URL="https://executor.send-rpc.local/rpc"
COPYBOT_EXECUTOR_ROUTE_JITO_SEND_RPC_URL="https://executor.send-rpc.local/jito"
COPYBOT_EXECUTOR_ROUTE_JITO_SEND_RPC_FALLBACK_URL="https://executor.send-rpc-fallback.local/jito"
EOF
}

write_adapter_env_preflight() {
  local env_path="$1"
  local port="$2"
  local token="$3"
  local allowlist="${4:-paper,rpc,jito}"
  cat >"$env_path" <<EOF
COPYBOT_ADAPTER_ROUTE_ALLOWLIST="${allowlist}"
COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL="http://127.0.0.1:${port}/submit"
COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL="http://127.0.0.1:${port}/simulate"
COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN="${token}"
EOF
}

write_fake_curl_executor_preflight() {
  local fake_bin_dir="$1"
  local token="$2"
  local script_path="$fake_bin_dir/curl"
  mkdir -p "$fake_bin_dir"
  cat >"$script_path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

expected_token='__EXPECTED_TOKEN__'
simulate_without_auth_status="${FAKE_EXECUTOR_SIMULATE_WITHOUT_AUTH_STATUS:-200}"
simulate_with_auth_status="${FAKE_EXECUTOR_SIMULATE_WITH_AUTH_STATUS:-200}"
simulate_invalid_auth_status="${FAKE_EXECUTOR_SIMULATE_INVALID_AUTH_STATUS:-200}"
health_enabled_routes_csv="${FAKE_EXECUTOR_HEALTH_ENABLED_ROUTES_CSV:-paper,rpc,jito}"
health_send_rpc_enabled_routes_csv="${FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_CSV:-rpc,jito}"
health_send_rpc_fallback_routes_csv="${FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_CSV:-jito}"
output_file=""
auth_header=""
url=""
status_code="200"
body='{"status":"not_found"}'

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

csv_to_json_array() {
  local csv="$1"
  local out=""
  local token normalized
  local -a values=()
  IFS=',' read -r -a values <<< "$csv"
  for token in "${values[@]-}"; do
    normalized="$(printf '%s' "$(trim_string "$token")" | tr '[:upper:]' '[:lower:]')"
    [[ -z "$normalized" ]] && continue
    out+="${out:+,}\"${normalized}\""
  done
  printf '[%s]' "$out"
}

while (($#)); do
  case "$1" in
    -o)
      output_file="$2"
      shift 2
      ;;
    -w)
      shift 2
      ;;
    -H)
      header="$2"
      header_lower="$(printf '%s' "$header" | tr '[:upper:]' '[:lower:]')"
      if [[ "$header_lower" == authorization:* ]]; then
        auth_header="${header#*:}"
        auth_header="${auth_header#"${auth_header%%[![:space:]]*}"}"
        auth_header="${auth_header%"${auth_header##*[![:space:]]}"}"
      fi
      shift 2
      ;;
    --data|-d|-X|-m|--connect-timeout)
      shift 2
      ;;
    -s|-S)
      shift
      ;;
    http://*|https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ "$url" == *"/healthz" ]]; then
  health_enabled_routes_json="$(csv_to_json_array "$health_enabled_routes_csv")"
  health_send_rpc_enabled_routes_json="$(csv_to_json_array "$health_send_rpc_enabled_routes_csv")"
  health_send_rpc_fallback_routes_json="$(csv_to_json_array "$health_send_rpc_fallback_routes_csv")"
  body="{\"status\":\"ok\",\"contract_version\":\"v1\",\"enabled_routes\":${health_enabled_routes_json},\"signer_source\":\"kms\",\"idempotency_store_status\":\"ok\",\"send_rpc_enabled_routes\":${health_send_rpc_enabled_routes_json},\"send_rpc_fallback_routes\":${health_send_rpc_fallback_routes_json},\"send_rpc_routes\":${health_send_rpc_enabled_routes_json}}"
  status_code="200"
elif [[ "$url" == *"/simulate" ]]; then
  if [[ -z "$auth_header" ]]; then
    code="auth_missing"
    status_code="$simulate_without_auth_status"
  elif [[ "$auth_header" == "Bearer ${expected_token}" ]]; then
    code="invalid_request"
    status_code="$simulate_with_auth_status"
  else
    code="auth_invalid"
    status_code="$simulate_invalid_auth_status"
  fi
  body="{\"status\":\"reject\",\"retryable\":false,\"code\":\"${code}\",\"detail\":\"smoke preflight probe\"}"
fi

if [[ -n "$output_file" ]]; then
  printf '%s' "$body" >"$output_file"
fi
printf '%s' "$status_code"
EOF
  python3 - "$script_path" "$token" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
token = sys.argv[2]
content = path.read_text()
path.write_text(content.replace("__EXPECTED_TOKEN__", token))
PY
  chmod +x "$script_path"
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

extract_field_value() {
  local text="$1"
  local key="$2"
  printf '%s\n' "$text" | awk -F': ' -v key="$key" '
    $1 == key {
      print substr($0, index($0, ": ") + 2)
      exit
    }
  '
}

assert_sha256_field() {
  local text="$1"
  local key="$2"
  local value
  value="$(extract_field_value "$text" "$key")"
  if [[ -z "$value" ]]; then
    echo "expected sha256 field missing: $key" >&2
    exit 1
  fi
  if ! [[ "$value" =~ ^[0-9a-f]{64}$ ]]; then
    echo "expected $key to be 64-char lowercase hex sha256, got: $value" >&2
    exit 1
  fi
}

assert_field_equals() {
  local text="$1"
  local key="$2"
  local expected="$3"
  local value
  value="$(extract_field_value "$text" "$key")"
  if [[ "$value" != "$expected" ]]; then
    echo "expected $key to equal '$expected', got '$value'" >&2
    exit 1
  fi
}

assert_field_non_empty() {
  local text="$1"
  local key="$2"
  local value
  value="$(extract_field_value "$text" "$key")"
  if [[ -z "$value" ]]; then
    echo "expected $key to be non-empty" >&2
    exit 1
  fi
}

assert_sha256_field_matches_file() {
  local text="$1"
  local sha_key="$2"
  local path_key="$3"
  local expected_sha
  local file_path
  expected_sha="$(extract_field_value "$text" "$sha_key")"
  file_path="$(extract_field_value "$text" "$path_key")"
  if [[ -z "$expected_sha" || -z "$file_path" ]]; then
    echo "expected non-empty $sha_key and $path_key for sha/file consistency check" >&2
    exit 1
  fi
  if [[ ! -f "$file_path" ]]; then
    echo "expected file for $path_key at $file_path" >&2
    exit 1
  fi
  local actual_sha
  actual_sha="$(sha256_file_value "$file_path")"
  if [[ "$actual_sha" != "$expected_sha" ]]; then
    echo "expected $sha_key to match sha256($path_key), got $expected_sha vs $actual_sha" >&2
    exit 1
  fi
}

assert_bundled_summary_manifest_package_status_parity() {
  local text="$1"
  local bundle_path=""
  local summary_path=""
  local manifest_path=""
  bundle_path="$(extract_field_value "$text" "package_bundle_path")"
  summary_path="$(extract_field_value "$text" "artifact_summary")"
  manifest_path="$(extract_field_value "$text" "artifact_manifest")"
  if [[ -z "$bundle_path" || ! -f "$bundle_path" ]]; then
    echo "expected package bundle archive at $bundle_path" >&2
    exit 1
  fi
  if [[ -z "$summary_path" || -z "$manifest_path" ]]; then
    echo "expected non-empty artifact_summary/artifact_manifest fields for bundle content validation" >&2
    exit 1
  fi

  local bundle_tar_list=""
  bundle_tar_list="$(tar -tzf "$bundle_path")"
  local summary_artifact_basename=""
  local manifest_artifact_basename=""
  summary_artifact_basename="$(basename "$summary_path")"
  manifest_artifact_basename="$(basename "$manifest_path")"
  local summary_entry_in_tar=""
  local manifest_entry_in_tar=""
  summary_entry_in_tar="$(printf '%s\n' "$bundle_tar_list" | awk -F/ -v target="$summary_artifact_basename" '$NF==target{print; exit}')"
  manifest_entry_in_tar="$(printf '%s\n' "$bundle_tar_list" | awk -F/ -v target="$manifest_artifact_basename" '$NF==target{print; exit}')"
  if [[ -z "$summary_entry_in_tar" ]]; then
    echo "expected bundled summary artifact entry for $summary_artifact_basename" >&2
    exit 1
  fi
  if [[ -z "$manifest_entry_in_tar" ]]; then
    echo "expected bundled manifest artifact entry for $manifest_artifact_basename" >&2
    exit 1
  fi

  local bundled_summary_text=""
  local bundled_manifest_text=""
  bundled_summary_text="$(tar -xOf "$bundle_path" "$summary_entry_in_tar")"
  bundled_manifest_text="$(tar -xOf "$bundle_path" "$manifest_entry_in_tar")"
  assert_contains "$bundled_summary_text" "package_bundle_artifacts_written:"
  assert_contains "$bundled_summary_text" "package_bundle_exit_code:"
  local stdout_summary_sha=""
  stdout_summary_sha="$(extract_field_value "$text" "summary_sha256")"
  if [[ -n "$stdout_summary_sha" && "$stdout_summary_sha" != "n/a" ]]; then
    assert_contains "$bundled_manifest_text" "$stdout_summary_sha"
  fi

  local stdout_bundle_written=""
  local stdout_bundle_exit=""
  local bundled_bundle_written=""
  local bundled_bundle_exit=""
  stdout_bundle_written="$(extract_field_value "$text" "package_bundle_artifacts_written")"
  stdout_bundle_exit="$(extract_field_value "$text" "package_bundle_exit_code")"
  bundled_bundle_written="$(extract_field_value "$bundled_summary_text" "package_bundle_artifacts_written")"
  bundled_bundle_exit="$(extract_field_value "$bundled_summary_text" "package_bundle_exit_code")"
  if [[ "$bundled_bundle_written" != "$stdout_bundle_written" ]]; then
    echo "expected bundled summary package_bundle_artifacts_written=$stdout_bundle_written, got $bundled_bundle_written" >&2
    exit 1
  fi
  if [[ "$bundled_bundle_exit" != "$stdout_bundle_exit" ]]; then
    echo "expected bundled summary package_bundle_exit_code=$stdout_bundle_exit, got $bundled_bundle_exit" >&2
    exit 1
  fi
}

assert_field_in() {
  local text="$1"
  local key="$2"
  shift 2
  local value
  value="$(extract_field_value "$text" "$key")"
  local expected=""
  for expected in "$@"; do
    if [[ "$value" == "$expected" ]]; then
      return 0
    fi
  done
  echo "expected $key to match one of: $*, got '$value'" >&2
  exit 1
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
  assert_contains "$snapshot_output" "execution_batch_sample_available: true"
  assert_contains "$snapshot_output" "submit_dynamic_cu_policy_enabled_by_route: {\"rpc\": 2}"
  assert_contains "$snapshot_output" "submit_dynamic_cu_hint_api_by_route: {\"rpc\": 1}"
  assert_contains "$snapshot_output" "submit_dynamic_cu_hint_rpc_by_route: {\"rpc\": 1}"
  assert_contains "$snapshot_output" "submit_dynamic_cu_price_applied_by_route: {\"rpc\": 1}"
  assert_contains "$snapshot_output" "submit_dynamic_cu_static_fallback_by_route: {\"rpc\": 1}"
  assert_contains "$snapshot_output" "submit_dynamic_tip_policy_enabled_by_route: {\"rpc\": 2}"
  assert_contains "$snapshot_output" "submit_dynamic_tip_applied_by_route: {\"rpc\": 1}"
  assert_contains "$snapshot_output" "submit_dynamic_tip_static_floor_by_route: {\"rpc\": 1}"

  local go_nogo_output
  go_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$go_nogo_output" "=== Execution Go/No-Go Summary ==="
  assert_contains "$go_nogo_output" "fee_decomposition_verdict: SKIP"
  assert_contains "$go_nogo_output" "route_profile_verdict: SKIP"
  assert_contains "$go_nogo_output" "primary_route: n/a"
  assert_contains "$go_nogo_output" "fallback_route: n/a"
  assert_contains "$go_nogo_output" "confirmed_orders_total: n/a"
  assert_contains "$go_nogo_output" "preflight_verdict: SKIP"
  assert_contains "$go_nogo_output" "execution_batch_sample_available: true"
  assert_contains "$go_nogo_output" "submit_attempted_by_route: {\"rpc\": 3}"
  assert_contains "$go_nogo_output" "submit_dynamic_cu_policy_enabled_by_route: {\"rpc\": 2}"
  assert_contains "$go_nogo_output" "submit_dynamic_cu_hint_api_by_route: {\"rpc\": 1}"
  assert_contains "$go_nogo_output" "submit_dynamic_cu_hint_rpc_by_route: {\"rpc\": 1}"
  assert_contains "$go_nogo_output" "submit_dynamic_tip_static_floor_by_route: {\"rpc\": 1}"
  assert_contains "$go_nogo_output" "dynamic_cu_hint_api_total: 1"
  assert_contains "$go_nogo_output" "dynamic_cu_hint_rpc_total: 1"
  assert_contains "$go_nogo_output" "dynamic_cu_hint_api_configured: false"
  assert_contains "$go_nogo_output" "dynamic_cu_hint_source_verdict: SKIP"
  assert_field_equals "$go_nogo_output" "dynamic_cu_hint_source_reason_code" "policy_disabled"
  assert_contains "$go_nogo_output" "dynamic_cu_policy_config_enabled: false"
  assert_contains "$go_nogo_output" "dynamic_cu_policy_verdict: SKIP"
  assert_contains "$go_nogo_output" "dynamic_tip_policy_config_enabled: false"
  assert_contains "$go_nogo_output" "dynamic_tip_policy_verdict: SKIP"
  assert_contains "$go_nogo_output" "go_nogo_require_jito_rpc_policy: false"
  assert_contains "$go_nogo_output" "jito_rpc_policy_verdict: SKIP"
  assert_field_equals "$go_nogo_output" "jito_rpc_policy_reason_code" "gate_disabled"
  assert_contains "$go_nogo_output" "go_nogo_require_fastlane_disabled: false"
  assert_contains "$go_nogo_output" "submit_fastlane_enabled: false"
  assert_contains "$go_nogo_output" "fastlane_feature_flag_verdict: SKIP"
  assert_field_equals "$go_nogo_output" "fastlane_feature_flag_reason_code" "gate_disabled"
  assert_contains "$go_nogo_output" "artifacts_written: false"
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

run_calibration_invalid_env_bool_case() {
  local db_path="$1"
  local config_path="$2"
  local invalid_output=""
  if invalid_output="$(
    DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO="maybe" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" 24 2>&1
  )"; then
    echo "expected execution_fee_calibration_report.sh to fail for invalid SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO token, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "invalid boolean setting for env SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO"
  assert_contains "$invalid_output" "got: maybe"
  echo "[ok] calibration strict env bool gate"
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
  assert_contains "$go_nogo_output" "primary_route: paper"
  assert_contains "$go_nogo_output" "fallback_route: <none>"
  assert_contains "$go_nogo_output" "confirmed_orders_total: 1"
  assert_contains "$go_nogo_output" "fallback_used_events:"
  assert_contains "$go_nogo_output" "hint_mismatch_events:"
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
  assert_contains "$snapshot_output" "execution_batch_sample_available: false"
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
  assert_contains "$output" "artifact_manifest:"
  assert_contains "$output" "summary_sha256:"
  assert_sha256_field "$output" "calibration_sha256"
  assert_sha256_field "$output" "snapshot_sha256"
  assert_sha256_field "$output" "preflight_sha256"
  assert_sha256_field "$output" "summary_sha256"
  assert_sha256_field "$output" "manifest_sha256"
  assert_sha256_field_matches_file "$output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$output" "manifest_sha256" "artifact_manifest"
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
  if ! ls "$artifacts_dir"/execution_go_nogo_manifest_*.txt >/dev/null 2>&1; then
    echo "expected go/no-go manifest artifact in $artifacts_dir" >&2
    exit 1
  fi

  local bundle_artifacts_dir="$TMP_DIR/go-nogo-artifacts-with-bundle"
  local bundle_output_dir="$TMP_DIR/go-nogo-bundles"
  local bundle_output
  bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$bundle_artifacts_dir" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_go_nogo_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$bundle_output_dir" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_field_equals "$bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  local go_nogo_bundle_path
  go_nogo_bundle_path="$(extract_field_value "$bundle_output" "package_bundle_path")"
  if [[ ! -f "$go_nogo_bundle_path" ]]; then
    echo "expected package bundle archive at $go_nogo_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"

  local missing_output_dir_output=""
  if missing_output_dir_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      PACKAGE_BUNDLE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_go_nogo_report.sh to fail when PACKAGE_BUNDLE_ENABLED=true and OUTPUT_DIR is missing" >&2
    exit 1
  else
    local missing_output_dir_exit_code=$?
    if [[ "$missing_output_dir_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for missing OUTPUT_DIR with PACKAGE_BUNDLE_ENABLED=true, got $missing_output_dir_exit_code" >&2
      echo "$missing_output_dir_output" >&2
      exit 1
    fi
  fi
  assert_contains "$missing_output_dir_output" "PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set"
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
  assert_field_equals "$output" "overall_go_nogo_reason_code" "readiness_gate_unknown"
  echo "[ok] go-no-go UNKNOWN precedence"
}

run_go_nogo_dynamic_hint_source_gate_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_PRIMARY_URL="https://priority-fee.example/api" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$output" "dynamic_cu_policy_config_enabled: true"
  assert_contains "$output" "dynamic_cu_policy_verdict: PASS"
  assert_contains "$output" "dynamic_cu_hint_api_configured: true"
  assert_contains "$output" "dynamic_cu_hint_api_total: 1"
  assert_contains "$output" "dynamic_cu_hint_rpc_total: 1"
  assert_contains "$output" "dynamic_cu_hint_source_verdict: PASS"
  assert_field_equals "$output" "dynamic_cu_hint_source_reason_code" "api_hints_observed"
  echo "[ok] go-no-go dynamic hint source gate"
}

run_go_nogo_jito_rpc_policy_gate_case() {
  local db_path="$1"
  local config_path="$2"
  local output
  output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="true" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$output" "go_nogo_require_jito_rpc_policy: true"
  assert_contains "$output" "jito_rpc_policy_verdict: WARN"
  assert_field_in "$output" "jito_rpc_policy_reason_code" "target_mismatch" "route_profile_not_pass"
  assert_contains "$output" "overall_go_nogo_verdict: NO_GO"
  assert_field_equals "$output" "overall_go_nogo_reason_code" "jito_policy_not_pass"

  local invalid_output=""
  if invalid_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="maybe" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_go_nogo_report.sh to fail for invalid GO_NOGO_REQUIRE_JITO_RPC_POLICY token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid GO_NOGO_REQUIRE_JITO_RPC_POLICY token, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "GO_NOGO_REQUIRE_JITO_RPC_POLICY must be a boolean token"
  assert_contains "$invalid_output" "got: maybe"
  echo "[ok] go-no-go strict jito/rpc policy gate"
}

run_go_nogo_fastlane_disabled_gate_case() {
  local db_path="$1"
  local config_path="$2"
  local blocked_output
  blocked_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$blocked_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$blocked_output" "submit_fastlane_enabled: true"
  assert_contains "$blocked_output" "fastlane_feature_flag_verdict: WARN"
  assert_field_equals "$blocked_output" "fastlane_feature_flag_reason_code" "fastlane_enabled"
  assert_contains "$blocked_output" "overall_go_nogo_verdict: NO_GO"
  assert_field_equals "$blocked_output" "overall_go_nogo_reason_code" "fastlane_policy_not_pass"

  local pass_output
  pass_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60
  )"
  assert_contains "$pass_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$pass_output" "submit_fastlane_enabled: false"
  assert_contains "$pass_output" "fastlane_feature_flag_verdict: PASS"
  assert_field_equals "$pass_output" "fastlane_feature_flag_reason_code" "fastlane_disabled"
  assert_contains "$pass_output" "overall_go_nogo_verdict: GO"

  local invalid_output=""
  if invalid_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="sometimes" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_go_nogo_report.sh to fail for invalid GO_NOGO_REQUIRE_FASTLANE_DISABLED token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid GO_NOGO_REQUIRE_FASTLANE_DISABLED token, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "GO_NOGO_REQUIRE_FASTLANE_DISABLED must be a boolean token"
  assert_contains "$invalid_output" "got: sometimes"

  local invalid_test_mode_output=""
  if invalid_test_mode_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="sometimes" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_go_nogo_report.sh to fail for invalid GO_NOGO_TEST_MODE token" >&2
    exit 1
  else
    local invalid_test_mode_exit_code=$?
    if [[ "$invalid_test_mode_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid GO_NOGO_TEST_MODE token, got $invalid_test_mode_exit_code" >&2
      echo "$invalid_test_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_test_mode_output" "GO_NOGO_TEST_MODE must be a boolean token"
  assert_contains "$invalid_test_mode_output" "got: sometimes"

  local invalid_execution_fastlane_output=""
  if invalid_execution_fastlane_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED="maybe" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_go_nogo_report.sh to fail for invalid SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED token" >&2
    exit 1
  else
    local invalid_execution_fastlane_exit_code=$?
    if [[ "$invalid_execution_fastlane_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED token, got $invalid_execution_fastlane_exit_code" >&2
      echo "$invalid_execution_fastlane_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_execution_fastlane_output" "invalid boolean setting for env SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED"
  assert_contains "$invalid_execution_fastlane_output" "got: maybe"
  echo "[ok] go-no-go strict fastlane-disabled gate"
}

run_windowed_signoff_report_case() {
  local db_path="$1"
  local paper_cfg="$2"
  local adapter_cfg="$3"

  local hold_output=""
  if hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$paper_cfg" \
      SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for windowed signoff helper in paper mode" >&2
    exit 1
  else
    local hold_exit_code=$?
    if [[ "$hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for windowed signoff helper, got $hold_exit_code" >&2
      echo "$hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$hold_output" "window_24h_fee_decomposition_verdict: SKIP"
  assert_contains "$hold_output" "window_24h_route_profile_verdict: SKIP"
  assert_contains "$hold_output" "windowed_signoff_require_dynamic_hint_source_pass: false"
  assert_contains "$hold_output" "windowed_signoff_require_dynamic_tip_policy_pass: false"
  assert_contains "$hold_output" "artifacts_written: false"
  assert_contains "$hold_output" "signoff_verdict: HOLD"
  assert_contains "$hold_output" "go_nogo_test_mode: false"

  local invalid_bool_output=""
  if invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$paper_cfg" \
      SERVICE="copybot-smoke-service" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="sometimes" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for windowed signoff helper invalid WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS token" >&2
    exit 1
  else
    local invalid_bool_exit_code=$?
    if [[ "$invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for windowed signoff helper invalid WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS token, got $invalid_bool_exit_code" >&2
      echo "$invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_bool_output" "signoff_verdict: NO_GO"
  assert_contains "$invalid_bool_output" "input_error: WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS must be a boolean token"

  local empty_windows_output=""
  if empty_windows_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$paper_cfg" \
      SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" "," "60" 2>&1
  )"; then
    echo "expected NO_GO exit for windowed signoff helper empty windows csv" >&2
    exit 1
  else
    local empty_windows_exit_code=$?
    if [[ "$empty_windows_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for windowed signoff helper empty windows csv, got $empty_windows_exit_code" >&2
      echo "$empty_windows_output" >&2
      exit 1
    fi
  fi
  assert_contains "$empty_windows_output" "signoff_verdict: NO_GO"
  assert_contains "$empty_windows_output" "input_error: no valid windows parsed from WINDOWS_CSV=,"

  local hard_block_nogo_output=""
  if hard_block_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$paper_cfg" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for windowed signoff helper when nested overall go/no-go is not GO" >&2
    exit 1
  else
    local hard_block_nogo_exit_code=$?
    if [[ "$hard_block_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for windowed signoff helper, got $hard_block_nogo_exit_code" >&2
      echo "$hard_block_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$hard_block_nogo_output" "window_24h_overall_go_nogo_verdict: NO_GO"
  assert_contains "$hard_block_nogo_output" "window_24h_fee_decomposition_verdict: PASS"
  assert_contains "$hard_block_nogo_output" "window_24h_route_profile_verdict: PASS"
  assert_contains "$hard_block_nogo_output" "window_hard_block_count: 1"
  assert_contains "$hard_block_nogo_output" "artifacts_written: false"
  assert_contains "$hard_block_nogo_output" "signoff_verdict: NO_GO"

  local strict_hold_output=""
  if strict_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$paper_cfg" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="true" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for windowed signoff helper when strict jito->rpc gate is enabled in non-adapter mode" >&2
    exit 1
  else
    local strict_hold_exit_code=$?
    if [[ "$strict_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for strict jito/rpc policy windowed signoff case, got $strict_hold_exit_code" >&2
      echo "$strict_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$strict_hold_output" "window_24h_overall_go_nogo_verdict: HOLD"
  assert_contains "$strict_hold_output" "window_24h_fee_decomposition_verdict: PASS"
  assert_contains "$strict_hold_output" "window_24h_route_profile_verdict: PASS"
  assert_contains "$strict_hold_output" "go_nogo_require_jito_rpc_policy: true"
  assert_contains "$strict_hold_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$strict_hold_output" "window_24h_jito_rpc_policy_verdict: SKIP"
  assert_field_equals "$strict_hold_output" "window_24h_jito_rpc_policy_reason_code" "requires_adapter_mode"
  assert_contains "$strict_hold_output" "window_24h_fastlane_feature_flag_verdict: SKIP"
  assert_field_equals "$strict_hold_output" "window_24h_fastlane_feature_flag_reason_code" "requires_adapter_mode"
  assert_contains "$strict_hold_output" "window_hard_block_count: 0"
  assert_contains "$strict_hold_output" "artifacts_written: false"
  assert_contains "$strict_hold_output" "signoff_verdict: HOLD"

  local artifacts_dir="$TMP_DIR/windowed-signoff-artifacts"
  local go_output
  go_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$adapter_cfg" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$artifacts_dir" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60
  )"
  assert_contains "$go_output" "window_24h_fee_decomposition_verdict: PASS"
  assert_contains "$go_output" "window_24h_route_profile_verdict: PASS"
  assert_contains "$go_output" "windowed_signoff_require_dynamic_hint_source_pass: false"
  assert_contains "$go_output" "windowed_signoff_require_dynamic_tip_policy_pass: false"
  assert_contains "$go_output" "artifacts_written: true"
  assert_contains "$go_output" "signoff_verdict: GO"
  assert_contains "$go_output" "artifact_summary:"
  assert_contains "$go_output" "artifact_manifest:"
  assert_contains "$go_output" "window_24h_capture_path:"
  assert_contains "$go_output" "window_24h_capture_sha256:"
  assert_contains "$go_output" "window_24h_go_nogo_artifact_manifest:"
  assert_contains "$go_output" "window_24h_go_nogo_calibration_sha256:"
  assert_contains "$go_output" "window_24h_go_nogo_snapshot_sha256:"
  assert_contains "$go_output" "window_24h_go_nogo_preflight_sha256:"
  assert_contains "$go_output" "window_24h_go_nogo_summary_sha256:"
  assert_field_equals "$go_output" "window_24h_go_nogo_artifacts_written" "true"
  assert_field_equals "$go_output" "window_24h_go_nogo_nested_package_bundle_enabled" "false"
  assert_sha256_field "$go_output" "summary_sha256"
  assert_sha256_field "$go_output" "window_24h_capture_sha256"
  assert_sha256_field "$go_output" "window_24h_go_nogo_calibration_sha256"
  assert_sha256_field "$go_output" "window_24h_go_nogo_snapshot_sha256"
  assert_sha256_field "$go_output" "window_24h_go_nogo_preflight_sha256"
  assert_sha256_field "$go_output" "window_24h_go_nogo_summary_sha256"
  if ! ls "$artifacts_dir"/execution_windowed_signoff_summary_*.txt >/dev/null 2>&1; then
    echo "expected windowed signoff summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_windowed_signoff_manifest_*.txt >/dev/null 2>&1; then
    echo "expected windowed signoff manifest artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/window_24h/execution_go_nogo_captured_*.txt >/dev/null 2>&1; then
    echo "expected captured go/no-go artifact for 24h window in $artifacts_dir/window_24h" >&2
    exit 1
  fi

  local bundle_artifacts_dir="$TMP_DIR/windowed-signoff-artifacts-with-bundle"
  local bundle_output_dir="$TMP_DIR/windowed-signoff-bundles"
  local bundle_output
  bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$adapter_cfg" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$bundle_artifacts_dir" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_windowed_signoff_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$bundle_output_dir" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60
  )"
  assert_field_equals "$bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$bundle_output" "window_24h_go_nogo_nested_package_bundle_enabled" "false"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_sha256_field "$bundle_output" "manifest_sha256"
  assert_sha256_field_matches_file "$bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  local windowed_bundle_path
  windowed_bundle_path="$(extract_field_value "$bundle_output" "package_bundle_path")"
  if [[ ! -f "$windowed_bundle_path" ]]; then
    echo "expected package bundle archive at $windowed_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"
  local windowed_go_nogo_capture_path
  windowed_go_nogo_capture_path="$(extract_field_value "$bundle_output" "window_24h_capture_path")"
  if [[ -z "$windowed_go_nogo_capture_path" || "$windowed_go_nogo_capture_path" == "n/a" || ! -f "$windowed_go_nogo_capture_path" ]]; then
    echo "expected windowed signoff nested go/no-go capture artifact at $windowed_go_nogo_capture_path" >&2
    exit 1
  fi
  local windowed_go_nogo_capture_text
  windowed_go_nogo_capture_text="$(cat "$windowed_go_nogo_capture_path")"
  assert_contains "$windowed_go_nogo_capture_text" "package_bundle_enabled: false"

  local missing_output_dir_output=""
  if missing_output_dir_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$adapter_cfg" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      PACKAGE_BUNDLE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected execution_windowed_signoff_report.sh to fail when PACKAGE_BUNDLE_ENABLED=true and OUTPUT_DIR is missing" >&2
    exit 1
  else
    local missing_output_dir_exit_code=$?
    if [[ "$missing_output_dir_exit_code" -ne 3 ]]; then
      echo "expected exit code 3 for missing OUTPUT_DIR with PACKAGE_BUNDLE_ENABLED=true, got $missing_output_dir_exit_code" >&2
      echo "$missing_output_dir_output" >&2
      exit 1
    fi
  fi
  assert_contains "$missing_output_dir_output" "input_error: PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set"

  local dynamic_fake_bin_dir="$TMP_DIR/fake-bin-dynamic-hint-warn"
  mkdir -p "$dynamic_fake_bin_dir"
  cp "$FAKE_BIN_DIR/journalctl" "$dynamic_fake_bin_dir/journalctl"
  python3 - "$dynamic_fake_bin_dir/journalctl" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
text = text.replace('"submit_dynamic_cu_hint_api_by_route":{"rpc":1},', "")
path.write_text(text)
PY
  chmod +x "$dynamic_fake_bin_dir/journalctl"

  local dynamic_gate_hold_output=""
  if dynamic_gate_hold_output="$(
    PATH="$dynamic_fake_bin_dir:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$adapter_cfg" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_PRIMARY_URL="https://priority-fee.example/api" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="true" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for windowed signoff helper when dynamic hint source gate is required and not PASS" >&2
    exit 1
  else
    local dynamic_gate_hold_exit_code=$?
    if [[ "$dynamic_gate_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for required dynamic hint source gate, got $dynamic_gate_hold_exit_code" >&2
      echo "$dynamic_gate_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$dynamic_gate_hold_output" "windowed_signoff_require_dynamic_hint_source_pass: true"
  assert_contains "$dynamic_gate_hold_output" "window_24h_dynamic_cu_policy_config_enabled: true"
  assert_contains "$dynamic_gate_hold_output" "window_24h_dynamic_cu_hint_source_verdict: WARN"
  assert_contains "$dynamic_gate_hold_output" "signoff_verdict: HOLD"

  local tip_fake_bin_dir="$TMP_DIR/fake-bin-dynamic-tip-warn"
  mkdir -p "$tip_fake_bin_dir"
  cp "$FAKE_BIN_DIR/journalctl" "$tip_fake_bin_dir/journalctl"
  python3 - "$tip_fake_bin_dir/journalctl" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
text = text.replace('"submit_dynamic_tip_applied_by_route":{"rpc":1},', "")
path.write_text(text)
PY
  chmod +x "$tip_fake_bin_dir/journalctl"

  local tip_gate_hold_output=""
  if tip_gate_hold_output="$(
    PATH="$tip_fake_bin_dir:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$adapter_cfg" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_ENABLED="true" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="true" \
      bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for windowed signoff helper when dynamic tip gate is required and not PASS" >&2
    exit 1
  else
    local tip_gate_hold_exit_code=$?
    if [[ "$tip_gate_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for required dynamic tip gate, got $tip_gate_hold_exit_code" >&2
      echo "$tip_gate_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$tip_gate_hold_output" "windowed_signoff_require_dynamic_tip_policy_pass: true"
  assert_contains "$tip_gate_hold_output" "window_24h_dynamic_tip_policy_config_enabled: true"
  assert_contains "$tip_gate_hold_output" "window_24h_dynamic_tip_policy_verdict: WARN"
  assert_contains "$tip_gate_hold_output" "signoff_verdict: HOLD"
  echo "[ok] execution windowed signoff helper"
}

run_execution_route_fee_signoff_case() {
  local db_path="$1"
  local config_path="$2"
  local strict_config_path="$3"
  local hold_output
  if hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected HOLD exit for route/fee signoff helper on paper config" >&2
    exit 1
  else
    hold_status=$?
  fi
  if [[ "$hold_status" -ne 2 ]]; then
    echo "expected HOLD exit code 2 from route/fee signoff helper, got $hold_status" >&2
    exit 1
  fi
  assert_contains "$hold_output" "=== Execution Route/Fee Signoff Summary ==="
  assert_contains "$hold_output" "window_24h_overall_go_nogo_verdict: HOLD"
  assert_field_equals "$hold_output" "window_24h_overall_go_nogo_reason_code" "readiness_gate_skip"
  assert_contains "$hold_output" "window_24h_route_profile_verdict: SKIP"
  assert_contains "$hold_output" "window_24h_fee_decomposition_verdict: SKIP"
  assert_contains "$hold_output" "window_24h_route_verdict_parity: true"
  assert_contains "$hold_output" "window_24h_fee_verdict_parity: true"
  assert_field_equals "$hold_output" "window_24h_go_nogo_artifacts_written" "false"
  assert_field_equals "$hold_output" "window_24h_go_nogo_nested_package_bundle_enabled" "false"
  assert_contains "$hold_output" "go_nogo_require_jito_rpc_policy: false"
  assert_contains "$hold_output" "go_nogo_require_fastlane_disabled: false"
  assert_contains "$hold_output" "signoff_verdict: HOLD"
  assert_contains "$hold_output" "artifacts_written: false"

  local artifacts_dir="$TMP_DIR/route-fee-signoff-artifacts"
  local export_output
  if export_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$artifacts_dir" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "1,24" "60" 2>&1
  )"; then
    echo "expected HOLD exit for route/fee signoff helper export case on paper config" >&2
    exit 1
  else
    export_status=$?
  fi
  if [[ "$export_status" -ne 2 ]]; then
    echo "expected HOLD exit code 2 from route/fee signoff helper export case, got $export_status" >&2
    exit 1
  fi
  assert_contains "$export_output" "window_count: 2"
  assert_contains "$export_output" "artifacts_written: true"
  assert_contains "$export_output" "artifact_summary:"
  assert_contains "$export_output" "artifact_manifest:"
  assert_contains "$export_output" "window_1h_go_nogo_capture_path:"
  assert_contains "$export_output" "window_1h_calibration_capture_path:"
  assert_contains "$export_output" "window_24h_go_nogo_capture_path:"
  assert_contains "$export_output" "window_24h_calibration_capture_path:"
  assert_field_equals "$export_output" "window_1h_go_nogo_artifacts_written" "true"
  assert_field_equals "$export_output" "window_24h_go_nogo_artifacts_written" "true"
  assert_field_equals "$export_output" "window_1h_go_nogo_nested_package_bundle_enabled" "false"
  assert_field_equals "$export_output" "window_24h_go_nogo_nested_package_bundle_enabled" "false"
  assert_sha256_field "$export_output" "summary_sha256"
  assert_sha256_field "$export_output" "window_1h_go_nogo_capture_sha256"
  assert_sha256_field "$export_output" "window_1h_calibration_capture_sha256"
  assert_sha256_field "$export_output" "window_24h_go_nogo_capture_sha256"
  assert_sha256_field "$export_output" "window_24h_calibration_capture_sha256"
  if ! ls "$artifacts_dir"/execution_route_fee_signoff_summary_*.txt >/dev/null 2>&1; then
    echo "expected route/fee signoff summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_route_fee_signoff_manifest_*.txt >/dev/null 2>&1; then
    echo "expected route/fee signoff manifest artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/window_1h/execution_go_nogo_captured_*.txt >/dev/null 2>&1; then
    echo "expected 1h go-no-go capture artifact in $artifacts_dir/window_1h" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/window_1h/execution_fee_calibration_captured_*.txt >/dev/null 2>&1; then
    echo "expected 1h calibration capture artifact in $artifacts_dir/window_1h" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/window_24h/execution_go_nogo_captured_*.txt >/dev/null 2>&1; then
    echo "expected 24h go-no-go capture artifact in $artifacts_dir/window_24h" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/window_24h/execution_fee_calibration_captured_*.txt >/dev/null 2>&1; then
    echo "expected 24h calibration capture artifact in $artifacts_dir/window_24h" >&2
    exit 1
  fi
  manifest_path="$(extract_field_value "$export_output" "artifact_manifest")"
  if [[ -z "$manifest_path" || "$manifest_path" == "n/a" ]]; then
    echo "expected route/fee signoff manifest path in export output" >&2
    exit 1
  fi
  if ! grep -Fq "window_1h/execution_go_nogo_captured_" "$manifest_path"; then
    echo "expected window-qualified 1h go-no-go capture entry in manifest $manifest_path" >&2
    exit 1
  fi
  if ! grep -Fq "window_24h/execution_go_nogo_captured_" "$manifest_path"; then
    echo "expected window-qualified 24h go-no-go capture entry in manifest $manifest_path" >&2
    exit 1
  fi
  if ! grep -Fq "window_1h/execution_fee_calibration_captured_" "$manifest_path"; then
    echo "expected window-qualified 1h calibration capture entry in manifest $manifest_path" >&2
    exit 1
  fi
  if ! grep -Fq "window_24h/execution_fee_calibration_captured_" "$manifest_path"; then
    echo "expected window-qualified 24h calibration capture entry in manifest $manifest_path" >&2
    exit 1
  fi

  local bundle_artifacts_dir="$TMP_DIR/route-fee-signoff-artifacts-with-bundle"
  local bundle_output_dir="$TMP_DIR/route-fee-signoff-bundles"
  local bundle_output
  bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$bundle_artifacts_dir" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_route_fee_signoff_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$bundle_output_dir" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60"
  )"
  assert_field_equals "$bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$bundle_output" "window_24h_go_nogo_artifacts_written" "true"
  assert_field_equals "$bundle_output" "window_24h_go_nogo_nested_package_bundle_enabled" "false"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_sha256_field "$bundle_output" "manifest_sha256"
  assert_sha256_field_matches_file "$bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  local route_fee_bundle_path
  route_fee_bundle_path="$(extract_field_value "$bundle_output" "package_bundle_path")"
  if [[ ! -f "$route_fee_bundle_path" ]]; then
    echo "expected package bundle archive at $route_fee_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"

  local missing_output_dir_output=""
  if missing_output_dir_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      PACKAGE_BUNDLE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected execution_route_fee_signoff_report.sh to fail when PACKAGE_BUNDLE_ENABLED=true and OUTPUT_DIR is missing" >&2
    exit 1
  else
    local missing_output_dir_exit_code=$?
    if [[ "$missing_output_dir_exit_code" -ne 3 ]]; then
      echo "expected exit code 3 for missing OUTPUT_DIR with PACKAGE_BUNDLE_ENABLED=true, got $missing_output_dir_exit_code" >&2
      echo "$missing_output_dir_output" >&2
      exit 1
    fi
  fi
  assert_contains "$missing_output_dir_output" "input_error: PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set"

  local invalid_output
  if invalid_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24,invalid" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for route/fee signoff helper invalid windows" >&2
    exit 1
  else
    invalid_status=$?
  fi
  if [[ "$invalid_status" -ne 3 ]]; then
    echo "expected NO_GO exit code 3 from route/fee signoff helper invalid windows, got $invalid_status" >&2
    exit 1
  fi
  assert_contains "$invalid_output" "signoff_verdict: NO_GO"
  assert_contains "$invalid_output" "input_error: window token must be an integer (got: invalid)"

  local empty_windows_output
  if empty_windows_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "," "60" 2>&1
  )"; then
    echo "expected NO_GO exit for route/fee signoff helper empty windows" >&2
    exit 1
  else
    local empty_windows_status=$?
  fi
  if [[ "$empty_windows_status" -ne 3 ]]; then
    echo "expected NO_GO exit code 3 from route/fee signoff helper empty windows, got $empty_windows_status" >&2
    exit 1
  fi
  assert_contains "$empty_windows_output" "signoff_verdict: NO_GO"
  assert_contains "$empty_windows_output" "input_error: no valid windows parsed from WINDOWS_CSV=,"

  local invalid_bool_output
  if invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="maybe" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for route/fee signoff helper invalid GO_NOGO_REQUIRE_JITO_RPC_POLICY token" >&2
    exit 1
  else
    invalid_bool_status=$?
  fi
  if [[ "$invalid_bool_status" -ne 3 ]]; then
    echo "expected NO_GO exit code 3 for route/fee signoff helper invalid GO_NOGO_REQUIRE_JITO_RPC_POLICY token, got $invalid_bool_status" >&2
    exit 1
  fi
  assert_contains "$invalid_bool_output" "signoff_verdict: NO_GO"
  assert_contains "$invalid_bool_output" "input_error: GO_NOGO_REQUIRE_JITO_RPC_POLICY must be a boolean token"

  local override_without_test_mode_output
  if override_without_test_mode_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_TEST_MODE="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for route/fee signoff helper when test override is set without GO_NOGO_TEST_MODE=true" >&2
    exit 1
  else
    override_without_test_mode_status=$?
  fi
  if [[ "$override_without_test_mode_status" -ne 3 ]]; then
    echo "expected NO_GO exit code 3 from route/fee signoff helper override-without-test-mode case, got $override_without_test_mode_status" >&2
    exit 1
  fi
  assert_contains "$override_without_test_mode_output" "signoff_verdict: NO_GO"
  assert_contains "$override_without_test_mode_output" "input_error: route fee signoff test verdict override requires GO_NOGO_TEST_MODE=true"

  local strict_nogo_output
  if strict_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="true" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for route/fee signoff helper when strict jito/rpc policy is enforced and fails" >&2
    exit 1
  else
    strict_nogo_status=$?
  fi
  if [[ "$strict_nogo_status" -ne 3 ]]; then
    echo "expected NO_GO exit code 3 for strict jito/rpc policy route/fee signoff case, got $strict_nogo_status" >&2
    exit 1
  fi
  assert_contains "$strict_nogo_output" "go_nogo_require_jito_rpc_policy: true"
  assert_contains "$strict_nogo_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$strict_nogo_output" "window_24h_overall_go_nogo_verdict: NO_GO"
  assert_field_equals "$strict_nogo_output" "window_24h_overall_go_nogo_reason_code" "jito_policy_not_pass"
  assert_contains "$strict_nogo_output" "signoff_verdict: NO_GO"
  assert_field_equals "$strict_nogo_output" "signoff_reason_code" "window_hard_block"

  local final_hold_output=""
  if final_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/route-fee-final-hold" \
      bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected HOLD exit for final route/fee package helper on paper config" >&2
    exit 1
  else
    local final_hold_status=$?
    if [[ "$final_hold_status" -ne 2 ]]; then
      echo "expected HOLD exit code 2 from final route/fee package helper, got $final_hold_status" >&2
      echo "$final_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$final_hold_output" "=== Execution Route/Fee Final Evidence Package ==="
  assert_contains "$final_hold_output" "signoff_verdict: HOLD"
  assert_contains "$final_hold_output" "final_route_fee_package_verdict: HOLD"
  assert_field_equals "$final_hold_output" "signoff_artifacts_written" "true"
  assert_field_equals "$final_hold_output" "signoff_nested_package_bundle_enabled" "false"
  assert_sha256_field "$final_hold_output" "summary_sha256"
  assert_sha256_field "$final_hold_output" "signoff_capture_sha256"
  assert_sha256_field "$final_hold_output" "manifest_sha256"

  local final_invalid_bool_output=""
  if final_invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/route-fee-final-invalid-bool" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="sometimes" \
      bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "24" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for final route/fee package helper invalid GO_NOGO_REQUIRE_FASTLANE_DISABLED token" >&2
    exit 1
  else
    local final_invalid_bool_status=$?
    if [[ "$final_invalid_bool_status" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 from final route/fee package helper invalid GO_NOGO_REQUIRE_FASTLANE_DISABLED token, got $final_invalid_bool_status" >&2
      echo "$final_invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$final_invalid_bool_output" "input_error_count: 1"
  assert_contains "$final_invalid_bool_output" "input_error: GO_NOGO_REQUIRE_FASTLANE_DISABLED must be a boolean token"
  assert_field_equals "$final_invalid_bool_output" "signoff_reason_code" "input_error"
  assert_field_equals "$final_invalid_bool_output" "final_route_fee_package_reason_code" "input_error"
  assert_field_equals "$final_invalid_bool_output" "signoff_artifacts_written" "false"

  local final_go_output
  final_go_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/route-fee-final-go" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "24" "60"
  )"
  assert_contains "$final_go_output" "signoff_verdict: GO"
  assert_field_equals "$final_go_output" "signoff_reason_code" "test_override"
  assert_field_equals "$final_go_output" "signoff_nested_package_bundle_enabled" "false"
  assert_contains "$final_go_output" "final_route_fee_package_verdict: GO"
  assert_field_equals "$final_go_output" "final_route_fee_package_reason_code" "test_override"

  local final_bundle_output
  final_bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/route-fee-final-go-bundle" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_route_fee_final_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$TMP_DIR/route-fee-final-bundles" \
      bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "24" "60"
  )"
  assert_field_equals "$final_bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$final_bundle_output" "signoff_nested_package_bundle_enabled" "false"
  assert_sha256_field "$final_bundle_output" "package_bundle_sha256"
  assert_sha256_field "$final_bundle_output" "summary_sha256"
  assert_sha256_field "$final_bundle_output" "manifest_sha256"
  assert_sha256_field_matches_file "$final_bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$final_bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$final_bundle_output" "package_bundle_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_contents_manifest"
  local final_route_fee_bundle_path
  final_route_fee_bundle_path="$(extract_field_value "$final_bundle_output" "package_bundle_path")"
  if [[ ! -f "$final_route_fee_bundle_path" ]]; then
    echo "expected package bundle archive at $final_route_fee_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$final_bundle_output"
  local final_route_fee_signoff_capture_path
  final_route_fee_signoff_capture_path="$(extract_field_value "$final_bundle_output" "artifact_signoff_capture")"
  if [[ -z "$final_route_fee_signoff_capture_path" || ! -f "$final_route_fee_signoff_capture_path" ]]; then
    echo "expected route/fee final nested signoff capture artifact at $final_route_fee_signoff_capture_path" >&2
    exit 1
  fi
  local final_route_fee_signoff_capture_text
  final_route_fee_signoff_capture_text="$(cat "$final_route_fee_signoff_capture_path")"
  assert_contains "$final_route_fee_signoff_capture_text" "package_bundle_enabled: false"

  local final_nogo_output=""
  if final_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$strict_config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/route-fee-final-nogo" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "24,invalid" "60" 2>&1
  )"; then
    echo "expected NO_GO exit for final route/fee package helper invalid windows" >&2
    exit 1
  else
    local final_nogo_status=$?
    if [[ "$final_nogo_status" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 from final route/fee package helper invalid windows, got $final_nogo_status" >&2
      echo "$final_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$final_nogo_output" "signoff_verdict: NO_GO"
  assert_field_equals "$final_nogo_output" "signoff_reason_code" "input_error"
  assert_contains "$final_nogo_output" "final_route_fee_package_verdict: NO_GO"
  assert_field_equals "$final_nogo_output" "final_route_fee_package_reason_code" "input_error"

  echo "[ok] execution route/fee signoff helper"
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
  assert_field_equals "$output" "overall_go_nogo_reason_code" "preflight_fail"
  echo "[ok] go-no-go preflight fail gate"
}

run_adapter_preflight_case() {
  local db_path="$1"
  local pass_cfg="$TMP_DIR/adapter-preflight-pass.toml"
  local fail_cfg="$TMP_DIR/adapter-preflight-fail.toml"
  local empty_allowlist_cfg="$TMP_DIR/adapter-preflight-empty-allowlist.toml"
  local missing_map_cfg="$TMP_DIR/adapter-preflight-missing-map.toml"
  local invalid_route_order_cfg="$TMP_DIR/adapter-preflight-invalid-route-order.toml"
  local fastlane_cfg="$TMP_DIR/adapter-preflight-fastlane.toml"
  local missing_secret_cfg="$TMP_DIR/adapter-preflight-missing-secret.toml"
  local tip_above_max_cfg="$TMP_DIR/adapter-preflight-tip-above-max.toml"
  local default_cu_limit_too_low_cfg="$TMP_DIR/adapter-preflight-default-cu-limit-too-low.toml"
  local route_price_exceeds_pretrade_cfg="$TMP_DIR/adapter-preflight-route-price-exceeds-pretrade.toml"
  local secrets_dir="$TMP_DIR/secrets"
  local missing_map_output
  local invalid_route_order_output
  local missing_secret_output
  local env_override_output
  local fastlane_disabled_output
  local fastlane_enabled_output
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

  local env_invalid_bool_output
  if env_invalid_bool_output="$(
    CONFIG_PATH="$pass_cfg" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO="sometimes" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for invalid strict echo bool token" >&2
    exit 1
  fi
  assert_contains "$env_invalid_bool_output" "preflight_verdict: FAIL"
  assert_contains "$env_invalid_bool_output" "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO must be a boolean token"

  local execution_enabled_invalid_bool_output
  if execution_enabled_invalid_bool_output="$(
    CONFIG_PATH="$pass_cfg" \
      SOLANA_COPY_BOT_EXECUTION_ENABLED="sometimes" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for invalid execution.enabled bool token override" >&2
    exit 1
  fi
  assert_contains "$execution_enabled_invalid_bool_output" "preflight_verdict: FAIL"
  assert_contains "$execution_enabled_invalid_bool_output" "SOLANA_COPY_BOT_EXECUTION_ENABLED must be a boolean token"

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

  write_config_empty_allowlist "$empty_allowlist_cfg" "$db_path"
  local empty_allowlist_output
  if empty_allowlist_output="$(
    CONFIG_PATH="$empty_allowlist_cfg" \
      SOLANA_COPY_BOT_EXECUTION_ENABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_MODE="adapter_submit_confirm" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure for empty submit_allowed_routes in adapter mode" >&2
    exit 1
  fi
  assert_contains "$empty_allowlist_output" "preflight_verdict: FAIL"
  assert_contains "$empty_allowlist_output" "execution.submit_allowed_routes must not be empty in adapter_submit_confirm mode"

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

  write_config_adapter_preflight_fastlane_routes "$fastlane_cfg" "$db_path"
  if fastlane_disabled_output="$(
    CONFIG_PATH="$fastlane_cfg" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    echo "expected adapter preflight failure when fastlane route is configured while submit_fastlane_enabled=false" >&2
    exit 1
  fi
  assert_contains "$fastlane_disabled_output" "preflight_verdict: FAIL"
  assert_contains "$fastlane_disabled_output" "execution.submit_fastlane_enabled must be true"

  fastlane_enabled_output="$(
    CONFIG_PATH="$fastlane_cfg" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh"
  )"
  assert_contains "$fastlane_enabled_output" "preflight_verdict: PASS"
  assert_contains "$fastlane_enabled_output" "submit_fastlane_enabled: true"

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

  echo "[ok] adapter preflight pass/fail + empty-allowlist guard + route-policy + route-order + secret diagnostics + numeric parity guards"
}

run_executor_preflight_case() {
  local db_path="$1"
  local config_path="$TMP_DIR/executor-preflight.toml"
  local executor_env_path="$TMP_DIR/executor-preflight.env"
  local adapter_env_path="$TMP_DIR/adapter-preflight.env"
  local artifacts_dir="$TMP_DIR/executor-preflight-artifacts"
  local fake_curl_bin="$TMP_DIR/fake-curl-preflight"
  local auth_token="executor-smoke-token"
  local port="18090"

  write_config_adapter_preflight_pass "$config_path" "$db_path"
  write_executor_env_preflight "$executor_env_path" "$port" "$auth_token"
  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  write_fake_curl_executor_preflight "$fake_curl_bin" "$auth_token"

  local pass_output
  pass_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      OUTPUT_DIR="$artifacts_dir" \
      HTTP_TIMEOUT_SEC="3" \
      bash "$ROOT_DIR/tools/executor_preflight.sh"
  )"
  assert_contains "$pass_output" "=== Executor Preflight ==="
  assert_contains "$pass_output" "preflight_verdict: PASS"
  assert_field_equals "$pass_output" "preflight_reason_code" "checks_passed"
  assert_field_equals "$pass_output" "expected_send_rpc_enabled_routes_csv" "rpc,jito"
  assert_field_equals "$pass_output" "expected_send_rpc_fallback_routes_csv" "jito"
  assert_field_equals "$pass_output" "health_send_rpc_enabled_routes_csv" "rpc,jito"
  assert_field_equals "$pass_output" "health_send_rpc_fallback_routes_csv" "jito"
  assert_field_equals "$pass_output" "auth_probe_with_auth_http_status" "200"
  assert_contains "$pass_output" "artifacts_written: true"
  assert_sha256_field "$pass_output" "summary_sha256"
  assert_sha256_field "$pass_output" "manifest_sha256"
  assert_sha256_field_matches_file "$pass_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$pass_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$artifacts_dir"/executor_preflight_summary_*.txt >/dev/null 2>&1; then
    echo "expected executor preflight summary artifact file to be written" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/executor_preflight_manifest_*.txt >/dev/null 2>&1; then
    echo "expected executor preflight manifest artifact file to be written" >&2
    exit 1
  fi

  local invalid_enabled_output
  if invalid_enabled_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      SOLANA_COPY_BOT_EXECUTION_ENABLED="sometimes" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure for invalid SOLANA_COPY_BOT_EXECUTION_ENABLED bool token" >&2
    exit 1
  fi
  assert_contains "$invalid_enabled_output" "preflight_verdict: FAIL"
  assert_field_equals "$invalid_enabled_output" "preflight_reason_code" "config_error"
  assert_contains "$invalid_enabled_output" "SOLANA_COPY_BOT_EXECUTION_ENABLED must be boolean token"

  write_adapter_env_preflight "$adapter_env_path" "$port" "mismatch-token"
  local fail_output
  if fail_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure for adapter auth token mismatch" >&2
    exit 1
  fi
  assert_contains "$fail_output" "preflight_verdict: FAIL"
  assert_field_equals "$fail_output" "preflight_reason_code" "contract_checks_failed"
  assert_contains "$fail_output" "adapter auth token mismatch"

  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  local with_auth_5xx_output
  if with_auth_5xx_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      FAKE_EXECUTOR_SIMULATE_WITH_AUTH_STATUS="503" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure when /simulate with valid bearer returns non-200" >&2
    exit 1
  fi
  assert_contains "$with_auth_5xx_output" "preflight_verdict: FAIL"
  assert_contains "$with_auth_5xx_output" "auth probe with configured bearer token must return HTTP 200, got 503"

  local send_rpc_health_mismatch_output
  if send_rpc_health_mismatch_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_CSV="rpc" \
      FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_CSV="" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure for health send-rpc topology mismatch" >&2
    exit 1
  fi
  assert_contains "$send_rpc_health_mismatch_output" "preflight_verdict: FAIL"
  assert_contains "$send_rpc_health_mismatch_output" "health send-rpc enabled routes missing executor route=jito"

  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token" "paper,rpc,jito,fastlane"
  local allowlist_mismatch_output
  if allowlist_mismatch_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure for adapter route allowlist not subset of executor allowlist" >&2
    exit 1
  fi
  assert_contains "$allowlist_mismatch_output" "preflight_verdict: FAIL"
  assert_contains "$allowlist_mismatch_output" "adapter route allowlist includes route=fastlane that is not present in executor allowlist"

  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  write_executor_env_preflight "$executor_env_path" "$port" "$auth_token" "true"
  local unauth_mismatch_output
  if unauth_mismatch_output="$(
    PATH="$fake_curl_bin:$PATH" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      HTTP_TIMEOUT_SEC="3" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    echo "expected executor preflight failure when unauth mode is enabled but endpoint still requires auth" >&2
    exit 1
  fi
  assert_contains "$unauth_mismatch_output" "preflight_verdict: FAIL"
  assert_contains "$unauth_mismatch_output" "COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true but simulate endpoint still requires auth"

  echo "[ok] executor preflight helper"
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
  assert_contains "$pass_output" "artifacts_written: true"
  assert_contains "$pass_output" "artifact_report:"
  assert_contains "$pass_output" "artifact_manifest:"
  assert_contains "$pass_output" "report_sha256:"
  assert_sha256_field "$pass_output" "report_sha256"
  assert_sha256_field "$pass_output" "manifest_sha256"
  assert_sha256_field_matches_file "$pass_output" "report_sha256" "artifact_report"
  assert_sha256_field_matches_file "$pass_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$artifacts_dir"/adapter_secret_rotation_report_*.txt >/dev/null 2>&1; then
    echo "expected adapter secret rotation artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/adapter_secret_rotation_manifest_*.txt >/dev/null 2>&1; then
    echo "expected adapter secret rotation manifest artifact in $artifacts_dir" >&2
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
  assert_contains "$duplicate_key_output" "artifacts_written: false"

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

  local invalid_allow_unauth_env_path="$TMP_DIR/adapter-rotation-invalid-allow-unauth.env"
  cp "$env_path" "$invalid_allow_unauth_env_path"
  echo 'COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED="maybe"' >>"$invalid_allow_unauth_env_path"
  local invalid_allow_unauth_output=""
  if invalid_allow_unauth_output="$(
    ADAPTER_ENV_PATH="$invalid_allow_unauth_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for invalid COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED bool token" >&2
    exit 1
  else
    local invalid_allow_unauth_exit_code=$?
    if [[ "$invalid_allow_unauth_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for invalid COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED token, got $invalid_allow_unauth_exit_code" >&2
      echo "$invalid_allow_unauth_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_allow_unauth_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$invalid_allow_unauth_output" "COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED must be a boolean token"

  local no_file_keys_env_path="$TMP_DIR/adapter-rotation-no-file-keys.env"
  cat >"$no_file_keys_env_path" <<'EOF'
COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true
EOF
  local no_file_keys_output
  no_file_keys_output="$(
    ADAPTER_ENV_PATH="$no_file_keys_env_path" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh"
  )"
  assert_contains "$no_file_keys_output" "rotation_readiness_verdict: PASS"
  assert_contains "$no_file_keys_output" "secret_file_entries_total: 0"
  assert_contains "$no_file_keys_output" "secret_file_checks_warnings: 0"
  assert_contains "$no_file_keys_output" "secret_file_checks_errors: 0"

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
  echo "[ok] adapter secret rotation report pass/warn/fail + conflict + duplicate-key precedence + quoted-hash + underscore route conflict + fallback auth conflict + no-file-keys set-u guard"
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
  assert_contains "$output" "dynamic_cu_policy_verdict: SKIP"
  assert_contains "$output" "dynamic_tip_policy_verdict: SKIP"
  assert_contains "$output" "dynamic_cu_hint_api_total: 1"
  assert_contains "$output" "dynamic_cu_hint_rpc_total: 1"
  assert_field_equals "$output" "dynamic_cu_hint_api_configured" "false"
  assert_contains "$output" "dynamic_cu_hint_source_verdict: SKIP"
  assert_field_equals "$output" "dynamic_cu_hint_source_reason_code" "policy_disabled"
  assert_field_equals "$output" "go_nogo_require_jito_rpc_policy" "false"
  assert_contains "$output" "jito_rpc_policy_verdict: SKIP"
  assert_field_equals "$output" "jito_rpc_policy_reason_code" "gate_disabled"
  assert_field_equals "$output" "go_nogo_require_fastlane_disabled" "false"
  assert_field_equals "$output" "submit_fastlane_enabled" "false"
  assert_contains "$output" "fastlane_feature_flag_verdict: SKIP"
  assert_field_equals "$output" "fastlane_feature_flag_reason_code" "gate_disabled"
  assert_field_equals "$output" "windowed_signoff_required" "false"
  assert_contains "$output" "windowed_signoff_windows_csv: 1,6,24"
  assert_field_equals "$output" "windowed_signoff_require_dynamic_hint_source_pass" "false"
  assert_field_equals "$output" "windowed_signoff_require_dynamic_tip_policy_pass" "false"
  assert_contains "$output" "windowed_signoff_exit_code: 0"
  assert_contains "$output" "windowed_signoff_verdict: GO"
  assert_contains "$output" "windowed_signoff_artifact_manifest:"
  assert_contains "$output" "windowed_signoff_summary_sha256:"
  assert_field_equals "$output" "route_fee_signoff_required" "false"
  assert_contains "$output" "route_fee_signoff_windows_csv: 1,6,24"
  assert_contains "$output" "route_fee_signoff_verdict:"
  assert_field_non_empty "$output" "route_fee_signoff_reason_code"
  assert_contains "$output" "route_fee_signoff_artifact_manifest:"
  assert_contains "$output" "route_fee_signoff_summary_sha256:"
  assert_field_equals "$output" "route_fee_signoff_artifacts_written" "true"
  assert_contains "$output" "primary_route:"
  assert_contains "$output" "fallback_route:"
  assert_contains "$output" "confirmed_orders_total:"
  assert_contains "$output" "tests_run: false"
  assert_field_equals "$output" "go_nogo_artifacts_written" "true"
  assert_field_equals "$output" "windowed_signoff_artifacts_written" "true"
  assert_contains "$output" "artifacts_written: true"
  assert_contains "$output" "devnet_rehearsal_verdict: GO"
  assert_contains "$output" "artifact_summary:"
  assert_contains "$output" "artifact_preflight:"
  assert_contains "$output" "artifact_go_nogo:"
  assert_contains "$output" "artifact_windowed_signoff:"
  assert_contains "$output" "artifact_route_fee_signoff:"
  assert_contains "$output" "artifact_tests:"
  assert_contains "$output" "artifact_manifest:"
  assert_contains "$output" "summary_sha256:"
  assert_sha256_field "$output" "summary_sha256"
  assert_sha256_field "$output" "preflight_sha256"
  assert_sha256_field "$output" "go_nogo_sha256"
  assert_sha256_field "$output" "windowed_signoff_sha256"
  assert_sha256_field "$output" "route_fee_signoff_sha256"
  assert_sha256_field "$output" "tests_sha256"
  assert_sha256_field "$output" "go_nogo_nested_capture_sha256"
  assert_sha256_field "$output" "windowed_signoff_nested_capture_sha256"
  assert_sha256_field "$output" "route_fee_signoff_nested_capture_sha256"
  assert_sha256_field "$output" "manifest_sha256"
  assert_sha256_field_matches_file "$output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$output" "manifest_sha256" "artifact_manifest"
  assert_sha256_field "$output" "go_nogo_summary_sha256"
  assert_sha256_field "$output" "windowed_signoff_summary_sha256"
  assert_sha256_field "$output" "route_fee_signoff_summary_sha256"
  assert_contains "$output" "go_nogo_artifact_manifest:"
  assert_contains "$output" "go_nogo_summary_sha256:"
  assert_field_equals "$output" "go_nogo_nested_package_bundle_enabled" "false"
  assert_contains "$output" "windowed_signoff_artifact_manifest:"
  assert_contains "$output" "windowed_signoff_summary_sha256:"
  assert_field_equals "$output" "windowed_signoff_nested_package_bundle_enabled" "false"
  assert_contains "$output" "route_fee_signoff_artifact_manifest:"
  assert_contains "$output" "route_fee_signoff_summary_sha256:"
  assert_field_equals "$output" "route_fee_signoff_nested_package_bundle_enabled" "false"
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
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_windowed_signoff_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal windowed signoff artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_route_fee_signoff_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal route/fee signoff artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_tests_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal tests artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_manifest_*.txt >/dev/null 2>&1; then
    echo "expected devnet rehearsal manifest artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/go_nogo/execution_go_nogo_captured_*.txt >/dev/null 2>&1; then
    echo "expected nested go/no-go capture artifact in $artifacts_dir/go_nogo" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/windowed_signoff/execution_windowed_signoff_summary_*.txt >/dev/null 2>&1; then
    echo "expected nested windowed signoff summary artifact in $artifacts_dir/windowed_signoff" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/windowed_signoff/execution_windowed_signoff_captured_*.txt >/dev/null 2>&1; then
    echo "expected nested windowed signoff capture artifact in $artifacts_dir/windowed_signoff" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/route_fee_signoff/execution_route_fee_signoff_summary_*.txt >/dev/null 2>&1; then
    echo "expected nested route/fee signoff summary artifact in $artifacts_dir/route_fee_signoff" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/route_fee_signoff/execution_route_fee_signoff_captured_*.txt >/dev/null 2>&1; then
    echo "expected nested route/fee signoff capture artifact in $artifacts_dir/route_fee_signoff" >&2
    exit 1
  fi

  local bundle_artifacts_dir="$TMP_DIR/devnet-rehearsal-artifacts-with-bundle"
  local bundle_output_dir="$TMP_DIR/devnet-rehearsal-bundles"
  local bundle_output
  bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$bundle_artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_devnet_rehearsal_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$bundle_output_dir" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60
  )"
  assert_field_equals "$bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$bundle_output" "go_nogo_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "windowed_signoff_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "route_fee_signoff_nested_package_bundle_enabled" "false"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  local rehearsal_bundle_path
  rehearsal_bundle_path="$(extract_field_value "$bundle_output" "package_bundle_path")"
  if [[ ! -f "$rehearsal_bundle_path" ]]; then
    echo "expected package bundle archive at $rehearsal_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"
  local rehearsal_go_nogo_capture_path=""
  local rehearsal_windowed_signoff_capture_path=""
  local rehearsal_route_fee_signoff_capture_path=""
  rehearsal_go_nogo_capture_path="$(extract_field_value "$bundle_output" "artifact_go_nogo_nested_capture")"
  rehearsal_windowed_signoff_capture_path="$(extract_field_value "$bundle_output" "artifact_windowed_signoff_nested_capture")"
  rehearsal_route_fee_signoff_capture_path="$(extract_field_value "$bundle_output" "artifact_route_fee_signoff_nested_capture")"
  if [[ -z "$rehearsal_go_nogo_capture_path" || ! -f "$rehearsal_go_nogo_capture_path" ]]; then
    echo "expected devnet rehearsal nested go/no-go capture artifact at $rehearsal_go_nogo_capture_path" >&2
    exit 1
  fi
  if [[ -z "$rehearsal_windowed_signoff_capture_path" || ! -f "$rehearsal_windowed_signoff_capture_path" ]]; then
    echo "expected devnet rehearsal nested windowed signoff capture artifact at $rehearsal_windowed_signoff_capture_path" >&2
    exit 1
  fi
  if [[ -z "$rehearsal_route_fee_signoff_capture_path" || ! -f "$rehearsal_route_fee_signoff_capture_path" ]]; then
    echo "expected devnet rehearsal nested route/fee signoff capture artifact at $rehearsal_route_fee_signoff_capture_path" >&2
    exit 1
  fi
  local rehearsal_go_nogo_capture_text=""
  local rehearsal_windowed_signoff_capture_text=""
  local rehearsal_route_fee_signoff_capture_text=""
  rehearsal_go_nogo_capture_text="$(cat "$rehearsal_go_nogo_capture_path")"
  rehearsal_windowed_signoff_capture_text="$(cat "$rehearsal_windowed_signoff_capture_path")"
  rehearsal_route_fee_signoff_capture_text="$(cat "$rehearsal_route_fee_signoff_capture_path")"
  assert_contains "$rehearsal_go_nogo_capture_text" "package_bundle_enabled: false"
  assert_contains "$rehearsal_windowed_signoff_capture_text" "package_bundle_enabled: false"
  assert_contains "$rehearsal_route_fee_signoff_capture_text" "package_bundle_enabled: false"

  local missing_output_dir_output=""
  if missing_output_dir_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      PACKAGE_BUNDLE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail when PACKAGE_BUNDLE_ENABLED=true and OUTPUT_DIR is missing" >&2
    exit 1
  else
    local missing_output_dir_exit_code=$?
    if [[ "$missing_output_dir_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for missing OUTPUT_DIR with PACKAGE_BUNDLE_ENABLED=true, got $missing_output_dir_exit_code" >&2
      echo "$missing_output_dir_output" >&2
      exit 1
    fi
  fi
  assert_contains "$missing_output_dir_output" "PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set"

  local core_only_output=""
  core_only_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      DEVNET_REHEARSAL_PROFILE="core_only" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60
  )"
  assert_field_equals "$core_only_output" "devnet_rehearsal_profile" "core_only"
  assert_field_equals "$core_only_output" "devnet_rehearsal_run_windowed_signoff" "false"
  assert_field_equals "$core_only_output" "devnet_rehearsal_run_route_fee_signoff" "false"
  assert_field_equals "$core_only_output" "windowed_signoff_verdict" "SKIP"
  assert_contains "$core_only_output" "windowed_signoff_reason: windowed signoff stage disabled via DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=false"
  assert_field_equals "$core_only_output" "windowed_signoff_artifacts_written" "n/a"
  assert_field_equals "$core_only_output" "windowed_signoff_nested_package_bundle_enabled" "n/a"
  assert_field_equals "$core_only_output" "route_fee_signoff_verdict" "SKIP"
  assert_field_equals "$core_only_output" "route_fee_signoff_reason_code" "stage_disabled"
  assert_field_equals "$core_only_output" "route_fee_signoff_artifacts_written" "n/a"
  assert_field_equals "$core_only_output" "route_fee_signoff_nested_package_bundle_enabled" "n/a"
  assert_field_equals "$core_only_output" "devnet_rehearsal_verdict" "GO"
  assert_field_equals "$core_only_output" "devnet_rehearsal_reason_code" "test_mode_override"

  local invalid_profile_output=""
  if invalid_profile_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      DEVNET_REHEARSAL_PROFILE="bogus_profile" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail for invalid DEVNET_REHEARSAL_PROFILE" >&2
    exit 1
  else
    local invalid_profile_exit_code=$?
    if [[ "$invalid_profile_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid DEVNET_REHEARSAL_PROFILE, got $invalid_profile_exit_code" >&2
      echo "$invalid_profile_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_profile_output" "DEVNET_REHEARSAL_PROFILE must be one of: full,core_only"

  local required_windowed_disabled_output=""
  if required_windowed_disabled_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="true" \
      DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF="false" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for required windowed signoff with stage disabled" >&2
    exit 1
  else
    local required_windowed_disabled_exit_code=$?
    if [[ "$required_windowed_disabled_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required windowed signoff with stage disabled, got $required_windowed_disabled_exit_code" >&2
      echo "$required_windowed_disabled_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$required_windowed_disabled_output" "windowed_signoff_required" "true"
  assert_field_equals "$required_windowed_disabled_output" "devnet_rehearsal_run_windowed_signoff" "false"
  assert_field_equals "$required_windowed_disabled_output" "windowed_signoff_verdict" "SKIP"
  assert_field_equals "$required_windowed_disabled_output" "devnet_rehearsal_verdict" "NO_GO"
  assert_field_equals "$required_windowed_disabled_output" "devnet_rehearsal_reason_code" "config_error"
  assert_contains "$required_windowed_disabled_output" "config_error: WINDOWED_SIGNOFF_REQUIRED=true requires DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=true"

  local required_route_fee_disabled_output=""
  if required_route_fee_disabled_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF="false" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for required route/fee signoff with stage disabled" >&2
    exit 1
  else
    local required_route_fee_disabled_exit_code=$?
    if [[ "$required_route_fee_disabled_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required route/fee signoff with stage disabled, got $required_route_fee_disabled_exit_code" >&2
      echo "$required_route_fee_disabled_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$required_route_fee_disabled_output" "route_fee_signoff_required" "true"
  assert_field_equals "$required_route_fee_disabled_output" "devnet_rehearsal_run_route_fee_signoff" "false"
  assert_field_equals "$required_route_fee_disabled_output" "route_fee_signoff_verdict" "SKIP"
  assert_field_equals "$required_route_fee_disabled_output" "devnet_rehearsal_verdict" "NO_GO"
  assert_field_equals "$required_route_fee_disabled_output" "devnet_rehearsal_reason_code" "config_error"
  assert_contains "$required_route_fee_disabled_output" "config_error: ROUTE_FEE_SIGNOFF_REQUIRED=true requires DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=true"

  local required_nogo_output=""
  if required_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="true" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      WINDOWED_SIGNOFF_REQUIRED="true" WINDOWED_SIGNOFF_WINDOWS_CSV="1,invalid" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="true" WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="true" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for devnet rehearsal helper when required windowed signoff returns NO_GO" >&2
    exit 1
  else
    local required_nogo_exit_code=$?
    if [[ "$required_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required windowed signoff branch, got $required_nogo_exit_code" >&2
      echo "$required_nogo_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$required_nogo_output" "windowed_signoff_required" "true"
  assert_field_equals "$required_nogo_output" "windowed_signoff_require_dynamic_hint_source_pass" "true"
  assert_field_equals "$required_nogo_output" "windowed_signoff_require_dynamic_tip_policy_pass" "true"
  assert_field_equals "$required_nogo_output" "go_nogo_require_jito_rpc_policy" "true"
  assert_contains "$required_nogo_output" "jito_rpc_policy_verdict: WARN"
  assert_field_in "$required_nogo_output" "jito_rpc_policy_reason_code" "target_mismatch" "route_profile_not_pass"
  assert_field_equals "$required_nogo_output" "go_nogo_require_fastlane_disabled" "true"
  assert_field_equals "$required_nogo_output" "submit_fastlane_enabled" "false"
  assert_contains "$required_nogo_output" "fastlane_feature_flag_verdict: PASS"
  assert_field_equals "$required_nogo_output" "fastlane_feature_flag_reason_code" "fastlane_disabled"
  assert_contains "$required_nogo_output" "windowed_signoff_verdict: NO_GO"
  assert_contains "$required_nogo_output" "artifacts_written: false"
  assert_contains "$required_nogo_output" "devnet_rehearsal_verdict: NO_GO"

  local fastlane_strict_nogo_output=""
  if fastlane_strict_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED="true" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for devnet rehearsal helper when strict fastlane-disabled gate is violated" >&2
    exit 1
  else
    local fastlane_strict_nogo_exit_code=$?
    if [[ "$fastlane_strict_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for strict fastlane-disabled rehearsal branch, got $fastlane_strict_nogo_exit_code" >&2
      echo "$fastlane_strict_nogo_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$fastlane_strict_nogo_output" "go_nogo_require_fastlane_disabled" "true"
  assert_field_equals "$fastlane_strict_nogo_output" "submit_fastlane_enabled" "true"
  assert_contains "$fastlane_strict_nogo_output" "fastlane_feature_flag_verdict: WARN"
  assert_field_equals "$fastlane_strict_nogo_output" "fastlane_feature_flag_reason_code" "fastlane_enabled"
  assert_contains "$fastlane_strict_nogo_output" "overall_go_nogo_verdict: NO_GO"
  assert_contains "$fastlane_strict_nogo_output" "devnet_rehearsal_verdict: NO_GO"
  assert_field_equals "$fastlane_strict_nogo_output" "devnet_rehearsal_reason_code" "go_nogo_no_go"

  local route_fee_required_nogo_output=""
  if route_fee_required_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" ROUTE_FEE_SIGNOFF_WINDOWS_CSV="1,invalid" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for devnet rehearsal helper when required route/fee signoff returns NO_GO" >&2
    exit 1
  else
    local route_fee_required_nogo_exit_code=$?
    if [[ "$route_fee_required_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required route/fee signoff branch, got $route_fee_required_nogo_exit_code" >&2
      echo "$route_fee_required_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_verdict: NO_GO"
  assert_field_equals "$route_fee_required_nogo_output" "route_fee_signoff_reason_code" "input_error"
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_windows_csv: 1,invalid"
  assert_contains "$route_fee_required_nogo_output" "devnet_rehearsal_verdict: NO_GO"
  assert_field_equals "$route_fee_required_nogo_output" "devnet_rehearsal_reason_code" "route_fee_signoff_no_go"

  local route_fee_required_hold_output=""
  if route_fee_required_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="HOLD" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for devnet rehearsal helper when required route/fee signoff returns HOLD" >&2
    exit 1
  else
    local route_fee_required_hold_exit_code=$?
    if [[ "$route_fee_required_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for required route/fee signoff HOLD branch, got $route_fee_required_hold_exit_code" >&2
      echo "$route_fee_required_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_fee_required_hold_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_hold_output" "route_fee_signoff_verdict: HOLD"
  assert_field_equals "$route_fee_required_hold_output" "route_fee_signoff_reason_code" "test_override"
  assert_contains "$route_fee_required_hold_output" "devnet_rehearsal_verdict: HOLD"
  assert_field_equals "$route_fee_required_hold_output" "devnet_rehearsal_reason_code" "route_fee_signoff_hold"

  local route_fee_required_go_output
  route_fee_required_go_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60
  )"
  assert_contains "$route_fee_required_go_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_go_output" "route_fee_signoff_verdict: GO"
  assert_field_equals "$route_fee_required_go_output" "route_fee_signoff_reason_code" "test_override"
  assert_contains "$route_fee_required_go_output" "devnet_rehearsal_verdict: GO"
  assert_field_equals "$route_fee_required_go_output" "devnet_rehearsal_reason_code" "test_mode_override"

  local invalid_windowed_required_output=""
  if invalid_windowed_required_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      WINDOWED_SIGNOFF_REQUIRED="maybe" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail for invalid WINDOWED_SIGNOFF_REQUIRED token" >&2
    exit 1
  else
    local invalid_windowed_required_exit_code=$?
    if [[ "$invalid_windowed_required_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid WINDOWED_SIGNOFF_REQUIRED token, got $invalid_windowed_required_exit_code" >&2
      echo "$invalid_windowed_required_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_windowed_required_output" "WINDOWED_SIGNOFF_REQUIRED must be a boolean token"
  assert_contains "$invalid_windowed_required_output" "got: maybe"

  local invalid_go_nogo_test_mode_output=""
  if invalid_go_nogo_test_mode_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="sometimes" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail for invalid GO_NOGO_TEST_MODE token" >&2
    exit 1
  else
    local invalid_go_nogo_test_mode_exit_code=$?
    if [[ "$invalid_go_nogo_test_mode_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid GO_NOGO_TEST_MODE token, got $invalid_go_nogo_test_mode_exit_code" >&2
      echo "$invalid_go_nogo_test_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_go_nogo_test_mode_output" "GO_NOGO_TEST_MODE must be a boolean token"
  assert_contains "$invalid_go_nogo_test_mode_output" "got: sometimes"

  local invalid_execution_enabled_output=""
  if invalid_execution_enabled_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      SOLANA_COPY_BOT_EXECUTION_ENABLED="sometimes" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail for invalid SOLANA_COPY_BOT_EXECUTION_ENABLED token" >&2
    exit 1
  else
    local invalid_execution_enabled_exit_code=$?
    if [[ "$invalid_execution_enabled_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid SOLANA_COPY_BOT_EXECUTION_ENABLED token, got $invalid_execution_enabled_exit_code" >&2
      echo "$invalid_execution_enabled_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_execution_enabled_output" "SOLANA_COPY_BOT_EXECUTION_ENABLED must be a boolean token"
  assert_contains "$invalid_execution_enabled_output" "got: sometimes"

  local invalid_route_fee_mode_output=""
  if invalid_route_fee_mode_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" DB_PATH="$db_path" CONFIG_PATH="$config_path" SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" DEVNET_REHEARSAL_TEST_MODE="true" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="sometimes" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 2>&1
  )"; then
    echo "expected execution_devnet_rehearsal.sh to fail for invalid ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE token" >&2
    exit 1
  else
    local invalid_route_fee_mode_exit_code=$?
    if [[ "$invalid_route_fee_mode_exit_code" -ne 1 ]]; then
      echo "expected exit code 1 for invalid ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE token, got $invalid_route_fee_mode_exit_code" >&2
      echo "$invalid_route_fee_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_route_fee_mode_output" "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE must be a boolean token"
  assert_contains "$invalid_route_fee_mode_output" "got: sometimes"
  echo "[ok] execution devnet rehearsal helper"
}

run_executor_signer_rotation_report_case() {
  local secrets_dir="$TMP_DIR/executor-secrets"
  mkdir -p "$secrets_dir"
  printf '[1,2,3]\n' >"$secrets_dir/executor_signer.json"
  chmod 600 "$secrets_dir/executor_signer.json"

  local env_path="$TMP_DIR/executor-signer-rotation.env"
  cat >"$env_path" <<EOF
COPYBOT_EXECUTOR_SIGNER_SOURCE=file
COPYBOT_EXECUTOR_SIGNER_PUBKEY=11111111111111111111111111111111
COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE=executor-secrets/executor_signer.json
EOF

  local artifacts_dir="$TMP_DIR/executor-signer-rotation-artifacts"
  local output
  output="$(
    EXECUTOR_ENV_PATH="$env_path" \
      OUTPUT_DIR="$artifacts_dir" \
      bash "$ROOT_DIR/tools/executor_signer_rotation_report.sh"
  )"
  assert_contains "$output" "=== Executor Signer Rotation Report ==="
  assert_contains "$output" "rotation_readiness_verdict: PASS"
  assert_contains "$output" "artifacts_written: true"
  assert_contains "$output" "signer_source: file"
  assert_contains "$output" "signer_file_permissions_owner_only: true"
  assert_contains "$output" "artifact_report:"
  assert_contains "$output" "artifact_manifest:"
  assert_contains "$output" "report_sha256:"
  assert_sha256_field "$output" "report_sha256"
  assert_sha256_field "$output" "manifest_sha256"
  assert_sha256_field_matches_file "$output" "report_sha256" "artifact_report"
  assert_sha256_field_matches_file "$output" "manifest_sha256" "artifact_manifest"
  if ! ls "$artifacts_dir"/executor_signer_rotation_report_*.txt >/dev/null 2>&1; then
    echo "expected executor signer rotation report artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/executor_signer_rotation_manifest_*.txt >/dev/null 2>&1; then
    echo "expected executor signer rotation manifest artifact in $artifacts_dir" >&2
    exit 1
  fi

  local kms_missing_env_path="$TMP_DIR/executor-signer-rotation-kms-missing.env"
  cat >"$kms_missing_env_path" <<EOF
COPYBOT_EXECUTOR_SIGNER_SOURCE=kms
COPYBOT_EXECUTOR_SIGNER_PUBKEY=11111111111111111111111111111111
EOF
  local kms_missing_output=""
  if kms_missing_output="$(
    EXECUTOR_ENV_PATH="$kms_missing_env_path" \
      bash "$ROOT_DIR/tools/executor_signer_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit when kms signer source has no kms key id" >&2
    exit 1
  else
    local kms_missing_exit_code=$?
    if [[ "$kms_missing_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for missing kms key id, got $kms_missing_exit_code" >&2
      echo "$kms_missing_output" >&2
      exit 1
    fi
  fi
  assert_contains "$kms_missing_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$kms_missing_output" "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set"

  chmod 644 "$secrets_dir/executor_signer.json"
  local perm_fail_output=""
  if perm_fail_output="$(
    EXECUTOR_ENV_PATH="$env_path" \
      bash "$ROOT_DIR/tools/executor_signer_rotation_report.sh" 2>&1
  )"; then
    echo "expected FAIL exit for broad signer keypair permissions" >&2
    exit 1
  else
    local perm_fail_exit_code=$?
    if [[ "$perm_fail_exit_code" -ne 1 ]]; then
      echo "expected FAIL exit code 1 for broad signer keypair permissions, got $perm_fail_exit_code" >&2
      echo "$perm_fail_output" >&2
      exit 1
    fi
  fi
  assert_contains "$perm_fail_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$perm_fail_output" "must use owner-only permissions"
  echo "[ok] executor signer rotation report pass/fail checks"
}

run_executor_rollout_evidence_case() {
  local db_path="$1"
  local config_path="$2"
  local executor_env_path="$TMP_DIR/executor-rollout.env"
  local adapter_env_path="$TMP_DIR/adapter-rollout-for-executor.env"
  local artifacts_dir="$TMP_DIR/executor-rollout-artifacts"
  local final_artifacts_dir="$TMP_DIR/executor-final-package"
  local fake_curl_bin="$TMP_DIR/fake-curl-executor-rollout"
  local auth_token="executor-rollout-token"
  local port="18091"

  write_executor_env_preflight "$executor_env_path" "$port" "$auth_token"
  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  write_fake_curl_executor_preflight "$fake_curl_bin" "$auth_token"

  local pass_output
  pass_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60
  )"
  assert_contains "$pass_output" "=== Executor Rollout Evidence Summary ==="
  assert_field_equals "$pass_output" "executor_rollout_profile" "full"
  assert_field_equals "$pass_output" "executor_rollout_run_rotation" "true"
  assert_field_equals "$pass_output" "executor_rollout_run_preflight" "true"
  assert_field_equals "$pass_output" "executor_rollout_run_rehearsal" "true"
  assert_field_equals "$pass_output" "rotation_readiness_verdict" "PASS"
  assert_field_equals "$pass_output" "preflight_verdict" "PASS"
  assert_field_equals "$pass_output" "devnet_rehearsal_verdict" "GO"
  assert_field_equals "$pass_output" "rotation_artifacts_written" "true"
  assert_field_equals "$pass_output" "preflight_artifacts_written" "true"
  assert_field_equals "$pass_output" "rehearsal_artifacts_written" "true"
  assert_field_equals "$pass_output" "rehearsal_nested_package_bundle_enabled" "false"
  assert_field_equals "$pass_output" "executor_rollout_verdict" "GO"
  assert_field_equals "$pass_output" "executor_rollout_reason_code" "gates_pass"
  assert_contains "$pass_output" "artifacts_written: true"
  assert_sha256_field "$pass_output" "summary_sha256"
  assert_sha256_field "$pass_output" "rotation_capture_sha256"
  assert_sha256_field "$pass_output" "preflight_capture_sha256"
  assert_sha256_field "$pass_output" "rehearsal_capture_sha256"
  assert_sha256_field "$pass_output" "manifest_sha256"
  assert_sha256_field_matches_file "$pass_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$pass_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$artifacts_dir"/executor_rollout_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected executor rollout summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/executor_signer_rotation_captured_*.txt >/dev/null 2>&1; then
    echo "expected executor signer rotation capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/executor_preflight_captured_*.txt >/dev/null 2>&1; then
    echo "expected executor preflight capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_captured_*.txt >/dev/null 2>&1; then
    echo "expected execution devnet rehearsal capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/executor_rollout_evidence_manifest_*.txt >/dev/null 2>&1; then
    echo "expected executor rollout manifest artifact in $artifacts_dir" >&2
    exit 1
  fi

  local rollout_bundle_output_dir="$TMP_DIR/executor-rollout-with-bundle"
  local rollout_bundle_archive_dir="$TMP_DIR/executor-rollout-bundles"
  local rollout_bundle_output
  rollout_bundle_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$rollout_bundle_output_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="executor_rollout_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$rollout_bundle_archive_dir" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$rollout_bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$rollout_bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$rollout_bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$rollout_bundle_output" "rotation_artifacts_written" "true"
  assert_field_equals "$rollout_bundle_output" "preflight_artifacts_written" "true"
  assert_field_equals "$rollout_bundle_output" "rehearsal_artifacts_written" "true"
  assert_field_equals "$rollout_bundle_output" "rehearsal_nested_package_bundle_enabled" "false"
  assert_sha256_field "$rollout_bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$rollout_bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$rollout_bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_path"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_contents_manifest"
  local executor_rollout_bundle_path
  executor_rollout_bundle_path="$(extract_field_value "$rollout_bundle_output" "package_bundle_path")"
  if [[ ! -f "$executor_rollout_bundle_path" ]]; then
    echo "expected package bundle archive at $executor_rollout_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$rollout_bundle_output"
  local executor_rollout_rehearsal_capture_path
  executor_rollout_rehearsal_capture_path="$(extract_field_value "$rollout_bundle_output" "artifact_rehearsal_capture")"
  if [[ -z "$executor_rollout_rehearsal_capture_path" || ! -f "$executor_rollout_rehearsal_capture_path" ]]; then
    echo "expected executor rollout nested rehearsal capture artifact at $executor_rollout_rehearsal_capture_path" >&2
    exit 1
  fi
  local executor_rollout_rehearsal_capture_text
  executor_rollout_rehearsal_capture_text="$(cat "$executor_rollout_rehearsal_capture_path")"
  assert_contains "$executor_rollout_rehearsal_capture_text" "package_bundle_enabled: false"

  local invalid_bool_output=""
  if invalid_bool_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$TMP_DIR/missing-executor.env" \
      ADAPTER_ENV_PATH="$TMP_DIR/missing-adapter.env" \
      CONFIG_PATH="$TMP_DIR/missing-config.toml" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="maybe" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when GO_NOGO_REQUIRE_JITO_RPC_POLICY token is invalid" >&2
    exit 1
  else
    local invalid_bool_exit_code=$?
    if [[ "$invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for invalid executor rollout bool token, got $invalid_bool_exit_code" >&2
      echo "$invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_bool_output" "GO_NOGO_REQUIRE_JITO_RPC_POLICY must be a boolean token"
  assert_contains "$invalid_bool_output" "got: maybe"
  assert_field_equals "$invalid_bool_output" "rotation_readiness_verdict" "UNKNOWN"
  assert_field_equals "$invalid_bool_output" "preflight_verdict" "UNKNOWN"
  assert_field_equals "$invalid_bool_output" "devnet_rehearsal_reason_code" "input_error"

  write_adapter_env_preflight "$adapter_env_path" "$port" "mismatch-token"
  local preflight_fail_output=""
  if preflight_fail_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when preflight fails" >&2
    exit 1
  else
    local preflight_fail_exit_code=$?
    if [[ "$preflight_fail_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for executor rollout preflight failure, got $preflight_fail_exit_code" >&2
      echo "$preflight_fail_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$preflight_fail_output" "preflight_verdict" "FAIL"
  assert_field_equals "$preflight_fail_output" "executor_rollout_verdict" "NO_GO"
  assert_field_equals "$preflight_fail_output" "executor_rollout_reason_code" "preflight_fail"

  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  local hold_output=""
  if hold_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="HOLD" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for executor rollout helper when rehearsal route/fee signoff is HOLD" >&2
    exit 1
  else
    local hold_exit_code=$?
    if [[ "$hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for executor rollout helper, got $hold_exit_code" >&2
      echo "$hold_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$hold_output" "devnet_rehearsal_verdict" "HOLD"
  assert_field_equals "$hold_output" "executor_rollout_verdict" "HOLD"
  assert_field_equals "$hold_output" "executor_rollout_reason_code" "rehearsal_hold"

  local precheck_profile_output
  precheck_profile_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      EXECUTOR_ROLLOUT_PROFILE="precheck_only" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$precheck_profile_output" "executor_rollout_profile" "precheck_only"
  assert_field_equals "$precheck_profile_output" "executor_rollout_run_rotation" "true"
  assert_field_equals "$precheck_profile_output" "executor_rollout_run_preflight" "true"
  assert_field_equals "$precheck_profile_output" "executor_rollout_run_rehearsal" "false"
  assert_field_equals "$precheck_profile_output" "rotation_readiness_verdict" "PASS"
  assert_field_equals "$precheck_profile_output" "preflight_verdict" "PASS"
  assert_field_equals "$precheck_profile_output" "devnet_rehearsal_verdict" "SKIP"
  assert_field_equals "$precheck_profile_output" "devnet_rehearsal_reason_code" "stage_disabled"
  assert_field_equals "$precheck_profile_output" "executor_rollout_verdict" "GO"
  assert_field_equals "$precheck_profile_output" "executor_rollout_reason_code" "gates_pass"

  local rehearsal_only_profile_output
  rehearsal_only_profile_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      EXECUTOR_ROLLOUT_PROFILE="rehearsal_only" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_profile" "rehearsal_only"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_run_rotation" "false"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_run_preflight" "false"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_run_rehearsal" "true"
  assert_field_equals "$rehearsal_only_profile_output" "rotation_readiness_verdict" "SKIP"
  assert_field_equals "$rehearsal_only_profile_output" "preflight_verdict" "SKIP"
  assert_field_equals "$rehearsal_only_profile_output" "devnet_rehearsal_verdict" "GO"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_verdict" "GO"
  assert_field_equals "$rehearsal_only_profile_output" "executor_rollout_reason_code" "gates_pass"

  local invalid_profile_output=""
  if invalid_profile_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ROLLOUT_PROFILE="bogus_profile" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when EXECUTOR_ROLLOUT_PROFILE is invalid" >&2
    exit 1
  else
    local invalid_profile_exit_code=$?
    if [[ "$invalid_profile_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for invalid executor rollout profile, got $invalid_profile_exit_code" >&2
      echo "$invalid_profile_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_profile_output" "EXECUTOR_ROLLOUT_PROFILE must be one of: full,precheck_only,rehearsal_only"
  assert_field_equals "$invalid_profile_output" "executor_rollout_reason_code" "input_error"

  local all_disabled_output=""
  if all_disabled_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      EXECUTOR_ROLLOUT_RUN_ROTATION="false" \
      EXECUTOR_ROLLOUT_RUN_PREFLIGHT="false" \
      EXECUTOR_ROLLOUT_RUN_REHEARSAL="false" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when all stages are disabled" >&2
    exit 1
  else
    local all_disabled_exit_code=$?
    if [[ "$all_disabled_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for executor rollout all-stages-disabled case, got $all_disabled_exit_code" >&2
      echo "$all_disabled_output" >&2
      exit 1
    fi
  fi
  assert_contains "$all_disabled_output" "at least one stage must be enabled"
  assert_field_equals "$all_disabled_output" "executor_rollout_reason_code" "input_error"

  local required_windowed_disabled_rehearsal_output=""
  if required_windowed_disabled_rehearsal_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      WINDOWED_SIGNOFF_REQUIRED="true" \
      EXECUTOR_ROLLOUT_RUN_REHEARSAL="false" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when WINDOWED_SIGNOFF_REQUIRED=true and rehearsal stage disabled" >&2
    exit 1
  else
    local required_windowed_disabled_rehearsal_exit_code=$?
    if [[ "$required_windowed_disabled_rehearsal_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for WINDOWED_SIGNOFF_REQUIRED + disabled rehearsal, got $required_windowed_disabled_rehearsal_exit_code" >&2
      echo "$required_windowed_disabled_rehearsal_output" >&2
      exit 1
    fi
  fi
  assert_contains "$required_windowed_disabled_rehearsal_output" "WINDOWED_SIGNOFF_REQUIRED=true requires EXECUTOR_ROLLOUT_RUN_REHEARSAL=true"
  assert_field_equals "$required_windowed_disabled_rehearsal_output" "executor_rollout_reason_code" "input_error"

  local required_route_fee_disabled_rehearsal_output=""
  if required_route_fee_disabled_rehearsal_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      EXECUTOR_ROLLOUT_RUN_REHEARSAL="false" \
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor rollout helper when ROUTE_FEE_SIGNOFF_REQUIRED=true and rehearsal stage disabled" >&2
    exit 1
  else
    local required_route_fee_disabled_rehearsal_exit_code=$?
    if [[ "$required_route_fee_disabled_rehearsal_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for ROUTE_FEE_SIGNOFF_REQUIRED + disabled rehearsal, got $required_route_fee_disabled_rehearsal_exit_code" >&2
      echo "$required_route_fee_disabled_rehearsal_output" >&2
      exit 1
    fi
  fi
  assert_contains "$required_route_fee_disabled_rehearsal_output" "ROUTE_FEE_SIGNOFF_REQUIRED=true requires EXECUTOR_ROLLOUT_RUN_REHEARSAL=true"
  assert_field_equals "$required_route_fee_disabled_rehearsal_output" "executor_rollout_reason_code" "input_error"

  local final_output
  final_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$final_artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" 24 60
  )"
  assert_contains "$final_output" "=== Executor Final Evidence Package ==="
  assert_field_equals "$final_output" "rollout_verdict" "GO"
  assert_field_equals "$final_output" "rollout_reason_code" "gates_pass"
  assert_field_equals "$final_output" "rollout_nested_package_bundle_enabled" "false"
  assert_field_equals "$final_output" "final_executor_package_verdict" "GO"
  assert_field_equals "$final_output" "final_executor_package_reason_code" "gates_pass"
  assert_contains "$final_output" "artifacts_written: true"
  assert_field_equals "$final_output" "rollout_artifacts_written" "true"
  assert_sha256_field "$final_output" "summary_sha256"
  assert_sha256_field "$final_output" "rollout_capture_sha256"
  assert_sha256_field "$final_output" "manifest_sha256"
  assert_sha256_field_matches_file "$final_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$final_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$final_artifacts_dir"/executor_final_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected executor final package summary artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/executor_final_evidence_manifest_*.txt >/dev/null 2>&1; then
    echo "expected executor final package manifest artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/executor_rollout_evidence_captured_*.txt >/dev/null 2>&1; then
    echo "expected executor final package captured rollout artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/rollout/executor_rollout_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected nested executor rollout summary artifact in $final_artifacts_dir/rollout" >&2
    exit 1
  fi

  local final_bundle_output_dir="$TMP_DIR/executor-final-package-with-bundle"
  local final_bundle_archive_dir="$TMP_DIR/executor-final-package-bundles"
  local final_bundle_output
  final_bundle_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$final_bundle_output_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="executor_rollout_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$final_bundle_archive_dir" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" 24 60
  )"
  assert_field_equals "$final_bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$final_bundle_output" "rollout_nested_package_bundle_enabled" "false"
  assert_sha256_field "$final_bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$final_bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$final_bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$final_bundle_output" "package_bundle_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_contents_manifest"
  local executor_package_bundle_path
  executor_package_bundle_path="$(extract_field_value "$final_bundle_output" "package_bundle_path")"
  if [[ ! -f "$executor_package_bundle_path" ]]; then
    echo "expected package bundle archive at $executor_package_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$final_bundle_output"
  local executor_final_rollout_capture_path
  executor_final_rollout_capture_path="$(extract_field_value "$final_bundle_output" "artifact_rollout_capture")"
  if [[ -z "$executor_final_rollout_capture_path" || ! -f "$executor_final_rollout_capture_path" ]]; then
    echo "expected executor final nested rollout capture artifact at $executor_final_rollout_capture_path" >&2
    exit 1
  fi
  local executor_final_rollout_capture_text
  executor_final_rollout_capture_text="$(cat "$executor_final_rollout_capture_path")"
  assert_contains "$executor_final_rollout_capture_text" "package_bundle_enabled: false"

  local final_hold_output=""
  if final_hold_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/executor-final-package-hold" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="HOLD" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for executor final package helper when rollout gate is HOLD" >&2
    exit 1
  else
    local final_hold_exit_code=$?
    if [[ "$final_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for executor final package helper, got $final_hold_exit_code" >&2
      echo "$final_hold_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$final_hold_output" "rollout_verdict" "HOLD"
  assert_field_equals "$final_hold_output" "rollout_reason_code" "rehearsal_hold"
  assert_field_equals "$final_hold_output" "final_executor_package_verdict" "HOLD"
  assert_field_equals "$final_hold_output" "final_executor_package_reason_code" "rehearsal_hold"

  write_adapter_env_preflight "$adapter_env_path" "$port" "mismatch-token"
  local final_nogo_output=""
  if final_nogo_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/executor-final-package-nogo" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor final package helper when rollout gate fails" >&2
    exit 1
  else
    local final_nogo_exit_code=$?
    if [[ "$final_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for executor final package helper, got $final_nogo_exit_code" >&2
      echo "$final_nogo_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$final_nogo_output" "rollout_verdict" "NO_GO"
  assert_field_equals "$final_nogo_output" "rollout_reason_code" "preflight_fail"
  assert_field_equals "$final_nogo_output" "final_executor_package_verdict" "NO_GO"
  assert_field_equals "$final_nogo_output" "final_executor_package_reason_code" "preflight_fail"

  local final_invalid_bool_output=""
  if final_invalid_bool_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/executor-final-package-invalid-bool" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="sometimes" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for executor final package helper when GO_NOGO_REQUIRE_FASTLANE_DISABLED token is invalid" >&2
    exit 1
  else
    local final_invalid_bool_exit_code=$?
    if [[ "$final_invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for executor final invalid bool token, got $final_invalid_bool_exit_code" >&2
      echo "$final_invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$final_invalid_bool_output" "GO_NOGO_REQUIRE_FASTLANE_DISABLED must be a boolean token"
  assert_field_equals "$final_invalid_bool_output" "rollout_verdict" "NO_GO"
  assert_field_equals "$final_invalid_bool_output" "rollout_reason_code" "input_error"
  assert_field_equals "$final_invalid_bool_output" "rollout_artifacts_written" "false"
  assert_field_equals "$final_invalid_bool_output" "final_executor_package_reason_code" "input_error"
  echo "[ok] executor rollout/final evidence helpers"
}

run_execution_server_rollout_report_case() {
  local db_path="$1"
  local config_path="$2"
  local executor_env_path="$TMP_DIR/server-rollout-executor.env"
  local adapter_env_path="$TMP_DIR/server-rollout-adapter.env"
  local fake_curl_bin="$TMP_DIR/fake-curl-server-rollout"
  local auth_token="server-rollout-token"
  local port="18093"
  local output_root="$TMP_DIR/server-rollout-output"
  local bundle_output_dir="$TMP_DIR/server-rollout-bundles"

  write_executor_env_preflight "$executor_env_path" "$port" "$auth_token"
  write_adapter_env_preflight "$adapter_env_path" "$port" "$auth_token"
  echo "COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true" >>"$adapter_env_path"
  write_fake_curl_executor_preflight "$fake_curl_bin" "$auth_token"

  local output
  local rollout_exit_code=0
  if output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$output_root" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60
  )"; then
    rollout_exit_code=0
  else
    rollout_exit_code=$?
  fi
  if [[ "$rollout_exit_code" -ne 2 ]]; then
    echo "expected server rollout hold exit code 2, got $rollout_exit_code" >&2
    echo "$output" >&2
    exit 1
  fi
  assert_contains "$output" "=== Execution Server Rollout Report ==="
  assert_field_equals "$output" "preflight_verdict" "PASS"
  assert_field_equals "$output" "fee_decomposition_verdict" "WARN"
  assert_field_equals "$output" "route_profile_verdict" "WARN"
  assert_field_equals "$output" "go_nogo_verdict" "GO"
  assert_field_equals "$output" "rehearsal_verdict" "GO"
  assert_field_equals "$output" "executor_final_verdict" "GO"
  assert_field_equals "$output" "adapter_final_verdict" "GO"
  assert_field_equals "$output" "go_nogo_artifacts_written" "true"
  assert_field_equals "$output" "rehearsal_artifacts_written" "true"
  assert_field_equals "$output" "executor_final_artifacts_written" "true"
  assert_field_equals "$output" "adapter_final_artifacts_written" "true"
  assert_field_equals "$output" "server_rollout_verdict" "HOLD"
  assert_field_equals "$output" "server_rollout_reason_code" "calibration_fee_not_pass"
  assert_contains "$output" "artifacts_written: true"
  assert_sha256_field "$output" "summary_sha256"
  assert_sha256_field "$output" "manifest_sha256"
  assert_sha256_field "$output" "preflight_capture_sha256"
  assert_sha256_field "$output" "calibration_capture_sha256"
  assert_sha256_field "$output" "go_nogo_capture_sha256"
  assert_sha256_field "$output" "rehearsal_capture_sha256"
  assert_sha256_field "$output" "executor_final_capture_sha256"
  assert_sha256_field "$output" "adapter_final_capture_sha256"
  assert_sha256_field_matches_file "$output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$output" "manifest_sha256" "artifact_manifest"
  if ! ls "$output_root"/execution_server_rollout_summary_*.txt >/dev/null 2>&1; then
    echo "expected server rollout summary artifact in $output_root" >&2
    exit 1
  fi
  if ! ls "$output_root"/execution_server_rollout_manifest_*.txt >/dev/null 2>&1; then
    echo "expected server rollout manifest artifact in $output_root" >&2
    exit 1
  fi

  local skip_direct_output=""
  local skip_direct_exit_code=0
  if skip_direct_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/server-rollout-output-skip-direct" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT="false" \
      SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT="false" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60
  )"; then
    skip_direct_exit_code=0
  else
    skip_direct_exit_code=$?
  fi
  if [[ "$skip_direct_exit_code" -ne 2 ]]; then
    echo "expected server rollout skip-direct hold exit code 2, got $skip_direct_exit_code" >&2
    echo "$skip_direct_output" >&2
    exit 1
  fi
  assert_field_equals "$skip_direct_output" "server_rollout_run_go_nogo_direct" "false"
  assert_field_equals "$skip_direct_output" "server_rollout_run_rehearsal_direct" "false"
  assert_field_equals "$skip_direct_output" "go_nogo_verdict" "SKIP"
  assert_field_equals "$skip_direct_output" "rehearsal_verdict" "SKIP"
  assert_field_equals "$skip_direct_output" "go_nogo_reason_code" "stage_disabled"
  assert_field_equals "$skip_direct_output" "rehearsal_reason_code" "stage_disabled"
  assert_field_equals "$skip_direct_output" "executor_final_verdict" "GO"
  assert_field_equals "$skip_direct_output" "adapter_final_verdict" "GO"
  assert_field_equals "$skip_direct_output" "server_rollout_verdict" "HOLD"
  assert_field_equals "$skip_direct_output" "server_rollout_reason_code" "calibration_fee_not_pass"

  local profile_skip_output=""
  local profile_skip_exit_code=0
  if profile_skip_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/server-rollout-output-profile-skip" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      SERVER_ROLLOUT_PROFILE="finals_only" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60
  )"; then
    profile_skip_exit_code=0
  else
    profile_skip_exit_code=$?
  fi
  if [[ "$profile_skip_exit_code" -ne 2 ]]; then
    echo "expected server rollout finals_only profile hold exit code 2, got $profile_skip_exit_code" >&2
    echo "$profile_skip_output" >&2
    exit 1
  fi
  assert_field_equals "$profile_skip_output" "server_rollout_profile" "finals_only"
  assert_field_equals "$profile_skip_output" "server_rollout_run_go_nogo_direct" "false"
  assert_field_equals "$profile_skip_output" "server_rollout_run_rehearsal_direct" "false"
  assert_field_equals "$profile_skip_output" "go_nogo_verdict" "SKIP"
  assert_field_equals "$profile_skip_output" "rehearsal_verdict" "SKIP"
  assert_field_equals "$profile_skip_output" "go_nogo_reason_code" "stage_disabled"
  assert_field_equals "$profile_skip_output" "rehearsal_reason_code" "stage_disabled"

  local bundle_output
  local bundle_exit_code=0
  if bundle_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/server-rollout-output-with-bundle" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_server_rollout_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$bundle_output_dir" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60
  )"; then
    bundle_exit_code=0
  else
    bundle_exit_code=$?
  fi
  if [[ "$bundle_exit_code" -ne 2 ]]; then
    echo "expected bundled server rollout hold exit code 2, got $bundle_exit_code" >&2
    echo "$bundle_output" >&2
    exit 1
  fi
  assert_field_equals "$bundle_output" "server_rollout_verdict" "HOLD"
  assert_field_equals "$bundle_output" "server_rollout_reason_code" "calibration_fee_not_pass"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$bundle_output" "go_nogo_artifacts_written" "true"
  assert_field_equals "$bundle_output" "rehearsal_artifacts_written" "true"
  assert_field_equals "$bundle_output" "executor_final_artifacts_written" "true"
  assert_field_equals "$bundle_output" "adapter_final_artifacts_written" "true"
  assert_field_equals "$bundle_output" "go_nogo_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "rehearsal_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "executor_final_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "adapter_final_nested_package_bundle_enabled" "false"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  local server_rollout_bundle_path=""
  server_rollout_bundle_path="$(extract_field_value "$bundle_output" "package_bundle_path")"
  if [[ ! -f "$server_rollout_bundle_path" ]]; then
    echo "expected package bundle archive at $server_rollout_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"
  local bundled_manifest_text=""
  local bundled_manifest_entry=""
  bundled_manifest_entry="$(tar -tzf "$server_rollout_bundle_path" | awk -F/ -v target="$(basename "$(extract_field_value "$bundle_output" "artifact_manifest")")" '$NF==target{print; exit}')"
  bundled_manifest_text="$(tar -xOf "$server_rollout_bundle_path" "$bundled_manifest_entry")"
  assert_contains "$bundled_manifest_text" "preflight_capture_sha256:"

  local invalid_bool_output=""
  if invalid_bool_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="maybe" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60 2>&1
  )"; then
    echo "expected server rollout report to fail for invalid GO_NOGO_REQUIRE_JITO_RPC_POLICY token" >&2
    exit 1
  else
    local invalid_bool_exit_code=$?
    if [[ "$invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected server rollout invalid bool exit code 3, got $invalid_bool_exit_code" >&2
      echo "$invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_bool_output" "GO_NOGO_REQUIRE_JITO_RPC_POLICY must be a boolean token"
  assert_field_equals "$invalid_bool_output" "server_rollout_verdict" "NO_GO"
  assert_field_equals "$invalid_bool_output" "server_rollout_reason_code" "input_error"

  local invalid_profile_output=""
  if invalid_profile_output="$(
    PATH="$fake_curl_bin:$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      EXECUTOR_ENV_PATH="$executor_env_path" \
      ADAPTER_ENV_PATH="$adapter_env_path" \
      CONFIG_PATH="$config_path" \
      SERVER_ROLLOUT_PROFILE="bogus_profile" \
      bash "$ROOT_DIR/tools/execution_server_rollout_report.sh" 24 60 2>&1
  )"; then
    echo "expected server rollout report to fail for invalid SERVER_ROLLOUT_PROFILE" >&2
    exit 1
  else
    local invalid_profile_exit_code=$?
    if [[ "$invalid_profile_exit_code" -ne 3 ]]; then
      echo "expected server rollout invalid profile exit code 3, got $invalid_profile_exit_code" >&2
      echo "$invalid_profile_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_profile_output" "SERVER_ROLLOUT_PROFILE must be one of: full,finals_only"
  assert_field_equals "$invalid_profile_output" "server_rollout_reason_code" "input_error"
  echo "[ok] execution server rollout report"
}

run_adapter_rollout_evidence_case() {
  local db_path="$1"
  local config_path="$2"
  local env_path="$TMP_DIR/adapter-rollout.env"
  local secrets_dir="$TMP_DIR/secrets"
  local artifacts_dir="$TMP_DIR/adapter-rollout-artifacts"
  write_fake_journalctl
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
  chmod 600 "$secrets_dir"/*.token "$secrets_dir"/*.secret

  local pass_output
  pass_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60
  )"
  assert_contains "$pass_output" "=== Adapter Rollout Evidence Summary ==="
  assert_field_equals "$pass_output" "adapter_rollout_profile" "full"
  assert_field_equals "$pass_output" "adapter_rollout_run_rotation" "true"
  assert_field_equals "$pass_output" "adapter_rollout_run_rehearsal" "true"
  assert_field_equals "$pass_output" "adapter_rollout_run_route_fee_signoff" "true"
  assert_contains "$pass_output" "rotation_readiness_verdict: PASS"
  assert_contains "$pass_output" "rotation_artifact_manifest:"
  assert_contains "$pass_output" "rotation_report_sha256:"
  assert_field_equals "$pass_output" "rotation_artifacts_written" "true"
  assert_contains "$pass_output" "devnet_rehearsal_verdict: GO"
  assert_contains "$pass_output" "dynamic_cu_policy_verdict: SKIP"
  assert_contains "$pass_output" "dynamic_tip_policy_verdict: SKIP"
  assert_contains "$pass_output" "dynamic_cu_hint_api_total: 1"
  assert_contains "$pass_output" "dynamic_cu_hint_rpc_total: 1"
  assert_field_equals "$pass_output" "dynamic_cu_hint_api_configured" "false"
  assert_contains "$pass_output" "dynamic_cu_hint_source_verdict: SKIP"
  assert_field_equals "$pass_output" "dynamic_cu_hint_source_reason_code" "policy_disabled"
  assert_field_equals "$pass_output" "go_nogo_require_jito_rpc_policy" "false"
  assert_contains "$pass_output" "jito_rpc_policy_verdict: SKIP"
  assert_field_equals "$pass_output" "jito_rpc_policy_reason_code" "gate_disabled"
  assert_field_equals "$pass_output" "go_nogo_require_fastlane_disabled" "false"
  assert_field_equals "$pass_output" "submit_fastlane_enabled" "false"
  assert_contains "$pass_output" "fastlane_feature_flag_verdict: SKIP"
  assert_field_equals "$pass_output" "fastlane_feature_flag_reason_code" "gate_disabled"
  assert_field_equals "$pass_output" "route_fee_signoff_required" "false"
  assert_contains "$pass_output" "route_fee_signoff_verdict:"
  assert_field_non_empty "$pass_output" "route_fee_signoff_reason_code"
  assert_field_equals "$pass_output" "rehearsal_nested_package_bundle_enabled" "false"
  assert_field_equals "$pass_output" "route_fee_signoff_nested_package_bundle_enabled" "false"
  assert_contains "$pass_output" "route_fee_signoff_windows_csv: 1,6,24"
  assert_contains "$pass_output" "route_fee_signoff_artifact_manifest:"
  assert_contains "$pass_output" "route_fee_signoff_summary_sha256:"
  assert_field_equals "$pass_output" "route_fee_signoff_artifacts_written" "true"
  assert_contains "$pass_output" "route_fee_window_count:"
  assert_field_equals "$pass_output" "windowed_signoff_required" "false"
  assert_contains "$pass_output" "windowed_signoff_windows_csv: 1,6,24"
  assert_field_equals "$pass_output" "windowed_signoff_require_dynamic_hint_source_pass" "false"
  assert_field_equals "$pass_output" "windowed_signoff_require_dynamic_tip_policy_pass" "false"
  assert_contains "$pass_output" "windowed_signoff_verdict: GO"
  assert_contains "$pass_output" "windowed_signoff_artifact_manifest:"
  assert_contains "$pass_output" "windowed_signoff_summary_sha256:"
  assert_field_equals "$pass_output" "rehearsal_route_fee_signoff_required" "false"
  assert_contains "$pass_output" "rehearsal_route_fee_signoff_windows_csv: 1,6,24"
  assert_contains "$pass_output" "rehearsal_route_fee_signoff_verdict:"
  assert_field_non_empty "$pass_output" "rehearsal_route_fee_signoff_reason_code"
  assert_contains "$pass_output" "rehearsal_route_fee_signoff_artifact_manifest:"
  assert_contains "$pass_output" "rehearsal_route_fee_signoff_summary_sha256:"
  assert_field_equals "$pass_output" "rehearsal_route_fee_signoff_artifacts_written" "true"
  assert_contains "$pass_output" "primary_route:"
  assert_contains "$pass_output" "fallback_route:"
  assert_contains "$pass_output" "confirmed_orders_total:"
  assert_contains "$pass_output" "rehearsal_artifact_manifest:"
  assert_contains "$pass_output" "rehearsal_summary_sha256:"
  assert_field_equals "$pass_output" "go_nogo_artifacts_written" "true"
  assert_field_equals "$pass_output" "windowed_signoff_artifacts_written" "true"
  assert_field_equals "$pass_output" "rehearsal_artifacts_written" "true"
  assert_contains "$pass_output" "artifacts_written: true"
  assert_contains "$pass_output" "adapter_rollout_verdict: GO"
  assert_contains "$pass_output" "artifact_summary:"
  assert_contains "$pass_output" "artifact_route_fee_signoff_capture:"
  assert_contains "$pass_output" "artifact_manifest:"
  assert_contains "$pass_output" "summary_sha256:"
  assert_sha256_field "$pass_output" "summary_sha256"
  assert_sha256_field "$pass_output" "rotation_capture_sha256"
  assert_sha256_field "$pass_output" "rehearsal_capture_sha256"
  assert_sha256_field "$pass_output" "route_fee_signoff_capture_sha256"
  assert_sha256_field "$pass_output" "rotation_report_sha256"
  assert_sha256_field "$pass_output" "rehearsal_summary_sha256"
  assert_sha256_field "$pass_output" "rehearsal_preflight_sha256"
  assert_sha256_field "$pass_output" "rehearsal_go_nogo_sha256"
  assert_sha256_field "$pass_output" "rehearsal_tests_sha256"
  assert_sha256_field "$pass_output" "route_fee_signoff_summary_sha256"
  assert_sha256_field "$pass_output" "windowed_signoff_summary_sha256"
  assert_sha256_field "$pass_output" "go_nogo_calibration_sha256"
  assert_sha256_field "$pass_output" "go_nogo_snapshot_sha256"
  assert_sha256_field "$pass_output" "go_nogo_preflight_sha256"
  assert_sha256_field "$pass_output" "go_nogo_summary_sha256"
  assert_sha256_field "$pass_output" "manifest_sha256"
  assert_sha256_field_matches_file "$pass_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$pass_output" "manifest_sha256" "artifact_manifest"
  assert_contains "$pass_output" "go_nogo_artifact_manifest:"
  assert_contains "$pass_output" "go_nogo_summary_sha256:"
  if ! ls "$artifacts_dir"/adapter_rollout_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected adapter rollout summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/adapter_secret_rotation_captured_*.txt >/dev/null 2>&1; then
    echo "expected adapter rollout rotation capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_devnet_rehearsal_captured_*.txt >/dev/null 2>&1; then
    echo "expected adapter rollout rehearsal capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_route_fee_signoff_captured_*.txt >/dev/null 2>&1; then
    echo "expected adapter rollout route/fee signoff capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/rehearsal/windowed_signoff/execution_windowed_signoff_summary_*.txt >/dev/null 2>&1; then
    echo "expected nested windowed signoff summary artifact in $artifacts_dir/rehearsal/windowed_signoff" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/adapter_rollout_evidence_manifest_*.txt >/dev/null 2>&1; then
    echo "expected adapter rollout manifest artifact in $artifacts_dir" >&2
    exit 1
  fi

  local rollout_bundle_output_dir="$TMP_DIR/adapter-rollout-with-bundle"
  local rollout_bundle_archive_dir="$TMP_DIR/adapter-rollout-bundles"
  local rollout_bundle_output
  rollout_bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_DIR="$rollout_bundle_output_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="adapter_rollout_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$rollout_bundle_archive_dir" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$rollout_bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$rollout_bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$rollout_bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$rollout_bundle_output" "rehearsal_nested_package_bundle_enabled" "false"
  assert_field_equals "$rollout_bundle_output" "route_fee_signoff_nested_package_bundle_enabled" "false"
  assert_sha256_field "$rollout_bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$rollout_bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$rollout_bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_path"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$rollout_bundle_output" "package_bundle_contents_manifest"
  local adapter_rollout_bundle_path
  adapter_rollout_bundle_path="$(extract_field_value "$rollout_bundle_output" "package_bundle_path")"
  if [[ ! -f "$adapter_rollout_bundle_path" ]]; then
    echo "expected package bundle archive at $adapter_rollout_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$rollout_bundle_output"
  local adapter_rollout_rehearsal_capture_path=""
  local adapter_rollout_route_fee_capture_path=""
  adapter_rollout_rehearsal_capture_path="$(extract_field_value "$rollout_bundle_output" "artifact_rehearsal_capture")"
  adapter_rollout_route_fee_capture_path="$(extract_field_value "$rollout_bundle_output" "artifact_route_fee_signoff_capture")"
  if [[ -z "$adapter_rollout_rehearsal_capture_path" || ! -f "$adapter_rollout_rehearsal_capture_path" ]]; then
    echo "expected adapter rollout nested rehearsal capture artifact at $adapter_rollout_rehearsal_capture_path" >&2
    exit 1
  fi
  if [[ -z "$adapter_rollout_route_fee_capture_path" || ! -f "$adapter_rollout_route_fee_capture_path" ]]; then
    echo "expected adapter rollout nested route/fee signoff capture artifact at $adapter_rollout_route_fee_capture_path" >&2
    exit 1
  fi
  local adapter_rollout_rehearsal_capture_text=""
  local adapter_rollout_route_fee_capture_text=""
  adapter_rollout_rehearsal_capture_text="$(cat "$adapter_rollout_rehearsal_capture_path")"
  adapter_rollout_route_fee_capture_text="$(cat "$adapter_rollout_route_fee_capture_path")"
  assert_contains "$adapter_rollout_rehearsal_capture_text" "package_bundle_enabled: false"
  assert_contains "$adapter_rollout_route_fee_capture_text" "package_bundle_enabled: false"

  local invalid_bool_output=""
  if invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$TMP_DIR/missing-adapter.env" \
      CONFIG_PATH="$TMP_DIR/missing-config.toml" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="perhaps" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED token is invalid" >&2
    exit 1
  else
    local invalid_bool_exit_code=$?
    if [[ "$invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for invalid adapter rollout bool token, got $invalid_bool_exit_code" >&2
      echo "$invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_bool_output" "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED must be a boolean token"
  assert_contains "$invalid_bool_output" "got: perhaps"
  assert_field_equals "$invalid_bool_output" "rotation_readiness_verdict" "UNKNOWN"
  assert_field_equals "$invalid_bool_output" "route_fee_signoff_verdict" "UNKNOWN"
  assert_field_equals "$invalid_bool_output" "devnet_rehearsal_reason_code" "input_error"

  local rehearsal_only_profile_output
  rehearsal_only_profile_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ADAPTER_ROLLOUT_PROFILE="rehearsal_only" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_profile" "rehearsal_only"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_run_rotation" "false"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_run_rehearsal" "true"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_run_route_fee_signoff" "false"
  assert_field_equals "$rehearsal_only_profile_output" "rotation_readiness_verdict" "SKIP"
  assert_field_equals "$rehearsal_only_profile_output" "devnet_rehearsal_verdict" "GO"
  assert_field_equals "$rehearsal_only_profile_output" "route_fee_signoff_verdict" "SKIP"
  assert_field_equals "$rehearsal_only_profile_output" "route_fee_signoff_reason_code" "stage_disabled"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_verdict" "GO"
  assert_field_equals "$rehearsal_only_profile_output" "adapter_rollout_reason_code" "gates_pass"

  local route_fee_only_profile_output
  route_fee_only_profile_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ADAPTER_ROLLOUT_PROFILE="route_fee_only" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60
  )"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_profile" "route_fee_only"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_run_rotation" "false"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_run_rehearsal" "false"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_run_route_fee_signoff" "true"
  assert_field_equals "$route_fee_only_profile_output" "rotation_readiness_verdict" "SKIP"
  assert_field_equals "$route_fee_only_profile_output" "devnet_rehearsal_verdict" "SKIP"
  assert_field_equals "$route_fee_only_profile_output" "route_fee_signoff_verdict" "GO"
  assert_field_equals "$route_fee_only_profile_output" "route_fee_signoff_reason_code" "test_override"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_verdict" "GO"
  assert_field_equals "$route_fee_only_profile_output" "adapter_rollout_reason_code" "gates_pass_with_route_fee"

  local invalid_profile_output=""
  if invalid_profile_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      ADAPTER_ROLLOUT_PROFILE="bogus_profile" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when ADAPTER_ROLLOUT_PROFILE is invalid" >&2
    exit 1
  else
    local invalid_profile_exit_code=$?
    if [[ "$invalid_profile_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for invalid adapter rollout profile, got $invalid_profile_exit_code" >&2
      echo "$invalid_profile_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_profile_output" "ADAPTER_ROLLOUT_PROFILE must be one of: full,rehearsal_only,route_fee_only"
  assert_field_equals "$invalid_profile_output" "adapter_rollout_reason_code" "input_error"

  local all_disabled_output=""
  if all_disabled_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      ADAPTER_ROLLOUT_RUN_ROTATION="false" \
      ADAPTER_ROLLOUT_RUN_REHEARSAL="false" \
      ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when all stages are disabled" >&2
    exit 1
  else
    local all_disabled_exit_code=$?
    if [[ "$all_disabled_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for adapter rollout all-stages-disabled case, got $all_disabled_exit_code" >&2
      echo "$all_disabled_output" >&2
      exit 1
    fi
  fi
  assert_contains "$all_disabled_output" "at least one stage must be enabled"
  assert_field_equals "$all_disabled_output" "adapter_rollout_reason_code" "input_error"

  local required_rehearsal_route_fee_disabled_stage_output=""
  if required_rehearsal_route_fee_disabled_stage_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ADAPTER_ROLLOUT_RUN_REHEARSAL="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED=true and rehearsal stage disabled" >&2
    exit 1
  else
    local required_rehearsal_route_fee_disabled_stage_exit_code=$?
    if [[ "$required_rehearsal_route_fee_disabled_stage_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED + disabled rehearsal, got $required_rehearsal_route_fee_disabled_stage_exit_code" >&2
      echo "$required_rehearsal_route_fee_disabled_stage_output" >&2
      exit 1
    fi
  fi
  assert_contains "$required_rehearsal_route_fee_disabled_stage_output" "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED=true requires ADAPTER_ROLLOUT_RUN_REHEARSAL=true"
  assert_field_equals "$required_rehearsal_route_fee_disabled_stage_output" "adapter_rollout_reason_code" "input_error"

  local required_windowed_disabled_stage_output=""
  if required_windowed_disabled_stage_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      WINDOWED_SIGNOFF_REQUIRED="true" \
      ADAPTER_ROLLOUT_RUN_REHEARSAL="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when WINDOWED_SIGNOFF_REQUIRED=true and rehearsal stage disabled" >&2
    exit 1
  else
    local required_windowed_disabled_stage_exit_code=$?
    if [[ "$required_windowed_disabled_stage_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for WINDOWED_SIGNOFF_REQUIRED + disabled rehearsal, got $required_windowed_disabled_stage_exit_code" >&2
      echo "$required_windowed_disabled_stage_output" >&2
      exit 1
    fi
  fi
  assert_contains "$required_windowed_disabled_stage_output" "WINDOWED_SIGNOFF_REQUIRED=true requires ADAPTER_ROLLOUT_RUN_REHEARSAL=true"
  assert_field_equals "$required_windowed_disabled_stage_output" "adapter_rollout_reason_code" "input_error"

  local required_route_fee_disabled_stage_output=""
  if required_route_fee_disabled_stage_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for adapter rollout helper when ROUTE_FEE_SIGNOFF_REQUIRED=true and route-fee stage disabled" >&2
    exit 1
  else
    local required_route_fee_disabled_stage_exit_code=$?
    if [[ "$required_route_fee_disabled_stage_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for ROUTE_FEE_SIGNOFF_REQUIRED + disabled route-fee stage, got $required_route_fee_disabled_stage_exit_code" >&2
      echo "$required_route_fee_disabled_stage_output" >&2
      exit 1
    fi
  fi
  assert_contains "$required_route_fee_disabled_stage_output" "ROUTE_FEE_SIGNOFF_REQUIRED=true requires ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF=true"
  assert_field_equals "$required_route_fee_disabled_stage_output" "adapter_rollout_reason_code" "input_error"

  local final_artifacts_dir="$TMP_DIR/adapter-rollout-final-package"
  local final_output
  final_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$final_artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" 24 60
  )"
  assert_contains "$final_output" "=== Adapter Rollout Final Evidence Package ==="
  assert_field_equals "$final_output" "rollout_verdict" "GO"
  assert_field_equals "$final_output" "rollout_reason_code" "gates_pass"
  assert_field_equals "$final_output" "rollout_nested_package_bundle_enabled" "false"
  assert_field_equals "$final_output" "final_rollout_package_verdict" "GO"
  assert_field_equals "$final_output" "final_rollout_package_reason_code" "gates_pass"
  assert_contains "$final_output" "artifacts_written: true"
  assert_field_equals "$final_output" "rollout_artifacts_written" "true"
  assert_field_non_empty "$final_output" "rollout_artifact_summary"
  assert_field_non_empty "$final_output" "rollout_artifact_manifest"
  assert_sha256_field "$final_output" "summary_sha256"
  assert_sha256_field "$final_output" "rollout_capture_sha256"
  assert_sha256_field "$final_output" "manifest_sha256"
  assert_sha256_field_matches_file "$final_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$final_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$final_artifacts_dir"/adapter_rollout_final_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected final rollout package summary artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/adapter_rollout_final_evidence_manifest_*.txt >/dev/null 2>&1; then
    echo "expected final rollout package manifest artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/adapter_rollout_evidence_captured_*.txt >/dev/null 2>&1; then
    echo "expected final rollout package captured rollout artifact in $final_artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$final_artifacts_dir"/rollout/adapter_rollout_evidence_summary_*.txt >/dev/null 2>&1; then
    echo "expected nested rollout summary artifact in $final_artifacts_dir/rollout" >&2
    exit 1
  fi

  local final_bundle_output_dir="$TMP_DIR/adapter-rollout-final-package-with-bundle"
  local final_bundle_archive_dir="$TMP_DIR/adapter-rollout-final-package-bundles"
  local final_bundle_output
  final_bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$final_bundle_output_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="adapter_rollout_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$final_bundle_archive_dir" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" 24 60
  )"
  assert_field_equals "$final_bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$final_bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$final_bundle_output" "rollout_nested_package_bundle_enabled" "false"
  assert_sha256_field "$final_bundle_output" "package_bundle_sha256"
  assert_sha256_field_matches_file "$final_bundle_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$final_bundle_output" "manifest_sha256" "artifact_manifest"
  assert_field_non_empty "$final_bundle_output" "package_bundle_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$final_bundle_output" "package_bundle_contents_manifest"
  local package_bundle_path
  package_bundle_path="$(extract_field_value "$final_bundle_output" "package_bundle_path")"
  if [[ ! -f "$package_bundle_path" ]]; then
    echo "expected package bundle archive at $package_bundle_path" >&2
    exit 1
  fi
  assert_bundled_summary_manifest_package_status_parity "$final_bundle_output"
  local adapter_final_rollout_capture_path
  adapter_final_rollout_capture_path="$(extract_field_value "$final_bundle_output" "artifact_rollout_capture")"
  if [[ -z "$adapter_final_rollout_capture_path" || ! -f "$adapter_final_rollout_capture_path" ]]; then
    echo "expected adapter final nested rollout capture artifact at $adapter_final_rollout_capture_path" >&2
    exit 1
  fi
  local adapter_final_rollout_capture_text
  adapter_final_rollout_capture_text="$(cat "$adapter_final_rollout_capture_path")"
  assert_contains "$adapter_final_rollout_capture_text" "package_bundle_enabled: false"

  local final_nested_isolation_output=""
  if final_nested_isolation_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/adapter-rollout-final-package-isolation" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="1,invalid" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for final rollout package helper when nested rehearsal route/fee signoff is invalid" >&2
    exit 1
  else
    local final_nested_isolation_exit_code=$?
    if [[ "$final_nested_isolation_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for final rollout package nested override isolation, got $final_nested_isolation_exit_code" >&2
      echo "$final_nested_isolation_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$final_nested_isolation_output" "rollout_verdict" "NO_GO"
  assert_field_equals "$final_nested_isolation_output" "rollout_reason_code" "rehearsal_no_go"
  assert_field_equals "$final_nested_isolation_output" "final_rollout_package_verdict" "NO_GO"
  assert_field_equals "$final_nested_isolation_output" "final_rollout_package_reason_code" "rehearsal_no_go"

  local final_hold_output=""
  if final_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/adapter-rollout-final-package-hold" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="HOLD" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for final rollout package helper when required top-level route/fee signoff is HOLD" >&2
    exit 1
  else
    local final_hold_exit_code=$?
    if [[ "$final_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for final rollout package helper, got $final_hold_exit_code" >&2
      echo "$final_hold_output" >&2
      exit 1
    fi
  fi
  assert_field_equals "$final_hold_output" "rollout_verdict" "HOLD"
  assert_field_equals "$final_hold_output" "rollout_reason_code" "route_fee_signoff_hold"
  assert_field_equals "$final_hold_output" "final_rollout_package_verdict" "HOLD"
  assert_field_equals "$final_hold_output" "final_rollout_package_reason_code" "route_fee_signoff_hold"

  local final_invalid_bool_output=""
  if final_invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/adapter-rollout-final-package-invalid-bool" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="oops" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for final rollout package helper when REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE token is invalid" >&2
    exit 1
  else
    local final_invalid_bool_exit_code=$?
    if [[ "$final_invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for adapter final invalid bool token, got $final_invalid_bool_exit_code" >&2
      echo "$final_invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$final_invalid_bool_output" "REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE must be a boolean token"
  assert_field_equals "$final_invalid_bool_output" "rollout_verdict" "NO_GO"
  assert_field_equals "$final_invalid_bool_output" "rollout_reason_code" "input_error"
  assert_field_equals "$final_invalid_bool_output" "rollout_artifacts_written" "false"
  assert_field_equals "$final_invalid_bool_output" "final_rollout_package_reason_code" "input_error"

  local windowed_nogo_output=""
  if windowed_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="true" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      WINDOWED_SIGNOFF_REQUIRED="true" \
      WINDOWED_SIGNOFF_WINDOWS_CSV="1,invalid" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="true" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="true" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when required windowed signoff returns NO_GO" >&2
    exit 1
  else
    local windowed_nogo_exit_code=$?
    if [[ "$windowed_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required windowed signoff rollout branch, got $windowed_nogo_exit_code" >&2
      echo "$windowed_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$windowed_nogo_output" "windowed_signoff_required: true"
  assert_contains "$windowed_nogo_output" "windowed_signoff_require_dynamic_hint_source_pass: true"
  assert_contains "$windowed_nogo_output" "windowed_signoff_require_dynamic_tip_policy_pass: true"
  assert_contains "$windowed_nogo_output" "go_nogo_require_jito_rpc_policy: true"
  assert_contains "$windowed_nogo_output" "jito_rpc_policy_verdict: WARN"
  assert_field_in "$windowed_nogo_output" "jito_rpc_policy_reason_code" "target_mismatch" "route_profile_not_pass"
  assert_contains "$windowed_nogo_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$windowed_nogo_output" "submit_fastlane_enabled: false"
  assert_contains "$windowed_nogo_output" "fastlane_feature_flag_verdict: PASS"
  assert_field_equals "$windowed_nogo_output" "fastlane_feature_flag_reason_code" "fastlane_disabled"
  assert_contains "$windowed_nogo_output" "windowed_signoff_verdict: NO_GO"
  assert_contains "$windowed_nogo_output" "devnet_rehearsal_verdict: NO_GO"
  assert_contains "$windowed_nogo_output" "artifacts_written: false"
  assert_contains "$windowed_nogo_output" "adapter_rollout_verdict: NO_GO"

  local fastlane_strict_nogo_output=""
  if fastlane_strict_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="true" \
      SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED="true" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when strict fastlane-disabled gate is violated" >&2
    exit 1
  else
    local fastlane_strict_rollout_nogo_exit_code=$?
    if [[ "$fastlane_strict_rollout_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for strict fastlane-disabled rollout branch, got $fastlane_strict_rollout_nogo_exit_code" >&2
      echo "$fastlane_strict_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$fastlane_strict_nogo_output" "go_nogo_require_fastlane_disabled: true"
  assert_contains "$fastlane_strict_nogo_output" "submit_fastlane_enabled: true"
  assert_contains "$fastlane_strict_nogo_output" "fastlane_feature_flag_verdict: WARN"
  assert_field_equals "$fastlane_strict_nogo_output" "fastlane_feature_flag_reason_code" "fastlane_enabled"
  assert_contains "$fastlane_strict_nogo_output" "devnet_rehearsal_verdict: NO_GO"
  assert_field_equals "$fastlane_strict_nogo_output" "devnet_rehearsal_reason_code" "go_nogo_no_go"
  assert_contains "$fastlane_strict_nogo_output" "adapter_rollout_verdict: NO_GO"
  assert_field_equals "$fastlane_strict_nogo_output" "adapter_rollout_reason_code" "rehearsal_no_go"

  local route_fee_required_nogo_output=""
  if route_fee_required_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="1,invalid" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when required route/fee signoff returns NO_GO" >&2
    exit 1
  else
    local route_fee_required_nogo_exit_code=$?
    if [[ "$route_fee_required_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for required route/fee signoff rollout branch, got $route_fee_required_nogo_exit_code" >&2
      echo "$route_fee_required_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_verdict: NO_GO"
  assert_field_equals "$route_fee_required_nogo_output" "route_fee_signoff_reason_code" "input_error"
  assert_contains "$route_fee_required_nogo_output" "route_fee_signoff_windows_csv: 1,invalid"
  assert_contains "$route_fee_required_nogo_output" "adapter_rollout_verdict: NO_GO"
  assert_field_equals "$route_fee_required_nogo_output" "adapter_rollout_reason_code" "route_fee_signoff_no_go"

  local route_fee_required_hold_output=""
  if route_fee_required_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="HOLD" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for rollout helper when required route/fee signoff returns HOLD" >&2
    exit 1
  else
    local route_fee_required_hold_exit_code=$?
    if [[ "$route_fee_required_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for required route/fee signoff HOLD branch, got $route_fee_required_hold_exit_code" >&2
      echo "$route_fee_required_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_fee_required_hold_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_hold_output" "route_fee_signoff_verdict: HOLD"
  assert_field_equals "$route_fee_required_hold_output" "route_fee_signoff_reason_code" "test_override"
  assert_contains "$route_fee_required_hold_output" "devnet_rehearsal_verdict: GO"
  assert_contains "$route_fee_required_hold_output" "adapter_rollout_verdict: HOLD"
  assert_field_equals "$route_fee_required_hold_output" "adapter_rollout_reason_code" "route_fee_signoff_hold"

  local route_fee_required_go_output
  route_fee_required_go_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="24" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60
  )"
  assert_contains "$route_fee_required_go_output" "route_fee_signoff_required: true"
  assert_contains "$route_fee_required_go_output" "route_fee_signoff_verdict: GO"
  assert_field_equals "$route_fee_required_go_output" "route_fee_signoff_reason_code" "test_override"
  assert_contains "$route_fee_required_go_output" "devnet_rehearsal_verdict: GO"
  assert_contains "$route_fee_required_go_output" "adapter_rollout_verdict: GO"
  assert_field_equals "$route_fee_required_go_output" "adapter_rollout_reason_code" "gates_pass_with_route_fee"

  local route_fee_source_split_output=""
  if route_fee_source_split_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="true" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="1,invalid" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when nested rehearsal route/fee signoff is NO_GO" >&2
    exit 1
  else
    local route_fee_source_split_exit_code=$?
    if [[ "$route_fee_source_split_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for route/fee source split scenario, got $route_fee_source_split_exit_code" >&2
      echo "$route_fee_source_split_output" >&2
      exit 1
    fi
  fi
  assert_contains "$route_fee_source_split_output" "route_fee_signoff_verdict: GO"
  assert_field_equals "$route_fee_source_split_output" "route_fee_signoff_reason_code" "test_override"
  assert_contains "$route_fee_source_split_output" "rehearsal_route_fee_signoff_required: true"
  assert_contains "$route_fee_source_split_output" "rehearsal_route_fee_signoff_windows_csv: 1,invalid"
  assert_contains "$route_fee_source_split_output" "rehearsal_route_fee_signoff_verdict: NO_GO"
  assert_field_equals "$route_fee_source_split_output" "rehearsal_route_fee_signoff_reason_code" "input_error"
  assert_contains "$route_fee_source_split_output" "devnet_rehearsal_verdict: NO_GO"
  assert_contains "$route_fee_source_split_output" "adapter_rollout_verdict: NO_GO"
  assert_field_equals "$route_fee_source_split_output" "adapter_rollout_reason_code" "rehearsal_no_go"

  local rehearsal_hold_output=""
  if rehearsal_hold_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="SKIP" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="SKIP" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for rollout helper when rehearsal verdict is HOLD" >&2
    exit 1
  else
    local rehearsal_hold_exit_code=$?
    if [[ "$rehearsal_hold_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2 for rehearsal HOLD branch, got $rehearsal_hold_exit_code" >&2
      echo "$rehearsal_hold_output" >&2
      exit 1
    fi
  fi
  assert_contains "$rehearsal_hold_output" "rotation_readiness_verdict: PASS"
  assert_contains "$rehearsal_hold_output" "devnet_rehearsal_verdict: HOLD"
  assert_contains "$rehearsal_hold_output" "adapter_rollout_verdict: HOLD"

  local rehearsal_nogo_output=""
  if rehearsal_nogo_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="WARN" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when rehearsal verdict is NO_GO" >&2
    exit 1
  else
    local rehearsal_nogo_exit_code=$?
    if [[ "$rehearsal_nogo_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for rehearsal NO_GO branch, got $rehearsal_nogo_exit_code" >&2
      echo "$rehearsal_nogo_output" >&2
      exit 1
    fi
  fi
  assert_contains "$rehearsal_nogo_output" "rotation_readiness_verdict: PASS"
  assert_contains "$rehearsal_nogo_output" "devnet_rehearsal_verdict: NO_GO"
  assert_contains "$rehearsal_nogo_output" "adapter_rollout_verdict: NO_GO"

  chmod 644 "$secrets_dir/adapter_bearer.token"
  local warn_output=""
  if warn_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected HOLD exit for rollout helper when rotation readiness is WARN" >&2
    exit 1
  else
    local warn_exit_code=$?
    if [[ "$warn_exit_code" -ne 2 ]]; then
      echo "expected HOLD exit code 2, got $warn_exit_code" >&2
      echo "$warn_output" >&2
      exit 1
    fi
  fi
  assert_contains "$warn_output" "rotation_readiness_verdict: WARN"
  assert_contains "$warn_output" "adapter_rollout_verdict: HOLD"

  chmod 600 "$secrets_dir/adapter_bearer.token"
  rm -f "$secrets_dir/route_rpc_auth.token"
  local fail_output=""
  if fail_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper when rotation readiness is FAIL" >&2
    exit 1
  else
    local fail_exit_code=$?
    if [[ "$fail_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3, got $fail_exit_code" >&2
      echo "$fail_output" >&2
      exit 1
    fi
  fi
  assert_contains "$fail_output" "rotation_readiness_verdict: FAIL"
  assert_contains "$fail_output" "adapter_rollout_verdict: NO_GO"

  local missing_env_output=""
  if missing_env_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$TMP_DIR/adapter-rollout-missing.env" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 2>&1
  )"; then
    echo "expected NO_GO exit for rollout helper with missing adapter env input" >&2
    exit 1
  else
    local missing_env_exit_code=$?
    if [[ "$missing_env_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for missing env input, got $missing_env_exit_code" >&2
      echo "$missing_env_output" >&2
      exit 1
    fi
  fi
  assert_contains "$missing_env_output" "=== Adapter Rollout Evidence Summary ==="
  assert_contains "$missing_env_output" "adapter_rollout_verdict: NO_GO"
  assert_contains "$missing_env_output" "input_error: adapter env file not found:"
  echo "[ok] adapter rollout evidence helper"
}

run_execution_runtime_readiness_report_case() {
  local db_path="$1"
  local config_path="$2"
  local env_path="$TMP_DIR/runtime-readiness-adapter.env"
  local secrets_dir="$TMP_DIR/secrets"
  local artifacts_dir="$TMP_DIR/runtime-readiness-artifacts"
  write_fake_journalctl
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
  chmod 600 "$secrets_dir"/*.token "$secrets_dir"/*.secret

  local pass_output=""
  pass_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$artifacts_dir" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24"
  )"
  assert_contains "$pass_output" "=== Execution Runtime Readiness Report ==="
  assert_field_equals "$pass_output" "adapter_final_verdict" "GO"
  assert_field_equals "$pass_output" "route_fee_final_verdict" "GO"
  assert_field_equals "$pass_output" "runtime_readiness_verdict" "GO"
  assert_field_equals "$pass_output" "final_runtime_package_verdict" "GO"
  assert_field_equals "$pass_output" "final_runtime_package_reason_code" "gates_pass"
  assert_field_equals "$pass_output" "adapter_final_artifacts_written" "true"
  assert_field_equals "$pass_output" "route_fee_final_artifacts_written" "true"
  assert_contains "$pass_output" "artifacts_written: true"
  assert_contains "$pass_output" "artifact_summary:"
  assert_contains "$pass_output" "artifact_adapter_capture:"
  assert_contains "$pass_output" "artifact_route_fee_capture:"
  assert_contains "$pass_output" "artifact_manifest:"
  assert_sha256_field "$pass_output" "summary_sha256"
  assert_sha256_field "$pass_output" "adapter_capture_sha256"
  assert_sha256_field "$pass_output" "route_fee_capture_sha256"
  assert_sha256_field "$pass_output" "manifest_sha256"
  assert_sha256_field_matches_file "$pass_output" "summary_sha256" "artifact_summary"
  assert_sha256_field_matches_file "$pass_output" "manifest_sha256" "artifact_manifest"
  if ! ls "$artifacts_dir"/execution_runtime_readiness_summary_*.txt >/dev/null 2>&1; then
    echo "expected runtime readiness summary artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_runtime_readiness_manifest_*.txt >/dev/null 2>&1; then
    echo "expected runtime readiness manifest artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/adapter_rollout_final_captured_*.txt >/dev/null 2>&1; then
    echo "expected adapter final capture artifact in $artifacts_dir" >&2
    exit 1
  fi
  if ! ls "$artifacts_dir"/execution_route_fee_final_captured_*.txt >/dev/null 2>&1; then
    echo "expected route/fee final capture artifact in $artifacts_dir" >&2
    exit 1
  fi

  local skip_route_fee_output=""
  skip_route_fee_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/runtime-readiness-artifacts-skip-route-fee" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      RUNTIME_READINESS_RUN_ADAPTER_FINAL="true" \
      RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL="false" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24"
  )"
  assert_field_equals "$skip_route_fee_output" "runtime_readiness_run_adapter_final" "true"
  assert_field_equals "$skip_route_fee_output" "runtime_readiness_run_route_fee_final" "false"
  assert_field_equals "$skip_route_fee_output" "adapter_final_verdict" "GO"
  assert_field_equals "$skip_route_fee_output" "route_fee_final_verdict" "SKIP"
  assert_field_equals "$skip_route_fee_output" "route_fee_final_reason_code" "stage_disabled"
  assert_field_equals "$skip_route_fee_output" "runtime_readiness_verdict" "GO"
  assert_field_equals "$skip_route_fee_output" "final_runtime_package_reason_code" "gates_pass"

  local profile_adapter_only_output=""
  profile_adapter_only_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/runtime-readiness-artifacts-profile-adapter-only" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      RUNTIME_READINESS_PROFILE="adapter_only" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24"
  )"
  assert_field_equals "$profile_adapter_only_output" "runtime_readiness_profile" "adapter_only"
  assert_field_equals "$profile_adapter_only_output" "runtime_readiness_run_adapter_final" "true"
  assert_field_equals "$profile_adapter_only_output" "runtime_readiness_run_route_fee_final" "false"
  assert_field_equals "$profile_adapter_only_output" "adapter_final_verdict" "GO"
  assert_field_equals "$profile_adapter_only_output" "route_fee_final_verdict" "SKIP"
  assert_field_equals "$profile_adapter_only_output" "runtime_readiness_verdict" "GO"

  local profile_route_fee_only_output=""
  profile_route_fee_only_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/runtime-readiness-artifacts-profile-route-fee-only" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      RUNTIME_READINESS_PROFILE="route_fee_only" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24"
  )"
  assert_field_equals "$profile_route_fee_only_output" "runtime_readiness_profile" "route_fee_only"
  assert_field_equals "$profile_route_fee_only_output" "runtime_readiness_run_adapter_final" "false"
  assert_field_equals "$profile_route_fee_only_output" "runtime_readiness_run_route_fee_final" "true"
  assert_field_equals "$profile_route_fee_only_output" "adapter_final_verdict" "SKIP"
  assert_field_equals "$profile_route_fee_only_output" "route_fee_final_verdict" "GO"
  assert_field_equals "$profile_route_fee_only_output" "runtime_readiness_verdict" "GO"

  local bundle_output=""
  bundle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      SERVICE="copybot-smoke-service" \
      OUTPUT_ROOT="$TMP_DIR/runtime-readiness-artifacts-bundle" \
      RUN_TESTS="false" \
      DEVNET_REHEARSAL_TEST_MODE="true" \
      GO_NOGO_TEST_MODE="true" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="PASS" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="false" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="false" \
      WINDOWED_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="false" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="GO" \
      PACKAGE_BUNDLE_ENABLED="true" \
      PACKAGE_BUNDLE_LABEL="execution_runtime_readiness_smoke_bundle" \
      PACKAGE_BUNDLE_OUTPUT_DIR="$TMP_DIR/runtime-readiness-bundles" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24"
  )"
  assert_field_equals "$bundle_output" "package_bundle_enabled" "true"
  assert_field_equals "$bundle_output" "package_bundle_artifacts_written" "true"
  assert_field_equals "$bundle_output" "package_bundle_exit_code" "0"
  assert_field_equals "$bundle_output" "adapter_final_artifacts_written" "true"
  assert_field_equals "$bundle_output" "route_fee_final_artifacts_written" "true"
  assert_field_equals "$bundle_output" "adapter_final_nested_package_bundle_enabled" "false"
  assert_field_equals "$bundle_output" "route_fee_final_nested_package_bundle_enabled" "false"
  assert_sha256_field "$bundle_output" "package_bundle_sha256"
  assert_field_non_empty "$bundle_output" "package_bundle_path"
  assert_field_non_empty "$bundle_output" "package_bundle_sha256_path"
  assert_field_non_empty "$bundle_output" "package_bundle_contents_manifest"
  assert_bundled_summary_manifest_package_status_parity "$bundle_output"
  local bundle_adapter_capture_path=""
  local bundle_route_fee_capture_path=""
  bundle_adapter_capture_path="$(extract_field_value "$bundle_output" "artifact_adapter_capture")"
  bundle_route_fee_capture_path="$(extract_field_value "$bundle_output" "artifact_route_fee_capture")"
  if [[ -z "$bundle_adapter_capture_path" || ! -f "$bundle_adapter_capture_path" ]]; then
    echo "expected runtime readiness adapter capture artifact at $bundle_adapter_capture_path" >&2
    exit 1
  fi
  if [[ -z "$bundle_route_fee_capture_path" || ! -f "$bundle_route_fee_capture_path" ]]; then
    echo "expected runtime readiness route/fee capture artifact at $bundle_route_fee_capture_path" >&2
    exit 1
  fi
  local bundle_adapter_capture_text=""
  local bundle_route_fee_capture_text=""
  bundle_adapter_capture_text="$(cat "$bundle_adapter_capture_path")"
  bundle_route_fee_capture_text="$(cat "$bundle_route_fee_capture_path")"
  assert_contains "$bundle_adapter_capture_text" "package_bundle_enabled: false"
  assert_contains "$bundle_route_fee_capture_text" "package_bundle_enabled: false"

  local invalid_bool_output=""
  if invalid_bool_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$TMP_DIR/missing-adapter.env" \
      CONFIG_PATH="$TMP_DIR/missing-config.toml" \
      PACKAGE_BUNDLE_ENABLED="maybe" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24" 2>&1
  )"; then
    echo "expected NO_GO exit for runtime readiness helper invalid PACKAGE_BUNDLE_ENABLED token" >&2
    exit 1
  else
    local invalid_bool_exit_code=$?
    if [[ "$invalid_bool_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for invalid runtime readiness bool token, got $invalid_bool_exit_code" >&2
      echo "$invalid_bool_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_bool_output" "input_error: PACKAGE_BUNDLE_ENABLED must be a boolean token"
  assert_field_equals "$invalid_bool_output" "runtime_readiness_reason_code" "input_error"
  assert_field_equals "$invalid_bool_output" "final_runtime_package_reason_code" "input_error"

  local invalid_stage_toggle_output=""
  if invalid_stage_toggle_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      RUNTIME_READINESS_RUN_ADAPTER_FINAL="false" \
      RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL="false" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24" 2>&1
  )"; then
    echo "expected NO_GO exit for runtime readiness with both stages disabled" >&2
    exit 1
  else
    local invalid_stage_toggle_exit_code=$?
    if [[ "$invalid_stage_toggle_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for runtime readiness stage toggle misconfiguration, got $invalid_stage_toggle_exit_code" >&2
      echo "$invalid_stage_toggle_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_stage_toggle_output" "input_error: at least one stage must be enabled"
  assert_field_equals "$invalid_stage_toggle_output" "runtime_readiness_reason_code" "input_error"

  local invalid_profile_output=""
  if invalid_profile_output="$(
    PATH="$FAKE_BIN_DIR:$PATH" \
      DB_PATH="$db_path" \
      ADAPTER_ENV_PATH="$env_path" \
      CONFIG_PATH="$config_path" \
      RUNTIME_READINESS_PROFILE="bogus_profile" \
      bash "$ROOT_DIR/tools/execution_runtime_readiness_report.sh" "24" "60" "24" 2>&1
  )"; then
    echo "expected NO_GO exit for runtime readiness invalid profile" >&2
    exit 1
  else
    local invalid_profile_exit_code=$?
    if [[ "$invalid_profile_exit_code" -ne 3 ]]; then
      echo "expected NO_GO exit code 3 for runtime readiness invalid profile, got $invalid_profile_exit_code" >&2
      echo "$invalid_profile_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_profile_output" "input_error: RUNTIME_READINESS_PROFILE must be one of: full,adapter_only,route_fee_only"
  assert_field_equals "$invalid_profile_output" "runtime_readiness_reason_code" "input_error"
  echo "[ok] execution runtime readiness report"
}

run_common_strict_bool_parser_case() {
  local true_output=""
  true_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_bool_token_strict " yes "
    '
  )"
  if [[ "$true_output" != "true" ]]; then
    echo "expected parse_bool_token_strict to normalize \"yes\" into true, got: $true_output" >&2
    exit 1
  fi

  local false_output=""
  false_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_bool_token_strict "off"
    '
  )"
  if [[ "$false_output" != "false" ]]; then
    echo "expected parse_bool_token_strict to normalize \"off\" into false, got: $false_output" >&2
    exit 1
  fi

  local empty_output=""
  if empty_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_bool_token_strict ""
    ' 2>&1
  )"; then
    echo "expected parse_bool_token_strict to fail for empty token" >&2
    exit 1
  else
    local empty_exit_code=$?
    if [[ "$empty_exit_code" -ne 1 ]]; then
      echo "expected parse_bool_token_strict empty token exit code 1, got $empty_exit_code" >&2
      echo "$empty_output" >&2
      exit 1
    fi
  fi

  local invalid_output=""
  if invalid_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_bool_token_strict "maybe"
    ' 2>&1
  )"; then
    echo "expected parse_bool_token_strict to fail for invalid token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected parse_bool_token_strict invalid token exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi

  echo "[ok] common strict bool parser"
}

run_common_bool_compat_wrapper_case() {
  local true_output=""
  true_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      normalize_bool_token " yes "
    '
  )"
  if [[ "$true_output" != "true" ]]; then
    echo "expected normalize_bool_token to normalize \"yes\" into true, got: $true_output" >&2
    exit 1
  fi

  local empty_output=""
  empty_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      normalize_bool_token ""
    '
  )"
  if [[ "$empty_output" != "false" ]]; then
    echo "expected normalize_bool_token to normalize empty token into false, got: $empty_output" >&2
    exit 1
  fi

  local invalid_output=""
  if invalid_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      normalize_bool_token "maybe"
    ' 2>&1
  )"; then
    echo "expected normalize_bool_token to fail for invalid token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected normalize_bool_token invalid token exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "invalid boolean token (expected true/false/1/0/yes/no/on/off), got: maybe"
  echo "[ok] common bool compat wrapper"
}

run_common_timeout_parser_case() {
  local valid_output=""
  valid_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_timeout_sec_strict "600" 1 86400
    '
  )"
  if [[ "$valid_output" != "600" ]]; then
    echo "expected parse_timeout_sec_strict to accept 600, got: $valid_output" >&2
    exit 1
  fi

  local zero_output=""
  if zero_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_timeout_sec_strict "0" 1 86400
    ' 2>&1
  )"; then
    echo "expected parse_timeout_sec_strict to reject 0" >&2
    exit 1
  else
    local zero_exit_code=$?
    if [[ "$zero_exit_code" -ne 1 ]]; then
      echo "expected parse_timeout_sec_strict zero timeout exit code 1, got $zero_exit_code" >&2
      echo "$zero_output" >&2
      exit 1
    fi
  fi

  local over_limit_output=""
  if over_limit_output="$(
    ROOT_DIR="$ROOT_DIR" bash -c '
      set -euo pipefail
      # shellcheck source=tools/lib/common.sh
      source "$ROOT_DIR/tools/lib/common.sh"
      parse_timeout_sec_strict "86401" 1 86400
    ' 2>&1
  )"; then
    echo "expected parse_timeout_sec_strict to reject values above upper bound" >&2
    exit 1
  else
    local over_limit_exit_code=$?
    if [[ "$over_limit_exit_code" -ne 1 ]]; then
      echo "expected parse_timeout_sec_strict over-limit exit code 1, got $over_limit_exit_code" >&2
      echo "$over_limit_output" >&2
      exit 1
    fi
  fi

  echo "[ok] common timeout parser"
}

run_audit_standard_strict_bool_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="maybe" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_SKIP_OPS_SMOKE token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_SKIP_OPS_SMOKE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_SKIP_OPS_SMOKE must be boolean token"
  echo "[ok] audit standard strict bool guard"
}

run_audit_standard_invalid_diff_range_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_DIFF_RANGE="not-a-valid-diff-range...HEAD" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_DIFF_RANGE" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_DIFF_RANGE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_DIFF_RANGE is invalid: not-a-valid-diff-range...HEAD"
  if grep -Fq "[audit:standard] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_standard.sh to fail before quick baseline for invalid AUDIT_DIFF_RANGE" >&2
    exit 1
  fi
  echo "[ok] audit standard invalid diff range guard"
}

run_audit_standard_contract_smoke_strict_bool_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="maybe" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_SKIP_CONTRACT_SMOKE token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_SKIP_CONTRACT_SMOKE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token"
  if grep -Fq "[audit:standard] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_standard.sh to fail before quick baseline for invalid AUDIT_SKIP_CONTRACT_SMOKE" >&2
    exit 1
  fi
  echo "[ok] audit standard contract-smoke strict bool guard"
}

run_audit_standard_package_test_timeout_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_PACKAGE_TEST_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_PACKAGE_TEST_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_PACKAGE_TEST_TIMEOUT_SEC exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_PACKAGE_TEST_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:standard] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_standard.sh to fail before quick baseline for invalid package-test timeout" >&2
    exit 1
  fi
  echo "[ok] audit standard package-test timeout strict guard"
}

run_audit_ops_smoke_timeout_guard_case() {
  local invalid_standard_output=""
  if invalid_standard_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_OPS_SMOKE_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_OPS_SMOKE_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_standard_exit_code=$?
    if [[ "$invalid_standard_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_OPS_SMOKE_TIMEOUT_SEC exit code 1, got $invalid_standard_exit_code" >&2
      echo "$invalid_standard_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_standard_output" "AUDIT_OPS_SMOKE_TIMEOUT_SEC must be integer seconds >= 1"

  local invalid_full_output=""
  if invalid_full_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_OPS_SMOKE_TIMEOUT_SEC="0" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for zero AUDIT_OPS_SMOKE_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_full_exit_code=$?
    if [[ "$invalid_full_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_OPS_SMOKE_TIMEOUT_SEC exit code 1, got $invalid_full_exit_code" >&2
      echo "$invalid_full_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_full_output" "AUDIT_OPS_SMOKE_TIMEOUT_SEC must be integer seconds >= 1"
  echo "[ok] audit ops smoke timeout strict guard"
}

run_audit_contract_smoke_timeout_guard_case() {
  local invalid_quick_output=""
  if invalid_quick_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="false" \
      AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_quick_exit_code=$?
    if [[ "$invalid_quick_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC exit code 1, got $invalid_quick_exit_code" >&2
      echo "$invalid_quick_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_quick_output" "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:quick] cargo test -p copybot-executor -q" <<<"$invalid_quick_output"; then
    echo "expected audit_quick.sh to fail before baseline tests for invalid contract-smoke timeout" >&2
    exit 1
  fi

  local invalid_standard_output=""
  if invalid_standard_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC="0" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for zero AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_standard_exit_code=$?
    if [[ "$invalid_standard_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC exit code 1, got $invalid_standard_exit_code" >&2
      echo "$invalid_standard_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_standard_output" "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1"
  echo "[ok] audit contract smoke timeout strict guard"
}

run_audit_executor_test_timeout_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_EXECUTOR_TEST_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_EXECUTOR_TEST_TIMEOUT_SEC exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:quick] cargo test -p copybot-executor -q" <<<"$invalid_output"; then
    echo "expected audit_quick.sh to fail before executor test baseline for invalid timeout" >&2
    exit 1
  fi
  echo "[ok] audit executor test timeout strict guard"
}

run_audit_workspace_test_timeout_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_WORKSPACE_TEST_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_WORKSPACE_TEST_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_WORKSPACE_TEST_TIMEOUT_SEC exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_WORKSPACE_TEST_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:full] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_full.sh to fail before baseline for invalid workspace timeout" >&2
    exit 1
  fi
  echo "[ok] audit workspace test timeout strict guard"
}

run_audit_standard_executor_test_timeout_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="0" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for zero AUDIT_EXECUTOR_TEST_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_EXECUTOR_TEST_TIMEOUT_SEC exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:standard] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_standard.sh to fail before baseline for invalid executor-test timeout" >&2
    exit 1
  fi
  echo "[ok] audit standard executor-test timeout strict guard"
}

run_audit_full_executor_test_timeout_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="abc" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_EXECUTOR_TEST_TIMEOUT_SEC token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_EXECUTOR_TEST_TIMEOUT_SEC exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1"
  if grep -Fq "[audit:full] running quick baseline" <<<"$invalid_output"; then
    echo "expected audit_full.sh to fail before baseline for invalid executor-test timeout" >&2
    exit 1
  fi
  echo "[ok] audit full executor-test timeout strict guard"
}

run_audit_timeout_upper_bound_guard_batch_case() {
  local quick_over_limit_output=""
  if quick_over_limit_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="86401" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for over-limit AUDIT_EXECUTOR_TEST_TIMEOUT_SEC" >&2
    exit 1
  else
    local quick_over_limit_exit_code=$?
    if [[ "$quick_over_limit_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh over-limit AUDIT_EXECUTOR_TEST_TIMEOUT_SEC exit code 1, got $quick_over_limit_exit_code" >&2
      echo "$quick_over_limit_output" >&2
      exit 1
    fi
  fi
  assert_contains "$quick_over_limit_output" "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= 86400"

  local standard_over_limit_output=""
  if standard_over_limit_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_PACKAGE_TEST_TIMEOUT_SEC="86401" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for over-limit AUDIT_PACKAGE_TEST_TIMEOUT_SEC" >&2
    exit 1
  else
    local standard_over_limit_exit_code=$?
    if [[ "$standard_over_limit_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh over-limit AUDIT_PACKAGE_TEST_TIMEOUT_SEC exit code 1, got $standard_over_limit_exit_code" >&2
      echo "$standard_over_limit_output" >&2
      exit 1
    fi
  fi
  assert_contains "$standard_over_limit_output" "AUDIT_PACKAGE_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= 86400"
  if grep -Fq "[audit:standard] running quick baseline" <<<"$standard_over_limit_output"; then
    echo "expected audit_standard.sh to fail before baseline for over-limit package timeout" >&2
    exit 1
  fi

  local full_over_limit_output=""
  if full_over_limit_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_WORKSPACE_TEST_TIMEOUT_SEC="86401" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for over-limit AUDIT_WORKSPACE_TEST_TIMEOUT_SEC" >&2
    exit 1
  else
    local full_over_limit_exit_code=$?
    if [[ "$full_over_limit_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh over-limit AUDIT_WORKSPACE_TEST_TIMEOUT_SEC exit code 1, got $full_over_limit_exit_code" >&2
      echo "$full_over_limit_output" >&2
      exit 1
    fi
  fi
  assert_contains "$full_over_limit_output" "AUDIT_WORKSPACE_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= 86400"
  if grep -Fq "[audit:full] running quick baseline" <<<"$full_over_limit_output"; then
    echo "expected audit_full.sh to fail before baseline for over-limit workspace timeout" >&2
    exit 1
  fi

  echo "[ok] audit timeout upper-bound guard batch"
}

run_audit_skip_gate_strict_bool_batch_case() {
  local quick_output=""
  if quick_output="$(
    AUDIT_SKIP_EXECUTOR_TESTS="maybe" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_SKIP_EXECUTOR_TESTS token" >&2
    exit 1
  else
    local quick_exit_code=$?
    if [[ "$quick_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_SKIP_EXECUTOR_TESTS exit code 1, got $quick_exit_code" >&2
      echo "$quick_output" >&2
      exit 1
    fi
  fi
  assert_contains "$quick_output" "AUDIT_SKIP_EXECUTOR_TESTS must be boolean token"

  local standard_output=""
  if standard_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_PACKAGE_TESTS="maybe" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for invalid AUDIT_SKIP_PACKAGE_TESTS token" >&2
    exit 1
  else
    local standard_exit_code=$?
    if [[ "$standard_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh invalid AUDIT_SKIP_PACKAGE_TESTS exit code 1, got $standard_exit_code" >&2
      echo "$standard_output" >&2
      exit 1
    fi
  fi
  assert_contains "$standard_output" "AUDIT_SKIP_PACKAGE_TESTS must be boolean token"

  local full_output=""
  if full_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_WORKSPACE_TESTS="maybe" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_SKIP_WORKSPACE_TESTS token" >&2
    exit 1
  else
    local full_exit_code=$?
    if [[ "$full_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_SKIP_WORKSPACE_TESTS exit code 1, got $full_exit_code" >&2
      echo "$full_output" >&2
      exit 1
    fi
  fi
  assert_contains "$full_output" "AUDIT_SKIP_WORKSPACE_TESTS must be boolean token"
  echo "[ok] audit skip-gate strict bool batch"
}

run_audit_full_strict_bool_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="maybe" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_SKIP_OPS_SMOKE token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_SKIP_OPS_SMOKE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_SKIP_OPS_SMOKE must be boolean token"
  echo "[ok] audit full strict bool guard"
}

run_audit_full_contract_smoke_strict_bool_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="maybe" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_SKIP_CONTRACT_SMOKE token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_SKIP_CONTRACT_SMOKE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token"
  echo "[ok] audit full contract-smoke strict bool guard"
}

run_audit_quick_strict_bool_guard_case() {
  local invalid_output=""
  if invalid_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="maybe" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_SKIP_CONTRACT_SMOKE token" >&2
    exit 1
  else
    local invalid_exit_code=$?
    if [[ "$invalid_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_SKIP_CONTRACT_SMOKE exit code 1, got $invalid_exit_code" >&2
      echo "$invalid_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_output" "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token"
  echo "[ok] audit quick strict bool guard"
}

run_audit_contract_smoke_mode_guard_case() {
  local invalid_mode_output=""
  if invalid_mode_output="$(
    AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_CONTRACT_SMOKE_MODE="bogus_mode" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_CONTRACT_SMOKE_MODE" >&2
    exit 1
  else
    local invalid_mode_exit_code=$?
    if [[ "$invalid_mode_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_CONTRACT_SMOKE_MODE exit code 1, got $invalid_mode_exit_code" >&2
      echo "$invalid_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_mode_output" "AUDIT_CONTRACT_SMOKE_MODE must be one of: full,targeted"

  local empty_targets_output=""
  if empty_targets_output="$(
    AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_CONTRACT_SMOKE_MODE="targeted" \
      AUDIT_CONTRACT_SMOKE_TARGET_TESTS=" " \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for empty AUDIT_CONTRACT_SMOKE_TARGET_TESTS in targeted mode" >&2
    exit 1
  else
    local empty_targets_exit_code=$?
    if [[ "$empty_targets_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh empty AUDIT_CONTRACT_SMOKE_TARGET_TESTS exit code 1, got $empty_targets_exit_code" >&2
      echo "$empty_targets_output" >&2
      exit 1
    fi
  fi
  assert_contains "$empty_targets_output" "AUDIT_CONTRACT_SMOKE_TARGET_TESTS must be non-empty when AUDIT_CONTRACT_SMOKE_MODE=targeted"

  local standard_targeted_output=""
  standard_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="false" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_PACKAGE_TESTS="true" \
      AUDIT_CONTRACT_SMOKE_MODE="targeted" \
      AUDIT_CONTRACT_SMOKE_TARGET_TESTS="constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_standard.sh"
  )"
  assert_contains "$standard_targeted_output" "[audit:quick] tools/executor_contract_smoke_test.sh (mode=targeted"
  assert_contains "$standard_targeted_output" "[audit:standard] PASS"

  local full_targeted_output=""
  full_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="false" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_CONTRACT_SMOKE_MODE="targeted" \
      AUDIT_CONTRACT_SMOKE_TARGET_TESTS="constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_full.sh"
  )"
  assert_contains "$full_targeted_output" "[audit:quick] tools/executor_contract_smoke_test.sh (mode=targeted"
  assert_contains "$full_targeted_output" "[audit:full] PASS"
  echo "[ok] audit contract smoke mode guard"
}

run_audit_executor_test_mode_guard_case() {
  local invalid_mode_output=""
  if invalid_mode_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="bogus_mode" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for invalid AUDIT_EXECUTOR_TEST_MODE" >&2
    exit 1
  else
    local invalid_mode_exit_code=$?
    if [[ "$invalid_mode_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh invalid AUDIT_EXECUTOR_TEST_MODE exit code 1, got $invalid_mode_exit_code" >&2
      echo "$invalid_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_mode_output" "AUDIT_EXECUTOR_TEST_MODE must be one of: full,targeted"

  local empty_targets_output=""
  if empty_targets_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS=" " \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for empty AUDIT_EXECUTOR_TEST_TARGETS in targeted mode" >&2
    exit 1
  else
    local empty_targets_exit_code=$?
    if [[ "$empty_targets_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh empty AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $empty_targets_exit_code" >&2
      echo "$empty_targets_output" >&2
      exit 1
    fi
  fi
  assert_contains "$empty_targets_output" "AUDIT_EXECUTOR_TEST_TARGETS must be non-empty when AUDIT_EXECUTOR_TEST_MODE=targeted"

  local unknown_target_output=""
  if unknown_target_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="definitely_nonexistent_test_name_12345" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for unknown AUDIT_EXECUTOR_TEST_TARGETS entry in targeted mode" >&2
    exit 1
  else
    local unknown_target_exit_code=$?
    if [[ "$unknown_target_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh unknown AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $unknown_target_exit_code" >&2
      echo "$unknown_target_output" >&2
      exit 1
    fi
  fi
  assert_contains "$unknown_target_output" "unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: definitely_nonexistent_test_name_12345"

  local ambiguous_target_output=""
  if ambiguous_target_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="route" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for ambiguous AUDIT_EXECUTOR_TEST_TARGETS entry in targeted mode" >&2
    exit 1
  else
    local ambiguous_target_exit_code=$?
    if [[ "$ambiguous_target_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh ambiguous AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $ambiguous_target_exit_code" >&2
      echo "$ambiguous_target_output" >&2
      exit 1
    fi
  fi
  assert_contains "$ambiguous_target_output" "ambiguous executor test target in AUDIT_EXECUTOR_TEST_TARGETS: route"
  assert_contains "$ambiguous_target_output" "matched tests:"

  local duplicate_target_output=""
  if duplicate_target_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="constant_time_eq_checks_content,constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_quick.sh" 2>&1
  )"; then
    echo "expected audit_quick.sh to fail for duplicate AUDIT_EXECUTOR_TEST_TARGETS entry in targeted mode" >&2
    exit 1
  else
    local duplicate_target_exit_code=$?
    if [[ "$duplicate_target_exit_code" -ne 1 ]]; then
      echo "expected audit_quick.sh duplicate AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $duplicate_target_exit_code" >&2
      echo "$duplicate_target_output" >&2
      exit 1
    fi
  fi
  assert_contains "$duplicate_target_output" "duplicate executor test target in AUDIT_EXECUTOR_TEST_TARGETS after resolution"

  local standard_unknown_target_output=""
  if standard_unknown_target_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_SKIP_PACKAGE_TESTS="true" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="definitely_nonexistent_test_name_12345" \
      bash "$ROOT_DIR/tools/audit_standard.sh" 2>&1
  )"; then
    echo "expected audit_standard.sh to fail for unknown AUDIT_EXECUTOR_TEST_TARGETS entry in targeted mode" >&2
    exit 1
  else
    local standard_unknown_target_exit_code=$?
    if [[ "$standard_unknown_target_exit_code" -ne 1 ]]; then
      echo "expected audit_standard.sh unknown AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $standard_unknown_target_exit_code" >&2
      echo "$standard_unknown_target_output" >&2
      exit 1
    fi
  fi
  assert_contains "$standard_unknown_target_output" "unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: definitely_nonexistent_test_name_12345"

  local full_unknown_target_output=""
  if full_unknown_target_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="definitely_nonexistent_test_name_12345" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for unknown AUDIT_EXECUTOR_TEST_TARGETS entry in targeted mode" >&2
    exit 1
  else
    local full_unknown_target_exit_code=$?
    if [[ "$full_unknown_target_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh unknown AUDIT_EXECUTOR_TEST_TARGETS exit code 1, got $full_unknown_target_exit_code" >&2
      echo "$full_unknown_target_output" >&2
      exit 1
    fi
  fi
  assert_contains "$full_unknown_target_output" "unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: definitely_nonexistent_test_name_12345"

  local quick_targeted_output=""
  quick_targeted_output="$(
    AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_quick.sh"
  )"
  assert_contains "$quick_targeted_output" "[audit:quick] cargo test -p copybot-executor -q (mode=targeted"
  assert_contains "$quick_targeted_output" "[audit:quick] PASS"

  local standard_targeted_output=""
  standard_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_SKIP_PACKAGE_TESTS="true" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_standard.sh"
  )"
  assert_contains "$standard_targeted_output" "[audit:quick] cargo test -p copybot-executor -q (mode=targeted"
  assert_contains "$standard_targeted_output" "[audit:standard] PASS"

  local full_targeted_output=""
  full_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="true" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="false" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_EXECUTOR_TEST_MODE="targeted" \
      AUDIT_EXECUTOR_TEST_TARGETS="constant_time_eq_checks_content" \
      bash "$ROOT_DIR/tools/audit_full.sh"
  )"
  assert_contains "$full_targeted_output" "[audit:quick] cargo test -p copybot-executor -q (mode=targeted"
  assert_contains "$full_targeted_output" "[audit:full] PASS"
  echo "[ok] audit executor test mode guard"
}

run_audit_ops_smoke_mode_guard_case() {
  local invalid_mode_output=""
  if invalid_mode_output="$(
    AUDIT_SKIP_OPS_SMOKE="false" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_OPS_SMOKE_MODE="bogus_mode" \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for invalid AUDIT_OPS_SMOKE_MODE" >&2
    exit 1
  else
    local invalid_mode_exit_code=$?
    if [[ "$invalid_mode_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh invalid AUDIT_OPS_SMOKE_MODE exit code 1, got $invalid_mode_exit_code" >&2
      echo "$invalid_mode_output" >&2
      exit 1
    fi
  fi
  assert_contains "$invalid_mode_output" "AUDIT_OPS_SMOKE_MODE must be one of: full,targeted"

  local empty_targets_output=""
  if empty_targets_output="$(
    AUDIT_SKIP_OPS_SMOKE="false" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_OPS_SMOKE_MODE="targeted" \
      AUDIT_OPS_SMOKE_TARGET_CASES=" " \
      bash "$ROOT_DIR/tools/audit_full.sh" 2>&1
  )"; then
    echo "expected audit_full.sh to fail for empty AUDIT_OPS_SMOKE_TARGET_CASES in targeted mode" >&2
    exit 1
  else
    local empty_targets_exit_code=$?
    if [[ "$empty_targets_exit_code" -ne 1 ]]; then
      echo "expected audit_full.sh empty AUDIT_OPS_SMOKE_TARGET_CASES exit code 1, got $empty_targets_exit_code" >&2
      echo "$empty_targets_output" >&2
      exit 1
    fi
  fi
  assert_contains "$empty_targets_output" "AUDIT_OPS_SMOKE_TARGET_CASES must be non-empty when AUDIT_OPS_SMOKE_MODE=targeted"

  local full_targeted_output=""
  full_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="false" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_WORKSPACE_TESTS="true" \
      AUDIT_OPS_SMOKE_MODE="targeted" \
      AUDIT_OPS_SMOKE_TARGET_CASES="common_timeout_parser" \
      bash "$ROOT_DIR/tools/audit_full.sh"
  )"
  assert_contains "$full_targeted_output" "[ok] common timeout parser"
  assert_contains "$full_targeted_output" "ops scripts smoke targeted: PASS (cases=common_timeout_parser)"
  assert_contains "$full_targeted_output" "[audit:full] PASS"

  local standard_marker_path="$ROOT_DIR/ops/.audit_ops_smoke_targeted_marker.tmp"
  printf 'marker\n' >"$standard_marker_path"

  local standard_targeted_output=""
  if standard_targeted_output="$(
    AUDIT_SKIP_OPS_SMOKE="false" \
      AUDIT_SKIP_CONTRACT_SMOKE="true" \
      AUDIT_SKIP_EXECUTOR_TESTS="true" \
      AUDIT_SKIP_PACKAGE_TESTS="true" \
      AUDIT_OPS_SMOKE_MODE="targeted" \
      AUDIT_OPS_SMOKE_TARGET_CASES="common_timeout_parser" \
      bash "$ROOT_DIR/tools/audit_standard.sh"
  )"; then
    rm -f "$standard_marker_path"
  else
    local standard_targeted_exit_code=$?
    rm -f "$standard_marker_path"
    echo "expected audit_standard.sh targeted ops-smoke run to pass, got exit code $standard_targeted_exit_code" >&2
    echo "$standard_targeted_output" >&2
    exit 1
  fi

  assert_contains "$standard_targeted_output" "[audit:standard] ops scope touched -> running tools/ops_scripts_smoke_test.sh (mode=targeted)"
  assert_contains "$standard_targeted_output" "[ok] common timeout parser"
  assert_contains "$standard_targeted_output" "ops scripts smoke targeted: PASS (cases=common_timeout_parser)"
  assert_contains "$standard_targeted_output" "[audit:standard] PASS"
  echo "[ok] audit ops smoke mode guard"
}

run_evidence_bundle_pack_case() {
  local evidence_dir="$TMP_DIR/evidence-pack-input"
  local output_dir="$TMP_DIR/evidence-pack-out"
  mkdir -p "$evidence_dir/nested" "$output_dir"
  printf 'summary-line\n' >"$evidence_dir/summary.txt"
  printf 'capture-line\n' >"$evidence_dir/nested/captured.log"
  printf 'incident-archive-placeholder\n' >"$evidence_dir/incident_20260226T000030Z.tar.gz"

  local bundle_output=""
  bundle_output="$(
    OUTPUT_DIR="$output_dir" \
      BUNDLE_LABEL="executor_rollout_bundle" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$evidence_dir"
  )"

  local evidence_dir_canonical="$evidence_dir"
  local output_dir_canonical="$output_dir"
  evidence_dir_canonical="$(cd "$evidence_dir" && pwd -P)"
  output_dir_canonical="$(cd "$output_dir" && pwd -P)"

  assert_field_equals "$bundle_output" "artifacts_written" "true"
  assert_field_equals "$bundle_output" "file_count" "3"
  assert_field_equals "$bundle_output" "evidence_dir" "$evidence_dir_canonical"
  assert_field_equals "$bundle_output" "output_dir" "$output_dir_canonical"
  assert_sha256_field "$bundle_output" "bundle_sha256"

  local bundle_path=""
  local bundle_sha256_path=""
  local contents_manifest=""
  local bundle_sha256=""
  bundle_path="$(extract_field_value "$bundle_output" "bundle_path")"
  bundle_sha256_path="$(extract_field_value "$bundle_output" "bundle_sha256_path")"
  contents_manifest="$(extract_field_value "$bundle_output" "contents_manifest")"
  bundle_sha256="$(extract_field_value "$bundle_output" "bundle_sha256")"

  if [[ ! -f "$bundle_path" ]]; then
    echo "expected evidence bundle file to exist: $bundle_path" >&2
    exit 1
  fi
  if [[ ! -f "$bundle_sha256_path" ]]; then
    echo "expected evidence bundle sha file to exist: $bundle_sha256_path" >&2
    exit 1
  fi
  if [[ ! -f "$contents_manifest" ]]; then
    echo "expected evidence bundle contents manifest to exist: $contents_manifest" >&2
    exit 1
  fi

  local bundle_sha_from_file=""
  bundle_sha_from_file="$(awk '{print $1}' "$bundle_sha256_path")"
  if [[ "$bundle_sha_from_file" != "$bundle_sha256" ]]; then
    echo "expected bundle sha256 from output and sha file to match" >&2
    exit 1
  fi

  local contents_text=""
  contents_text="$(cat "$contents_manifest")"
  assert_contains "$contents_text" "summary.txt"
  assert_contains "$contents_text" "nested/captured.log"

  local tar_list=""
  tar_list="$(tar -tzf "$bundle_path")"
  assert_contains "$tar_list" "summary.txt"
  assert_contains "$tar_list" "nested/captured.log"
  assert_contains "$tar_list" "incident_20260226T000030Z.tar.gz"

  local self_output_dir="$TMP_DIR/evidence-pack-self"
  mkdir -p "$self_output_dir"
  printf 'self-run-line\n' >"$self_output_dir/self.txt"

  local first_self_output=""
  first_self_output="$(
    BUNDLE_LABEL="executor_self_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000000Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$self_output_dir"
  )"
  assert_field_equals "$first_self_output" "file_count" "1"

  local second_self_output=""
  second_self_output="$(
    BUNDLE_LABEL="executor_self_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000000Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$self_output_dir"
  )"
  assert_field_equals "$second_self_output" "file_count" "1"

  local first_self_bundle_path=""
  local second_self_bundle_path=""
  first_self_bundle_path="$(extract_field_value "$first_self_output" "bundle_path")"
  second_self_bundle_path="$(extract_field_value "$second_self_output" "bundle_path")"
  if [[ "$first_self_bundle_path" == "$second_self_bundle_path" ]]; then
    echo "expected second self-output bundle path to avoid name collision" >&2
    exit 1
  fi

  local self_tar_list=""
  self_tar_list="$(tar -tzf "$second_self_bundle_path")"
  assert_contains "$self_tar_list" "self.txt"
  if grep -Fq ".tar.gz" <<<"$self_tar_list"; then
    echo "expected self-output bundle not to include previous bundle artifacts" >&2
    exit 1
  fi
  if grep -Fq ".sha256" <<<"$self_tar_list"; then
    echo "expected self-output bundle not to include previous checksum artifacts" >&2
    exit 1
  fi

  local first_cross_label_output=""
  first_cross_label_output="$(
    BUNDLE_LABEL="executor_label_a_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000010Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$self_output_dir"
  )"
  assert_field_equals "$first_cross_label_output" "file_count" "1"

  local second_cross_label_output=""
  second_cross_label_output="$(
    BUNDLE_LABEL="executor_label_b_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000011Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$self_output_dir"
  )"
  assert_field_equals "$second_cross_label_output" "file_count" "1"

  local second_cross_label_bundle_path=""
  second_cross_label_bundle_path="$(extract_field_value "$second_cross_label_output" "bundle_path")"
  local cross_label_tar_list=""
  cross_label_tar_list="$(tar -tzf "$second_cross_label_bundle_path")"
  assert_contains "$cross_label_tar_list" "self.txt"
  if grep -Fq ".tar.gz" <<<"$cross_label_tar_list"; then
    echo "expected cross-label bundle not to include prior bundle archives" >&2
    exit 1
  fi
  if grep -Fq ".sha256" <<<"$cross_label_tar_list"; then
    echo "expected cross-label bundle not to include prior checksum artifacts" >&2
    exit 1
  fi

  local poison_dir="$TMP_DIR/evidence-pack-poison"
  mkdir -p "$poison_dir"
  printf 'keep-me\n' >"$poison_dir/keep.txt"
  printf 'keep.txt\n' >"$poison_dir/.copybot_evidence_bundle_outputs.txt"

  local poison_output=""
  poison_output="$(
    BUNDLE_LABEL="poison_check_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000020Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$poison_dir"
  )"
  assert_field_equals "$poison_output" "file_count" "1"
  local poison_bundle_path=""
  poison_bundle_path="$(extract_field_value "$poison_output" "bundle_path")"
  local poison_tar_list=""
  poison_tar_list="$(tar -tzf "$poison_bundle_path")"
  assert_contains "$poison_tar_list" "keep.txt"

  local outside_input_dir="$TMP_DIR/evidence-pack-outside-input"
  local outside_output_dir="$TMP_DIR/evidence-pack-outside-output"
  mkdir -p "$outside_input_dir" "$outside_output_dir"
  printf 'outside-keep\n' >"$outside_input_dir/outside_keep.txt"
  printf 'outside-triplet-tar\n' >"$outside_input_dir/outside_triplet_20260226T000030Z.tar.gz"
  printf 'outside-triplet-sha\n' >"$outside_input_dir/outside_triplet_20260226T000030Z.sha256"
  printf 'outside-triplet-contents\n' >"$outside_input_dir/outside_triplet_20260226T000030Z.contents.sha256"
  cat >"$outside_output_dir/.copybot_evidence_bundle_outputs.txt" <<'EOF_POISON_INDEX'
outside_triplet_20260226T000030Z.tar.gz
outside_triplet_20260226T000030Z.sha256
outside_triplet_20260226T000030Z.contents.sha256
EOF_POISON_INDEX

  local outside_output=""
  outside_output="$(
    OUTPUT_DIR="$outside_output_dir" \
      BUNDLE_LABEL="outside_bundle" \
      BUNDLE_TIMESTAMP_UTC="20260226T000021Z" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$outside_input_dir"
  )"
  assert_field_equals "$outside_output" "file_count" "4"
  local outside_bundle_path=""
  outside_bundle_path="$(extract_field_value "$outside_output" "bundle_path")"
  local outside_tar_list=""
  outside_tar_list="$(tar -tzf "$outside_bundle_path")"
  assert_contains "$outside_tar_list" "outside_keep.txt"
  assert_contains "$outside_tar_list" "outside_triplet_20260226T000030Z.tar.gz"
  assert_contains "$outside_tar_list" "outside_triplet_20260226T000030Z.sha256"
  assert_contains "$outside_tar_list" "outside_triplet_20260226T000030Z.contents.sha256"
  echo "[ok] evidence bundle pack"
}

run_targeted_smoke_cases() {
  local target_cases_raw="$1"
  local -a target_cases=()
  IFS=',' read -r -a target_cases <<<"$target_cases_raw"
  if ((${#target_cases[@]} == 0)); then
    echo "OPS_SMOKE_TARGET_CASES must contain at least one case name" >&2
    exit 1
  fi

  write_fake_journalctl

  local legacy_db="$TMP_DIR/targeted-legacy.db"
  local legacy_cfg="$TMP_DIR/targeted-legacy.toml"
  local devnet_rehearsal_cfg="$TMP_DIR/targeted-devnet-rehearsal.toml"
  local fixtures_ready="false"
  local executed_cases=0

  local target_case_raw=""
  local target_case=""
  for target_case_raw in "${target_cases[@]-}"; do
    target_case="$(trim_string "$target_case_raw")"
    if [[ -z "$target_case" ]]; then
      continue
    fi
    case "$target_case" in
    common_strict_bool_parser | run_common_strict_bool_parser_case)
      run_common_strict_bool_parser_case
      executed_cases=$((executed_cases + 1))
      ;;
    common_bool_compat_wrapper | run_common_bool_compat_wrapper_case)
      run_common_bool_compat_wrapper_case
      executed_cases=$((executed_cases + 1))
      ;;
    common_timeout_parser | run_common_timeout_parser_case)
      run_common_timeout_parser_case
      executed_cases=$((executed_cases + 1))
      ;;
    audit_quick_bool_guard | run_audit_quick_strict_bool_guard_case)
      run_audit_quick_strict_bool_guard_case
      executed_cases=$((executed_cases + 1))
      ;;
    audit_standard_bool_guard | run_audit_standard_strict_bool_guard_case)
      run_audit_standard_strict_bool_guard_case
      executed_cases=$((executed_cases + 1))
      ;;
    audit_contract_smoke_mode_guard | run_audit_contract_smoke_mode_guard_case)
      run_audit_contract_smoke_mode_guard_case
      executed_cases=$((executed_cases + 1))
      ;;
    audit_executor_test_mode_guard | run_audit_executor_test_mode_guard_case)
      run_audit_executor_test_mode_guard_case
      executed_cases=$((executed_cases + 1))
      ;;
    audit_ops_smoke_mode_guard | run_audit_ops_smoke_mode_guard_case)
      run_audit_ops_smoke_mode_guard_case
      executed_cases=$((executed_cases + 1))
      ;;
    evidence_bundle_pack | run_evidence_bundle_pack_case)
      run_evidence_bundle_pack_case
      executed_cases=$((executed_cases + 1))
      ;;
    windowed_signoff | run_windowed_signoff_report_case | route_fee_signoff | run_execution_route_fee_signoff_case | devnet_rehearsal | run_devnet_rehearsal_case | executor_rollout_evidence | run_executor_rollout_evidence_case | adapter_rollout_evidence | run_adapter_rollout_evidence_case | execution_server_rollout | run_execution_server_rollout_report_case | execution_runtime_readiness | run_execution_runtime_readiness_report_case)
      if [[ "$fixtures_ready" != "true" ]]; then
        create_legacy_db "$legacy_db"
        write_config "$legacy_cfg" "$legacy_db"
        write_config_devnet_rehearsal "$devnet_rehearsal_cfg" "$legacy_db"
        fixtures_ready="true"
      fi
      case "$target_case" in
      windowed_signoff | run_windowed_signoff_report_case)
        run_windowed_signoff_report_case "$legacy_db" "$legacy_cfg" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      route_fee_signoff | run_execution_route_fee_signoff_case)
        run_execution_route_fee_signoff_case "$legacy_db" "$legacy_cfg" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      devnet_rehearsal | run_devnet_rehearsal_case)
        run_devnet_rehearsal_case "$legacy_db" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      executor_rollout_evidence | run_executor_rollout_evidence_case)
        run_executor_rollout_evidence_case "$legacy_db" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      adapter_rollout_evidence | run_adapter_rollout_evidence_case)
        run_adapter_rollout_evidence_case "$legacy_db" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      execution_server_rollout | run_execution_server_rollout_report_case)
        run_execution_server_rollout_report_case "$legacy_db" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      execution_runtime_readiness | run_execution_runtime_readiness_report_case)
        run_execution_runtime_readiness_report_case "$legacy_db" "$devnet_rehearsal_cfg"
        executed_cases=$((executed_cases + 1))
        ;;
      esac
      ;;
    *)
      echo "unknown OPS_SMOKE_TARGET_CASES entry: $target_case" >&2
      echo "known values: common_strict_bool_parser, common_bool_compat_wrapper, common_timeout_parser, audit_quick_bool_guard, audit_standard_bool_guard, audit_contract_smoke_mode_guard, audit_executor_test_mode_guard, audit_ops_smoke_mode_guard, evidence_bundle_pack, windowed_signoff, route_fee_signoff, devnet_rehearsal, executor_rollout_evidence, adapter_rollout_evidence, execution_server_rollout, execution_runtime_readiness" >&2
      exit 1
      ;;
    esac
  done

  if ((executed_cases == 0)); then
    echo "OPS_SMOKE_TARGET_CASES must contain at least one non-empty case name" >&2
    exit 1
  fi

  echo "ops scripts smoke targeted: PASS (cases=$target_cases_raw)"
}

run_ops_smoke_targeted_dispatch_case() {
  local targeted_output=""
  targeted_output="$(
    OPS_SMOKE_TARGET_CASES="common_strict_bool_parser,common_timeout_parser" \
      bash "$ROOT_DIR/tools/ops_scripts_smoke_test.sh"
  )"
  assert_contains "$targeted_output" "[ok] common strict bool parser"
  assert_contains "$targeted_output" "[ok] common timeout parser"
  assert_contains "$targeted_output" "ops scripts smoke targeted: PASS"
  if grep -Fq "[ok] execution runtime readiness report" <<<"$targeted_output"; then
    echo "targeted smoke dispatcher must not execute unrelated heavy cases" >&2
    exit 1
  fi

  local invalid_output_path="$TMP_DIR/ops-smoke-target-invalid.out"
  if OPS_SMOKE_TARGET_CASES="unknown_case" bash "$ROOT_DIR/tools/ops_scripts_smoke_test.sh" >"$invalid_output_path" 2>&1; then
    echo "expected targeted smoke mode to fail on unknown case entry" >&2
    exit 1
  fi
  local invalid_output=""
  invalid_output="$(cat "$invalid_output_path")"
  assert_contains "$invalid_output" "unknown OPS_SMOKE_TARGET_CASES entry: unknown_case"
  echo "[ok] ops smoke targeted dispatcher"
}

main() {
  if [[ -n "$OPS_SMOKE_TARGET_CASES" ]]; then
    run_targeted_smoke_cases "$OPS_SMOKE_TARGET_CASES"
    return 0
  fi

  write_fake_journalctl
  run_common_strict_bool_parser_case
  run_common_bool_compat_wrapper_case
  run_common_timeout_parser_case
  run_audit_quick_strict_bool_guard_case
  run_audit_standard_strict_bool_guard_case
  run_audit_standard_invalid_diff_range_guard_case
  run_audit_standard_contract_smoke_strict_bool_guard_case
  run_audit_skip_gate_strict_bool_batch_case
  run_audit_standard_package_test_timeout_guard_case
  run_audit_ops_smoke_timeout_guard_case
  run_audit_contract_smoke_timeout_guard_case
  run_audit_executor_test_timeout_guard_case
  run_audit_standard_executor_test_timeout_guard_case
  run_audit_full_executor_test_timeout_guard_case
  run_audit_timeout_upper_bound_guard_batch_case
  run_audit_workspace_test_timeout_guard_case
  run_audit_full_strict_bool_guard_case
  run_audit_full_contract_smoke_strict_bool_guard_case
  run_audit_contract_smoke_mode_guard_case
  run_audit_executor_test_mode_guard_case
  run_audit_ops_smoke_mode_guard_case
  run_evidence_bundle_pack_case
  run_ops_smoke_targeted_dispatch_case

  local legacy_db="$TMP_DIR/legacy.db"
  local legacy_cfg="$TMP_DIR/legacy.toml"
  create_legacy_db "$legacy_db"
  write_config "$legacy_cfg" "$legacy_db"
  run_ops_scripts_for_db "legacy schema" "$legacy_db" "$legacy_cfg"
  run_runtime_snapshot_no_ingestion_case "$legacy_db" "$legacy_cfg"
  run_go_nogo_artifact_export_case "$legacy_db" "$legacy_cfg"
  run_go_nogo_unknown_precedence_case "$legacy_db" "$legacy_cfg"
  run_go_nogo_dynamic_hint_source_gate_case "$legacy_db" "$legacy_cfg"
  local devnet_rehearsal_cfg="$TMP_DIR/devnet-rehearsal.toml"
  write_config_devnet_rehearsal "$devnet_rehearsal_cfg" "$legacy_db"
  run_go_nogo_jito_rpc_policy_gate_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_go_nogo_fastlane_disabled_gate_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_windowed_signoff_report_case "$legacy_db" "$legacy_cfg" "$devnet_rehearsal_cfg"
  run_execution_route_fee_signoff_case "$legacy_db" "$legacy_cfg" "$devnet_rehearsal_cfg"
  run_adapter_preflight_case "$legacy_db"
  run_executor_preflight_case "$legacy_db"
  run_adapter_secret_rotation_report_case
  run_executor_signer_rotation_report_case
  run_executor_rollout_evidence_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_go_nogo_preflight_fail_case "$legacy_db"
  run_devnet_rehearsal_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_adapter_rollout_evidence_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_execution_server_rollout_report_case "$legacy_db" "$devnet_rehearsal_cfg"
  run_execution_runtime_readiness_report_case "$legacy_db" "$devnet_rehearsal_cfg"

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
  run_calibration_invalid_env_bool_case "$rpc_only_db" "$runtime_default_fallback_cfg"

  local adapter_mode_cfg="$TMP_DIR/adapter-mode.toml"
  write_config_adapter_mode "$adapter_mode_cfg" "$modern_db"
  run_calibration_adapter_mode_route_profile_case "$modern_db" "$adapter_mode_cfg"

  echo "ops scripts smoke: PASS"
}

main "$@"
