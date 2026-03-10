#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-60}"

EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"
ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-/etc/solana-copy-bot/adapter.env}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/live.toml}}"
SERVICE="${SERVICE:-solana-copy-bot}"
OUTPUT_ROOT="${OUTPUT_ROOT:-state/server-rollout-$(date -u +"%Y%m%dT%H%M%SZ")}"
EXACT_MONEY_CUTOVER_HELPER_PATH="${EXACT_MONEY_CUTOVER_HELPER_PATH:-$ROOT_DIR/tools/exact_money_cutover_evidence_report.sh}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_server_rollout}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_ROOT}"
SERVER_ROLLOUT_PROFILE="${SERVER_ROLLOUT_PROFILE:-full}"
SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM="${SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM:-true}"

RUN_TESTS="${RUN_TESTS:-true}"
DEVNET_REHEARSAL_TEST_MODE="${DEVNET_REHEARSAL_TEST_MODE:-false}"
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}"
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}"

WINDOWED_SIGNOFF_REQUIRED="${WINDOWED_SIGNOFF_REQUIRED:-true}"
WINDOWED_SIGNOFF_WINDOWS_CSV="${WINDOWED_SIGNOFF_WINDOWS_CSV:-1,6,24}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-true}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-true}"
GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-true}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-true}"
GO_NOGO_REQUIRE_INGESTION_GRPC="${GO_NOGO_REQUIRE_INGESTION_GRPC:-true}"
GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="${GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY:-false}"
GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="${GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER:-false}"
GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="${GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT:-false}"
GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="${GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE:-true}"
GO_NOGO_MIN_CONFIRMED_ORDERS="${GO_NOGO_MIN_CONFIRMED_ORDERS:-1}"
GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="${GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY:-true}"
GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="${GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS:-50000000}"
GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="${GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS:-1000}"
EXACT_MONEY_CUTOVER_REQUIRED="${EXACT_MONEY_CUTOVER_REQUIRED:-true}"

ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-true}"
ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"

REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="${REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED:-true}"
REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"

now_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
now_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

declare -a input_errors=()
if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  input_errors+=("window hours must be an integer (got: $WINDOW_HOURS)")
fi
if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  input_errors+=("risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)")
fi
if [[ ! -f "$EXECUTOR_ENV_PATH" ]]; then
  input_errors+=("executor env file not found: $EXECUTOR_ENV_PATH")
fi
if [[ ! -f "$ADAPTER_ENV_PATH" ]]; then
  input_errors+=("adapter env file not found: $ADAPTER_ENV_PATH")
fi
if [[ ! -f "$CONFIG_PATH" ]]; then
  input_errors+=("config file not found: $CONFIG_PATH")
fi

server_rollout_run_go_nogo_direct_default="true"
server_rollout_run_rehearsal_direct_default="true"
case "$SERVER_ROLLOUT_PROFILE" in
full)
  ;;
finals_only)
  server_rollout_run_go_nogo_direct_default="false"
  server_rollout_run_rehearsal_direct_default="false"
  ;;
*)
  input_errors+=("SERVER_ROLLOUT_PROFILE must be one of: full,finals_only (got: $SERVER_ROLLOUT_PROFILE)")
  ;;
esac

server_rollout_run_go_nogo_direct_raw="$server_rollout_run_go_nogo_direct_default"
if [[ -n "${SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT+x}" ]]; then
  server_rollout_run_go_nogo_direct_raw="${SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT}"
fi
server_rollout_run_rehearsal_direct_raw="$server_rollout_run_rehearsal_direct_default"
if [[ -n "${SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT+x}" ]]; then
  server_rollout_run_rehearsal_direct_raw="${SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT}"
fi

parse_bool_setting_into() {
  local setting_name="$1"
  local raw_value="$2"
  local output_var="$3"
  local parsed_value=""
  if ! parsed_value="$(parse_bool_token_strict "$raw_value")"; then
    input_errors+=("${setting_name} must be a boolean token (true/false/1/0/yes/no/on/off), got: ${raw_value}")
    parsed_value="false"
  fi
  printf -v "$output_var" '%s' "$parsed_value"
}

parse_positive_u64_setting_into() {
  local setting_name="$1"
  local raw_value="$2"
  local output_var="$3"
  local parsed_value=""
  if ! parsed_value="$(parse_positive_u64_token_strict "$raw_value")"; then
    input_errors+=("${setting_name} must be an integer >= 1, got: ${raw_value}")
    parsed_value="1"
  fi
  printf -v "$output_var" '%s' "$parsed_value"
}

cfg_value() {
  local section="$1"
  local key="$2"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      left = line
      sub(/=.*/, "", left)
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = line
        sub(/^[^=]*=/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$CONFIG_PATH"
}

read_env_file_key() {
  local env_path="$1"
  local key="$2"
  if [[ ! -f "$env_path" ]]; then
    return 0
  fi
  awk -F'=' -v key="$key" '
    {
      line = $0
      sub(/#.*/, "", line)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line == "") {
        next
      }
      if (index(line, "=") == 0) {
        next
      }
      lhs = line
      sub(/=.*/, "", lhs)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", lhs)
      if (lhs != key) {
        next
      }
      rhs = line
      sub(/^[^=]*=/, "", rhs)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", rhs)
      gsub(/^"|"$/, "", rhs)
      print rhs
      exit
    }
  ' "$env_path"
}

parse_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_INGESTION_GRPC" "$GO_NOGO_REQUIRE_INGESTION_GRPC" go_nogo_require_ingestion_grpc_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" "$GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" go_nogo_require_followlist_activity_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" "$GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" go_nogo_require_non_bootstrap_signer_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" "$GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" go_nogo_require_submit_verify_strict_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" "$GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" go_nogo_require_confirmed_execution_sample_norm
parse_positive_u64_setting_into "GO_NOGO_MIN_CONFIRMED_ORDERS" "$GO_NOGO_MIN_CONFIRMED_ORDERS" go_nogo_min_confirmed_orders_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY" "$GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY" go_nogo_require_pretrade_fee_policy_norm
parse_positive_u64_setting_into "GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS" "$GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS" go_nogo_min_pretrade_sol_reserve_lamports_norm
parse_positive_u64_setting_into "GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS" "$GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS" go_nogo_max_pretrade_fee_overhead_bps_norm
parse_bool_setting_into "EXACT_MONEY_CUTOVER_REQUIRED" "$EXACT_MONEY_CUTOVER_REQUIRED" exact_money_cutover_required_norm
parse_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_bool_setting_into "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" route_fee_signoff_go_nogo_test_mode_norm
parse_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" "$REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" rehearsal_route_fee_signoff_required_norm
parse_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" rehearsal_route_fee_signoff_go_nogo_test_mode_norm
parse_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm
parse_bool_setting_into "SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT" "$server_rollout_run_go_nogo_direct_raw" server_rollout_run_go_nogo_direct_norm
parse_bool_setting_into "SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT" "$server_rollout_run_rehearsal_direct_raw" server_rollout_run_rehearsal_direct_norm
parse_bool_setting_into "SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM" "$SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM" server_rollout_require_executor_upstream_norm

executor_backend_mode_raw="$(trim_string "$(read_env_file_key "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_BACKEND_MODE")")"
executor_backend_mode="upstream"
if [[ -n "$executor_backend_mode_raw" ]]; then
  executor_backend_mode="$(printf '%s' "$executor_backend_mode_raw" | tr '[:upper:]' '[:lower:]')"
fi
case "$executor_backend_mode" in
upstream|mock)
  ;;
*)
  input_errors+=("COPYBOT_EXECUTOR_BACKEND_MODE in $EXECUTOR_ENV_PATH must be one of: upstream,mock (got: ${executor_backend_mode_raw:-<empty>})")
  executor_backend_mode="upstream"
  ;;
esac
if [[ "$server_rollout_require_executor_upstream_norm" == "true" && "$executor_backend_mode" != "upstream" ]]; then
  input_errors+=("SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true requires COPYBOT_EXECUTOR_BACKEND_MODE=upstream in $EXECUTOR_ENV_PATH (got: $executor_backend_mode)")
fi

mkdir -p "$OUTPUT_ROOT"
step_root="$OUTPUT_ROOT/steps"
mkdir -p "$step_root"

preflight_output=""
preflight_exit_code=3
preflight_verdict="UNKNOWN"
preflight_reason="not executed"
preflight_reason_code="not_executed"
preflight_capture="$step_root/preflight_capture_${now_compact}.txt"

calibration_output=""
calibration_exit_code=3
fee_decomposition_verdict="UNKNOWN"
fee_decomposition_reason="not executed"
route_profile_verdict="UNKNOWN"
route_profile_reason="not executed"
calibration_summary_sha256="n/a"
calibration_capture="$step_root/calibration_capture_${now_compact}.txt"

go_nogo_output=""
go_nogo_exit_code=3
overall_go_nogo_verdict="UNKNOWN"
overall_go_nogo_reason="not executed"
overall_go_nogo_reason_code="not_executed"
go_nogo_summary_sha256="n/a"
go_nogo_capture="$step_root/go_nogo_capture_${now_compact}.txt"
go_nogo_artifacts_written="n/a"
go_nogo_nested_package_bundle_enabled="n/a"
go_nogo_executor_backend_mode_guard_verdict="n/a"
go_nogo_executor_backend_mode_guard_reason_code="n/a"
go_nogo_executor_upstream_endpoint_guard_verdict="n/a"
go_nogo_executor_upstream_endpoint_guard_reason_code="n/a"
go_nogo_ingestion_grpc_guard_verdict="n/a"
go_nogo_ingestion_grpc_guard_reason_code="n/a"
go_nogo_require_ingestion_grpc="n/a"
go_nogo_followlist_activity_guard_verdict="n/a"
go_nogo_followlist_activity_guard_reason_code="n/a"
go_nogo_require_followlist_activity="n/a"
go_nogo_non_bootstrap_signer_guard_verdict="n/a"
go_nogo_non_bootstrap_signer_guard_reason_code="n/a"
go_nogo_require_non_bootstrap_signer="n/a"
go_nogo_require_pretrade_fee_policy="n/a"
go_nogo_min_pretrade_sol_reserve_lamports="n/a"
go_nogo_max_pretrade_fee_overhead_bps="n/a"
go_nogo_pretrade_min_sol_reserve_lamports_observed="n/a"
go_nogo_pretrade_max_fee_overhead_bps_observed="n/a"
go_nogo_pretrade_fee_policy_guard_verdict="n/a"
go_nogo_pretrade_fee_policy_guard_reason_code="n/a"
go_nogo_require_confirmed_execution_sample="n/a"
go_nogo_min_confirmed_orders="n/a"
go_nogo_confirmed_execution_sample_guard_verdict="n/a"
go_nogo_confirmed_execution_sample_guard_reason_code="n/a"

rehearsal_output=""
rehearsal_exit_code=3
rehearsal_verdict="UNKNOWN"
rehearsal_reason="not executed"
rehearsal_reason_code="not_executed"
rehearsal_summary_sha256="n/a"
rehearsal_capture="$step_root/rehearsal_capture_${now_compact}.txt"
rehearsal_artifacts_written="n/a"
rehearsal_nested_package_bundle_enabled="n/a"

executor_final_output=""
executor_final_exit_code=3
executor_final_verdict="UNKNOWN"
executor_final_reason="not executed"
executor_final_reason_code="not_executed"
executor_final_summary_sha256="n/a"
executor_final_capture="$step_root/executor_final_capture_${now_compact}.txt"
executor_final_artifacts_written="n/a"
executor_final_nested_package_bundle_enabled="n/a"
executor_final_go_nogo_require_executor_upstream="n/a"
executor_final_go_nogo_require_jito_rpc_policy="n/a"
executor_final_go_nogo_require_fastlane_disabled="n/a"
executor_final_go_nogo_require_ingestion_grpc="n/a"
executor_final_go_nogo_require_followlist_activity="n/a"
executor_final_go_nogo_require_non_bootstrap_signer="n/a"
executor_final_go_nogo_require_submit_verify_strict="n/a"
executor_final_go_nogo_require_confirmed_execution_sample="n/a"
executor_final_go_nogo_min_confirmed_orders="n/a"
executor_final_go_nogo_require_pretrade_fee_policy="n/a"
executor_final_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
executor_final_go_nogo_max_pretrade_fee_overhead_bps="n/a"
executor_final_executor_env_path="n/a"
executor_final_rollout_nested_go_nogo_require_executor_upstream="n/a"
executor_final_rollout_nested_go_nogo_require_jito_rpc_policy="n/a"
executor_final_rollout_nested_jito_rpc_policy_verdict="n/a"
executor_final_rollout_nested_jito_rpc_policy_reason_code="n/a"
executor_final_rollout_nested_go_nogo_require_fastlane_disabled="n/a"
executor_final_rollout_nested_fastlane_feature_flag_verdict="n/a"
executor_final_rollout_nested_fastlane_feature_flag_reason_code="n/a"
executor_final_rollout_nested_go_nogo_require_ingestion_grpc="n/a"
executor_final_rollout_nested_go_nogo_require_followlist_activity="n/a"
executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer="n/a"
executor_final_rollout_nested_go_nogo_require_submit_verify_strict="n/a"
executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample="n/a"
executor_final_rollout_nested_go_nogo_min_confirmed_orders="n/a"
executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy="n/a"
executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
executor_final_rollout_nested_executor_env_path="n/a"
executor_final_rollout_nested_executor_backend_mode_guard_verdict="n/a"
executor_final_rollout_nested_executor_backend_mode_guard_reason_code="n/a"
executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict="n/a"
executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="n/a"
executor_final_rollout_nested_ingestion_grpc_guard_verdict="n/a"
executor_final_rollout_nested_ingestion_grpc_guard_reason_code="n/a"
executor_final_rollout_nested_followlist_activity_guard_verdict="n/a"
executor_final_rollout_nested_followlist_activity_guard_reason_code="n/a"
executor_final_rollout_nested_non_bootstrap_signer_guard_verdict="n/a"
executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code="n/a"
executor_final_rollout_nested_confirmed_execution_sample_guard_verdict="n/a"
executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code="n/a"
executor_final_rollout_nested_pretrade_fee_policy_guard_verdict="n/a"
executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code="n/a"
executor_final_rollout_nested_preflight_executor_submit_verify_strict="n/a"
executor_final_rollout_nested_preflight_executor_submit_verify_configured="n/a"
executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured="n/a"

adapter_final_output=""
adapter_final_exit_code=3
adapter_final_verdict="UNKNOWN"
adapter_final_reason="not executed"
adapter_final_reason_code="not_executed"
adapter_final_summary_sha256="n/a"
adapter_final_capture="$step_root/adapter_final_capture_${now_compact}.txt"
adapter_final_artifacts_written="n/a"
adapter_final_nested_package_bundle_enabled="n/a"
adapter_final_go_nogo_require_executor_upstream="n/a"
adapter_final_go_nogo_require_jito_rpc_policy="n/a"
adapter_final_go_nogo_require_fastlane_disabled="n/a"
adapter_final_go_nogo_require_ingestion_grpc="n/a"
adapter_final_go_nogo_require_followlist_activity="n/a"
adapter_final_go_nogo_require_non_bootstrap_signer="n/a"
adapter_final_go_nogo_require_submit_verify_strict="n/a"
adapter_final_go_nogo_require_confirmed_execution_sample="n/a"
adapter_final_go_nogo_min_confirmed_orders="n/a"
adapter_final_go_nogo_require_pretrade_fee_policy="n/a"
adapter_final_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
adapter_final_go_nogo_max_pretrade_fee_overhead_bps="n/a"
adapter_final_executor_env_path="n/a"
adapter_final_rollout_nested_go_nogo_require_executor_upstream="n/a"
adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy="n/a"
adapter_final_rollout_nested_jito_rpc_policy_verdict="n/a"
adapter_final_rollout_nested_jito_rpc_policy_reason_code="n/a"
adapter_final_rollout_nested_go_nogo_require_fastlane_disabled="n/a"
adapter_final_rollout_nested_fastlane_feature_flag_verdict="n/a"
adapter_final_rollout_nested_fastlane_feature_flag_reason_code="n/a"
adapter_final_rollout_nested_go_nogo_require_ingestion_grpc="n/a"
adapter_final_rollout_nested_go_nogo_require_followlist_activity="n/a"
adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer="n/a"
adapter_final_rollout_nested_go_nogo_require_submit_verify_strict="n/a"
adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample="n/a"
adapter_final_rollout_nested_go_nogo_min_confirmed_orders="n/a"
adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy="n/a"
adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
adapter_final_rollout_nested_executor_env_path="n/a"
adapter_final_rollout_nested_executor_backend_mode_guard_verdict="n/a"
adapter_final_rollout_nested_executor_backend_mode_guard_reason_code="n/a"
adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict="n/a"
adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="n/a"
adapter_final_rollout_nested_ingestion_grpc_guard_verdict="n/a"
adapter_final_rollout_nested_ingestion_grpc_guard_reason_code="n/a"
adapter_final_rollout_nested_followlist_activity_guard_verdict="n/a"
adapter_final_rollout_nested_followlist_activity_guard_reason_code="n/a"
adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict="n/a"
adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code="n/a"
adapter_final_rollout_nested_submit_verify_guard_verdict="n/a"
adapter_final_rollout_nested_submit_verify_guard_reason_code="n/a"
adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict="n/a"
adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code="n/a"
adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict="n/a"
adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code="n/a"

exact_money_db_path="$(first_non_empty "${DB_PATH:-}" "$(if [[ -f "$CONFIG_PATH" ]]; then cfg_value sqlite path; fi)")"
exact_money_output_root="$step_root/exact_money_cutover_evidence"
exact_money_output=""
exact_money_exit_code="3"
exact_money_guard_verdict="UNKNOWN"
exact_money_guard_reason_code="not_executed"
exact_money_cutover_present="n/a"
exact_money_cutover_ts="n/a"
exact_money_post_cutover_surface_failures="n/a"
exact_money_invalid_exact_rows_total="n/a"
exact_money_forbidden_merge_rows_total="n/a"
exact_money_readiness_exit_code="n/a"
exact_money_readiness_guard_verdict="n/a"
exact_money_readiness_guard_reason_code="n/a"
exact_money_legacy_export_exit_code="n/a"
exact_money_legacy_export_verdict="n/a"
exact_money_legacy_export_reason_code="n/a"
exact_money_legacy_approximate_rows_total="n/a"
exact_money_post_cutover_approximate_rows_total="n/a"
exact_money_artifact_summary="n/a"
exact_money_summary_sha256="n/a"
exact_money_artifact_manifest="n/a"
exact_money_manifest_sha256="n/a"

if ((${#input_errors[@]} == 0)); then
  if preflight_output="$(
    CONFIG_PATH="$CONFIG_PATH" \
      bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
  )"; then
    preflight_exit_code=0
  else
    preflight_exit_code=$?
  fi
  preflight_verdict="$(normalize_preflight_verdict "$(extract_field "preflight_verdict" "$preflight_output")")"
  preflight_reason="$(trim_string "$(extract_field "preflight_reason" "$preflight_output")")"
  preflight_reason_code="$(trim_string "$(extract_field "preflight_reason_code" "$preflight_output")")"

  if calibration_output="$(
    DB_PATH="${DB_PATH:-}" \
      CONFIG_PATH="$CONFIG_PATH" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" "$WINDOW_HOURS" 2>&1
  )"; then
    calibration_exit_code=0
  else
    calibration_exit_code=$?
  fi
  fee_decomposition_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$calibration_output")")"
  fee_decomposition_reason="$(trim_string "$(extract_field "fee_decomposition_reason" "$calibration_output")")"
  route_profile_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$calibration_output")")"
  route_profile_reason="$(trim_string "$(extract_field "route_profile_reason" "$calibration_output")")"
  calibration_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$calibration_output")")"

  if [[ "$server_rollout_run_go_nogo_direct_norm" == "true" ]]; then
    go_nogo_output_dir="$step_root/go_nogo"
    mkdir -p "$go_nogo_output_dir"
    if go_nogo_output="$(
      DB_PATH="${DB_PATH:-}" \
        CONFIG_PATH="$CONFIG_PATH" \
        EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
        SERVICE="$SERVICE" \
        OUTPUT_DIR="$go_nogo_output_dir" \
        GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$server_rollout_require_executor_upstream_norm" \
        GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
        GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
        GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
        GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
        GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
        GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
        GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
        GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
        GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
        GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
        GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
        GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
        GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
        GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
        PACKAGE_BUNDLE_ENABLED="false" \
        bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
    )"; then
      go_nogo_exit_code=0
    else
      go_nogo_exit_code=$?
    fi
    overall_go_nogo_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
    overall_go_nogo_reason="$(trim_string "$(extract_field "overall_go_nogo_reason" "$go_nogo_output")")"
    overall_go_nogo_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$go_nogo_output")")"
    go_nogo_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$go_nogo_output")")"
    go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$go_nogo_output")")"
    if ! go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_ingestion_grpc must be boolean token, got: ${go_nogo_require_ingestion_grpc_raw:-<empty>}")
      go_nogo_require_ingestion_grpc="unknown"
    elif [[ "$go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_ingestion_grpc mismatch: nested=${go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
    fi
    go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "go_nogo_require_followlist_activity" "$go_nogo_output")")"
    if ! go_nogo_require_followlist_activity="$(extract_bool_field_strict "go_nogo_require_followlist_activity" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_followlist_activity must be boolean token, got: ${go_nogo_require_followlist_activity_raw:-<empty>}")
      go_nogo_require_followlist_activity="unknown"
    elif [[ "$go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_followlist_activity mismatch: nested=${go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
    fi
    go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")")"
    if ! go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_non_bootstrap_signer must be boolean token, got: ${go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
      go_nogo_require_non_bootstrap_signer="unknown"
    elif [[ "$go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_non_bootstrap_signer mismatch: nested=${go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
    fi
    go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "go_nogo_require_submit_verify_strict" "$go_nogo_output")")"
    if ! go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "go_nogo_require_submit_verify_strict" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_submit_verify_strict must be boolean token, got: ${go_nogo_require_submit_verify_strict_raw:-<empty>}")
      go_nogo_require_submit_verify_strict="unknown"
    elif [[ "$go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_submit_verify_strict mismatch: nested=${go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
    fi
    go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "go_nogo_require_confirmed_execution_sample" "$go_nogo_output")")"
    if ! go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "go_nogo_require_confirmed_execution_sample" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_confirmed_execution_sample must be boolean token, got: ${go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
      go_nogo_require_confirmed_execution_sample="unknown"
    elif [[ "$go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_confirmed_execution_sample mismatch: nested=${go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
    fi
    go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "go_nogo_min_confirmed_orders" "$go_nogo_output")")"
    if ! go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$go_nogo_min_confirmed_orders_raw")"; then
      input_errors+=("nested go/no-go go_nogo_min_confirmed_orders must be an integer >= 1, got: ${go_nogo_min_confirmed_orders_raw:-<empty>}")
      go_nogo_min_confirmed_orders="0"
    elif [[ "$go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_min_confirmed_orders mismatch: nested=${go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
    fi
    go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "go_nogo_require_pretrade_fee_policy" "$go_nogo_output")")"
    if ! go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "go_nogo_require_pretrade_fee_policy" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go go_nogo_require_pretrade_fee_policy must be boolean token, got: ${go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
      go_nogo_require_pretrade_fee_policy="unknown"
    elif [[ "$go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_require_pretrade_fee_policy mismatch: nested=${go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
    fi
    go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "go_nogo_min_pretrade_sol_reserve_lamports" "$go_nogo_output")")"
    if ! go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
      input_errors+=("nested go/no-go go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
      go_nogo_min_pretrade_sol_reserve_lamports="0"
    elif [[ "$go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
    fi
    go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "go_nogo_max_pretrade_fee_overhead_bps" "$go_nogo_output")")"
    if ! go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
      input_errors+=("nested go/no-go go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
      go_nogo_max_pretrade_fee_overhead_bps="0"
    elif [[ "$go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
      input_errors+=("nested go/no-go go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
    fi
    go_nogo_pretrade_min_sol_reserve_lamports_observed_raw="$(trim_string "$(extract_field "pretrade_min_sol_reserve_lamports_observed" "$go_nogo_output")")"
    if [[ -n "$go_nogo_pretrade_min_sol_reserve_lamports_observed_raw" && "$go_nogo_pretrade_min_sol_reserve_lamports_observed_raw" != "n/a" ]]; then
      if ! go_nogo_pretrade_min_sol_reserve_lamports_observed="$(parse_u64_token_strict "$go_nogo_pretrade_min_sol_reserve_lamports_observed_raw")"; then
        input_errors+=("nested go/no-go pretrade_min_sol_reserve_lamports_observed must be an integer >= 0, got: ${go_nogo_pretrade_min_sol_reserve_lamports_observed_raw:-<empty>}")
        go_nogo_pretrade_min_sol_reserve_lamports_observed="n/a"
      fi
    fi
    go_nogo_pretrade_max_fee_overhead_bps_observed_raw="$(trim_string "$(extract_field "pretrade_max_fee_overhead_bps_observed" "$go_nogo_output")")"
    if [[ -n "$go_nogo_pretrade_max_fee_overhead_bps_observed_raw" && "$go_nogo_pretrade_max_fee_overhead_bps_observed_raw" != "n/a" ]]; then
      if ! go_nogo_pretrade_max_fee_overhead_bps_observed="$(parse_u64_token_strict "$go_nogo_pretrade_max_fee_overhead_bps_observed_raw")"; then
        input_errors+=("nested go/no-go pretrade_max_fee_overhead_bps_observed must be an integer >= 0, got: ${go_nogo_pretrade_max_fee_overhead_bps_observed_raw:-<empty>}")
        go_nogo_pretrade_max_fee_overhead_bps_observed="n/a"
      fi
    fi
    go_nogo_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "executor_backend_mode_guard_verdict" "$go_nogo_output")")"
    go_nogo_executor_backend_mode_guard_verdict="$(printf '%s' "$go_nogo_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$go_nogo_executor_backend_mode_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go executor_backend_mode_guard_verdict must be non-empty")
      go_nogo_executor_backend_mode_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_executor_backend_mode_guard_verdict" != "PASS" && "$go_nogo_executor_backend_mode_guard_verdict" != "WARN" && "$go_nogo_executor_backend_mode_guard_verdict" != "UNKNOWN" && "$go_nogo_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested go/no-go executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_executor_backend_mode_guard_verdict_raw})")
      go_nogo_executor_backend_mode_guard_verdict="UNKNOWN"
    fi
    go_nogo_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "executor_backend_mode_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_executor_backend_mode_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go executor_backend_mode_guard_reason_code must be non-empty")
      go_nogo_executor_backend_mode_guard_reason_code="n/a"
    fi
    go_nogo_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_verdict" "$go_nogo_output")")"
    go_nogo_executor_upstream_endpoint_guard_verdict="$(printf '%s' "$go_nogo_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$go_nogo_executor_upstream_endpoint_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict must be non-empty")
      go_nogo_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_executor_upstream_endpoint_guard_verdict" != "PASS" && "$go_nogo_executor_upstream_endpoint_guard_verdict" != "WARN" && "$go_nogo_executor_upstream_endpoint_guard_verdict" != "UNKNOWN" && "$go_nogo_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_executor_upstream_endpoint_guard_verdict_raw})")
      go_nogo_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    fi
    go_nogo_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_executor_upstream_endpoint_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go executor_upstream_endpoint_guard_reason_code must be non-empty")
      go_nogo_executor_upstream_endpoint_guard_reason_code="n/a"
    fi
    go_nogo_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "ingestion_grpc_guard_verdict" "$go_nogo_output")")"
    go_nogo_ingestion_grpc_guard_verdict="$(printf '%s' "$go_nogo_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$go_nogo_ingestion_grpc_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go ingestion_grpc_guard_verdict must be non-empty")
      go_nogo_ingestion_grpc_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_ingestion_grpc_guard_verdict" != "PASS" && "$go_nogo_ingestion_grpc_guard_verdict" != "WARN" && "$go_nogo_ingestion_grpc_guard_verdict" != "UNKNOWN" && "$go_nogo_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested go/no-go ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_ingestion_grpc_guard_verdict_raw})")
      go_nogo_ingestion_grpc_guard_verdict="UNKNOWN"
    fi
    go_nogo_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "ingestion_grpc_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_ingestion_grpc_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go ingestion_grpc_guard_reason_code must be non-empty")
      go_nogo_ingestion_grpc_guard_reason_code="n/a"
    fi
    go_nogo_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "followlist_activity_guard_verdict" "$go_nogo_output")")"
    go_nogo_followlist_activity_guard_verdict="$(printf '%s' "$go_nogo_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$go_nogo_followlist_activity_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go followlist_activity_guard_verdict must be non-empty")
      go_nogo_followlist_activity_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_followlist_activity_guard_verdict" != "PASS" && "$go_nogo_followlist_activity_guard_verdict" != "WARN" && "$go_nogo_followlist_activity_guard_verdict" != "UNKNOWN" && "$go_nogo_followlist_activity_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested go/no-go followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_followlist_activity_guard_verdict_raw})")
      go_nogo_followlist_activity_guard_verdict="UNKNOWN"
    fi
    go_nogo_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "followlist_activity_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_followlist_activity_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go followlist_activity_guard_reason_code must be non-empty")
      go_nogo_followlist_activity_guard_reason_code="n/a"
    fi
    if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
      if [[ "$go_nogo_followlist_activity_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested go/no-go followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
      fi
    else
      if [[ "$go_nogo_followlist_activity_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested go/no-go followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${go_nogo_followlist_activity_guard_verdict})")
      fi
    fi
    go_nogo_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "non_bootstrap_signer_guard_verdict" "$go_nogo_output")")"
    go_nogo_non_bootstrap_signer_guard_verdict="$(printf '%s' "$go_nogo_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$go_nogo_non_bootstrap_signer_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be non-empty")
      go_nogo_non_bootstrap_signer_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_non_bootstrap_signer_guard_verdict" != "PASS" && "$go_nogo_non_bootstrap_signer_guard_verdict" != "WARN" && "$go_nogo_non_bootstrap_signer_guard_verdict" != "UNKNOWN" && "$go_nogo_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_non_bootstrap_signer_guard_verdict_raw})")
      go_nogo_non_bootstrap_signer_guard_verdict="UNKNOWN"
    fi
    go_nogo_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "non_bootstrap_signer_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_non_bootstrap_signer_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go non_bootstrap_signer_guard_reason_code must be non-empty")
      go_nogo_non_bootstrap_signer_guard_reason_code="n/a"
    fi
    if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
      if [[ "$go_nogo_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
      fi
    else
      if [[ "$go_nogo_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${go_nogo_non_bootstrap_signer_guard_verdict})")
      fi
    fi
    go_nogo_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "confirmed_execution_sample_guard_verdict" "$go_nogo_output")")"
    go_nogo_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    go_nogo_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_confirmed_execution_sample_guard_verdict_raw")"
    if [[ -z "$go_nogo_confirmed_execution_sample_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be non-empty")
      go_nogo_confirmed_execution_sample_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_confirmed_execution_sample_guard_verdict_raw})")
      go_nogo_confirmed_execution_sample_guard_verdict="UNKNOWN"
    fi
    go_nogo_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "confirmed_execution_sample_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_confirmed_execution_sample_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go confirmed_execution_sample_guard_reason_code must be non-empty")
      go_nogo_confirmed_execution_sample_guard_reason_code="n/a"
    fi
    go_nogo_pretrade_fee_policy_guard_verdict_raw="$(trim_string "$(extract_field "pretrade_fee_policy_guard_verdict" "$go_nogo_output")")"
    go_nogo_pretrade_fee_policy_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_pretrade_fee_policy_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    go_nogo_pretrade_fee_policy_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_pretrade_fee_policy_guard_verdict_raw")"
    if [[ -z "$go_nogo_pretrade_fee_policy_guard_verdict_raw" ]]; then
      input_errors+=("nested go/no-go pretrade_fee_policy_guard_verdict must be non-empty")
      go_nogo_pretrade_fee_policy_guard_verdict="UNKNOWN"
    elif [[ "$go_nogo_pretrade_fee_policy_guard_verdict_raw_upper" != "PASS" && "$go_nogo_pretrade_fee_policy_guard_verdict_raw_upper" != "WARN" && "$go_nogo_pretrade_fee_policy_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_pretrade_fee_policy_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested go/no-go pretrade_fee_policy_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_pretrade_fee_policy_guard_verdict_raw})")
      go_nogo_pretrade_fee_policy_guard_verdict="UNKNOWN"
    fi
    go_nogo_pretrade_fee_policy_guard_reason_code="$(trim_string "$(extract_field "pretrade_fee_policy_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$go_nogo_pretrade_fee_policy_guard_reason_code" ]]; then
      input_errors+=("nested go/no-go pretrade_fee_policy_guard_reason_code must be non-empty")
      go_nogo_pretrade_fee_policy_guard_reason_code="n/a"
    fi
    if [[ "$go_nogo_require_pretrade_fee_policy_norm" == "true" ]]; then
      if [[ "$go_nogo_pretrade_fee_policy_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested go/no-go pretrade_fee_policy_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=true")
      fi
    else
      if [[ "$go_nogo_pretrade_fee_policy_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested go/no-go pretrade_fee_policy_guard_verdict must be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=false (got: ${go_nogo_pretrade_fee_policy_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
      if [[ "$go_nogo_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
      fi
    else
      if [[ "$go_nogo_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${go_nogo_confirmed_execution_sample_guard_verdict})")
      fi
    fi
    go_nogo_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$go_nogo_output")")"
    if ! go_nogo_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go artifacts_written must be boolean token, got: ${go_nogo_artifacts_written_raw:-<empty>}")
      go_nogo_artifacts_written="unknown"
    elif [[ "$go_nogo_artifacts_written" != "true" ]]; then
      input_errors+=("nested go/no-go artifacts_written must be true")
    fi
    go_nogo_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$go_nogo_output")")"
    if ! go_nogo_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$go_nogo_output")"; then
      input_errors+=("nested go/no-go package_bundle_enabled must be boolean token, got: ${go_nogo_nested_package_bundle_enabled_raw:-<empty>}")
      go_nogo_nested_package_bundle_enabled="unknown"
    elif [[ "$go_nogo_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("nested go/no-go helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
  else
    go_nogo_exit_code=0
    overall_go_nogo_verdict="SKIP"
    overall_go_nogo_reason="direct go/no-go stage disabled via SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT=false"
    overall_go_nogo_reason_code="stage_disabled"
    go_nogo_artifacts_written="n/a"
    go_nogo_nested_package_bundle_enabled="n/a"
    go_nogo_executor_backend_mode_guard_verdict="SKIP"
    go_nogo_executor_backend_mode_guard_reason_code="stage_disabled"
    go_nogo_executor_upstream_endpoint_guard_verdict="SKIP"
    go_nogo_executor_upstream_endpoint_guard_reason_code="stage_disabled"
    go_nogo_ingestion_grpc_guard_verdict="SKIP"
    go_nogo_ingestion_grpc_guard_reason_code="stage_disabled"
    go_nogo_require_ingestion_grpc="$go_nogo_require_ingestion_grpc_norm"
    go_nogo_followlist_activity_guard_verdict="SKIP"
    go_nogo_followlist_activity_guard_reason_code="stage_disabled"
    go_nogo_require_followlist_activity="$go_nogo_require_followlist_activity_norm"
    go_nogo_non_bootstrap_signer_guard_verdict="SKIP"
    go_nogo_non_bootstrap_signer_guard_reason_code="stage_disabled"
    go_nogo_require_non_bootstrap_signer="$go_nogo_require_non_bootstrap_signer_norm"
    go_nogo_require_pretrade_fee_policy="$go_nogo_require_pretrade_fee_policy_norm"
    go_nogo_min_pretrade_sol_reserve_lamports="$go_nogo_min_pretrade_sol_reserve_lamports_norm"
    go_nogo_max_pretrade_fee_overhead_bps="$go_nogo_max_pretrade_fee_overhead_bps_norm"
    go_nogo_pretrade_min_sol_reserve_lamports_observed="n/a"
    go_nogo_pretrade_max_fee_overhead_bps_observed="n/a"
    go_nogo_pretrade_fee_policy_guard_verdict="SKIP"
    go_nogo_pretrade_fee_policy_guard_reason_code="stage_disabled"
    go_nogo_require_submit_verify_strict="$go_nogo_require_submit_verify_strict_norm"
    go_nogo_require_confirmed_execution_sample="$go_nogo_require_confirmed_execution_sample_norm"
    go_nogo_min_confirmed_orders="$go_nogo_min_confirmed_orders_norm"
    go_nogo_confirmed_execution_sample_guard_verdict="SKIP"
    go_nogo_confirmed_execution_sample_guard_reason_code="stage_disabled"
    go_nogo_output="overall_go_nogo_verdict: SKIP
overall_go_nogo_reason: direct go/no-go stage disabled via SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT=false
overall_go_nogo_reason_code: stage_disabled
artifacts_written: false
package_bundle_enabled: false"
  fi

  if [[ "$server_rollout_run_rehearsal_direct_norm" == "true" ]]; then
    rehearsal_output_dir="$step_root/rehearsal"
    mkdir -p "$rehearsal_output_dir"
    if rehearsal_output="$(
      DB_PATH="${DB_PATH:-}" \
        CONFIG_PATH="$CONFIG_PATH" \
        EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
        SERVICE="$SERVICE" \
        OUTPUT_DIR="$rehearsal_output_dir" \
        RUN_TESTS="$run_tests_norm" \
        DEVNET_REHEARSAL_TEST_MODE="$devnet_rehearsal_test_mode_norm" \
        GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$server_rollout_require_executor_upstream_norm" \
        GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
        GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
        GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
        GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
        GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
        GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
        GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
        GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
        GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
        GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
        GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
        WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
        WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
        WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
        WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
        GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
        GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
        ROUTE_FEE_SIGNOFF_REQUIRED="$route_fee_signoff_required_norm" \
        ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
        ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode_norm" \
        ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
        ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
        ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
        PACKAGE_BUNDLE_ENABLED="false" \
        bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
    )"; then
      rehearsal_exit_code=0
    else
      rehearsal_exit_code=$?
    fi
    rehearsal_verdict="$(normalize_rehearsal_verdict "$(extract_field "devnet_rehearsal_verdict" "$rehearsal_output")")"
    rehearsal_reason="$(trim_string "$(extract_field "devnet_rehearsal_reason" "$rehearsal_output")")"
    rehearsal_reason_code="$(trim_string "$(extract_field "devnet_rehearsal_reason_code" "$rehearsal_output")")"
    rehearsal_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$rehearsal_output")")"
    rehearsal_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rehearsal_output")")"
    if ! rehearsal_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rehearsal_output")"; then
      input_errors+=("nested devnet rehearsal artifacts_written must be boolean token, got: ${rehearsal_artifacts_written_raw:-<empty>}")
      rehearsal_artifacts_written="unknown"
    elif [[ "$rehearsal_artifacts_written" != "true" ]]; then
      input_errors+=("nested devnet rehearsal artifacts_written must be true")
    fi
    rehearsal_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$rehearsal_output")")"
    if ! rehearsal_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$rehearsal_output")"; then
      input_errors+=("nested devnet rehearsal package_bundle_enabled must be boolean token, got: ${rehearsal_nested_package_bundle_enabled_raw:-<empty>}")
      rehearsal_nested_package_bundle_enabled="unknown"
    elif [[ "$rehearsal_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("nested devnet rehearsal helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
  else
    rehearsal_exit_code=0
    rehearsal_verdict="SKIP"
    rehearsal_reason="direct devnet rehearsal stage disabled via SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT=false"
    rehearsal_reason_code="stage_disabled"
    rehearsal_artifacts_written="n/a"
    rehearsal_nested_package_bundle_enabled="n/a"
    rehearsal_output="devnet_rehearsal_verdict: SKIP
devnet_rehearsal_reason: direct devnet rehearsal stage disabled via SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT=false
devnet_rehearsal_reason_code: stage_disabled
artifacts_written: false
package_bundle_enabled: false"
  fi

  executor_final_output_dir="$step_root/executor_final"
  mkdir -p "$executor_final_output_dir"
  if executor_final_output="$(
    DB_PATH="${DB_PATH:-}" \
      EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_ROOT="$executor_final_output_dir" \
      RUN_TESTS="$run_tests_norm" \
      DEVNET_REHEARSAL_TEST_MODE="$devnet_rehearsal_test_mode_norm" \
      GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$server_rollout_require_executor_upstream_norm" \
      GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
      GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
      GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
      GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
      GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
      GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
      GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
      GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
      GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
      WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
      ROUTE_FEE_SIGNOFF_REQUIRED="$route_fee_signoff_required_norm" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode_norm" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/executor_final_evidence_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    executor_final_exit_code=0
  else
    executor_final_exit_code=$?
  fi
  executor_final_verdict="$(normalize_go_nogo_verdict "$(extract_field "final_executor_package_verdict" "$executor_final_output")")"
  executor_final_reason="$(trim_string "$(extract_field "final_executor_package_reason" "$executor_final_output")")"
  executor_final_reason_code="$(trim_string "$(extract_field "final_executor_package_reason_code" "$executor_final_output")")"
  executor_final_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$executor_final_output")")"
  executor_final_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$executor_final_output")")"
  if ! executor_final_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$executor_final_output")"; then
    input_errors+=("nested executor final artifacts_written must be boolean token, got: ${executor_final_artifacts_written_raw:-<empty>}")
    executor_final_artifacts_written="unknown"
  elif [[ "$executor_final_artifacts_written" != "true" ]]; then
    input_errors+=("nested executor final artifacts_written must be true")
  fi
  executor_final_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$executor_final_output")")"
  if ! executor_final_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$executor_final_output")"; then
    input_errors+=("nested executor final package_bundle_enabled must be boolean token, got: ${executor_final_nested_package_bundle_enabled_raw:-<empty>}")
    executor_final_nested_package_bundle_enabled="unknown"
  elif [[ "$executor_final_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested executor final helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
  executor_final_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_executor_upstream must be boolean token, got: ${executor_final_go_nogo_require_executor_upstream_raw:-<empty>}")
    executor_final_go_nogo_require_executor_upstream="unknown"
  elif [[ "$executor_final_go_nogo_require_executor_upstream" != "$server_rollout_require_executor_upstream_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_executor_upstream mismatch: nested=${executor_final_go_nogo_require_executor_upstream} expected=${server_rollout_require_executor_upstream_norm}")
  fi
  executor_final_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "go_nogo_require_jito_rpc_policy" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "go_nogo_require_jito_rpc_policy" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_jito_rpc_policy must be boolean token, got: ${executor_final_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    executor_final_go_nogo_require_jito_rpc_policy="unknown"
  elif [[ "$executor_final_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_jito_rpc_policy mismatch: nested=${executor_final_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
  fi
  executor_final_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "go_nogo_require_fastlane_disabled" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "go_nogo_require_fastlane_disabled" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_fastlane_disabled must be boolean token, got: ${executor_final_go_nogo_require_fastlane_disabled_raw:-<empty>}")
    executor_final_go_nogo_require_fastlane_disabled="unknown"
  elif [[ "$executor_final_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_fastlane_disabled mismatch: nested=${executor_final_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
  fi
  executor_final_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_ingestion_grpc must be boolean token, got: ${executor_final_go_nogo_require_ingestion_grpc_raw:-<empty>}")
    executor_final_go_nogo_require_ingestion_grpc="unknown"
  elif [[ "$executor_final_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_ingestion_grpc mismatch: nested=${executor_final_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
  fi
  executor_final_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "go_nogo_require_followlist_activity" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_followlist_activity="$(extract_bool_field_strict "go_nogo_require_followlist_activity" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_followlist_activity must be boolean token, got: ${executor_final_go_nogo_require_followlist_activity_raw:-<empty>}")
    executor_final_go_nogo_require_followlist_activity="unknown"
  elif [[ "$executor_final_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_followlist_activity mismatch: nested=${executor_final_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
  fi
  executor_final_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_non_bootstrap_signer must be boolean token, got: ${executor_final_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
    executor_final_go_nogo_require_non_bootstrap_signer="unknown"
  elif [[ "$executor_final_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_non_bootstrap_signer mismatch: nested=${executor_final_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
  fi
  executor_final_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "go_nogo_require_submit_verify_strict" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "go_nogo_require_submit_verify_strict" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_submit_verify_strict must be boolean token, got: ${executor_final_go_nogo_require_submit_verify_strict_raw:-<empty>}")
    executor_final_go_nogo_require_submit_verify_strict="unknown"
  elif [[ "$executor_final_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_submit_verify_strict mismatch: nested=${executor_final_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
  fi
  executor_final_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "go_nogo_require_confirmed_execution_sample" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "go_nogo_require_confirmed_execution_sample" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_confirmed_execution_sample must be boolean token, got: ${executor_final_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
    executor_final_go_nogo_require_confirmed_execution_sample="unknown"
  elif [[ "$executor_final_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_confirmed_execution_sample mismatch: nested=${executor_final_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
  fi
  executor_final_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "go_nogo_min_confirmed_orders" "$executor_final_output")")"
  if ! executor_final_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$executor_final_go_nogo_min_confirmed_orders_raw")"; then
    input_errors+=("nested executor final go_nogo_min_confirmed_orders must be an integer >= 1, got: ${executor_final_go_nogo_min_confirmed_orders_raw:-<empty>}")
    executor_final_go_nogo_min_confirmed_orders="0"
  elif [[ "$executor_final_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
    input_errors+=("nested executor final go_nogo_min_confirmed_orders mismatch: nested=${executor_final_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
  fi
  executor_final_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "go_nogo_require_pretrade_fee_policy" "$executor_final_output")")"
  if ! executor_final_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "go_nogo_require_pretrade_fee_policy" "$executor_final_output")"; then
    input_errors+=("nested executor final go_nogo_require_pretrade_fee_policy must be boolean token, got: ${executor_final_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
    executor_final_go_nogo_require_pretrade_fee_policy="unknown"
  elif [[ "$executor_final_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
    input_errors+=("nested executor final go_nogo_require_pretrade_fee_policy mismatch: nested=${executor_final_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
  fi
  executor_final_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "go_nogo_min_pretrade_sol_reserve_lamports" "$executor_final_output")")"
  if ! executor_final_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$executor_final_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
    input_errors+=("nested executor final go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${executor_final_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
    executor_final_go_nogo_min_pretrade_sol_reserve_lamports="0"
  elif [[ "$executor_final_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
    input_errors+=("nested executor final go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${executor_final_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
  fi
  executor_final_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "go_nogo_max_pretrade_fee_overhead_bps" "$executor_final_output")")"
  if ! executor_final_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$executor_final_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
    input_errors+=("nested executor final go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${executor_final_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
    executor_final_go_nogo_max_pretrade_fee_overhead_bps="0"
  elif [[ "$executor_final_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
    input_errors+=("nested executor final go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${executor_final_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
  fi
  executor_final_executor_env_path="$(trim_string "$(extract_field "executor_env_path" "$executor_final_output")")"
  if [[ -z "$executor_final_executor_env_path" ]]; then
    input_errors+=("nested executor final executor_env_path must be non-empty")
    executor_final_executor_env_path="n/a"
  elif [[ "$executor_final_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
    input_errors+=("nested executor final executor_env_path mismatch: nested=${executor_final_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
  fi
  executor_final_rollout_nested_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_executor_upstream" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_executor_upstream="$(extract_bool_field_strict "rollout_nested_go_nogo_require_executor_upstream" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_executor_upstream must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_executor_upstream_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_executor_upstream="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_executor_upstream" != "$server_rollout_require_executor_upstream_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_executor_upstream mismatch: nested=${executor_final_rollout_nested_go_nogo_require_executor_upstream} expected=${server_rollout_require_executor_upstream_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_jito_rpc_policy" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_jito_rpc_policy" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_jito_rpc_policy must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_jito_rpc_policy="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_jito_rpc_policy mismatch: nested=${executor_final_rollout_nested_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
  fi
  executor_final_rollout_nested_jito_rpc_policy_verdict_raw="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_jito_rpc_policy_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_jito_rpc_policy_verdict="$(normalize_gate_verdict "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_jito_rpc_policy_verdict must be non-empty")
    executor_final_rollout_nested_jito_rpc_policy_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "NO_DATA" && "$executor_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_jito_rpc_policy_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${executor_final_rollout_nested_jito_rpc_policy_verdict_raw})")
    executor_final_rollout_nested_jito_rpc_policy_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_jito_rpc_policy_reason_code="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_jito_rpc_policy_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_jito_rpc_policy_reason_code must be non-empty")
    executor_final_rollout_nested_jito_rpc_policy_reason_code="n/a"
  fi
  executor_final_rollout_nested_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_fastlane_disabled" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "rollout_nested_go_nogo_require_fastlane_disabled" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_fastlane_disabled must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_fastlane_disabled_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_fastlane_disabled="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_fastlane_disabled mismatch: nested=${executor_final_rollout_nested_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
  fi
  executor_final_rollout_nested_fastlane_feature_flag_verdict_raw="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_fastlane_feature_flag_verdict="$(normalize_gate_verdict "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_fastlane_feature_flag_verdict must be non-empty")
    executor_final_rollout_nested_fastlane_feature_flag_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "NO_DATA" && "$executor_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_fastlane_feature_flag_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${executor_final_rollout_nested_fastlane_feature_flag_verdict_raw})")
    executor_final_rollout_nested_fastlane_feature_flag_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_fastlane_feature_flag_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_fastlane_feature_flag_reason_code must be non-empty")
    executor_final_rollout_nested_fastlane_feature_flag_reason_code="n/a"
  fi
  executor_final_rollout_nested_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_ingestion_grpc" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "rollout_nested_go_nogo_require_ingestion_grpc" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_ingestion_grpc must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_ingestion_grpc_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_ingestion_grpc="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_ingestion_grpc mismatch: nested=${executor_final_rollout_nested_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_followlist_activity" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_followlist_activity="$(extract_bool_field_strict "rollout_nested_go_nogo_require_followlist_activity" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_followlist_activity must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_followlist_activity_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_followlist_activity="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_followlist_activity mismatch: nested=${executor_final_rollout_nested_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_non_bootstrap_signer" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "rollout_nested_go_nogo_require_non_bootstrap_signer" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_non_bootstrap_signer must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_non_bootstrap_signer mismatch: nested=${executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_submit_verify_strict" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "rollout_nested_go_nogo_require_submit_verify_strict" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_submit_verify_strict must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_submit_verify_strict_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_submit_verify_strict="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_submit_verify_strict mismatch: nested=${executor_final_rollout_nested_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_confirmed_execution_sample" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "rollout_nested_go_nogo_require_confirmed_execution_sample" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_confirmed_execution_sample must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_confirmed_execution_sample mismatch: nested=${executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
  fi
  executor_final_rollout_nested_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_confirmed_orders" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$executor_final_rollout_nested_go_nogo_min_confirmed_orders_raw")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_min_confirmed_orders must be an integer >= 1, got: ${executor_final_rollout_nested_go_nogo_min_confirmed_orders_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_min_confirmed_orders="0"
  elif [[ "$executor_final_rollout_nested_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_min_confirmed_orders mismatch: nested=${executor_final_rollout_nested_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
  fi
  executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_pretrade_fee_policy" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_pretrade_fee_policy" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_pretrade_fee_policy must be boolean token, got: ${executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy="unknown"
  elif [[ "$executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_require_pretrade_fee_policy mismatch: nested=${executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
  fi
  executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="0"
  elif [[ "$executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
  fi
  executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_max_pretrade_fee_overhead_bps" "$executor_final_output")")"
  if ! executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
    input_errors+=("nested executor final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
    executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="0"
  elif [[ "$executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
    input_errors+=("nested executor final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
  fi
  executor_final_rollout_nested_executor_env_path="$(trim_string "$(extract_field "rollout_nested_executor_env_path" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_executor_env_path" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_env_path must be non-empty")
    executor_final_rollout_nested_executor_env_path="n/a"
  elif [[ "$executor_final_rollout_nested_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_env_path mismatch: nested=${executor_final_rollout_nested_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
  fi
  executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_backend_mode_guard_verdict must be non-empty")
    executor_final_rollout_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_executor_backend_mode_guard_verdict_raw})")
    executor_final_rollout_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_executor_backend_mode_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_backend_mode_guard_reason_code must be non-empty")
    executor_final_rollout_nested_executor_backend_mode_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_upstream_endpoint_guard_verdict must be non-empty")
    executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw})")
    executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_executor_upstream_endpoint_guard_reason_code must be non-empty")
    executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_ingestion_grpc_guard_verdict must be non-empty")
    executor_final_rollout_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_ingestion_grpc_guard_verdict_raw})")
    executor_final_rollout_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_ingestion_grpc_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_ingestion_grpc_guard_reason_code must be non-empty")
    executor_final_rollout_nested_ingestion_grpc_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_followlist_activity_guard_verdict must be non-empty")
    executor_final_rollout_nested_followlist_activity_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_followlist_activity_guard_verdict_raw})")
    executor_final_rollout_nested_followlist_activity_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_followlist_activity_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_followlist_activity_guard_reason_code must be non-empty")
    executor_final_rollout_nested_followlist_activity_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_non_bootstrap_signer_guard_verdict must be non-empty")
    executor_final_rollout_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw})")
    executor_final_rollout_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_non_bootstrap_signer_guard_reason_code must be non-empty")
    executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_confirmed_execution_sample_guard_verdict must be non-empty")
    executor_final_rollout_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw})")
    executor_final_rollout_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_confirmed_execution_sample_guard_reason_code must be non-empty")
    executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_verdict" "$executor_final_output")")"
  executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper="$(printf '%s' "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  executor_final_rollout_nested_pretrade_fee_policy_guard_verdict="$(normalize_strict_guard_verdict "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw")"
  if [[ -z "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw" ]]; then
    input_errors+=("nested executor final rollout_nested_pretrade_fee_policy_guard_verdict must be non-empty")
    executor_final_rollout_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
  elif [[ "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "PASS" && "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "WARN" && "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested executor final rollout_nested_pretrade_fee_policy_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw})")
    executor_final_rollout_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
  fi
  executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_reason_code" "$executor_final_output")")"
  if [[ -z "$executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code" ]]; then
    input_errors+=("nested executor final rollout_nested_pretrade_fee_policy_guard_reason_code must be non-empty")
    executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code="n/a"
  fi
  executor_final_rollout_nested_preflight_executor_submit_verify_strict_raw="$(trim_string "$(extract_field "rollout_nested_preflight_executor_submit_verify_strict" "$executor_final_output")")"
  if ! executor_final_rollout_nested_preflight_executor_submit_verify_strict="$(extract_bool_field_strict "rollout_nested_preflight_executor_submit_verify_strict" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_preflight_executor_submit_verify_strict must be boolean token, got: ${executor_final_rollout_nested_preflight_executor_submit_verify_strict_raw:-<empty>}")
    executor_final_rollout_nested_preflight_executor_submit_verify_strict="unknown"
  fi
  executor_final_rollout_nested_preflight_executor_submit_verify_configured_raw="$(trim_string "$(extract_field "rollout_nested_preflight_executor_submit_verify_configured" "$executor_final_output")")"
  if ! executor_final_rollout_nested_preflight_executor_submit_verify_configured="$(extract_bool_field_strict "rollout_nested_preflight_executor_submit_verify_configured" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_preflight_executor_submit_verify_configured must be boolean token, got: ${executor_final_rollout_nested_preflight_executor_submit_verify_configured_raw:-<empty>}")
    executor_final_rollout_nested_preflight_executor_submit_verify_configured="unknown"
  fi
  executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured_raw="$(trim_string "$(extract_field "rollout_nested_preflight_executor_submit_verify_fallback_configured" "$executor_final_output")")"
  if ! executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured="$(extract_bool_field_strict "rollout_nested_preflight_executor_submit_verify_fallback_configured" "$executor_final_output")"; then
    input_errors+=("nested executor final rollout_nested_preflight_executor_submit_verify_fallback_configured must be boolean token, got: ${executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured_raw:-<empty>}")
    executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured="unknown"
  fi
  if [[ "$executor_final_rollout_nested_preflight_executor_submit_verify_strict" == "true" && "$executor_final_rollout_nested_preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("nested executor final rollout nested preflight submit verify strict=true requires configured=true")
  fi
  if [[ "$executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured" == "true" && "$executor_final_rollout_nested_preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("nested executor final rollout nested preflight submit verify fallback_configured=true requires configured=true")
  fi
  if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" && "$executor_final_rollout_nested_preflight_executor_submit_verify_strict" != "true" ]]; then
    input_errors+=("nested executor final rollout nested preflight submit verify strict must be true when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
  fi
  if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" && "$executor_final_rollout_nested_preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("nested executor final rollout nested preflight submit verify configured must be true when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
  fi
  if [[ "$server_rollout_require_executor_upstream_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_executor_backend_mode_guard_verdict cannot be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
    if [[ "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_executor_upstream_endpoint_guard_verdict cannot be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_executor_backend_mode_guard_verdict must be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${executor_final_rollout_nested_executor_backend_mode_guard_verdict})")
    fi
    if [[ "$executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_executor_upstream_endpoint_guard_verdict must be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_jito_rpc_policy_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_jito_rpc_policy_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_jito_rpc_policy_verdict cannot be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_jito_rpc_policy_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_jito_rpc_policy_verdict must be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=false (got: ${executor_final_rollout_nested_jito_rpc_policy_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_fastlane_disabled_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_fastlane_feature_flag_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_fastlane_feature_flag_verdict cannot be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_fastlane_feature_flag_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_fastlane_feature_flag_verdict must be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=false (got: ${executor_final_rollout_nested_fastlane_feature_flag_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${executor_final_rollout_nested_ingestion_grpc_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_followlist_activity_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_followlist_activity_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${executor_final_rollout_nested_followlist_activity_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${executor_final_rollout_nested_confirmed_execution_sample_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_pretrade_fee_policy_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_pretrade_fee_policy_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_pretrade_fee_policy_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_pretrade_fee_policy_guard_verdict must be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=false (got: ${executor_final_rollout_nested_pretrade_fee_policy_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
    if [[ "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
    fi
  else
    if [[ "$executor_final_rollout_nested_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested executor final rollout_nested_non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${executor_final_rollout_nested_non_bootstrap_signer_guard_verdict})")
    fi
  fi

  adapter_final_output_dir="$step_root/adapter_final"
  mkdir -p "$adapter_final_output_dir"
  if adapter_final_output="$(
    DB_PATH="${DB_PATH:-}" \
      ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_ROOT="$adapter_final_output_dir" \
      RUN_TESTS="$run_tests_norm" \
      DEVNET_REHEARSAL_TEST_MODE="$devnet_rehearsal_test_mode_norm" \
      EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$server_rollout_require_executor_upstream_norm" \
      GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
      GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
      GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
      GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
      GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
      GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
      GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
      GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
      GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
      GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
      WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
      ROUTE_FEE_SIGNOFF_REQUIRED="$route_fee_signoff_required_norm" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode_norm" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="$rehearsal_route_fee_signoff_required_norm" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$rehearsal_route_fee_signoff_go_nogo_test_mode_norm" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    adapter_final_exit_code=0
  else
    adapter_final_exit_code=$?
  fi
  adapter_final_verdict="$(normalize_go_nogo_verdict "$(extract_field "final_rollout_package_verdict" "$adapter_final_output")")"
  adapter_final_reason="$(trim_string "$(extract_field "final_rollout_package_reason" "$adapter_final_output")")"
  adapter_final_reason_code="$(trim_string "$(extract_field "final_rollout_package_reason_code" "$adapter_final_output")")"
  adapter_final_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$adapter_final_output")")"
  adapter_final_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$adapter_final_output")")"
  if ! adapter_final_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$adapter_final_output")"; then
    input_errors+=("nested adapter final artifacts_written must be boolean token, got: ${adapter_final_artifacts_written_raw:-<empty>}")
    adapter_final_artifacts_written="unknown"
  elif [[ "$adapter_final_artifacts_written" != "true" ]]; then
    input_errors+=("nested adapter final artifacts_written must be true")
  fi
  adapter_final_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$adapter_final_output")")"
  if ! adapter_final_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$adapter_final_output")"; then
    input_errors+=("nested adapter final package_bundle_enabled must be boolean token, got: ${adapter_final_nested_package_bundle_enabled_raw:-<empty>}")
    adapter_final_nested_package_bundle_enabled="unknown"
  elif [[ "$adapter_final_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested adapter final helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
  adapter_final_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_executor_upstream must be boolean token, got: ${adapter_final_go_nogo_require_executor_upstream_raw:-<empty>}")
    adapter_final_go_nogo_require_executor_upstream="unknown"
  elif [[ "$adapter_final_go_nogo_require_executor_upstream" != "$server_rollout_require_executor_upstream_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_executor_upstream mismatch: nested=${adapter_final_go_nogo_require_executor_upstream} expected=${server_rollout_require_executor_upstream_norm}")
  fi
  adapter_final_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "go_nogo_require_jito_rpc_policy" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "go_nogo_require_jito_rpc_policy" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_jito_rpc_policy must be boolean token, got: ${adapter_final_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    adapter_final_go_nogo_require_jito_rpc_policy="unknown"
  elif [[ "$adapter_final_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_jito_rpc_policy mismatch: nested=${adapter_final_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
  fi
  adapter_final_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "go_nogo_require_fastlane_disabled" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "go_nogo_require_fastlane_disabled" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_fastlane_disabled must be boolean token, got: ${adapter_final_go_nogo_require_fastlane_disabled_raw:-<empty>}")
    adapter_final_go_nogo_require_fastlane_disabled="unknown"
  elif [[ "$adapter_final_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_fastlane_disabled mismatch: nested=${adapter_final_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
  fi
  adapter_final_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_ingestion_grpc must be boolean token, got: ${adapter_final_go_nogo_require_ingestion_grpc_raw:-<empty>}")
    adapter_final_go_nogo_require_ingestion_grpc="unknown"
  elif [[ "$adapter_final_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_ingestion_grpc mismatch: nested=${adapter_final_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
  fi
  adapter_final_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "go_nogo_require_followlist_activity" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_followlist_activity="$(extract_bool_field_strict "go_nogo_require_followlist_activity" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_followlist_activity must be boolean token, got: ${adapter_final_go_nogo_require_followlist_activity_raw:-<empty>}")
    adapter_final_go_nogo_require_followlist_activity="unknown"
  elif [[ "$adapter_final_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_followlist_activity mismatch: nested=${adapter_final_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
  fi
  adapter_final_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_non_bootstrap_signer must be boolean token, got: ${adapter_final_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
    adapter_final_go_nogo_require_non_bootstrap_signer="unknown"
  elif [[ "$adapter_final_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_non_bootstrap_signer mismatch: nested=${adapter_final_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
  fi
  adapter_final_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "go_nogo_require_submit_verify_strict" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "go_nogo_require_submit_verify_strict" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_submit_verify_strict must be boolean token, got: ${adapter_final_go_nogo_require_submit_verify_strict_raw:-<empty>}")
    adapter_final_go_nogo_require_submit_verify_strict="unknown"
  elif [[ "$adapter_final_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_submit_verify_strict mismatch: nested=${adapter_final_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
  fi
  adapter_final_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "go_nogo_require_confirmed_execution_sample" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "go_nogo_require_confirmed_execution_sample" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_confirmed_execution_sample must be boolean token, got: ${adapter_final_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
    adapter_final_go_nogo_require_confirmed_execution_sample="unknown"
  elif [[ "$adapter_final_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_confirmed_execution_sample mismatch: nested=${adapter_final_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
  fi
  adapter_final_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "go_nogo_min_confirmed_orders" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$adapter_final_go_nogo_min_confirmed_orders_raw")"; then
    input_errors+=("nested adapter final go_nogo_min_confirmed_orders must be an integer >= 1, got: ${adapter_final_go_nogo_min_confirmed_orders_raw:-<empty>}")
    adapter_final_go_nogo_min_confirmed_orders="0"
  elif [[ "$adapter_final_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_min_confirmed_orders mismatch: nested=${adapter_final_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
  fi
  adapter_final_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "go_nogo_require_pretrade_fee_policy" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "go_nogo_require_pretrade_fee_policy" "$adapter_final_output")"; then
    input_errors+=("nested adapter final go_nogo_require_pretrade_fee_policy must be boolean token, got: ${adapter_final_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
    adapter_final_go_nogo_require_pretrade_fee_policy="unknown"
  elif [[ "$adapter_final_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_require_pretrade_fee_policy mismatch: nested=${adapter_final_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
  fi
  adapter_final_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "go_nogo_min_pretrade_sol_reserve_lamports" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$adapter_final_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
    input_errors+=("nested adapter final go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${adapter_final_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
    adapter_final_go_nogo_min_pretrade_sol_reserve_lamports="0"
  elif [[ "$adapter_final_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${adapter_final_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
  fi
  adapter_final_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "go_nogo_max_pretrade_fee_overhead_bps" "$adapter_final_output")")"
  if ! adapter_final_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$adapter_final_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
    input_errors+=("nested adapter final go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${adapter_final_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
    adapter_final_go_nogo_max_pretrade_fee_overhead_bps="0"
  elif [[ "$adapter_final_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
    input_errors+=("nested adapter final go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${adapter_final_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
  fi
  adapter_final_executor_env_path="$(trim_string "$(extract_field "executor_env_path" "$adapter_final_output")")"
  if [[ -z "$adapter_final_executor_env_path" ]]; then
    input_errors+=("nested adapter final executor_env_path must be non-empty")
    adapter_final_executor_env_path="n/a"
  elif [[ "$adapter_final_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
    input_errors+=("nested adapter final executor_env_path mismatch: nested=${adapter_final_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
  fi
  adapter_final_rollout_nested_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_executor_upstream" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_executor_upstream="$(extract_bool_field_strict "rollout_nested_go_nogo_require_executor_upstream" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_executor_upstream must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_executor_upstream_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_executor_upstream="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_executor_upstream" != "$server_rollout_require_executor_upstream_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_executor_upstream mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_executor_upstream} expected=${server_rollout_require_executor_upstream_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_jito_rpc_policy" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_jito_rpc_policy" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_jito_rpc_policy must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_jito_rpc_policy mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
  fi
  adapter_final_rollout_nested_jito_rpc_policy_verdict_raw="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_jito_rpc_policy_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_jito_rpc_policy_verdict="$(normalize_gate_verdict "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_jito_rpc_policy_verdict must be non-empty")
    adapter_final_rollout_nested_jito_rpc_policy_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "NO_DATA" && "$adapter_final_rollout_nested_jito_rpc_policy_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_jito_rpc_policy_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${adapter_final_rollout_nested_jito_rpc_policy_verdict_raw})")
    adapter_final_rollout_nested_jito_rpc_policy_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_jito_rpc_policy_reason_code="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_jito_rpc_policy_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_jito_rpc_policy_reason_code must be non-empty")
    adapter_final_rollout_nested_jito_rpc_policy_reason_code="n/a"
  fi
  adapter_final_rollout_nested_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_fastlane_disabled" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "rollout_nested_go_nogo_require_fastlane_disabled" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_fastlane_disabled must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_fastlane_disabled_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_fastlane_disabled="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_fastlane_disabled mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
  fi
  adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_fastlane_feature_flag_verdict="$(normalize_gate_verdict "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_fastlane_feature_flag_verdict must be non-empty")
    adapter_final_rollout_nested_fastlane_feature_flag_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "NO_DATA" && "$adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_fastlane_feature_flag_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${adapter_final_rollout_nested_fastlane_feature_flag_verdict_raw})")
    adapter_final_rollout_nested_fastlane_feature_flag_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_fastlane_feature_flag_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_fastlane_feature_flag_reason_code must be non-empty")
    adapter_final_rollout_nested_fastlane_feature_flag_reason_code="n/a"
  fi
  adapter_final_rollout_nested_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_ingestion_grpc" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "rollout_nested_go_nogo_require_ingestion_grpc" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_ingestion_grpc must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_ingestion_grpc_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_ingestion_grpc="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_ingestion_grpc mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_followlist_activity" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_followlist_activity="$(extract_bool_field_strict "rollout_nested_go_nogo_require_followlist_activity" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_followlist_activity must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_followlist_activity_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_followlist_activity="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_followlist_activity mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_non_bootstrap_signer" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "rollout_nested_go_nogo_require_non_bootstrap_signer" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_non_bootstrap_signer must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_non_bootstrap_signer mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_submit_verify_strict" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "rollout_nested_go_nogo_require_submit_verify_strict" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_submit_verify_strict must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_submit_verify_strict_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_submit_verify_strict="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_submit_verify_strict mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_confirmed_execution_sample" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "rollout_nested_go_nogo_require_confirmed_execution_sample" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_confirmed_execution_sample must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_confirmed_execution_sample mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_confirmed_orders" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$adapter_final_rollout_nested_go_nogo_min_confirmed_orders_raw")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_min_confirmed_orders must be an integer >= 1, got: ${adapter_final_rollout_nested_go_nogo_min_confirmed_orders_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_min_confirmed_orders="0"
  elif [[ "$adapter_final_rollout_nested_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_min_confirmed_orders mismatch: nested=${adapter_final_rollout_nested_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_pretrade_fee_policy" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_pretrade_fee_policy" "$adapter_final_output")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_pretrade_fee_policy must be boolean token, got: ${adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy="unknown"
  elif [[ "$adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_require_pretrade_fee_policy mismatch: nested=${adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports="0"
  elif [[ "$adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
  fi
  adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_max_pretrade_fee_overhead_bps" "$adapter_final_output")")"
  if ! adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
    adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps="0"
  elif [[ "$adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
    input_errors+=("nested adapter final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
  fi
  adapter_final_rollout_nested_executor_env_path="$(trim_string "$(extract_field "rollout_nested_executor_env_path" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_executor_env_path" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_env_path must be non-empty")
    adapter_final_rollout_nested_executor_env_path="n/a"
  elif [[ "$adapter_final_rollout_nested_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_env_path mismatch: nested=${adapter_final_rollout_nested_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
  fi
  adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_backend_mode_guard_verdict must be non-empty")
    adapter_final_rollout_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_executor_backend_mode_guard_verdict_raw})")
    adapter_final_rollout_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_executor_backend_mode_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_backend_mode_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_executor_backend_mode_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_upstream_endpoint_guard_verdict must be non-empty")
    adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict_raw})")
    adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_executor_upstream_endpoint_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_ingestion_grpc_guard_verdict must be non-empty")
    adapter_final_rollout_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_ingestion_grpc_guard_verdict_raw})")
    adapter_final_rollout_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_ingestion_grpc_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_ingestion_grpc_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_ingestion_grpc_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_followlist_activity_guard_verdict must be non-empty")
    adapter_final_rollout_nested_followlist_activity_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_followlist_activity_guard_verdict_raw})")
    adapter_final_rollout_nested_followlist_activity_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_followlist_activity_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_followlist_activity_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_followlist_activity_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_non_bootstrap_signer_guard_verdict must be non-empty")
    adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict_raw})")
    adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_non_bootstrap_signer_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_submit_verify_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_submit_verify_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_submit_verify_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_submit_verify_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_submit_verify_guard_verdict must be non-empty")
    adapter_final_rollout_nested_submit_verify_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_submit_verify_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_submit_verify_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_submit_verify_guard_verdict_raw})")
    adapter_final_rollout_nested_submit_verify_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_submit_verify_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_submit_verify_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_submit_verify_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_submit_verify_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_submit_verify_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_confirmed_execution_sample_guard_verdict must be non-empty")
    adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict_raw})")
    adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_confirmed_execution_sample_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code="n/a"
  fi
  adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_verdict" "$adapter_final_output")")"
  adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper="$(printf '%s' "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict="$(normalize_strict_guard_verdict "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw")"
  if [[ -z "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw" ]]; then
    input_errors+=("nested adapter final rollout_nested_pretrade_fee_policy_guard_verdict must be non-empty")
    adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
  elif [[ "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "PASS" && "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "WARN" && "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested adapter final rollout_nested_pretrade_fee_policy_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict_raw})")
    adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
  fi
  adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_reason_code" "$adapter_final_output")")"
  if [[ -z "$adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code" ]]; then
    input_errors+=("nested adapter final rollout_nested_pretrade_fee_policy_guard_reason_code must be non-empty")
    adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code="n/a"
  fi
  if [[ "$server_rollout_require_executor_upstream_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_executor_backend_mode_guard_verdict cannot be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
    if [[ "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_executor_upstream_endpoint_guard_verdict cannot be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_executor_backend_mode_guard_verdict must be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${adapter_final_rollout_nested_executor_backend_mode_guard_verdict})")
    fi
    if [[ "$adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_executor_upstream_endpoint_guard_verdict must be SKIP when SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_jito_rpc_policy_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_jito_rpc_policy_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_jito_rpc_policy_verdict cannot be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_jito_rpc_policy_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_jito_rpc_policy_verdict must be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=false (got: ${adapter_final_rollout_nested_jito_rpc_policy_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_fastlane_disabled_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_fastlane_feature_flag_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_fastlane_feature_flag_verdict cannot be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_fastlane_feature_flag_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_fastlane_feature_flag_verdict must be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=false (got: ${adapter_final_rollout_nested_fastlane_feature_flag_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${adapter_final_rollout_nested_ingestion_grpc_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_followlist_activity_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_followlist_activity_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${adapter_final_rollout_nested_followlist_activity_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_pretrade_fee_policy_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_pretrade_fee_policy_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_pretrade_fee_policy_guard_verdict must be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=false (got: ${adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" ]]; then
    if [[ "$adapter_final_rollout_nested_submit_verify_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_submit_verify_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
    fi
  else
    if [[ "$adapter_final_rollout_nested_submit_verify_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested adapter final rollout_nested_submit_verify_guard_verdict must be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=false (got: ${adapter_final_rollout_nested_submit_verify_guard_verdict})")
    fi
  fi
fi

if [[ "$exact_money_cutover_required_norm" == "true" ]]; then
  if [[ -z "$exact_money_db_path" ]]; then
    exact_money_exit_code=0
    exact_money_guard_verdict="UNKNOWN"
    exact_money_guard_reason_code="db_path_missing"
    exact_money_readiness_exit_code="0"
    exact_money_readiness_guard_verdict="UNKNOWN"
    exact_money_readiness_guard_reason_code="db_path_missing"
    exact_money_legacy_export_exit_code="0"
    exact_money_legacy_export_verdict="SKIP"
    exact_money_legacy_export_reason_code="not_run"
    exact_money_output="exact_money_cutover_evidence_verdict: UNKNOWN
exact_money_cutover_evidence_reason_code: db_path_missing
exact_money_cutover_present: n/a
exact_money_cutover_ts: n/a
readiness_exit_code: 0
readiness_guard_verdict: UNKNOWN
readiness_guard_reason_code: db_path_missing
readiness_post_cutover_surface_failures: n/a
readiness_invalid_exact_rows_total: n/a
readiness_forbidden_merge_rows_total: n/a
legacy_export_exit_code: 0
legacy_export_verdict: SKIP
legacy_export_reason_code: not_run
legacy_approximate_rows_total: n/a
post_cutover_approximate_rows_total: n/a
artifact_summary: n/a
summary_sha256: n/a
artifact_manifest: n/a
manifest_sha256: n/a"
  elif [[ ! -f "$exact_money_db_path" ]]; then
    exact_money_exit_code=0
    exact_money_guard_verdict="UNKNOWN"
    exact_money_guard_reason_code="db_missing"
    exact_money_readiness_exit_code="0"
    exact_money_readiness_guard_verdict="UNKNOWN"
    exact_money_readiness_guard_reason_code="db_missing"
    exact_money_legacy_export_exit_code="0"
    exact_money_legacy_export_verdict="SKIP"
    exact_money_legacy_export_reason_code="not_run"
    exact_money_output="exact_money_cutover_evidence_verdict: UNKNOWN
exact_money_cutover_evidence_reason_code: db_missing
exact_money_cutover_present: n/a
exact_money_cutover_ts: n/a
readiness_exit_code: 0
readiness_guard_verdict: UNKNOWN
readiness_guard_reason_code: db_missing
readiness_post_cutover_surface_failures: n/a
readiness_invalid_exact_rows_total: n/a
readiness_forbidden_merge_rows_total: n/a
legacy_export_exit_code: 0
legacy_export_verdict: SKIP
legacy_export_reason_code: not_run
legacy_approximate_rows_total: n/a
post_cutover_approximate_rows_total: n/a
artifact_summary: n/a
summary_sha256: n/a
artifact_manifest: n/a
manifest_sha256: n/a"
  else
    if exact_money_output="$(
      OUTPUT_DIR="$exact_money_output_root" \
        bash "$EXACT_MONEY_CUTOVER_HELPER_PATH" "$exact_money_db_path" 2>&1
    )"; then
      exact_money_exit_code=0
    else
      exact_money_exit_code=$?
    fi
    exact_money_guard_verdict_raw="$(trim_string "$(extract_field "exact_money_cutover_evidence_verdict" "$exact_money_output")")"
    exact_money_guard_verdict_raw_upper="$(printf '%s' "$exact_money_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    exact_money_guard_verdict="$(normalize_strict_guard_verdict "$exact_money_guard_verdict_raw")"
    if [[ -z "$exact_money_guard_verdict_raw" ]]; then
      input_errors+=("exact money evidence exact_money_cutover_evidence_verdict must be non-empty")
      exact_money_guard_verdict="UNKNOWN"
    elif [[ "$exact_money_guard_verdict_raw_upper" != "PASS" && "$exact_money_guard_verdict_raw_upper" != "WARN" && "$exact_money_guard_verdict_raw_upper" != "UNKNOWN" && "$exact_money_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("exact money evidence exact_money_cutover_evidence_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${exact_money_guard_verdict_raw})")
      exact_money_guard_verdict="UNKNOWN"
    fi
    exact_money_guard_reason_code="$(trim_string "$(extract_field "exact_money_cutover_evidence_reason_code" "$exact_money_output")")"
    if [[ -z "$exact_money_guard_reason_code" ]]; then
      input_errors+=("exact money evidence exact_money_cutover_evidence_reason_code must be non-empty")
      exact_money_guard_reason_code="n/a"
    fi
    exact_money_cutover_present="$(trim_string "$(extract_field "exact_money_cutover_present" "$exact_money_output")")"
    exact_money_cutover_ts="$(trim_string "$(extract_field "exact_money_cutover_ts" "$exact_money_output")")"
    exact_money_readiness_exit_code_raw="$(trim_string "$(extract_field "readiness_exit_code" "$exact_money_output")")"
    if [[ -z "$exact_money_readiness_exit_code_raw" || "$exact_money_readiness_exit_code_raw" == "n/a" ]]; then
      input_errors+=("exact money evidence readiness_exit_code must be a non-negative integer")
      exact_money_readiness_exit_code="n/a"
    elif ! exact_money_readiness_exit_code="$(parse_u64_token_strict "$exact_money_readiness_exit_code_raw")"; then
      input_errors+=("exact money evidence readiness_exit_code must be a non-negative integer (got: ${exact_money_readiness_exit_code_raw})")
      exact_money_readiness_exit_code="n/a"
    fi
    exact_money_readiness_guard_verdict_raw="$(trim_string "$(extract_field "readiness_guard_verdict" "$exact_money_output")")"
    exact_money_readiness_guard_verdict_raw_upper="$(printf '%s' "$exact_money_readiness_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    exact_money_readiness_guard_verdict="$(normalize_strict_guard_verdict "$exact_money_readiness_guard_verdict_raw")"
    if [[ -z "$exact_money_readiness_guard_verdict_raw" ]]; then
      input_errors+=("exact money evidence readiness_guard_verdict must be non-empty")
      exact_money_readiness_guard_verdict="UNKNOWN"
    elif [[ "$exact_money_readiness_guard_verdict_raw_upper" != "PASS" && "$exact_money_readiness_guard_verdict_raw_upper" != "WARN" && "$exact_money_readiness_guard_verdict_raw_upper" != "UNKNOWN" && "$exact_money_readiness_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("exact money evidence readiness_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${exact_money_readiness_guard_verdict_raw})")
      exact_money_readiness_guard_verdict="UNKNOWN"
    fi
    exact_money_readiness_guard_reason_code="$(trim_string "$(extract_field "readiness_guard_reason_code" "$exact_money_output")")"
    if [[ -z "$exact_money_readiness_guard_reason_code" ]]; then
      input_errors+=("exact money evidence readiness_guard_reason_code must be non-empty")
      exact_money_readiness_guard_reason_code="n/a"
    fi
    exact_money_legacy_export_exit_code_raw="$(trim_string "$(extract_field "legacy_export_exit_code" "$exact_money_output")")"
    if [[ -z "$exact_money_legacy_export_exit_code_raw" || "$exact_money_legacy_export_exit_code_raw" == "n/a" ]]; then
      input_errors+=("exact money evidence legacy_export_exit_code must be a non-negative integer")
      exact_money_legacy_export_exit_code="n/a"
    elif ! exact_money_legacy_export_exit_code="$(parse_u64_token_strict "$exact_money_legacy_export_exit_code_raw")"; then
      input_errors+=("exact money evidence legacy_export_exit_code must be a non-negative integer (got: ${exact_money_legacy_export_exit_code_raw})")
      exact_money_legacy_export_exit_code="n/a"
    fi
    exact_money_legacy_export_verdict_raw="$(trim_string "$(extract_field "legacy_export_verdict" "$exact_money_output")")"
    exact_money_legacy_export_verdict_raw_upper="$(printf '%s' "$exact_money_legacy_export_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    exact_money_legacy_export_verdict="$(normalize_strict_guard_verdict "$exact_money_legacy_export_verdict_raw")"
    if [[ -z "$exact_money_legacy_export_verdict_raw" ]]; then
      input_errors+=("exact money evidence legacy_export_verdict must be non-empty")
      exact_money_legacy_export_verdict="UNKNOWN"
    elif [[ "$exact_money_legacy_export_verdict_raw_upper" != "PASS" && "$exact_money_legacy_export_verdict_raw_upper" != "WARN" && "$exact_money_legacy_export_verdict_raw_upper" != "UNKNOWN" && "$exact_money_legacy_export_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("exact money evidence legacy_export_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${exact_money_legacy_export_verdict_raw})")
      exact_money_legacy_export_verdict="UNKNOWN"
    fi
    exact_money_legacy_export_reason_code="$(trim_string "$(extract_field "legacy_export_reason_code" "$exact_money_output")")"
    if [[ -z "$exact_money_legacy_export_reason_code" ]]; then
      input_errors+=("exact money evidence legacy_export_reason_code must be non-empty")
      exact_money_legacy_export_reason_code="n/a"
    fi
    if [[ "$exact_money_guard_verdict" == "PASS" || "$exact_money_guard_verdict" == "WARN" ]]; then
      exact_money_post_cutover_surface_failures_raw="$(trim_string "$(extract_field "readiness_post_cutover_surface_failures" "$exact_money_output")")"
      if [[ -z "$exact_money_post_cutover_surface_failures_raw" || "$exact_money_post_cutover_surface_failures_raw" == "n/a" ]]; then
        input_errors+=("exact money evidence readiness_post_cutover_surface_failures must be a non-negative integer")
        exact_money_post_cutover_surface_failures="n/a"
      elif ! exact_money_post_cutover_surface_failures="$(parse_u64_token_strict "$exact_money_post_cutover_surface_failures_raw")"; then
        input_errors+=("exact money evidence readiness_post_cutover_surface_failures must be a non-negative integer (got: ${exact_money_post_cutover_surface_failures_raw})")
        exact_money_post_cutover_surface_failures="n/a"
      fi
      exact_money_invalid_exact_rows_total_raw="$(trim_string "$(extract_field "readiness_invalid_exact_rows_total" "$exact_money_output")")"
      if [[ -z "$exact_money_invalid_exact_rows_total_raw" || "$exact_money_invalid_exact_rows_total_raw" == "n/a" ]]; then
        input_errors+=("exact money evidence readiness_invalid_exact_rows_total must be a non-negative integer")
        exact_money_invalid_exact_rows_total="n/a"
      elif ! exact_money_invalid_exact_rows_total="$(parse_u64_token_strict "$exact_money_invalid_exact_rows_total_raw")"; then
        input_errors+=("exact money evidence readiness_invalid_exact_rows_total must be a non-negative integer (got: ${exact_money_invalid_exact_rows_total_raw})")
        exact_money_invalid_exact_rows_total="n/a"
      fi
      exact_money_forbidden_merge_rows_total_raw="$(trim_string "$(extract_field "readiness_forbidden_merge_rows_total" "$exact_money_output")")"
      if [[ -z "$exact_money_forbidden_merge_rows_total_raw" || "$exact_money_forbidden_merge_rows_total_raw" == "n/a" ]]; then
        input_errors+=("exact money evidence readiness_forbidden_merge_rows_total must be a non-negative integer")
        exact_money_forbidden_merge_rows_total="n/a"
      elif ! exact_money_forbidden_merge_rows_total="$(parse_u64_token_strict "$exact_money_forbidden_merge_rows_total_raw")"; then
        input_errors+=("exact money evidence readiness_forbidden_merge_rows_total must be a non-negative integer (got: ${exact_money_forbidden_merge_rows_total_raw})")
        exact_money_forbidden_merge_rows_total="n/a"
      fi
      exact_money_legacy_approximate_rows_total_raw="$(trim_string "$(extract_field "legacy_approximate_rows_total" "$exact_money_output")")"
      if [[ -z "$exact_money_legacy_approximate_rows_total_raw" || "$exact_money_legacy_approximate_rows_total_raw" == "n/a" ]]; then
        input_errors+=("exact money evidence legacy_approximate_rows_total must be a non-negative integer")
        exact_money_legacy_approximate_rows_total="n/a"
      elif ! exact_money_legacy_approximate_rows_total="$(parse_u64_token_strict "$exact_money_legacy_approximate_rows_total_raw")"; then
        input_errors+=("exact money evidence legacy_approximate_rows_total must be a non-negative integer (got: ${exact_money_legacy_approximate_rows_total_raw})")
        exact_money_legacy_approximate_rows_total="n/a"
      fi
      exact_money_post_cutover_approximate_rows_total_raw="$(trim_string "$(extract_field "post_cutover_approximate_rows_total" "$exact_money_output")")"
      if [[ -z "$exact_money_post_cutover_approximate_rows_total_raw" || "$exact_money_post_cutover_approximate_rows_total_raw" == "n/a" ]]; then
        input_errors+=("exact money evidence post_cutover_approximate_rows_total must be a non-negative integer")
        exact_money_post_cutover_approximate_rows_total="n/a"
      elif ! exact_money_post_cutover_approximate_rows_total="$(parse_u64_token_strict "$exact_money_post_cutover_approximate_rows_total_raw")"; then
        input_errors+=("exact money evidence post_cutover_approximate_rows_total must be a non-negative integer (got: ${exact_money_post_cutover_approximate_rows_total_raw})")
        exact_money_post_cutover_approximate_rows_total="n/a"
      fi
    else
      exact_money_post_cutover_surface_failures="n/a"
      exact_money_invalid_exact_rows_total="n/a"
      exact_money_forbidden_merge_rows_total="n/a"
      exact_money_legacy_approximate_rows_total="n/a"
      exact_money_post_cutover_approximate_rows_total="n/a"
    fi
    exact_money_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$exact_money_output")")"
    if [[ -z "$exact_money_artifact_summary" ]]; then
      input_errors+=("exact money evidence artifact_summary must be non-empty")
      exact_money_artifact_summary="n/a"
    fi
    exact_money_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$exact_money_output")")"
    if [[ -z "$exact_money_summary_sha256" ]]; then
      input_errors+=("exact money evidence summary_sha256 must be non-empty")
      exact_money_summary_sha256="n/a"
    fi
    exact_money_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$exact_money_output")")"
    if [[ -z "$exact_money_artifact_manifest" ]]; then
      input_errors+=("exact money evidence artifact_manifest must be non-empty")
      exact_money_artifact_manifest="n/a"
    fi
    exact_money_manifest_sha256="$(trim_string "$(extract_field "manifest_sha256" "$exact_money_output")")"
    if [[ -z "$exact_money_manifest_sha256" ]]; then
      input_errors+=("exact money evidence manifest_sha256 must be non-empty")
      exact_money_manifest_sha256="n/a"
    fi
  fi
else
  exact_money_exit_code=0
  exact_money_guard_verdict="SKIP"
  exact_money_guard_reason_code="gate_disabled"
  exact_money_cutover_present="n/a"
  exact_money_cutover_ts="n/a"
  exact_money_post_cutover_surface_failures="n/a"
  exact_money_invalid_exact_rows_total="n/a"
  exact_money_forbidden_merge_rows_total="n/a"
  exact_money_readiness_exit_code="0"
  exact_money_readiness_guard_verdict="SKIP"
  exact_money_readiness_guard_reason_code="gate_disabled"
  exact_money_legacy_export_exit_code="0"
  exact_money_legacy_export_verdict="SKIP"
  exact_money_legacy_export_reason_code="gate_disabled"
  exact_money_output="exact_money_cutover_evidence_verdict: SKIP
exact_money_cutover_evidence_reason_code: gate_disabled
exact_money_cutover_present: n/a
exact_money_cutover_ts: n/a
readiness_exit_code: 0
readiness_guard_verdict: SKIP
readiness_guard_reason_code: gate_disabled
readiness_post_cutover_surface_failures: n/a
readiness_invalid_exact_rows_total: n/a
readiness_forbidden_merge_rows_total: n/a
legacy_export_exit_code: 0
legacy_export_verdict: SKIP
legacy_export_reason_code: gate_disabled
legacy_approximate_rows_total: n/a
post_cutover_approximate_rows_total: n/a
artifact_summary: n/a
summary_sha256: n/a
artifact_manifest: n/a
manifest_sha256: n/a"
fi

printf '%s\n' "$preflight_output" >"$preflight_capture"
printf '%s\n' "$calibration_output" >"$calibration_capture"
printf '%s\n' "$go_nogo_output" >"$go_nogo_capture"
printf '%s\n' "$rehearsal_output" >"$rehearsal_capture"
printf '%s\n' "$executor_final_output" >"$executor_final_capture"
printf '%s\n' "$adapter_final_output" >"$adapter_final_capture"
exact_money_capture="$step_root/exact_money_cutover_evidence_capture_${now_compact}.txt"
printf '%s\n' "$exact_money_output" >"$exact_money_capture"

preflight_capture_sha256="$(sha256_file_value "$preflight_capture")"
calibration_capture_sha256="$(sha256_file_value "$calibration_capture")"
go_nogo_capture_sha256="$(sha256_file_value "$go_nogo_capture")"
rehearsal_capture_sha256="$(sha256_file_value "$rehearsal_capture")"
executor_final_capture_sha256="$(sha256_file_value "$executor_final_capture")"
adapter_final_capture_sha256="$(sha256_file_value "$adapter_final_capture")"
exact_money_capture_sha256="$(sha256_file_value "$exact_money_capture")"

overall_verdict="GO"
overall_reason="all rollout stages passed"
overall_reason_code="all_stages_passed"

set_hold_if_go() {
  local reason="$1"
  local reason_code="$2"
  if [[ "$overall_verdict" == "GO" ]]; then
    overall_verdict="HOLD"
    overall_reason="$reason"
    overall_reason_code="$reason_code"
  fi
}

set_no_go() {
  local reason="$1"
  local reason_code="$2"
  overall_verdict="NO_GO"
  overall_reason="$reason"
  overall_reason_code="$reason_code"
}

if ((${#input_errors[@]} > 0)); then
  set_no_go "${input_errors[0]}" "input_error"
elif [[ "$preflight_verdict" == "FAIL" || "$preflight_verdict" == "UNKNOWN" ]]; then
  set_no_go "execution adapter preflight not PASS (${preflight_verdict}): ${preflight_reason:-n/a}" "preflight_not_pass"
else
  if [[ "$fee_decomposition_verdict" == "UNKNOWN" || "$route_profile_verdict" == "UNKNOWN" ]]; then
    set_no_go "calibration verdict unknown (fee=${fee_decomposition_verdict}, route=${route_profile_verdict})" "calibration_unknown"
  elif [[ "$fee_decomposition_verdict" != "PASS" ]]; then
    set_hold_if_go "calibration fee decomposition not PASS (${fee_decomposition_verdict}): ${fee_decomposition_reason:-n/a}" "calibration_fee_not_pass"
  fi
  if [[ "$route_profile_verdict" != "PASS" ]]; then
    set_hold_if_go "calibration route profile not PASS (${route_profile_verdict}): ${route_profile_reason:-n/a}" "calibration_route_not_pass"
  fi

  if [[ "$server_rollout_run_go_nogo_direct_norm" == "true" ]]; then
    if [[ "$overall_go_nogo_verdict" == "NO_GO" || "$overall_go_nogo_verdict" == "UNKNOWN" ]]; then
      set_no_go "go/no-go stage not GO (${overall_go_nogo_verdict}): ${overall_go_nogo_reason:-n/a}" "go_nogo_not_go"
    elif [[ "$overall_go_nogo_verdict" == "HOLD" ]]; then
      set_hold_if_go "go/no-go stage HOLD: ${overall_go_nogo_reason:-n/a}" "go_nogo_hold"
    fi
  fi

  if [[ "$server_rollout_run_rehearsal_direct_norm" == "true" ]]; then
    if [[ "$rehearsal_verdict" == "NO_GO" || "$rehearsal_verdict" == "UNKNOWN" ]]; then
      set_no_go "devnet rehearsal not GO (${rehearsal_verdict}): ${rehearsal_reason:-n/a}" "rehearsal_not_go"
    elif [[ "$rehearsal_verdict" == "HOLD" ]]; then
      set_hold_if_go "devnet rehearsal HOLD: ${rehearsal_reason:-n/a}" "rehearsal_hold"
    fi
  fi

  if [[ "$executor_final_verdict" == "NO_GO" || "$executor_final_verdict" == "UNKNOWN" ]]; then
    set_no_go "executor final package not GO (${executor_final_verdict}): ${executor_final_reason:-n/a}" "executor_final_not_go"
  elif [[ "$executor_final_verdict" == "HOLD" ]]; then
    set_hold_if_go "executor final package HOLD: ${executor_final_reason:-n/a}" "executor_final_hold"
  fi

  if [[ "$adapter_final_verdict" == "NO_GO" || "$adapter_final_verdict" == "UNKNOWN" ]]; then
    set_no_go "adapter final package not GO (${adapter_final_verdict}): ${adapter_final_reason:-n/a}" "adapter_final_not_go"
  elif [[ "$adapter_final_verdict" == "HOLD" ]]; then
    set_hold_if_go "adapter final package HOLD: ${adapter_final_reason:-n/a}" "adapter_final_hold"
  fi
  if [[ "$exact_money_cutover_required_norm" == "true" ]]; then
    if [[ "$exact_money_exit_code" -ne 0 ]]; then
      set_no_go "exact money cutover evidence helper failed with exit ${exact_money_exit_code}" "exact_money_cutover_failed"
    elif [[ "$exact_money_guard_verdict" == "UNKNOWN" ]]; then
      set_no_go "exact money cutover evidence verdict unknown: ${exact_money_guard_reason_code:-n/a}" "exact_money_cutover_unknown"
    elif [[ "$exact_money_guard_verdict" != "PASS" ]]; then
      set_no_go "exact money cutover evidence not PASS (${exact_money_guard_verdict}): ${exact_money_guard_reason_code:-n/a}" "exact_money_cutover_not_pass"
    fi
  fi
fi

summary_output="=== Execution Server Rollout Report ===
utc_now: $now_utc
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES
executor_env: $EXECUTOR_ENV_PATH
adapter_env: $ADAPTER_ENV_PATH
config: $CONFIG_PATH
output_root: $OUTPUT_ROOT
run_tests: $run_tests_norm
devnet_rehearsal_test_mode: $devnet_rehearsal_test_mode_norm
go_nogo_test_mode: $go_nogo_test_mode_norm
windowed_signoff_required: $windowed_signoff_required_norm
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy_norm
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled_norm
go_nogo_require_ingestion_grpc: $go_nogo_require_ingestion_grpc_norm
go_nogo_require_followlist_activity: $go_nogo_require_followlist_activity_norm
go_nogo_require_non_bootstrap_signer: $go_nogo_require_non_bootstrap_signer_norm
go_nogo_require_submit_verify_strict: $go_nogo_require_submit_verify_strict_norm
go_nogo_require_confirmed_execution_sample: $go_nogo_require_confirmed_execution_sample_norm
go_nogo_min_confirmed_orders: $go_nogo_min_confirmed_orders_norm
exact_money_cutover_required: $exact_money_cutover_required_norm
exact_money_db_path: ${exact_money_db_path:-n/a}
route_fee_signoff_required: $route_fee_signoff_required_norm
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
rehearsal_route_fee_signoff_required: $rehearsal_route_fee_signoff_required_norm
rehearsal_route_fee_signoff_windows_csv: $REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV
server_rollout_profile: $SERVER_ROLLOUT_PROFILE
server_rollout_run_go_nogo_direct: $server_rollout_run_go_nogo_direct_norm
server_rollout_run_rehearsal_direct: $server_rollout_run_rehearsal_direct_norm
server_rollout_require_executor_upstream: $server_rollout_require_executor_upstream_norm
executor_backend_mode: $executor_backend_mode
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: $PACKAGE_BUNDLE_OUTPUT_DIR
input_error_count: ${#input_errors[@]}
preflight_exit_code: $preflight_exit_code
preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
preflight_reason_code: ${preflight_reason_code:-n/a}
calibration_exit_code: $calibration_exit_code
fee_decomposition_verdict: $fee_decomposition_verdict
fee_decomposition_reason: ${fee_decomposition_reason:-n/a}
route_profile_verdict: $route_profile_verdict
route_profile_reason: ${route_profile_reason:-n/a}
calibration_summary_sha256: ${calibration_summary_sha256:-n/a}
go_nogo_exit_code: $go_nogo_exit_code
go_nogo_verdict: $overall_go_nogo_verdict
go_nogo_reason: ${overall_go_nogo_reason:-n/a}
go_nogo_reason_code: ${overall_go_nogo_reason_code:-n/a}
go_nogo_executor_backend_mode_guard_verdict: ${go_nogo_executor_backend_mode_guard_verdict:-n/a}
go_nogo_executor_backend_mode_guard_reason_code: ${go_nogo_executor_backend_mode_guard_reason_code:-n/a}
go_nogo_executor_upstream_endpoint_guard_verdict: ${go_nogo_executor_upstream_endpoint_guard_verdict:-n/a}
go_nogo_executor_upstream_endpoint_guard_reason_code: ${go_nogo_executor_upstream_endpoint_guard_reason_code:-n/a}
go_nogo_ingestion_grpc_guard_verdict: ${go_nogo_ingestion_grpc_guard_verdict:-n/a}
go_nogo_ingestion_grpc_guard_reason_code: ${go_nogo_ingestion_grpc_guard_reason_code:-n/a}
go_nogo_followlist_activity_guard_verdict: ${go_nogo_followlist_activity_guard_verdict:-n/a}
go_nogo_followlist_activity_guard_reason_code: ${go_nogo_followlist_activity_guard_reason_code:-n/a}
go_nogo_non_bootstrap_signer_guard_verdict: ${go_nogo_non_bootstrap_signer_guard_verdict:-n/a}
go_nogo_non_bootstrap_signer_guard_reason_code: ${go_nogo_non_bootstrap_signer_guard_reason_code:-n/a}
go_nogo_require_pretrade_fee_policy: ${go_nogo_require_pretrade_fee_policy:-n/a}
go_nogo_min_pretrade_sol_reserve_lamports: ${go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
go_nogo_max_pretrade_fee_overhead_bps: ${go_nogo_max_pretrade_fee_overhead_bps:-n/a}
go_nogo_pretrade_min_sol_reserve_lamports_observed: ${go_nogo_pretrade_min_sol_reserve_lamports_observed:-n/a}
go_nogo_pretrade_max_fee_overhead_bps_observed: ${go_nogo_pretrade_max_fee_overhead_bps_observed:-n/a}
go_nogo_pretrade_fee_policy_guard_verdict: ${go_nogo_pretrade_fee_policy_guard_verdict:-n/a}
go_nogo_pretrade_fee_policy_guard_reason_code: ${go_nogo_pretrade_fee_policy_guard_reason_code:-n/a}
go_nogo_confirmed_execution_sample_guard_verdict: ${go_nogo_confirmed_execution_sample_guard_verdict:-n/a}
go_nogo_confirmed_execution_sample_guard_reason_code: ${go_nogo_confirmed_execution_sample_guard_reason_code:-n/a}
go_nogo_summary_sha256: ${go_nogo_summary_sha256:-n/a}
go_nogo_artifacts_written: ${go_nogo_artifacts_written:-n/a}
go_nogo_nested_package_bundle_enabled: ${go_nogo_nested_package_bundle_enabled:-n/a}
rehearsal_exit_code: $rehearsal_exit_code
rehearsal_verdict: $rehearsal_verdict
rehearsal_reason: ${rehearsal_reason:-n/a}
rehearsal_reason_code: ${rehearsal_reason_code:-n/a}
rehearsal_summary_sha256: ${rehearsal_summary_sha256:-n/a}
rehearsal_artifacts_written: ${rehearsal_artifacts_written:-n/a}
rehearsal_nested_package_bundle_enabled: ${rehearsal_nested_package_bundle_enabled:-n/a}
executor_final_exit_code: $executor_final_exit_code
executor_final_verdict: $executor_final_verdict
executor_final_reason: ${executor_final_reason:-n/a}
executor_final_reason_code: ${executor_final_reason_code:-n/a}
executor_final_summary_sha256: ${executor_final_summary_sha256:-n/a}
executor_final_artifacts_written: ${executor_final_artifacts_written:-n/a}
executor_final_nested_package_bundle_enabled: ${executor_final_nested_package_bundle_enabled:-n/a}
executor_final_go_nogo_require_executor_upstream: ${executor_final_go_nogo_require_executor_upstream:-n/a}
executor_final_go_nogo_require_jito_rpc_policy: ${executor_final_go_nogo_require_jito_rpc_policy:-n/a}
executor_final_go_nogo_require_fastlane_disabled: ${executor_final_go_nogo_require_fastlane_disabled:-n/a}
executor_final_go_nogo_require_ingestion_grpc: ${executor_final_go_nogo_require_ingestion_grpc:-n/a}
executor_final_go_nogo_require_followlist_activity: ${executor_final_go_nogo_require_followlist_activity:-n/a}
executor_final_go_nogo_require_non_bootstrap_signer: ${executor_final_go_nogo_require_non_bootstrap_signer:-n/a}
executor_final_go_nogo_require_submit_verify_strict: ${executor_final_go_nogo_require_submit_verify_strict:-n/a}
executor_final_go_nogo_require_confirmed_execution_sample: ${executor_final_go_nogo_require_confirmed_execution_sample:-n/a}
executor_final_go_nogo_min_confirmed_orders: ${executor_final_go_nogo_min_confirmed_orders:-n/a}
executor_final_go_nogo_require_pretrade_fee_policy: ${executor_final_go_nogo_require_pretrade_fee_policy:-n/a}
executor_final_go_nogo_min_pretrade_sol_reserve_lamports: ${executor_final_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
executor_final_go_nogo_max_pretrade_fee_overhead_bps: ${executor_final_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
executor_final_executor_env_path: ${executor_final_executor_env_path:-n/a}
executor_final_rollout_nested_go_nogo_require_executor_upstream: ${executor_final_rollout_nested_go_nogo_require_executor_upstream:-n/a}
executor_final_rollout_nested_go_nogo_require_jito_rpc_policy: ${executor_final_rollout_nested_go_nogo_require_jito_rpc_policy:-n/a}
executor_final_rollout_nested_jito_rpc_policy_verdict: ${executor_final_rollout_nested_jito_rpc_policy_verdict:-n/a}
executor_final_rollout_nested_jito_rpc_policy_reason_code: ${executor_final_rollout_nested_jito_rpc_policy_reason_code:-n/a}
executor_final_rollout_nested_go_nogo_require_fastlane_disabled: ${executor_final_rollout_nested_go_nogo_require_fastlane_disabled:-n/a}
executor_final_rollout_nested_fastlane_feature_flag_verdict: ${executor_final_rollout_nested_fastlane_feature_flag_verdict:-n/a}
executor_final_rollout_nested_fastlane_feature_flag_reason_code: ${executor_final_rollout_nested_fastlane_feature_flag_reason_code:-n/a}
executor_final_rollout_nested_go_nogo_require_ingestion_grpc: ${executor_final_rollout_nested_go_nogo_require_ingestion_grpc:-n/a}
executor_final_rollout_nested_go_nogo_require_followlist_activity: ${executor_final_rollout_nested_go_nogo_require_followlist_activity:-n/a}
executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer: ${executor_final_rollout_nested_go_nogo_require_non_bootstrap_signer:-n/a}
executor_final_rollout_nested_go_nogo_require_submit_verify_strict: ${executor_final_rollout_nested_go_nogo_require_submit_verify_strict:-n/a}
executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample: ${executor_final_rollout_nested_go_nogo_require_confirmed_execution_sample:-n/a}
executor_final_rollout_nested_go_nogo_min_confirmed_orders: ${executor_final_rollout_nested_go_nogo_min_confirmed_orders:-n/a}
executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy: ${executor_final_rollout_nested_go_nogo_require_pretrade_fee_policy:-n/a}
executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports: ${executor_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps: ${executor_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
executor_final_rollout_nested_executor_env_path: ${executor_final_rollout_nested_executor_env_path:-n/a}
executor_final_rollout_nested_executor_backend_mode_guard_verdict: ${executor_final_rollout_nested_executor_backend_mode_guard_verdict:-n/a}
executor_final_rollout_nested_executor_backend_mode_guard_reason_code: ${executor_final_rollout_nested_executor_backend_mode_guard_reason_code:-n/a}
executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict: ${executor_final_rollout_nested_executor_upstream_endpoint_guard_verdict:-n/a}
executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code: ${executor_final_rollout_nested_executor_upstream_endpoint_guard_reason_code:-n/a}
executor_final_rollout_nested_ingestion_grpc_guard_verdict: ${executor_final_rollout_nested_ingestion_grpc_guard_verdict:-n/a}
executor_final_rollout_nested_ingestion_grpc_guard_reason_code: ${executor_final_rollout_nested_ingestion_grpc_guard_reason_code:-n/a}
executor_final_rollout_nested_followlist_activity_guard_verdict: ${executor_final_rollout_nested_followlist_activity_guard_verdict:-n/a}
executor_final_rollout_nested_followlist_activity_guard_reason_code: ${executor_final_rollout_nested_followlist_activity_guard_reason_code:-n/a}
executor_final_rollout_nested_non_bootstrap_signer_guard_verdict: ${executor_final_rollout_nested_non_bootstrap_signer_guard_verdict:-n/a}
executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code: ${executor_final_rollout_nested_non_bootstrap_signer_guard_reason_code:-n/a}
executor_final_rollout_nested_confirmed_execution_sample_guard_verdict: ${executor_final_rollout_nested_confirmed_execution_sample_guard_verdict:-n/a}
executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code: ${executor_final_rollout_nested_confirmed_execution_sample_guard_reason_code:-n/a}
executor_final_rollout_nested_pretrade_fee_policy_guard_verdict: ${executor_final_rollout_nested_pretrade_fee_policy_guard_verdict:-n/a}
executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code: ${executor_final_rollout_nested_pretrade_fee_policy_guard_reason_code:-n/a}
executor_final_rollout_nested_preflight_executor_submit_verify_strict: ${executor_final_rollout_nested_preflight_executor_submit_verify_strict:-n/a}
executor_final_rollout_nested_preflight_executor_submit_verify_configured: ${executor_final_rollout_nested_preflight_executor_submit_verify_configured:-n/a}
executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured: ${executor_final_rollout_nested_preflight_executor_submit_verify_fallback_configured:-n/a}
adapter_final_exit_code: $adapter_final_exit_code
adapter_final_verdict: $adapter_final_verdict
adapter_final_reason: ${adapter_final_reason:-n/a}
adapter_final_reason_code: ${adapter_final_reason_code:-n/a}
adapter_final_summary_sha256: ${adapter_final_summary_sha256:-n/a}
adapter_final_artifacts_written: ${adapter_final_artifacts_written:-n/a}
adapter_final_nested_package_bundle_enabled: ${adapter_final_nested_package_bundle_enabled:-n/a}
adapter_final_go_nogo_require_executor_upstream: ${adapter_final_go_nogo_require_executor_upstream:-n/a}
adapter_final_go_nogo_require_jito_rpc_policy: ${adapter_final_go_nogo_require_jito_rpc_policy:-n/a}
adapter_final_go_nogo_require_fastlane_disabled: ${adapter_final_go_nogo_require_fastlane_disabled:-n/a}
adapter_final_go_nogo_require_ingestion_grpc: ${adapter_final_go_nogo_require_ingestion_grpc:-n/a}
adapter_final_go_nogo_require_followlist_activity: ${adapter_final_go_nogo_require_followlist_activity:-n/a}
adapter_final_go_nogo_require_non_bootstrap_signer: ${adapter_final_go_nogo_require_non_bootstrap_signer:-n/a}
adapter_final_go_nogo_require_submit_verify_strict: ${adapter_final_go_nogo_require_submit_verify_strict:-n/a}
adapter_final_go_nogo_require_confirmed_execution_sample: ${adapter_final_go_nogo_require_confirmed_execution_sample:-n/a}
adapter_final_go_nogo_min_confirmed_orders: ${adapter_final_go_nogo_min_confirmed_orders:-n/a}
adapter_final_go_nogo_require_pretrade_fee_policy: ${adapter_final_go_nogo_require_pretrade_fee_policy:-n/a}
adapter_final_go_nogo_min_pretrade_sol_reserve_lamports: ${adapter_final_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
adapter_final_go_nogo_max_pretrade_fee_overhead_bps: ${adapter_final_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
adapter_final_executor_env_path: ${adapter_final_executor_env_path:-n/a}
adapter_final_rollout_nested_go_nogo_require_executor_upstream: ${adapter_final_rollout_nested_go_nogo_require_executor_upstream:-n/a}
adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy: ${adapter_final_rollout_nested_go_nogo_require_jito_rpc_policy:-n/a}
adapter_final_rollout_nested_jito_rpc_policy_verdict: ${adapter_final_rollout_nested_jito_rpc_policy_verdict:-n/a}
adapter_final_rollout_nested_jito_rpc_policy_reason_code: ${adapter_final_rollout_nested_jito_rpc_policy_reason_code:-n/a}
adapter_final_rollout_nested_go_nogo_require_fastlane_disabled: ${adapter_final_rollout_nested_go_nogo_require_fastlane_disabled:-n/a}
adapter_final_rollout_nested_fastlane_feature_flag_verdict: ${adapter_final_rollout_nested_fastlane_feature_flag_verdict:-n/a}
adapter_final_rollout_nested_fastlane_feature_flag_reason_code: ${adapter_final_rollout_nested_fastlane_feature_flag_reason_code:-n/a}
adapter_final_rollout_nested_go_nogo_require_ingestion_grpc: ${adapter_final_rollout_nested_go_nogo_require_ingestion_grpc:-n/a}
adapter_final_rollout_nested_go_nogo_require_followlist_activity: ${adapter_final_rollout_nested_go_nogo_require_followlist_activity:-n/a}
adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer: ${adapter_final_rollout_nested_go_nogo_require_non_bootstrap_signer:-n/a}
adapter_final_rollout_nested_go_nogo_require_submit_verify_strict: ${adapter_final_rollout_nested_go_nogo_require_submit_verify_strict:-n/a}
adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample: ${adapter_final_rollout_nested_go_nogo_require_confirmed_execution_sample:-n/a}
adapter_final_rollout_nested_go_nogo_min_confirmed_orders: ${adapter_final_rollout_nested_go_nogo_min_confirmed_orders:-n/a}
adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy: ${adapter_final_rollout_nested_go_nogo_require_pretrade_fee_policy:-n/a}
adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports: ${adapter_final_rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps: ${adapter_final_rollout_nested_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
adapter_final_rollout_nested_executor_env_path: ${adapter_final_rollout_nested_executor_env_path:-n/a}
adapter_final_rollout_nested_executor_backend_mode_guard_verdict: ${adapter_final_rollout_nested_executor_backend_mode_guard_verdict:-n/a}
adapter_final_rollout_nested_executor_backend_mode_guard_reason_code: ${adapter_final_rollout_nested_executor_backend_mode_guard_reason_code:-n/a}
adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict: ${adapter_final_rollout_nested_executor_upstream_endpoint_guard_verdict:-n/a}
adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code: ${adapter_final_rollout_nested_executor_upstream_endpoint_guard_reason_code:-n/a}
adapter_final_rollout_nested_ingestion_grpc_guard_verdict: ${adapter_final_rollout_nested_ingestion_grpc_guard_verdict:-n/a}
adapter_final_rollout_nested_ingestion_grpc_guard_reason_code: ${adapter_final_rollout_nested_ingestion_grpc_guard_reason_code:-n/a}
adapter_final_rollout_nested_followlist_activity_guard_verdict: ${adapter_final_rollout_nested_followlist_activity_guard_verdict:-n/a}
adapter_final_rollout_nested_followlist_activity_guard_reason_code: ${adapter_final_rollout_nested_followlist_activity_guard_reason_code:-n/a}
adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict: ${adapter_final_rollout_nested_non_bootstrap_signer_guard_verdict:-n/a}
adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code: ${adapter_final_rollout_nested_non_bootstrap_signer_guard_reason_code:-n/a}
adapter_final_rollout_nested_submit_verify_guard_verdict: ${adapter_final_rollout_nested_submit_verify_guard_verdict:-n/a}
adapter_final_rollout_nested_submit_verify_guard_reason_code: ${adapter_final_rollout_nested_submit_verify_guard_reason_code:-n/a}
adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict: ${adapter_final_rollout_nested_confirmed_execution_sample_guard_verdict:-n/a}
adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code: ${adapter_final_rollout_nested_confirmed_execution_sample_guard_reason_code:-n/a}
adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict: ${adapter_final_rollout_nested_pretrade_fee_policy_guard_verdict:-n/a}
adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code: ${adapter_final_rollout_nested_pretrade_fee_policy_guard_reason_code:-n/a}
exact_money_cutover_exit_code: ${exact_money_exit_code:-n/a}
exact_money_cutover_present: ${exact_money_cutover_present:-n/a}
exact_money_cutover_ts: ${exact_money_cutover_ts:-n/a}
exact_money_post_cutover_surface_failures: ${exact_money_post_cutover_surface_failures:-n/a}
exact_money_invalid_exact_rows_total: ${exact_money_invalid_exact_rows_total:-n/a}
exact_money_forbidden_merge_rows_total: ${exact_money_forbidden_merge_rows_total:-n/a}
exact_money_readiness_exit_code: ${exact_money_readiness_exit_code:-n/a}
exact_money_readiness_guard_verdict: ${exact_money_readiness_guard_verdict:-n/a}
exact_money_readiness_guard_reason_code: ${exact_money_readiness_guard_reason_code:-n/a}
exact_money_legacy_export_exit_code: ${exact_money_legacy_export_exit_code:-n/a}
exact_money_legacy_export_verdict: ${exact_money_legacy_export_verdict:-n/a}
exact_money_legacy_export_reason_code: ${exact_money_legacy_export_reason_code:-n/a}
exact_money_legacy_approximate_rows_total: ${exact_money_legacy_approximate_rows_total:-n/a}
exact_money_post_cutover_approximate_rows_total: ${exact_money_post_cutover_approximate_rows_total:-n/a}
exact_money_cutover_guard_verdict: ${exact_money_guard_verdict:-n/a}
exact_money_cutover_guard_reason_code: ${exact_money_guard_reason_code:-n/a}
artifact_exact_money_summary: ${exact_money_artifact_summary:-n/a}
exact_money_summary_sha256: ${exact_money_summary_sha256:-n/a}
artifact_exact_money_manifest: ${exact_money_artifact_manifest:-n/a}
exact_money_manifest_sha256: ${exact_money_manifest_sha256:-n/a}
server_rollout_verdict: $overall_verdict
server_rollout_reason: $overall_reason
server_rollout_reason_code: $overall_reason_code
artifacts_written: true"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

summary_path="$OUTPUT_ROOT/execution_server_rollout_summary_${now_compact}.txt"
manifest_path="$OUTPUT_ROOT/execution_server_rollout_manifest_${now_compact}.txt"
printf '%s\n' "$summary_output" >"$summary_path"

echo
echo "artifacts_written: true"
echo "artifact_summary: $summary_path"
echo "artifact_manifest: $manifest_path"
echo "artifact_preflight_capture: $preflight_capture"
echo "artifact_calibration_capture: $calibration_capture"
echo "artifact_go_nogo_capture: $go_nogo_capture"
echo "artifact_rehearsal_capture: $rehearsal_capture"
echo "artifact_executor_final_capture: $executor_final_capture"
echo "artifact_adapter_final_capture: $adapter_final_capture"
echo "artifact_exact_money_capture: $exact_money_capture"
echo "artifact_exact_money_summary: ${exact_money_artifact_summary:-n/a}"
echo "artifact_exact_money_manifest: ${exact_money_artifact_manifest:-n/a}"
echo "preflight_capture_sha256: $preflight_capture_sha256"
echo "calibration_capture_sha256: $calibration_capture_sha256"
echo "go_nogo_capture_sha256: $go_nogo_capture_sha256"
echo "rehearsal_capture_sha256: $rehearsal_capture_sha256"
echo "executor_final_capture_sha256: $executor_final_capture_sha256"
echo "adapter_final_capture_sha256: $adapter_final_capture_sha256"
echo "exact_money_capture_sha256: $exact_money_capture_sha256"
echo "exact_money_summary_sha256: ${exact_money_summary_sha256:-n/a}"
echo "exact_money_manifest_sha256: ${exact_money_manifest_sha256:-n/a}"

package_bundle_artifacts_written="false"
package_bundle_exit_code="n/a"
package_bundle_error="n/a"
package_bundle_path="n/a"
package_bundle_sha256="n/a"
package_bundle_sha256_path="n/a"
package_bundle_contents_manifest="n/a"
package_bundle_file_count="n/a"
run_package_bundle_once() {
  local package_bundle_output=""
  if package_bundle_output="$(
    OUTPUT_DIR="$PACKAGE_BUNDLE_OUTPUT_DIR" \
      BUNDLE_LABEL="$PACKAGE_BUNDLE_LABEL" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$OUTPUT_ROOT" 2>&1
  )"; then
    package_bundle_exit_code=0
    package_bundle_error="n/a"
    package_bundle_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$package_bundle_output")")"
    if ! package_bundle_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$package_bundle_output")"; then
      package_bundle_exit_code=1
      package_bundle_artifacts_written="false"
      package_bundle_error="bundle helper returned invalid artifacts_written token: ${package_bundle_artifacts_written_raw:-<empty>}"
      package_bundle_path="n/a"
      package_bundle_sha256="n/a"
      package_bundle_sha256_path="n/a"
      package_bundle_contents_manifest="n/a"
      package_bundle_file_count="n/a"
    else
      package_bundle_path="$(trim_string "$(extract_field "bundle_path" "$package_bundle_output")")"
      package_bundle_sha256="$(trim_string "$(extract_field "bundle_sha256" "$package_bundle_output")")"
      package_bundle_sha256_path="$(trim_string "$(extract_field "bundle_sha256_path" "$package_bundle_output")")"
      package_bundle_contents_manifest="$(trim_string "$(extract_field "contents_manifest" "$package_bundle_output")")"
      package_bundle_file_count="$(trim_string "$(extract_field "file_count" "$package_bundle_output")")"
    fi
  else
    package_bundle_exit_code=$?
    package_bundle_artifacts_written="false"
    package_bundle_error="$(trim_string "$(printf '%s\n' "$package_bundle_output" | tail -n 1)")"
    package_bundle_path="n/a"
    package_bundle_sha256="n/a"
    package_bundle_sha256_path="n/a"
    package_bundle_contents_manifest="n/a"
    package_bundle_file_count="n/a"
  fi
}

if [[ "$package_bundle_enabled_norm" == "true" ]]; then
  # First pass resolves actual package status used by artifact summary.
  run_package_bundle_once
fi

cat >>"$summary_path" <<EOF_SUMMARY
package_bundle_artifacts_written: $package_bundle_artifacts_written
package_bundle_exit_code: $package_bundle_exit_code
package_bundle_error: $package_bundle_error
EOF_SUMMARY

summary_sha256="$(sha256_file_value "$summary_path")"
cat >"$manifest_path" <<EOF_MANIFEST
summary_sha256: $summary_sha256
preflight_capture_sha256: $preflight_capture_sha256
calibration_capture_sha256: $calibration_capture_sha256
go_nogo_capture_sha256: $go_nogo_capture_sha256
rehearsal_capture_sha256: $rehearsal_capture_sha256
executor_final_capture_sha256: $executor_final_capture_sha256
adapter_final_capture_sha256: $adapter_final_capture_sha256
exact_money_capture_sha256: $exact_money_capture_sha256
exact_money_evidence_summary_sha256: ${exact_money_summary_sha256:-n/a}
exact_money_evidence_manifest_sha256: ${exact_money_manifest_sha256:-n/a}
calibration_summary_sha256: ${calibration_summary_sha256:-n/a}
go_nogo_summary_sha256: ${go_nogo_summary_sha256:-n/a}
rehearsal_summary_sha256: ${rehearsal_summary_sha256:-n/a}
executor_final_summary_sha256: ${executor_final_summary_sha256:-n/a}
adapter_final_summary_sha256: ${adapter_final_summary_sha256:-n/a}
EOF_MANIFEST
manifest_sha256="$(sha256_file_value "$manifest_path")"

if [[ "$package_bundle_enabled_norm" == "true" ]]; then
  if [[ "$package_bundle_artifacts_written" == "true" ]]; then
    # Second pass packages finalized summary/manifest into bundle payload.
    run_package_bundle_once
  fi
fi

echo "package_bundle_artifacts_written: $package_bundle_artifacts_written"
echo "package_bundle_exit_code: $package_bundle_exit_code"
echo "package_bundle_error: $package_bundle_error"
echo "package_bundle_path: $package_bundle_path"
echo "package_bundle_sha256: $package_bundle_sha256"
echo "package_bundle_sha256_path: $package_bundle_sha256_path"
echo "package_bundle_contents_manifest: $package_bundle_contents_manifest"
echo "package_bundle_file_count: $package_bundle_file_count"
echo "summary_sha256: $summary_sha256"
echo "manifest_sha256: $manifest_sha256"

if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" != "true" ]]; then
  exit 3
fi

case "$overall_verdict" in
GO)
  exit 0
  ;;
HOLD)
  exit 2
  ;;
*)
  exit 3
  ;;
esac
