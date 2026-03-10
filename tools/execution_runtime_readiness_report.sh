#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

ROUTE_FEE_WINDOWS_CSV="${1:-1,6,24}"
RISK_EVENTS_MINUTES_RAW="${2:-60}"
ADAPTER_WINDOWS_CSV="${3:-24}"

SERVICE="${SERVICE:-solana-copy-bot}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/live.toml}}"
ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-adapter.env}"
OUTPUT_ROOT="${OUTPUT_ROOT:-state/runtime-readiness-$(date -u +"%Y%m%dT%H%M%SZ")}"

RUN_TESTS="${RUN_TESTS:-false}"
DEVNET_REHEARSAL_TEST_MODE="${DEVNET_REHEARSAL_TEST_MODE:-false}"
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}"
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}"
GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-true}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-true}"
GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="${GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM:-true}"
GO_NOGO_REQUIRE_INGESTION_GRPC="${GO_NOGO_REQUIRE_INGESTION_GRPC:-false}"
GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="${GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY:-false}"
GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="${GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER:-false}"
GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="${GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT:-false}"
GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="${GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE:-true}"
GO_NOGO_MIN_CONFIRMED_ORDERS="${GO_NOGO_MIN_CONFIRMED_ORDERS:-1}"
GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="${GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY:-true}"
GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="${GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS:-50000000}"
GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="${GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS:-1000}"
WINDOWED_SIGNOFF_REQUIRED="${WINDOWED_SIGNOFF_REQUIRED:-true}"
WINDOWED_SIGNOFF_WINDOWS_CSV="${WINDOWED_SIGNOFF_WINDOWS_CSV:-1,6,24}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-true}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-true}"
ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-true}"
ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="${REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED:-true}"
REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"

PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_runtime_readiness}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_ROOT}"
RUNTIME_READINESS_PROFILE="${RUNTIME_READINESS_PROFILE:-full}"
EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

declare -a input_errors=()

parse_runtime_bool_setting_into() {
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

parse_runtime_positive_u64_setting_into() {
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

runtime_readiness_run_adapter_final_default="true"
runtime_readiness_run_route_fee_final_default="true"
case "$RUNTIME_READINESS_PROFILE" in
full)
  ;;
adapter_only)
  runtime_readiness_run_adapter_final_default="true"
  runtime_readiness_run_route_fee_final_default="false"
  ;;
route_fee_only)
  runtime_readiness_run_adapter_final_default="false"
  runtime_readiness_run_route_fee_final_default="true"
  ;;
*)
  input_errors+=("RUNTIME_READINESS_PROFILE must be one of: full,adapter_only,route_fee_only (got: $RUNTIME_READINESS_PROFILE)")
  ;;
esac

runtime_readiness_run_adapter_final_raw="$runtime_readiness_run_adapter_final_default"
if [[ -n "${RUNTIME_READINESS_RUN_ADAPTER_FINAL+x}" ]]; then
  runtime_readiness_run_adapter_final_raw="${RUNTIME_READINESS_RUN_ADAPTER_FINAL}"
fi
runtime_readiness_run_route_fee_final_raw="$runtime_readiness_run_route_fee_final_default"
if [[ -n "${RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL+x}" ]]; then
  runtime_readiness_run_route_fee_final_raw="${RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL}"
fi

parse_runtime_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_runtime_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_runtime_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" "$GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" go_nogo_require_executor_upstream_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_INGESTION_GRPC" "$GO_NOGO_REQUIRE_INGESTION_GRPC" go_nogo_require_ingestion_grpc_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" "$GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" go_nogo_require_followlist_activity_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" "$GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" go_nogo_require_non_bootstrap_signer_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" "$GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" go_nogo_require_submit_verify_strict_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" "$GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" go_nogo_require_confirmed_execution_sample_norm
parse_runtime_positive_u64_setting_into "GO_NOGO_MIN_CONFIRMED_ORDERS" "$GO_NOGO_MIN_CONFIRMED_ORDERS" go_nogo_min_confirmed_orders_norm
parse_runtime_bool_setting_into "GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY" "$GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY" go_nogo_require_pretrade_fee_policy_norm
parse_runtime_positive_u64_setting_into "GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS" "$GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS" go_nogo_min_pretrade_sol_reserve_lamports_norm
parse_runtime_positive_u64_setting_into "GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS" "$GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS" go_nogo_max_pretrade_fee_overhead_bps_norm
parse_runtime_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_runtime_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_runtime_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_runtime_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_runtime_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" "$REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" rehearsal_route_fee_signoff_required_norm
parse_runtime_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm
parse_runtime_bool_setting_into "RUNTIME_READINESS_RUN_ADAPTER_FINAL" "$runtime_readiness_run_adapter_final_raw" runtime_readiness_run_adapter_final_norm
parse_runtime_bool_setting_into "RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL" "$runtime_readiness_run_route_fee_final_raw" runtime_readiness_run_route_fee_final_norm

if [[ "$runtime_readiness_run_adapter_final_norm" != "true" && "$runtime_readiness_run_route_fee_final_norm" != "true" ]]; then
  input_errors+=("at least one stage must be enabled: RUNTIME_READINESS_RUN_ADAPTER_FINAL or RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL")
fi

risk_events_minutes=""
if ! risk_events_minutes="$(parse_u64_token_strict "$RISK_EVENTS_MINUTES_RAW")"; then
  input_errors+=("RISK_EVENTS_MINUTES must be an integer (got: ${RISK_EVENTS_MINUTES_RAW})")
  risk_events_minutes="60"
elif [[ "$risk_events_minutes" -lt 1 ]]; then
  input_errors+=("RISK_EVENTS_MINUTES must be >= 1 (got: ${risk_events_minutes})")
fi

mkdir -p "$OUTPUT_ROOT"
adapter_output_root="$OUTPUT_ROOT/adapter_final"
route_fee_output_root="$OUTPUT_ROOT/route_fee_final"
mkdir -p "$adapter_output_root" "$route_fee_output_root"

adapter_output=""
adapter_exit_code="3"
adapter_verdict="UNKNOWN"
adapter_reason="adapter final helper not executed"
adapter_reason_code="not_executed"
adapter_artifacts_written="false"
adapter_artifact_summary="n/a"
adapter_artifact_manifest="n/a"
adapter_summary_sha256="n/a"
adapter_manifest_sha256="n/a"
adapter_final_nested_package_bundle_enabled="n/a"
adapter_nested_go_nogo_require_executor_upstream="n/a"
adapter_nested_executor_env_path="n/a"
adapter_nested_executor_backend_mode_guard_verdict="n/a"
adapter_nested_executor_backend_mode_guard_reason_code="n/a"
adapter_nested_executor_upstream_endpoint_guard_verdict="n/a"
adapter_nested_executor_upstream_endpoint_guard_reason_code="n/a"
adapter_nested_go_nogo_require_jito_rpc_policy="n/a"
adapter_nested_jito_rpc_policy_verdict="n/a"
adapter_nested_jito_rpc_policy_reason_code="n/a"
adapter_nested_go_nogo_require_fastlane_disabled="n/a"
adapter_nested_fastlane_feature_flag_verdict="n/a"
adapter_nested_fastlane_feature_flag_reason_code="n/a"
adapter_nested_go_nogo_require_ingestion_grpc="n/a"
adapter_nested_ingestion_grpc_guard_verdict="n/a"
adapter_nested_ingestion_grpc_guard_reason_code="n/a"
adapter_nested_go_nogo_require_followlist_activity="n/a"
adapter_nested_followlist_activity_guard_verdict="n/a"
adapter_nested_followlist_activity_guard_reason_code="n/a"
adapter_nested_go_nogo_require_non_bootstrap_signer="n/a"
adapter_nested_non_bootstrap_signer_guard_verdict="n/a"
adapter_nested_non_bootstrap_signer_guard_reason_code="n/a"
adapter_nested_go_nogo_require_submit_verify_strict="n/a"
adapter_nested_submit_verify_guard_verdict="n/a"
adapter_nested_submit_verify_guard_reason_code="n/a"
adapter_nested_go_nogo_require_confirmed_execution_sample="n/a"
adapter_nested_go_nogo_min_confirmed_orders="n/a"
adapter_nested_confirmed_execution_sample_guard_verdict="n/a"
adapter_nested_confirmed_execution_sample_guard_reason_code="n/a"
adapter_nested_go_nogo_require_pretrade_fee_policy="n/a"
adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
adapter_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
adapter_nested_pretrade_fee_policy_guard_verdict="n/a"
adapter_nested_pretrade_fee_policy_guard_reason_code="n/a"

route_fee_output=""
route_fee_exit_code="3"
route_fee_verdict="UNKNOWN"
route_fee_reason="route/fee final helper not executed"
route_fee_reason_code="not_executed"
route_fee_artifacts_written="false"
route_fee_artifact_summary="n/a"
route_fee_artifact_manifest="n/a"
route_fee_summary_sha256="n/a"
route_fee_manifest_sha256="n/a"
route_fee_window_count="n/a"
route_fee_go_nogo_go_count="n/a"
route_fee_route_profile_pass_count="n/a"
route_fee_fee_decomposition_pass_count="n/a"
route_fee_primary_route_stable="n/a"
route_fee_stable_primary_route="n/a"
route_fee_fallback_route_stable="n/a"
route_fee_stable_fallback_route="n/a"
route_fee_final_nested_package_bundle_enabled="n/a"
route_fee_nested_go_nogo_require_executor_upstream="n/a"
route_fee_nested_executor_env_path="n/a"
route_fee_nested_executor_backend_mode_guard_verdict="n/a"
route_fee_nested_executor_backend_mode_guard_reason_code="n/a"
route_fee_nested_executor_upstream_endpoint_guard_verdict="n/a"
route_fee_nested_executor_upstream_endpoint_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_jito_rpc_policy="n/a"
route_fee_nested_jito_rpc_policy_verdict="n/a"
route_fee_nested_jito_rpc_policy_reason_code="n/a"
route_fee_nested_go_nogo_require_fastlane_disabled="n/a"
route_fee_nested_fastlane_feature_flag_verdict="n/a"
route_fee_nested_fastlane_feature_flag_reason_code="n/a"
route_fee_nested_go_nogo_require_ingestion_grpc="n/a"
route_fee_nested_ingestion_grpc_guard_verdict="n/a"
route_fee_nested_ingestion_grpc_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_followlist_activity="n/a"
route_fee_nested_followlist_activity_guard_verdict="n/a"
route_fee_nested_followlist_activity_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_non_bootstrap_signer="n/a"
route_fee_nested_non_bootstrap_signer_guard_verdict="n/a"
route_fee_nested_non_bootstrap_signer_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_submit_verify_strict="n/a"
route_fee_nested_submit_verify_guard_verdict="n/a"
route_fee_nested_submit_verify_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_confirmed_execution_sample="n/a"
route_fee_nested_go_nogo_min_confirmed_orders="n/a"
route_fee_nested_confirmed_execution_sample_guard_verdict="n/a"
route_fee_nested_confirmed_execution_sample_guard_reason_code="n/a"
route_fee_nested_go_nogo_require_pretrade_fee_policy="n/a"
route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
route_fee_nested_pretrade_fee_policy_guard_verdict="n/a"
route_fee_nested_pretrade_fee_policy_guard_reason_code="n/a"
route_fee_nested_signoff_guard_window_id="n/a"

if ((${#input_errors[@]} == 0)); then
  if [[ "$runtime_readiness_run_adapter_final_norm" == "true" ]]; then
    if adapter_output="$({
      ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
        CONFIG_PATH="$CONFIG_PATH" \
        SERVICE="$SERVICE" \
        OUTPUT_ROOT="$adapter_output_root" \
        RUN_TESTS="$run_tests_norm" \
        DEVNET_REHEARSAL_TEST_MODE="$devnet_rehearsal_test_mode_norm" \
        GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
        GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
        GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
        GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
        GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
        GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream_norm" \
        GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
        GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
        GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
        GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
        GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
        GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
        GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
        GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
        GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
        EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
        WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
        WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
        WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
        WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
        ROUTE_FEE_SIGNOFF_REQUIRED="$route_fee_signoff_required_norm" \
        ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
        REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="$rehearsal_route_fee_signoff_required_norm" \
        REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
        ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
        REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
        PACKAGE_BUNDLE_ENABLED="false" \
        bash "$ROOT_DIR/tools/adapter_rollout_final_evidence_report.sh" "$ADAPTER_WINDOWS_CSV" "$risk_events_minutes" 2>&1
    } )"; then
      adapter_exit_code=0
    else
      adapter_exit_code=$?
    fi

    adapter_verdict="$(normalize_go_nogo_verdict "$(first_non_empty "$(extract_field "final_rollout_package_verdict" "$adapter_output")" "$(extract_field "rollout_verdict" "$adapter_output")")")"
    adapter_reason="$(trim_string "$(first_non_empty "$(extract_field "final_rollout_package_reason" "$adapter_output")" "$(extract_field "rollout_reason" "$adapter_output")")")"
    adapter_reason_code="$(trim_string "$(first_non_empty "$(extract_field "final_rollout_package_reason_code" "$adapter_output")" "$(extract_field "rollout_reason_code" "$adapter_output")")")"
    adapter_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$adapter_output")")"
    if ! adapter_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final artifacts_written must be boolean token, got: ${adapter_artifacts_written_raw:-<empty>}")
      adapter_artifacts_written="unknown"
    elif [[ "$adapter_artifacts_written" != "true" ]]; then
      input_errors+=("nested adapter rollout final artifacts_written must be true")
    fi
    adapter_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$adapter_output")")"
    adapter_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$adapter_output")")"
    adapter_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$adapter_output")")"
    adapter_manifest_sha256="$(trim_string "$(extract_field "manifest_sha256" "$adapter_output")")"
    adapter_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$adapter_output")")"
    if ! adapter_final_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final package_bundle_enabled must be boolean token, got: ${adapter_nested_package_bundle_enabled_raw:-<empty>}")
      adapter_final_nested_package_bundle_enabled="unknown"
    elif [[ "$adapter_final_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("nested adapter rollout final helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
    adapter_nested_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final go_nogo_require_executor_upstream must be boolean token, got: ${adapter_nested_go_nogo_require_executor_upstream_raw:-<empty>}")
      adapter_nested_go_nogo_require_executor_upstream="unknown"
    elif [[ "$adapter_nested_go_nogo_require_executor_upstream" != "$go_nogo_require_executor_upstream_norm" ]]; then
      input_errors+=("nested adapter rollout final go_nogo_require_executor_upstream mismatch: nested=${adapter_nested_go_nogo_require_executor_upstream} expected=${go_nogo_require_executor_upstream_norm}")
    fi
    adapter_nested_executor_env_path="$(trim_string "$(extract_field "executor_env_path" "$adapter_output")")"
    if [[ -z "$adapter_nested_executor_env_path" ]]; then
      input_errors+=("nested adapter rollout final executor_env_path must be non-empty")
      adapter_nested_executor_env_path="n/a"
    elif [[ "$adapter_nested_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
      input_errors+=("nested adapter rollout final executor_env_path mismatch: nested=${adapter_nested_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
    fi
    adapter_nested_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_verdict" "$adapter_output")")"
    adapter_nested_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_executor_backend_mode_guard_verdict_raw")"
    if [[ -z "$adapter_nested_executor_backend_mode_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_backend_mode_guard_verdict must be non-empty")
      adapter_nested_executor_backend_mode_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_executor_backend_mode_guard_verdict_raw})")
      adapter_nested_executor_backend_mode_guard_verdict="UNKNOWN"
    fi
    adapter_nested_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_backend_mode_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_executor_backend_mode_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_backend_mode_guard_reason_code must be non-empty")
      adapter_nested_executor_backend_mode_guard_reason_code="n/a"
    fi
    adapter_nested_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_verdict" "$adapter_output")")"
    adapter_nested_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw")"
    if [[ -z "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_upstream_endpoint_guard_verdict must be non-empty")
      adapter_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_executor_upstream_endpoint_guard_verdict_raw})")
      adapter_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    fi
    adapter_nested_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_executor_upstream_endpoint_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_executor_upstream_endpoint_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_executor_upstream_endpoint_guard_reason_code must be non-empty")
      adapter_nested_executor_upstream_endpoint_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_jito_rpc_policy" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_jito_rpc_policy" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_jito_rpc_policy must be boolean token, got: ${adapter_nested_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
      adapter_nested_go_nogo_require_jito_rpc_policy="unknown"
    elif [[ "$adapter_nested_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_jito_rpc_policy mismatch: nested=${adapter_nested_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
    fi
    adapter_nested_jito_rpc_policy_verdict_raw="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_verdict" "$adapter_output")")"
    adapter_nested_jito_rpc_policy_verdict_raw_upper="$(printf '%s' "$adapter_nested_jito_rpc_policy_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_jito_rpc_policy_verdict="$(normalize_gate_verdict "$adapter_nested_jito_rpc_policy_verdict_raw")"
    if [[ -z "$adapter_nested_jito_rpc_policy_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_jito_rpc_policy_verdict must be non-empty")
      adapter_nested_jito_rpc_policy_verdict="UNKNOWN"
    elif [[ "$adapter_nested_jito_rpc_policy_verdict_raw_upper" != "PASS" && "$adapter_nested_jito_rpc_policy_verdict_raw_upper" != "WARN" && "$adapter_nested_jito_rpc_policy_verdict_raw_upper" != "NO_DATA" && "$adapter_nested_jito_rpc_policy_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_jito_rpc_policy_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${adapter_nested_jito_rpc_policy_verdict_raw})")
      adapter_nested_jito_rpc_policy_verdict="UNKNOWN"
    fi
    adapter_nested_jito_rpc_policy_reason_code="$(trim_string "$(extract_field "rollout_nested_jito_rpc_policy_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_jito_rpc_policy_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_jito_rpc_policy_reason_code must be non-empty")
      adapter_nested_jito_rpc_policy_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_fastlane_disabled" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "rollout_nested_go_nogo_require_fastlane_disabled" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_fastlane_disabled must be boolean token, got: ${adapter_nested_go_nogo_require_fastlane_disabled_raw:-<empty>}")
      adapter_nested_go_nogo_require_fastlane_disabled="unknown"
    elif [[ "$adapter_nested_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_fastlane_disabled mismatch: nested=${adapter_nested_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
    fi
    adapter_nested_fastlane_feature_flag_verdict_raw="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_verdict" "$adapter_output")")"
    adapter_nested_fastlane_feature_flag_verdict_raw_upper="$(printf '%s' "$adapter_nested_fastlane_feature_flag_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_fastlane_feature_flag_verdict="$(normalize_gate_verdict "$adapter_nested_fastlane_feature_flag_verdict_raw")"
    if [[ -z "$adapter_nested_fastlane_feature_flag_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_fastlane_feature_flag_verdict must be non-empty")
      adapter_nested_fastlane_feature_flag_verdict="UNKNOWN"
    elif [[ "$adapter_nested_fastlane_feature_flag_verdict_raw_upper" != "PASS" && "$adapter_nested_fastlane_feature_flag_verdict_raw_upper" != "WARN" && "$adapter_nested_fastlane_feature_flag_verdict_raw_upper" != "NO_DATA" && "$adapter_nested_fastlane_feature_flag_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_fastlane_feature_flag_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${adapter_nested_fastlane_feature_flag_verdict_raw})")
      adapter_nested_fastlane_feature_flag_verdict="UNKNOWN"
    fi
    adapter_nested_fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "rollout_nested_fastlane_feature_flag_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_fastlane_feature_flag_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_fastlane_feature_flag_reason_code must be non-empty")
      adapter_nested_fastlane_feature_flag_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_ingestion_grpc" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "rollout_nested_go_nogo_require_ingestion_grpc" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_ingestion_grpc must be boolean token, got: ${adapter_nested_go_nogo_require_ingestion_grpc_raw:-<empty>}")
      adapter_nested_go_nogo_require_ingestion_grpc="unknown"
    elif [[ "$adapter_nested_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_ingestion_grpc mismatch: nested=${adapter_nested_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
    fi
    adapter_nested_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_verdict" "$adapter_output")")"
    adapter_nested_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_ingestion_grpc_guard_verdict_raw")"
    if [[ -z "$adapter_nested_ingestion_grpc_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_ingestion_grpc_guard_verdict must be non-empty")
      adapter_nested_ingestion_grpc_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_ingestion_grpc_guard_verdict_raw})")
      adapter_nested_ingestion_grpc_guard_verdict="UNKNOWN"
    fi
    adapter_nested_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_ingestion_grpc_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_ingestion_grpc_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_ingestion_grpc_guard_reason_code must be non-empty")
      adapter_nested_ingestion_grpc_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_followlist_activity" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_followlist_activity="$(extract_bool_field_strict "rollout_nested_go_nogo_require_followlist_activity" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_followlist_activity must be boolean token, got: ${adapter_nested_go_nogo_require_followlist_activity_raw:-<empty>}")
      adapter_nested_go_nogo_require_followlist_activity="unknown"
    elif [[ "$adapter_nested_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_followlist_activity mismatch: nested=${adapter_nested_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
    fi
    adapter_nested_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_verdict" "$adapter_output")")"
    adapter_nested_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_followlist_activity_guard_verdict_raw")"
    if [[ -z "$adapter_nested_followlist_activity_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_followlist_activity_guard_verdict must be non-empty")
      adapter_nested_followlist_activity_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_followlist_activity_guard_verdict_raw})")
      adapter_nested_followlist_activity_guard_verdict="UNKNOWN"
    fi
    adapter_nested_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_followlist_activity_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_followlist_activity_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_followlist_activity_guard_reason_code must be non-empty")
      adapter_nested_followlist_activity_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_non_bootstrap_signer" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "rollout_nested_go_nogo_require_non_bootstrap_signer" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_non_bootstrap_signer must be boolean token, got: ${adapter_nested_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
      adapter_nested_go_nogo_require_non_bootstrap_signer="unknown"
    elif [[ "$adapter_nested_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_non_bootstrap_signer mismatch: nested=${adapter_nested_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
    fi
    adapter_nested_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_verdict" "$adapter_output")")"
    adapter_nested_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_non_bootstrap_signer_guard_verdict_raw")"
    if [[ -z "$adapter_nested_non_bootstrap_signer_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_non_bootstrap_signer_guard_verdict must be non-empty")
      adapter_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_non_bootstrap_signer_guard_verdict_raw})")
      adapter_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
    fi
    adapter_nested_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_non_bootstrap_signer_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_non_bootstrap_signer_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_non_bootstrap_signer_guard_reason_code must be non-empty")
      adapter_nested_non_bootstrap_signer_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_submit_verify_strict" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "rollout_nested_go_nogo_require_submit_verify_strict" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_submit_verify_strict must be boolean token, got: ${adapter_nested_go_nogo_require_submit_verify_strict_raw:-<empty>}")
      adapter_nested_go_nogo_require_submit_verify_strict="unknown"
    elif [[ "$adapter_nested_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_submit_verify_strict mismatch: nested=${adapter_nested_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
    fi
    adapter_nested_submit_verify_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_submit_verify_guard_verdict" "$adapter_output")")"
    adapter_nested_submit_verify_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_submit_verify_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_submit_verify_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_submit_verify_guard_verdict_raw")"
    if [[ -z "$adapter_nested_submit_verify_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_submit_verify_guard_verdict must be non-empty")
      adapter_nested_submit_verify_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_submit_verify_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_submit_verify_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_submit_verify_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_submit_verify_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_submit_verify_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_submit_verify_guard_verdict_raw})")
      adapter_nested_submit_verify_guard_verdict="UNKNOWN"
    fi
    adapter_nested_submit_verify_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_submit_verify_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_submit_verify_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_submit_verify_guard_reason_code must be non-empty")
      adapter_nested_submit_verify_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_confirmed_execution_sample" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "rollout_nested_go_nogo_require_confirmed_execution_sample" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_confirmed_execution_sample must be boolean token, got: ${adapter_nested_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
      adapter_nested_go_nogo_require_confirmed_execution_sample="unknown"
    elif [[ "$adapter_nested_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_confirmed_execution_sample mismatch: nested=${adapter_nested_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
    fi
    adapter_nested_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_confirmed_orders" "$adapter_output")")"
    if ! adapter_nested_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$adapter_nested_go_nogo_min_confirmed_orders_raw")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_min_confirmed_orders must be an integer >= 1, got: ${adapter_nested_go_nogo_min_confirmed_orders_raw:-<empty>}")
      adapter_nested_go_nogo_min_confirmed_orders="1"
    elif [[ "$adapter_nested_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_min_confirmed_orders mismatch: nested=${adapter_nested_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
    fi
    adapter_nested_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_verdict" "$adapter_output")")"
    adapter_nested_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_confirmed_execution_sample_guard_verdict_raw")"
    if [[ -z "$adapter_nested_confirmed_execution_sample_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_confirmed_execution_sample_guard_verdict must be non-empty")
      adapter_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_confirmed_execution_sample_guard_verdict_raw})")
      adapter_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
    fi
    adapter_nested_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_confirmed_execution_sample_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_confirmed_execution_sample_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_confirmed_execution_sample_guard_reason_code must be non-empty")
      adapter_nested_confirmed_execution_sample_guard_reason_code="n/a"
    fi
    adapter_nested_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_require_pretrade_fee_policy" "$adapter_output")")"
    if ! adapter_nested_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "rollout_nested_go_nogo_require_pretrade_fee_policy" "$adapter_output")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_pretrade_fee_policy must be boolean token, got: ${adapter_nested_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
      adapter_nested_go_nogo_require_pretrade_fee_policy="unknown"
    elif [[ "$adapter_nested_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_require_pretrade_fee_policy mismatch: nested=${adapter_nested_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
    fi
    adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports" "$adapter_output")")"
    if ! adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
      adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports="1"
    elif [[ "$adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
    fi
    adapter_nested_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "rollout_nested_go_nogo_max_pretrade_fee_overhead_bps" "$adapter_output")")"
    if ! adapter_nested_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$adapter_nested_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${adapter_nested_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
      adapter_nested_go_nogo_max_pretrade_fee_overhead_bps="1"
    elif [[ "$adapter_nested_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${adapter_nested_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
    fi
    adapter_nested_pretrade_fee_policy_guard_verdict_raw="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_verdict" "$adapter_output")")"
    adapter_nested_pretrade_fee_policy_guard_verdict_raw_upper="$(printf '%s' "$adapter_nested_pretrade_fee_policy_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    adapter_nested_pretrade_fee_policy_guard_verdict="$(normalize_strict_guard_verdict "$adapter_nested_pretrade_fee_policy_guard_verdict_raw")"
    if [[ -z "$adapter_nested_pretrade_fee_policy_guard_verdict_raw" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_pretrade_fee_policy_guard_verdict must be non-empty")
      adapter_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
    elif [[ "$adapter_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "PASS" && "$adapter_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "WARN" && "$adapter_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "UNKNOWN" && "$adapter_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_pretrade_fee_policy_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${adapter_nested_pretrade_fee_policy_guard_verdict_raw})")
      adapter_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
    fi
    adapter_nested_pretrade_fee_policy_guard_reason_code="$(trim_string "$(extract_field "rollout_nested_pretrade_fee_policy_guard_reason_code" "$adapter_output")")"
    if [[ -z "$adapter_nested_pretrade_fee_policy_guard_reason_code" ]]; then
      input_errors+=("nested adapter rollout final rollout_nested_pretrade_fee_policy_guard_reason_code must be non-empty")
      adapter_nested_pretrade_fee_policy_guard_reason_code="n/a"
    fi
    if [[ "$go_nogo_require_executor_upstream_norm" == "true" ]]; then
      if [[ "$adapter_nested_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_executor_backend_mode_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
      if [[ "$adapter_nested_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_executor_upstream_endpoint_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
    else
      if [[ "$adapter_nested_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_executor_backend_mode_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${adapter_nested_executor_backend_mode_guard_verdict})")
      fi
      if [[ "$adapter_nested_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_executor_upstream_endpoint_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${adapter_nested_executor_upstream_endpoint_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_jito_rpc_policy_norm" == "true" ]]; then
      if [[ "$adapter_nested_jito_rpc_policy_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_jito_rpc_policy_verdict cannot be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=true")
      fi
    else
      if [[ "$adapter_nested_jito_rpc_policy_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_jito_rpc_policy_verdict must be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=false (got: ${adapter_nested_jito_rpc_policy_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_fastlane_disabled_norm" == "true" ]]; then
      if [[ "$adapter_nested_fastlane_feature_flag_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_fastlane_feature_flag_verdict cannot be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=true")
      fi
    else
      if [[ "$adapter_nested_fastlane_feature_flag_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_fastlane_feature_flag_verdict must be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=false (got: ${adapter_nested_fastlane_feature_flag_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
      if [[ "$adapter_nested_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
      fi
    else
      if [[ "$adapter_nested_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${adapter_nested_ingestion_grpc_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
      if [[ "$adapter_nested_followlist_activity_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
      fi
    else
      if [[ "$adapter_nested_followlist_activity_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${adapter_nested_followlist_activity_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
      if [[ "$adapter_nested_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
      fi
    else
      if [[ "$adapter_nested_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${adapter_nested_non_bootstrap_signer_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" ]]; then
      if [[ "$adapter_nested_submit_verify_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_submit_verify_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
      fi
    else
      if [[ "$adapter_nested_submit_verify_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_submit_verify_guard_verdict must be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=false (got: ${adapter_nested_submit_verify_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
      if [[ "$adapter_nested_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
      fi
    else
      if [[ "$adapter_nested_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${adapter_nested_confirmed_execution_sample_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_pretrade_fee_policy_norm" == "true" ]]; then
      if [[ "$adapter_nested_pretrade_fee_policy_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_pretrade_fee_policy_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=true")
      fi
    else
      if [[ "$adapter_nested_pretrade_fee_policy_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested adapter rollout final rollout_nested_pretrade_fee_policy_guard_verdict must be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=false (got: ${adapter_nested_pretrade_fee_policy_guard_verdict})")
      fi
    fi

    if [[ "$adapter_verdict" == "UNKNOWN" ]]; then
      adapter_reason="unable to classify adapter final verdict (exit=${adapter_exit_code})"
      adapter_reason_code="unknown_verdict"
    elif [[ -z "$adapter_reason" ]]; then
      adapter_reason="adapter final helper reported ${adapter_verdict}"
      adapter_reason_code="missing_reason"
    fi
    if [[ -z "$adapter_reason_code" ]]; then
      adapter_reason_code="n/a"
    fi
  else
    adapter_exit_code=0
    adapter_verdict="SKIP"
    adapter_reason="adapter final stage disabled via RUNTIME_READINESS_RUN_ADAPTER_FINAL=false"
    adapter_reason_code="stage_disabled"
    adapter_artifacts_written="n/a"
    adapter_final_nested_package_bundle_enabled="n/a"
    adapter_nested_go_nogo_require_executor_upstream="n/a"
    adapter_nested_executor_env_path="n/a"
    adapter_nested_executor_backend_mode_guard_verdict="n/a"
    adapter_nested_executor_backend_mode_guard_reason_code="n/a"
    adapter_nested_executor_upstream_endpoint_guard_verdict="n/a"
    adapter_nested_executor_upstream_endpoint_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_jito_rpc_policy="n/a"
    adapter_nested_jito_rpc_policy_verdict="n/a"
    adapter_nested_jito_rpc_policy_reason_code="n/a"
    adapter_nested_go_nogo_require_fastlane_disabled="n/a"
    adapter_nested_fastlane_feature_flag_verdict="n/a"
    adapter_nested_fastlane_feature_flag_reason_code="n/a"
    adapter_nested_go_nogo_require_ingestion_grpc="n/a"
    adapter_nested_ingestion_grpc_guard_verdict="n/a"
    adapter_nested_ingestion_grpc_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_followlist_activity="n/a"
    adapter_nested_followlist_activity_guard_verdict="n/a"
    adapter_nested_followlist_activity_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_non_bootstrap_signer="n/a"
    adapter_nested_non_bootstrap_signer_guard_verdict="n/a"
    adapter_nested_non_bootstrap_signer_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_submit_verify_strict="n/a"
    adapter_nested_submit_verify_guard_verdict="n/a"
    adapter_nested_submit_verify_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_confirmed_execution_sample="n/a"
    adapter_nested_go_nogo_min_confirmed_orders="n/a"
    adapter_nested_confirmed_execution_sample_guard_verdict="n/a"
    adapter_nested_confirmed_execution_sample_guard_reason_code="n/a"
    adapter_nested_go_nogo_require_pretrade_fee_policy="n/a"
    adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
    adapter_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
    adapter_nested_pretrade_fee_policy_guard_verdict="n/a"
    adapter_nested_pretrade_fee_policy_guard_reason_code="n/a"
    adapter_output="final_rollout_package_verdict: SKIP
final_rollout_package_reason: adapter final stage disabled via RUNTIME_READINESS_RUN_ADAPTER_FINAL=false
final_rollout_package_reason_code: stage_disabled
artifacts_written: false
package_bundle_enabled: false"
  fi

  if [[ "$runtime_readiness_run_route_fee_final_norm" == "true" ]]; then
    if route_fee_output="$({
      CONFIG_PATH="$CONFIG_PATH" \
        SERVICE="$SERVICE" \
        OUTPUT_ROOT="$route_fee_output_root" \
        GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
        GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
        GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream_norm" \
        GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
        GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
        GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
        GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
        GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
        GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
        GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY="$go_nogo_require_pretrade_fee_policy_norm" \
        GO_NOGO_MIN_PRETRADE_SOL_RESERVE_LAMPORTS="$go_nogo_min_pretrade_sol_reserve_lamports_norm" \
        GO_NOGO_MAX_PRETRADE_FEE_OVERHEAD_BPS="$go_nogo_max_pretrade_fee_overhead_bps_norm" \
        EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
        GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
        GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
        GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
        ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
        PACKAGE_BUNDLE_ENABLED="false" \
        bash "$ROOT_DIR/tools/execution_route_fee_final_evidence_report.sh" "$ROUTE_FEE_WINDOWS_CSV" "$risk_events_minutes" 2>&1
    } )"; then
      route_fee_exit_code=0
    else
      route_fee_exit_code=$?
    fi

    route_fee_verdict="$(normalize_go_nogo_verdict "$(first_non_empty "$(extract_field "final_route_fee_package_verdict" "$route_fee_output")" "$(extract_field "signoff_verdict" "$route_fee_output")")")"
    route_fee_reason="$(trim_string "$(first_non_empty "$(extract_field "final_route_fee_package_reason" "$route_fee_output")" "$(extract_field "signoff_reason" "$route_fee_output")")")"
    route_fee_reason_code="$(trim_string "$(first_non_empty "$(extract_field "final_route_fee_package_reason_code" "$route_fee_output")" "$(extract_field "signoff_reason_code" "$route_fee_output")")")"
    route_fee_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$route_fee_output")")"
    if ! route_fee_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$route_fee_output")"; then
      input_errors+=("nested route fee final artifacts_written must be boolean token, got: ${route_fee_artifacts_written_raw:-<empty>}")
      route_fee_artifacts_written="unknown"
    elif [[ "$route_fee_artifacts_written" != "true" ]]; then
      input_errors+=("nested route fee final artifacts_written must be true")
    fi
    route_fee_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$route_fee_output")")"
    route_fee_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$route_fee_output")")"
    route_fee_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$route_fee_output")")"
    route_fee_manifest_sha256="$(trim_string "$(extract_field "manifest_sha256" "$route_fee_output")")"

    route_fee_window_count="$(trim_string "$(extract_field "window_count" "$route_fee_output")")"
    route_fee_go_nogo_go_count="$(trim_string "$(extract_field "go_nogo_go_count" "$route_fee_output")")"
    route_fee_route_profile_pass_count="$(trim_string "$(extract_field "route_profile_pass_count" "$route_fee_output")")"
    route_fee_fee_decomposition_pass_count="$(trim_string "$(extract_field "fee_decomposition_pass_count" "$route_fee_output")")"
    route_fee_primary_route_stable="$(trim_string "$(extract_field "primary_route_stable" "$route_fee_output")")"
    route_fee_stable_primary_route="$(trim_string "$(extract_field "stable_primary_route" "$route_fee_output")")"
    route_fee_fallback_route_stable="$(trim_string "$(extract_field "fallback_route_stable" "$route_fee_output")")"
    route_fee_stable_fallback_route="$(trim_string "$(extract_field "stable_fallback_route" "$route_fee_output")")"
    route_fee_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$route_fee_output")")"
    if ! route_fee_final_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$route_fee_output")"; then
      input_errors+=("nested route fee final package_bundle_enabled must be boolean token, got: ${route_fee_nested_package_bundle_enabled_raw:-<empty>}")
      route_fee_final_nested_package_bundle_enabled="unknown"
    elif [[ "$route_fee_final_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("nested route fee final helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
    route_fee_nested_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$route_fee_output")"; then
      input_errors+=("nested route fee final go_nogo_require_executor_upstream must be boolean token, got: ${route_fee_nested_go_nogo_require_executor_upstream_raw:-<empty>}")
      route_fee_nested_go_nogo_require_executor_upstream="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_executor_upstream" != "$go_nogo_require_executor_upstream_norm" ]]; then
      input_errors+=("nested route fee final go_nogo_require_executor_upstream mismatch: nested=${route_fee_nested_go_nogo_require_executor_upstream} expected=${go_nogo_require_executor_upstream_norm}")
    fi
    route_fee_nested_executor_env_path="$(trim_string "$(extract_field "executor_env_path" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_executor_env_path" ]]; then
      input_errors+=("nested route fee final executor_env_path must be non-empty")
      route_fee_nested_executor_env_path="n/a"
    elif [[ "$route_fee_nested_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
      input_errors+=("nested route fee final executor_env_path mismatch: nested=${route_fee_nested_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
    fi
    route_fee_nested_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_executor_backend_mode_guard_verdict" "$route_fee_output")")"
    route_fee_nested_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_executor_backend_mode_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_executor_backend_mode_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_backend_mode_guard_verdict must be non-empty")
      route_fee_nested_executor_backend_mode_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_executor_backend_mode_guard_verdict_raw})")
      route_fee_nested_executor_backend_mode_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_executor_backend_mode_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_executor_backend_mode_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_backend_mode_guard_reason_code must be non-empty")
      route_fee_nested_executor_backend_mode_guard_reason_code="n/a"
    fi
    route_fee_nested_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_executor_upstream_endpoint_guard_verdict" "$route_fee_output")")"
    route_fee_nested_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_upstream_endpoint_guard_verdict must be non-empty")
      route_fee_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_executor_upstream_endpoint_guard_verdict_raw})")
      route_fee_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_executor_upstream_endpoint_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_executor_upstream_endpoint_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_executor_upstream_endpoint_guard_reason_code must be non-empty")
      route_fee_nested_executor_upstream_endpoint_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_jito_rpc_policy" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "signoff_nested_go_nogo_require_jito_rpc_policy" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_jito_rpc_policy must be boolean token, got: ${route_fee_nested_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
      route_fee_nested_go_nogo_require_jito_rpc_policy="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_jito_rpc_policy mismatch: nested=${route_fee_nested_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
    fi
    route_fee_nested_jito_rpc_policy_verdict_raw="$(trim_string "$(extract_field "signoff_nested_jito_rpc_policy_verdict" "$route_fee_output")")"
    route_fee_nested_jito_rpc_policy_verdict_raw_upper="$(printf '%s' "$route_fee_nested_jito_rpc_policy_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_jito_rpc_policy_verdict="$(normalize_gate_verdict "$route_fee_nested_jito_rpc_policy_verdict_raw")"
    if [[ -z "$route_fee_nested_jito_rpc_policy_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_jito_rpc_policy_verdict must be non-empty")
      route_fee_nested_jito_rpc_policy_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_jito_rpc_policy_verdict_raw_upper" != "PASS" && "$route_fee_nested_jito_rpc_policy_verdict_raw_upper" != "WARN" && "$route_fee_nested_jito_rpc_policy_verdict_raw_upper" != "NO_DATA" && "$route_fee_nested_jito_rpc_policy_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_jito_rpc_policy_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${route_fee_nested_jito_rpc_policy_verdict_raw})")
      route_fee_nested_jito_rpc_policy_verdict="UNKNOWN"
    fi
    route_fee_nested_jito_rpc_policy_reason_code="$(trim_string "$(extract_field "signoff_nested_jito_rpc_policy_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_jito_rpc_policy_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_jito_rpc_policy_reason_code must be non-empty")
      route_fee_nested_jito_rpc_policy_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_fastlane_disabled" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "signoff_nested_go_nogo_require_fastlane_disabled" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_fastlane_disabled must be boolean token, got: ${route_fee_nested_go_nogo_require_fastlane_disabled_raw:-<empty>}")
      route_fee_nested_go_nogo_require_fastlane_disabled="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_fastlane_disabled mismatch: nested=${route_fee_nested_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
    fi
    route_fee_nested_fastlane_feature_flag_verdict_raw="$(trim_string "$(extract_field "signoff_nested_fastlane_feature_flag_verdict" "$route_fee_output")")"
    route_fee_nested_fastlane_feature_flag_verdict_raw_upper="$(printf '%s' "$route_fee_nested_fastlane_feature_flag_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_fastlane_feature_flag_verdict="$(normalize_gate_verdict "$route_fee_nested_fastlane_feature_flag_verdict_raw")"
    if [[ -z "$route_fee_nested_fastlane_feature_flag_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_fastlane_feature_flag_verdict must be non-empty")
      route_fee_nested_fastlane_feature_flag_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_fastlane_feature_flag_verdict_raw_upper" != "PASS" && "$route_fee_nested_fastlane_feature_flag_verdict_raw_upper" != "WARN" && "$route_fee_nested_fastlane_feature_flag_verdict_raw_upper" != "NO_DATA" && "$route_fee_nested_fastlane_feature_flag_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_fastlane_feature_flag_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${route_fee_nested_fastlane_feature_flag_verdict_raw})")
      route_fee_nested_fastlane_feature_flag_verdict="UNKNOWN"
    fi
    route_fee_nested_fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "signoff_nested_fastlane_feature_flag_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_fastlane_feature_flag_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_fastlane_feature_flag_reason_code must be non-empty")
      route_fee_nested_fastlane_feature_flag_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_ingestion_grpc" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "signoff_nested_go_nogo_require_ingestion_grpc" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_ingestion_grpc must be boolean token, got: ${route_fee_nested_go_nogo_require_ingestion_grpc_raw:-<empty>}")
      route_fee_nested_go_nogo_require_ingestion_grpc="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_ingestion_grpc mismatch: nested=${route_fee_nested_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
    fi
    route_fee_nested_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_ingestion_grpc_guard_verdict" "$route_fee_output")")"
    route_fee_nested_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_ingestion_grpc_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_ingestion_grpc_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_ingestion_grpc_guard_verdict must be non-empty")
      route_fee_nested_ingestion_grpc_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_ingestion_grpc_guard_verdict_raw})")
      route_fee_nested_ingestion_grpc_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_ingestion_grpc_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_ingestion_grpc_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_ingestion_grpc_guard_reason_code must be non-empty")
      route_fee_nested_ingestion_grpc_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_followlist_activity" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_followlist_activity="$(extract_bool_field_strict "signoff_nested_go_nogo_require_followlist_activity" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_followlist_activity must be boolean token, got: ${route_fee_nested_go_nogo_require_followlist_activity_raw:-<empty>}")
      route_fee_nested_go_nogo_require_followlist_activity="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_followlist_activity mismatch: nested=${route_fee_nested_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
    fi
    route_fee_nested_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_followlist_activity_guard_verdict" "$route_fee_output")")"
    route_fee_nested_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_followlist_activity_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_followlist_activity_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_followlist_activity_guard_verdict must be non-empty")
      route_fee_nested_followlist_activity_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_followlist_activity_guard_verdict_raw})")
      route_fee_nested_followlist_activity_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_followlist_activity_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_followlist_activity_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_followlist_activity_guard_reason_code must be non-empty")
      route_fee_nested_followlist_activity_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_non_bootstrap_signer" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "signoff_nested_go_nogo_require_non_bootstrap_signer" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_non_bootstrap_signer must be boolean token, got: ${route_fee_nested_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
      route_fee_nested_go_nogo_require_non_bootstrap_signer="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_non_bootstrap_signer mismatch: nested=${route_fee_nested_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
    fi
    route_fee_nested_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_non_bootstrap_signer_guard_verdict" "$route_fee_output")")"
    route_fee_nested_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_non_bootstrap_signer_guard_verdict must be non-empty")
      route_fee_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_non_bootstrap_signer_guard_verdict_raw})")
      route_fee_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_non_bootstrap_signer_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_non_bootstrap_signer_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_non_bootstrap_signer_guard_reason_code must be non-empty")
      route_fee_nested_non_bootstrap_signer_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_submit_verify_strict" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "signoff_nested_go_nogo_require_submit_verify_strict" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_submit_verify_strict must be boolean token, got: ${route_fee_nested_go_nogo_require_submit_verify_strict_raw:-<empty>}")
      route_fee_nested_go_nogo_require_submit_verify_strict="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_submit_verify_strict" != "$go_nogo_require_submit_verify_strict_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_submit_verify_strict mismatch: nested=${route_fee_nested_go_nogo_require_submit_verify_strict} expected=${go_nogo_require_submit_verify_strict_norm}")
    fi
    route_fee_nested_submit_verify_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_submit_verify_guard_verdict" "$route_fee_output")")"
    route_fee_nested_submit_verify_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_submit_verify_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_submit_verify_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_submit_verify_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_submit_verify_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_submit_verify_guard_verdict must be non-empty")
      route_fee_nested_submit_verify_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_submit_verify_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_submit_verify_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_submit_verify_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_submit_verify_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_submit_verify_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_submit_verify_guard_verdict_raw})")
      route_fee_nested_submit_verify_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_submit_verify_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_submit_verify_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_submit_verify_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_submit_verify_guard_reason_code must be non-empty")
      route_fee_nested_submit_verify_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_confirmed_execution_sample" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "signoff_nested_go_nogo_require_confirmed_execution_sample" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_confirmed_execution_sample must be boolean token, got: ${route_fee_nested_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
      route_fee_nested_go_nogo_require_confirmed_execution_sample="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_confirmed_execution_sample mismatch: nested=${route_fee_nested_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
    fi
    route_fee_nested_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_min_confirmed_orders" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$route_fee_nested_go_nogo_min_confirmed_orders_raw")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_min_confirmed_orders must be an integer >= 1, got: ${route_fee_nested_go_nogo_min_confirmed_orders_raw:-<empty>}")
      route_fee_nested_go_nogo_min_confirmed_orders="1"
    elif [[ "$route_fee_nested_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_min_confirmed_orders mismatch: nested=${route_fee_nested_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
    fi
    route_fee_nested_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_confirmed_execution_sample_guard_verdict" "$route_fee_output")")"
    route_fee_nested_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_confirmed_execution_sample_guard_verdict must be non-empty")
      route_fee_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_confirmed_execution_sample_guard_verdict_raw})")
      route_fee_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_confirmed_execution_sample_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_confirmed_execution_sample_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_confirmed_execution_sample_guard_reason_code must be non-empty")
      route_fee_nested_confirmed_execution_sample_guard_reason_code="n/a"
    fi
    route_fee_nested_go_nogo_require_pretrade_fee_policy_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_require_pretrade_fee_policy" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_require_pretrade_fee_policy="$(extract_bool_field_strict "signoff_nested_go_nogo_require_pretrade_fee_policy" "$route_fee_output")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_pretrade_fee_policy must be boolean token, got: ${route_fee_nested_go_nogo_require_pretrade_fee_policy_raw:-<empty>}")
      route_fee_nested_go_nogo_require_pretrade_fee_policy="unknown"
    elif [[ "$route_fee_nested_go_nogo_require_pretrade_fee_policy" != "$go_nogo_require_pretrade_fee_policy_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_require_pretrade_fee_policy mismatch: nested=${route_fee_nested_go_nogo_require_pretrade_fee_policy} expected=${go_nogo_require_pretrade_fee_policy_norm}")
    fi
    route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_min_pretrade_sol_reserve_lamports" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports="$(parse_positive_u64_token_strict "$route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_min_pretrade_sol_reserve_lamports must be an integer >= 1, got: ${route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports_raw:-<empty>}")
      route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports="1"
    elif [[ "$route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports" != "$go_nogo_min_pretrade_sol_reserve_lamports_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_min_pretrade_sol_reserve_lamports mismatch: nested=${route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports} expected=${go_nogo_min_pretrade_sol_reserve_lamports_norm}")
    fi
    route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps_raw="$(trim_string "$(extract_field "signoff_nested_go_nogo_max_pretrade_fee_overhead_bps" "$route_fee_output")")"
    if ! route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps="$(parse_positive_u64_token_strict "$route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps_raw")"; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_max_pretrade_fee_overhead_bps must be an integer >= 1, got: ${route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps_raw:-<empty>}")
      route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps="1"
    elif [[ "$route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps" != "$go_nogo_max_pretrade_fee_overhead_bps_norm" ]]; then
      input_errors+=("nested route fee final signoff_nested_go_nogo_max_pretrade_fee_overhead_bps mismatch: nested=${route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps} expected=${go_nogo_max_pretrade_fee_overhead_bps_norm}")
    fi
    route_fee_nested_pretrade_fee_policy_guard_verdict_raw="$(trim_string "$(extract_field "signoff_nested_pretrade_fee_policy_guard_verdict" "$route_fee_output")")"
    route_fee_nested_pretrade_fee_policy_guard_verdict_raw_upper="$(printf '%s' "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    route_fee_nested_pretrade_fee_policy_guard_verdict="$(normalize_strict_guard_verdict "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw")"
    if [[ -z "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw" ]]; then
      input_errors+=("nested route fee final signoff_nested_pretrade_fee_policy_guard_verdict must be non-empty")
      route_fee_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
    elif [[ "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "PASS" && "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "WARN" && "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "UNKNOWN" && "$route_fee_nested_pretrade_fee_policy_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("nested route fee final signoff_nested_pretrade_fee_policy_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${route_fee_nested_pretrade_fee_policy_guard_verdict_raw})")
      route_fee_nested_pretrade_fee_policy_guard_verdict="UNKNOWN"
    fi
    route_fee_nested_pretrade_fee_policy_guard_reason_code="$(trim_string "$(extract_field "signoff_nested_pretrade_fee_policy_guard_reason_code" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_pretrade_fee_policy_guard_reason_code" ]]; then
      input_errors+=("nested route fee final signoff_nested_pretrade_fee_policy_guard_reason_code must be non-empty")
      route_fee_nested_pretrade_fee_policy_guard_reason_code="n/a"
    fi
    route_fee_nested_signoff_guard_window_id="$(trim_string "$(extract_field "signoff_guard_window_id" "$route_fee_output")")"
    if [[ -z "$route_fee_nested_signoff_guard_window_id" ]]; then
      input_errors+=("nested route fee final signoff_guard_window_id must be non-empty")
      route_fee_nested_signoff_guard_window_id="n/a"
    fi
    if [[ "$go_nogo_require_executor_upstream_norm" == "true" ]]; then
      if [[ "$route_fee_nested_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_executor_backend_mode_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
      if [[ "$route_fee_nested_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_executor_upstream_endpoint_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
    else
      if [[ "$route_fee_nested_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_executor_backend_mode_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${route_fee_nested_executor_backend_mode_guard_verdict})")
      fi
      if [[ "$route_fee_nested_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_executor_upstream_endpoint_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${route_fee_nested_executor_upstream_endpoint_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_jito_rpc_policy_norm" == "true" ]]; then
      if [[ "$route_fee_nested_jito_rpc_policy_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_jito_rpc_policy_verdict cannot be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=true")
      fi
    else
      if [[ "$route_fee_nested_jito_rpc_policy_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_jito_rpc_policy_verdict must be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=false (got: ${route_fee_nested_jito_rpc_policy_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_fastlane_disabled_norm" == "true" ]]; then
      if [[ "$route_fee_nested_fastlane_feature_flag_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_fastlane_feature_flag_verdict cannot be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=true")
      fi
    else
      if [[ "$route_fee_nested_fastlane_feature_flag_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_fastlane_feature_flag_verdict must be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=false (got: ${route_fee_nested_fastlane_feature_flag_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
      if [[ "$route_fee_nested_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
      fi
    else
      if [[ "$route_fee_nested_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${route_fee_nested_ingestion_grpc_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
      if [[ "$route_fee_nested_followlist_activity_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
      fi
    else
      if [[ "$route_fee_nested_followlist_activity_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${route_fee_nested_followlist_activity_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
      if [[ "$route_fee_nested_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
      fi
    else
      if [[ "$route_fee_nested_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${route_fee_nested_non_bootstrap_signer_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" ]]; then
      if [[ "$route_fee_nested_submit_verify_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_submit_verify_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
      fi
    else
      if [[ "$route_fee_nested_submit_verify_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_submit_verify_guard_verdict must be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=false (got: ${route_fee_nested_submit_verify_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
      if [[ "$route_fee_nested_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
      fi
    else
      if [[ "$route_fee_nested_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${route_fee_nested_confirmed_execution_sample_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_pretrade_fee_policy_norm" == "true" ]]; then
      if [[ "$route_fee_nested_pretrade_fee_policy_guard_verdict" == "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_pretrade_fee_policy_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=true")
      fi
    else
      if [[ "$route_fee_nested_pretrade_fee_policy_guard_verdict" != "SKIP" ]]; then
        input_errors+=("nested route fee final signoff_nested_pretrade_fee_policy_guard_verdict must be SKIP when GO_NOGO_REQUIRE_PRETRADE_FEE_POLICY=false (got: ${route_fee_nested_pretrade_fee_policy_guard_verdict})")
      fi
    fi

    if [[ "$route_fee_verdict" == "UNKNOWN" ]]; then
      route_fee_reason="unable to classify route/fee final verdict (exit=${route_fee_exit_code})"
      route_fee_reason_code="unknown_verdict"
    elif [[ -z "$route_fee_reason" ]]; then
      route_fee_reason="route/fee final helper reported ${route_fee_verdict}"
      route_fee_reason_code="missing_reason"
    fi
    if [[ -z "$route_fee_reason_code" ]]; then
      route_fee_reason_code="n/a"
    fi
  else
    route_fee_exit_code=0
    route_fee_verdict="SKIP"
    route_fee_reason="route/fee final stage disabled via RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL=false"
    route_fee_reason_code="stage_disabled"
    route_fee_artifacts_written="n/a"
    route_fee_final_nested_package_bundle_enabled="n/a"
    route_fee_nested_go_nogo_require_executor_upstream="n/a"
    route_fee_nested_executor_env_path="n/a"
    route_fee_nested_executor_backend_mode_guard_verdict="n/a"
    route_fee_nested_executor_backend_mode_guard_reason_code="n/a"
    route_fee_nested_executor_upstream_endpoint_guard_verdict="n/a"
    route_fee_nested_executor_upstream_endpoint_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_jito_rpc_policy="n/a"
    route_fee_nested_jito_rpc_policy_verdict="n/a"
    route_fee_nested_jito_rpc_policy_reason_code="n/a"
    route_fee_nested_go_nogo_require_fastlane_disabled="n/a"
    route_fee_nested_fastlane_feature_flag_verdict="n/a"
    route_fee_nested_fastlane_feature_flag_reason_code="n/a"
    route_fee_nested_go_nogo_require_ingestion_grpc="n/a"
    route_fee_nested_ingestion_grpc_guard_verdict="n/a"
    route_fee_nested_ingestion_grpc_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_followlist_activity="n/a"
    route_fee_nested_followlist_activity_guard_verdict="n/a"
    route_fee_nested_followlist_activity_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_non_bootstrap_signer="n/a"
    route_fee_nested_non_bootstrap_signer_guard_verdict="n/a"
    route_fee_nested_non_bootstrap_signer_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_submit_verify_strict="n/a"
    route_fee_nested_submit_verify_guard_verdict="n/a"
    route_fee_nested_submit_verify_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_confirmed_execution_sample="n/a"
    route_fee_nested_go_nogo_min_confirmed_orders="n/a"
    route_fee_nested_confirmed_execution_sample_guard_verdict="n/a"
    route_fee_nested_confirmed_execution_sample_guard_reason_code="n/a"
    route_fee_nested_go_nogo_require_pretrade_fee_policy="n/a"
    route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports="n/a"
    route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps="n/a"
    route_fee_nested_pretrade_fee_policy_guard_verdict="n/a"
    route_fee_nested_pretrade_fee_policy_guard_reason_code="n/a"
    route_fee_nested_signoff_guard_window_id="n/a"
    route_fee_output="final_route_fee_package_verdict: SKIP
final_route_fee_package_reason: route/fee final stage disabled via RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL=false
final_route_fee_package_reason_code: stage_disabled
artifacts_written: false
package_bundle_enabled: false"
  fi
fi

adapter_capture_path="$OUTPUT_ROOT/adapter_rollout_final_captured_${timestamp_compact}.txt"
route_fee_capture_path="$OUTPUT_ROOT/execution_route_fee_final_captured_${timestamp_compact}.txt"
printf '%s\n' "$adapter_output" >"$adapter_capture_path"
printf '%s\n' "$route_fee_output" >"$route_fee_capture_path"

adapter_capture_sha256="$(sha256_file_value "$adapter_capture_path")"
route_fee_capture_sha256="$(sha256_file_value "$route_fee_capture_path")"

adapter_artifact_summary_sha256="n/a"
if [[ -n "$adapter_artifact_summary" && "$adapter_artifact_summary" != "n/a" && -f "$adapter_artifact_summary" ]]; then
  adapter_artifact_summary_sha256="$(sha256_file_value "$adapter_artifact_summary")"
fi
adapter_artifact_manifest_sha256="n/a"
if [[ -n "$adapter_artifact_manifest" && "$adapter_artifact_manifest" != "n/a" && -f "$adapter_artifact_manifest" ]]; then
  adapter_artifact_manifest_sha256="$(sha256_file_value "$adapter_artifact_manifest")"
fi

route_fee_artifact_summary_sha256="n/a"
if [[ -n "$route_fee_artifact_summary" && "$route_fee_artifact_summary" != "n/a" && -f "$route_fee_artifact_summary" ]]; then
  route_fee_artifact_summary_sha256="$(sha256_file_value "$route_fee_artifact_summary")"
fi
route_fee_artifact_manifest_sha256="n/a"
if [[ -n "$route_fee_artifact_manifest" && "$route_fee_artifact_manifest" != "n/a" && -f "$route_fee_artifact_manifest" ]]; then
  route_fee_artifact_manifest_sha256="$(sha256_file_value "$route_fee_artifact_manifest")"
fi

runtime_readiness_verdict="NO_GO"
runtime_readiness_reason="unknown runtime readiness state"
runtime_readiness_reason_code="unknown_state"

if ((${#input_errors[@]} > 0)); then
  runtime_readiness_verdict="NO_GO"
  runtime_readiness_reason="${input_errors[0]}"
  runtime_readiness_reason_code="input_error"
elif [[ "$runtime_readiness_run_adapter_final_norm" == "true" && "$adapter_verdict" == "UNKNOWN" ]]; then
  runtime_readiness_verdict="NO_GO"
  runtime_readiness_reason="adapter final verdict unknown: ${adapter_reason:-n/a}"
  runtime_readiness_reason_code="adapter_unknown_verdict"
elif [[ "$runtime_readiness_run_route_fee_final_norm" == "true" && "$route_fee_verdict" == "UNKNOWN" ]]; then
  runtime_readiness_verdict="NO_GO"
  runtime_readiness_reason="route/fee final verdict unknown: ${route_fee_reason:-n/a}"
  runtime_readiness_reason_code="route_fee_unknown_verdict"
elif [[ "$runtime_readiness_run_adapter_final_norm" == "true" && "$adapter_verdict" == "NO_GO" ]]; then
  runtime_readiness_verdict="NO_GO"
  runtime_readiness_reason="adapter final package is NO_GO: ${adapter_reason:-n/a}"
  runtime_readiness_reason_code="adapter_no_go"
elif [[ "$runtime_readiness_run_route_fee_final_norm" == "true" && "$route_fee_verdict" == "NO_GO" ]]; then
  runtime_readiness_verdict="NO_GO"
  runtime_readiness_reason="route/fee final package is NO_GO: ${route_fee_reason:-n/a}"
  runtime_readiness_reason_code="route_fee_no_go"
elif [[ "$runtime_readiness_run_adapter_final_norm" == "true" && "$adapter_verdict" == "HOLD" ]]; then
  runtime_readiness_verdict="HOLD"
  runtime_readiness_reason="adapter final package is HOLD: ${adapter_reason:-n/a}"
  runtime_readiness_reason_code="adapter_hold"
elif [[ "$runtime_readiness_run_route_fee_final_norm" == "true" && "$route_fee_verdict" == "HOLD" ]]; then
  runtime_readiness_verdict="HOLD"
  runtime_readiness_reason="route/fee final package is HOLD: ${route_fee_reason:-n/a}"
  runtime_readiness_reason_code="route_fee_hold"
else
  runtime_readiness_verdict="GO"
  runtime_readiness_reason="adapter + route/fee final evidence packages passed"
  runtime_readiness_reason_code="gates_pass"
fi

summary_output="=== Execution Runtime Readiness Report ===
utc_now: $timestamp_utc
service: $SERVICE
config: $CONFIG_PATH
adapter_env_path: $ADAPTER_ENV_PATH
route_fee_windows_csv: $ROUTE_FEE_WINDOWS_CSV
adapter_windows_csv: $ADAPTER_WINDOWS_CSV
risk_events_minutes: $risk_events_minutes
output_root: $OUTPUT_ROOT
adapter_output_root: $adapter_output_root
route_fee_output_root: $route_fee_output_root
run_tests: $run_tests_norm
devnet_rehearsal_test_mode: $devnet_rehearsal_test_mode_norm
go_nogo_test_mode: $go_nogo_test_mode_norm
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy_norm
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled_norm
go_nogo_require_executor_upstream: $go_nogo_require_executor_upstream_norm
go_nogo_require_ingestion_grpc: $go_nogo_require_ingestion_grpc_norm
go_nogo_require_followlist_activity: $go_nogo_require_followlist_activity_norm
go_nogo_require_non_bootstrap_signer: $go_nogo_require_non_bootstrap_signer_norm
go_nogo_require_submit_verify_strict: $go_nogo_require_submit_verify_strict_norm
go_nogo_require_confirmed_execution_sample: $go_nogo_require_confirmed_execution_sample_norm
go_nogo_min_confirmed_orders: $go_nogo_min_confirmed_orders_norm
go_nogo_require_pretrade_fee_policy: $go_nogo_require_pretrade_fee_policy_norm
go_nogo_min_pretrade_sol_reserve_lamports: $go_nogo_min_pretrade_sol_reserve_lamports_norm
go_nogo_max_pretrade_fee_overhead_bps: $go_nogo_max_pretrade_fee_overhead_bps_norm
executor_env_path: $EXECUTOR_ENV_PATH
windowed_signoff_required: $windowed_signoff_required_norm
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass_norm
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass_norm
route_fee_signoff_required: $route_fee_signoff_required_norm
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
rehearsal_route_fee_signoff_required: $rehearsal_route_fee_signoff_required_norm
rehearsal_route_fee_signoff_windows_csv: $REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV
route_fee_signoff_test_verdict_override: ${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-n/a}
rehearsal_route_fee_signoff_test_verdict_override: ${REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-n/a}
runtime_readiness_profile: $RUNTIME_READINESS_PROFILE
runtime_readiness_run_adapter_final: $runtime_readiness_run_adapter_final_norm
runtime_readiness_run_route_fee_final: $runtime_readiness_run_route_fee_final_norm
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}
input_error_count: ${#input_errors[@]}
adapter_final_exit_code: $adapter_exit_code
adapter_final_verdict: $adapter_verdict
adapter_final_reason: ${adapter_reason:-n/a}
adapter_final_reason_code: ${adapter_reason_code:-n/a}
adapter_final_artifacts_written: $adapter_artifacts_written
adapter_final_artifact_summary: ${adapter_artifact_summary:-n/a}
adapter_final_artifact_manifest: ${adapter_artifact_manifest:-n/a}
adapter_final_summary_sha256: ${adapter_summary_sha256:-n/a}
adapter_final_manifest_sha256: ${adapter_manifest_sha256:-n/a}
adapter_final_nested_package_bundle_enabled: ${adapter_final_nested_package_bundle_enabled:-n/a}
adapter_final_nested_go_nogo_require_executor_upstream: ${adapter_nested_go_nogo_require_executor_upstream:-n/a}
adapter_final_nested_executor_env_path: ${adapter_nested_executor_env_path:-n/a}
adapter_final_nested_executor_backend_mode_guard_verdict: ${adapter_nested_executor_backend_mode_guard_verdict:-n/a}
adapter_final_nested_executor_backend_mode_guard_reason_code: ${adapter_nested_executor_backend_mode_guard_reason_code:-n/a}
adapter_final_nested_executor_upstream_endpoint_guard_verdict: ${adapter_nested_executor_upstream_endpoint_guard_verdict:-n/a}
adapter_final_nested_executor_upstream_endpoint_guard_reason_code: ${adapter_nested_executor_upstream_endpoint_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_jito_rpc_policy: ${adapter_nested_go_nogo_require_jito_rpc_policy:-n/a}
adapter_final_nested_jito_rpc_policy_verdict: ${adapter_nested_jito_rpc_policy_verdict:-n/a}
adapter_final_nested_jito_rpc_policy_reason_code: ${adapter_nested_jito_rpc_policy_reason_code:-n/a}
adapter_final_nested_go_nogo_require_fastlane_disabled: ${adapter_nested_go_nogo_require_fastlane_disabled:-n/a}
adapter_final_nested_fastlane_feature_flag_verdict: ${adapter_nested_fastlane_feature_flag_verdict:-n/a}
adapter_final_nested_fastlane_feature_flag_reason_code: ${adapter_nested_fastlane_feature_flag_reason_code:-n/a}
adapter_final_nested_go_nogo_require_ingestion_grpc: ${adapter_nested_go_nogo_require_ingestion_grpc:-n/a}
adapter_final_nested_ingestion_grpc_guard_verdict: ${adapter_nested_ingestion_grpc_guard_verdict:-n/a}
adapter_final_nested_ingestion_grpc_guard_reason_code: ${adapter_nested_ingestion_grpc_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_followlist_activity: ${adapter_nested_go_nogo_require_followlist_activity:-n/a}
adapter_final_nested_followlist_activity_guard_verdict: ${adapter_nested_followlist_activity_guard_verdict:-n/a}
adapter_final_nested_followlist_activity_guard_reason_code: ${adapter_nested_followlist_activity_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_non_bootstrap_signer: ${adapter_nested_go_nogo_require_non_bootstrap_signer:-n/a}
adapter_final_nested_non_bootstrap_signer_guard_verdict: ${adapter_nested_non_bootstrap_signer_guard_verdict:-n/a}
adapter_final_nested_non_bootstrap_signer_guard_reason_code: ${adapter_nested_non_bootstrap_signer_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_submit_verify_strict: ${adapter_nested_go_nogo_require_submit_verify_strict:-n/a}
adapter_final_nested_submit_verify_guard_verdict: ${adapter_nested_submit_verify_guard_verdict:-n/a}
adapter_final_nested_submit_verify_guard_reason_code: ${adapter_nested_submit_verify_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_confirmed_execution_sample: ${adapter_nested_go_nogo_require_confirmed_execution_sample:-n/a}
adapter_final_nested_go_nogo_min_confirmed_orders: ${adapter_nested_go_nogo_min_confirmed_orders:-n/a}
adapter_final_nested_confirmed_execution_sample_guard_verdict: ${adapter_nested_confirmed_execution_sample_guard_verdict:-n/a}
adapter_final_nested_confirmed_execution_sample_guard_reason_code: ${adapter_nested_confirmed_execution_sample_guard_reason_code:-n/a}
adapter_final_nested_go_nogo_require_pretrade_fee_policy: ${adapter_nested_go_nogo_require_pretrade_fee_policy:-n/a}
adapter_final_nested_go_nogo_min_pretrade_sol_reserve_lamports: ${adapter_nested_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
adapter_final_nested_go_nogo_max_pretrade_fee_overhead_bps: ${adapter_nested_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
adapter_final_nested_pretrade_fee_policy_guard_verdict: ${adapter_nested_pretrade_fee_policy_guard_verdict:-n/a}
adapter_final_nested_pretrade_fee_policy_guard_reason_code: ${adapter_nested_pretrade_fee_policy_guard_reason_code:-n/a}
route_fee_final_exit_code: $route_fee_exit_code
route_fee_final_verdict: $route_fee_verdict
route_fee_final_reason: ${route_fee_reason:-n/a}
route_fee_final_reason_code: ${route_fee_reason_code:-n/a}
route_fee_final_artifacts_written: $route_fee_artifacts_written
route_fee_final_artifact_summary: ${route_fee_artifact_summary:-n/a}
route_fee_final_artifact_manifest: ${route_fee_artifact_manifest:-n/a}
route_fee_final_summary_sha256: ${route_fee_summary_sha256:-n/a}
route_fee_final_manifest_sha256: ${route_fee_manifest_sha256:-n/a}
route_fee_final_nested_package_bundle_enabled: ${route_fee_final_nested_package_bundle_enabled:-n/a}
route_fee_final_nested_go_nogo_require_executor_upstream: ${route_fee_nested_go_nogo_require_executor_upstream:-n/a}
route_fee_final_nested_executor_env_path: ${route_fee_nested_executor_env_path:-n/a}
route_fee_final_nested_executor_backend_mode_guard_verdict: ${route_fee_nested_executor_backend_mode_guard_verdict:-n/a}
route_fee_final_nested_executor_backend_mode_guard_reason_code: ${route_fee_nested_executor_backend_mode_guard_reason_code:-n/a}
route_fee_final_nested_executor_upstream_endpoint_guard_verdict: ${route_fee_nested_executor_upstream_endpoint_guard_verdict:-n/a}
route_fee_final_nested_executor_upstream_endpoint_guard_reason_code: ${route_fee_nested_executor_upstream_endpoint_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_jito_rpc_policy: ${route_fee_nested_go_nogo_require_jito_rpc_policy:-n/a}
route_fee_final_nested_jito_rpc_policy_verdict: ${route_fee_nested_jito_rpc_policy_verdict:-n/a}
route_fee_final_nested_jito_rpc_policy_reason_code: ${route_fee_nested_jito_rpc_policy_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_fastlane_disabled: ${route_fee_nested_go_nogo_require_fastlane_disabled:-n/a}
route_fee_final_nested_fastlane_feature_flag_verdict: ${route_fee_nested_fastlane_feature_flag_verdict:-n/a}
route_fee_final_nested_fastlane_feature_flag_reason_code: ${route_fee_nested_fastlane_feature_flag_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_ingestion_grpc: ${route_fee_nested_go_nogo_require_ingestion_grpc:-n/a}
route_fee_final_nested_ingestion_grpc_guard_verdict: ${route_fee_nested_ingestion_grpc_guard_verdict:-n/a}
route_fee_final_nested_ingestion_grpc_guard_reason_code: ${route_fee_nested_ingestion_grpc_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_followlist_activity: ${route_fee_nested_go_nogo_require_followlist_activity:-n/a}
route_fee_final_nested_followlist_activity_guard_verdict: ${route_fee_nested_followlist_activity_guard_verdict:-n/a}
route_fee_final_nested_followlist_activity_guard_reason_code: ${route_fee_nested_followlist_activity_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_non_bootstrap_signer: ${route_fee_nested_go_nogo_require_non_bootstrap_signer:-n/a}
route_fee_final_nested_non_bootstrap_signer_guard_verdict: ${route_fee_nested_non_bootstrap_signer_guard_verdict:-n/a}
route_fee_final_nested_non_bootstrap_signer_guard_reason_code: ${route_fee_nested_non_bootstrap_signer_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_submit_verify_strict: ${route_fee_nested_go_nogo_require_submit_verify_strict:-n/a}
route_fee_final_nested_submit_verify_guard_verdict: ${route_fee_nested_submit_verify_guard_verdict:-n/a}
route_fee_final_nested_submit_verify_guard_reason_code: ${route_fee_nested_submit_verify_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_confirmed_execution_sample: ${route_fee_nested_go_nogo_require_confirmed_execution_sample:-n/a}
route_fee_final_nested_go_nogo_min_confirmed_orders: ${route_fee_nested_go_nogo_min_confirmed_orders:-n/a}
route_fee_final_nested_confirmed_execution_sample_guard_verdict: ${route_fee_nested_confirmed_execution_sample_guard_verdict:-n/a}
route_fee_final_nested_confirmed_execution_sample_guard_reason_code: ${route_fee_nested_confirmed_execution_sample_guard_reason_code:-n/a}
route_fee_final_nested_go_nogo_require_pretrade_fee_policy: ${route_fee_nested_go_nogo_require_pretrade_fee_policy:-n/a}
route_fee_final_nested_go_nogo_min_pretrade_sol_reserve_lamports: ${route_fee_nested_go_nogo_min_pretrade_sol_reserve_lamports:-n/a}
route_fee_final_nested_go_nogo_max_pretrade_fee_overhead_bps: ${route_fee_nested_go_nogo_max_pretrade_fee_overhead_bps:-n/a}
route_fee_final_nested_pretrade_fee_policy_guard_verdict: ${route_fee_nested_pretrade_fee_policy_guard_verdict:-n/a}
route_fee_final_nested_pretrade_fee_policy_guard_reason_code: ${route_fee_nested_pretrade_fee_policy_guard_reason_code:-n/a}
route_fee_final_nested_signoff_guard_window_id: ${route_fee_nested_signoff_guard_window_id:-n/a}
route_fee_window_count: ${route_fee_window_count:-n/a}
route_fee_go_nogo_go_count: ${route_fee_go_nogo_go_count:-n/a}
route_fee_route_profile_pass_count: ${route_fee_route_profile_pass_count:-n/a}
route_fee_fee_decomposition_pass_count: ${route_fee_fee_decomposition_pass_count:-n/a}
route_fee_primary_route_stable: ${route_fee_primary_route_stable:-n/a}
route_fee_stable_primary_route: ${route_fee_stable_primary_route:-n/a}
route_fee_fallback_route_stable: ${route_fee_fallback_route_stable:-n/a}
route_fee_stable_fallback_route: ${route_fee_stable_fallback_route:-n/a}
runtime_readiness_verdict: $runtime_readiness_verdict
runtime_readiness_reason: ${runtime_readiness_reason:-n/a}
runtime_readiness_reason_code: ${runtime_readiness_reason_code:-n/a}
final_runtime_package_verdict: $runtime_readiness_verdict
final_runtime_package_reason: ${runtime_readiness_reason:-n/a}
final_runtime_package_reason_code: ${runtime_readiness_reason_code:-n/a}
artifacts_written: true"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

summary_path="$OUTPUT_ROOT/execution_runtime_readiness_summary_${timestamp_compact}.txt"
manifest_path="$OUTPUT_ROOT/execution_runtime_readiness_manifest_${timestamp_compact}.txt"
printf '%s\n' "$summary_output" >"$summary_path"

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
  if package_bundle_output="$({
    OUTPUT_DIR="$PACKAGE_BUNDLE_OUTPUT_DIR" \
      BUNDLE_LABEL="$PACKAGE_BUNDLE_LABEL" \
      bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$OUTPUT_ROOT" 2>&1
  } )"; then
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
adapter_capture_sha256: $adapter_capture_sha256
route_fee_capture_sha256: $route_fee_capture_sha256
adapter_final_summary_sha256: ${adapter_summary_sha256:-n/a}
adapter_final_manifest_sha256: ${adapter_manifest_sha256:-n/a}
route_fee_final_summary_sha256: ${route_fee_summary_sha256:-n/a}
route_fee_final_manifest_sha256: ${route_fee_manifest_sha256:-n/a}
adapter_artifact_summary_sha256: $adapter_artifact_summary_sha256
adapter_artifact_manifest_sha256: $adapter_artifact_manifest_sha256
route_fee_artifact_summary_sha256: $route_fee_artifact_summary_sha256
route_fee_artifact_manifest_sha256: $route_fee_artifact_manifest_sha256
EOF_MANIFEST
manifest_sha256="$(sha256_file_value "$manifest_path")"

if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
  run_package_bundle_once
fi

echo
echo "artifacts_written: true"
echo "artifact_summary: $summary_path"
echo "artifact_adapter_capture: $adapter_capture_path"
echo "artifact_route_fee_capture: $route_fee_capture_path"
echo "artifact_manifest: $manifest_path"
echo "summary_sha256: $summary_sha256"
echo "adapter_capture_sha256: $adapter_capture_sha256"
echo "route_fee_capture_sha256: $route_fee_capture_sha256"
echo "manifest_sha256: $manifest_sha256"
echo "package_bundle_artifacts_written: $package_bundle_artifacts_written"
echo "package_bundle_exit_code: $package_bundle_exit_code"
echo "package_bundle_error: $package_bundle_error"
echo "package_bundle_path: $package_bundle_path"
echo "package_bundle_sha256: $package_bundle_sha256"
echo "package_bundle_sha256_path: $package_bundle_sha256_path"
echo "package_bundle_contents_manifest: $package_bundle_contents_manifest"
echo "package_bundle_file_count: $package_bundle_file_count"

if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" != "true" ]]; then
  exit 3
fi

case "$runtime_readiness_verdict" in
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
