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
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
SERVICE="${SERVICE:-solana-copy-bot}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
RUN_TESTS="${RUN_TESTS:-true}"
DEVNET_REHEARSAL_TEST_MODE="${DEVNET_REHEARSAL_TEST_MODE:-false}"
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}"
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}"
WINDOWED_SIGNOFF_WINDOWS_CSV="${WINDOWED_SIGNOFF_WINDOWS_CSV:-1,6,24}"
WINDOWED_SIGNOFF_REQUIRED="${WINDOWED_SIGNOFF_REQUIRED:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-false}"
GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-false}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-false}"
GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="${GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM:-true}"
GO_NOGO_REQUIRE_INGESTION_GRPC="${GO_NOGO_REQUIRE_INGESTION_GRPC:-false}"
GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="${GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY:-false}"
GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="${GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER:-false}"
GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="${GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT:-false}"
GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="${GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE:-false}"
GO_NOGO_MIN_CONFIRMED_ORDERS="${GO_NOGO_MIN_CONFIRMED_ORDERS:-1}"
ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-false}"
ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-executor_rollout_evidence}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_DIR}"
EXECUTOR_ROLLOUT_PROFILE="${EXECUTOR_ROLLOUT_PROFILE:-full}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

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

parse_rollout_bool_setting_into() {
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

parse_rollout_positive_u64_setting_into() {
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

executor_rollout_run_rotation_default="true"
executor_rollout_run_preflight_default="true"
executor_rollout_run_rehearsal_default="true"
case "$EXECUTOR_ROLLOUT_PROFILE" in
full)
  ;;
precheck_only)
  executor_rollout_run_rehearsal_default="false"
  ;;
rehearsal_only)
  executor_rollout_run_rotation_default="false"
  executor_rollout_run_preflight_default="false"
  ;;
*)
  input_errors+=("EXECUTOR_ROLLOUT_PROFILE must be one of: full,precheck_only,rehearsal_only (got: $EXECUTOR_ROLLOUT_PROFILE)")
  ;;
esac

executor_rollout_run_rotation_raw="$executor_rollout_run_rotation_default"
if [[ -n "${EXECUTOR_ROLLOUT_RUN_ROTATION+x}" ]]; then
  executor_rollout_run_rotation_raw="$EXECUTOR_ROLLOUT_RUN_ROTATION"
fi
executor_rollout_run_preflight_raw="$executor_rollout_run_preflight_default"
if [[ -n "${EXECUTOR_ROLLOUT_RUN_PREFLIGHT+x}" ]]; then
  executor_rollout_run_preflight_raw="$EXECUTOR_ROLLOUT_RUN_PREFLIGHT"
fi
executor_rollout_run_rehearsal_raw="$executor_rollout_run_rehearsal_default"
if [[ -n "${EXECUTOR_ROLLOUT_RUN_REHEARSAL+x}" ]]; then
  executor_rollout_run_rehearsal_raw="$EXECUTOR_ROLLOUT_RUN_REHEARSAL"
fi

parse_rollout_bool_setting_into "EXECUTOR_ROLLOUT_RUN_ROTATION" "$executor_rollout_run_rotation_raw" executor_rollout_run_rotation_norm
parse_rollout_bool_setting_into "EXECUTOR_ROLLOUT_RUN_PREFLIGHT" "$executor_rollout_run_preflight_raw" executor_rollout_run_preflight_norm
parse_rollout_bool_setting_into "EXECUTOR_ROLLOUT_RUN_REHEARSAL" "$executor_rollout_run_rehearsal_raw" executor_rollout_run_rehearsal_norm
parse_rollout_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_rollout_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_rollout_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" "$GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" go_nogo_require_executor_upstream_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_INGESTION_GRPC" "$GO_NOGO_REQUIRE_INGESTION_GRPC" go_nogo_require_ingestion_grpc_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" "$GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" go_nogo_require_followlist_activity_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" "$GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" go_nogo_require_non_bootstrap_signer_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" "$GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" go_nogo_require_submit_verify_strict_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" "$GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" go_nogo_require_confirmed_execution_sample_norm
parse_rollout_positive_u64_setting_into "GO_NOGO_MIN_CONFIRMED_ORDERS" "$GO_NOGO_MIN_CONFIRMED_ORDERS" go_nogo_min_confirmed_orders_norm
parse_rollout_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_rollout_bool_setting_into "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" route_fee_signoff_go_nogo_test_mode_norm
parse_rollout_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm
if [[ "$executor_rollout_run_rotation_norm" != "true" && "$executor_rollout_run_preflight_norm" != "true" && "$executor_rollout_run_rehearsal_norm" != "true" ]]; then
  input_errors+=("at least one stage must be enabled (set EXECUTOR_ROLLOUT_RUN_ROTATION/EXECUTOR_ROLLOUT_RUN_PREFLIGHT/EXECUTOR_ROLLOUT_RUN_REHEARSAL)")
fi
if [[ "$windowed_signoff_required_norm" == "true" && "$executor_rollout_run_rehearsal_norm" != "true" ]]; then
  input_errors+=("WINDOWED_SIGNOFF_REQUIRED=true requires EXECUTOR_ROLLOUT_RUN_REHEARSAL=true")
fi
if [[ "$route_fee_signoff_required_norm" == "true" && "$executor_rollout_run_rehearsal_norm" != "true" ]]; then
  input_errors+=("ROUTE_FEE_SIGNOFF_REQUIRED=true requires EXECUTOR_ROLLOUT_RUN_REHEARSAL=true")
fi
if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" && "$executor_rollout_run_preflight_norm" != "true" ]]; then
  input_errors+=("GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true requires EXECUTOR_ROLLOUT_RUN_PREFLIGHT=true")
fi
if [[ "$package_bundle_enabled_norm" == "true" && -z "$OUTPUT_DIR" ]]; then
  input_errors+=("PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set")
fi

rotation_output_dir=""
preflight_output_dir=""
rehearsal_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  rotation_output_dir="$OUTPUT_DIR/rotation"
  preflight_output_dir="$OUTPUT_DIR/preflight"
  rehearsal_output_dir="$OUTPUT_DIR/rehearsal"
  mkdir -p "$rotation_output_dir" "$preflight_output_dir" "$rehearsal_output_dir"
fi

rotation_output=""
rotation_exit_code=3
rotation_verdict="UNKNOWN"
rotation_reason="executor signer rotation helper not executed"
rotation_artifact_report=""
rotation_artifact_manifest=""
rotation_report_sha256=""
rotation_artifacts_written="false"
if [[ "$executor_rollout_run_rotation_norm" != "true" ]]; then
  rotation_output=$'rotation_readiness_verdict: SKIP\nrotation_readiness_reason: stage_disabled\nartifacts_written: false'
  rotation_exit_code=0
  rotation_verdict="SKIP"
  rotation_reason="rotation stage disabled by EXECUTOR_ROLLOUT_RUN_ROTATION=false"
  rotation_artifacts_written="n/a"
elif ((${#input_errors[@]} == 0)) && [[ -f "$EXECUTOR_ENV_PATH" ]]; then
  if rotation_output="$(
    EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      OUTPUT_DIR="$rotation_output_dir" \
      bash "$ROOT_DIR/tools/executor_signer_rotation_report.sh" 2>&1
  )"; then
    rotation_exit_code=0
  else
    rotation_exit_code=$?
  fi

  rotation_verdict="$(normalize_rotation_verdict "$(extract_field "rotation_readiness_verdict" "$rotation_output")")"
  rotation_reason="$(trim_string "$(extract_field "rotation_readiness_reason" "$rotation_output")")"
  rotation_artifact_report="$(trim_string "$(extract_field "artifact_report" "$rotation_output")")"
  rotation_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rotation_output")")"
  rotation_report_sha256="$(trim_string "$(extract_field "report_sha256" "$rotation_output")")"
  rotation_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rotation_output")")"
  if ! rotation_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rotation_output")"; then
    input_errors+=("rotation helper artifacts_written must be boolean token, got: ${rotation_artifacts_written_raw:-<empty>}")
    rotation_artifacts_written="unknown"
  elif [[ -n "$rotation_output_dir" && "$rotation_artifacts_written" != "true" ]]; then
    input_errors+=("rotation helper artifacts_written must be true")
  fi
  if [[ "$rotation_verdict" == "UNKNOWN" ]]; then
    rotation_reason="unable to classify rotation helper verdict (exit=$rotation_exit_code)"
  elif [[ -z "$rotation_reason" ]]; then
    rotation_reason="rotation helper reported $rotation_verdict"
  fi
elif ((${#input_errors[@]} == 0)); then
  rotation_exit_code=1
  rotation_verdict="FAIL"
  rotation_reason="executor env file not found: $EXECUTOR_ENV_PATH"
fi

preflight_output=""
preflight_exit_code=3
preflight_verdict="UNKNOWN"
preflight_reason="executor preflight helper not executed"
preflight_reason_code="not_executed"
health_status=""
auth_probe_without_auth_code=""
auth_probe_with_auth_http_status=""
auth_probe_with_auth_code=""
preflight_artifact_summary=""
preflight_artifact_manifest=""
preflight_summary_sha256=""
preflight_artifacts_written="false"
preflight_executor_submit_verify_strict="n/a"
preflight_executor_submit_verify_configured="n/a"
preflight_executor_submit_verify_fallback_configured="n/a"
if [[ "$executor_rollout_run_preflight_norm" != "true" ]]; then
  preflight_output=$'preflight_verdict: SKIP\npreflight_reason: stage_disabled\npreflight_reason_code: stage_disabled\nartifacts_written: false'
  preflight_exit_code=0
  preflight_verdict="SKIP"
  preflight_reason="executor preflight stage disabled by EXECUTOR_ROLLOUT_RUN_PREFLIGHT=false"
  preflight_reason_code="stage_disabled"
  preflight_artifacts_written="n/a"
elif ((${#input_errors[@]} == 0)) && [[ -f "$CONFIG_PATH" && -f "$EXECUTOR_ENV_PATH" && -f "$ADAPTER_ENV_PATH" ]]; then
  if preflight_output="$(
    CONFIG_PATH="$CONFIG_PATH" \
      EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
      OUTPUT_DIR="$preflight_output_dir" \
      HTTP_TIMEOUT_SEC="${HTTP_TIMEOUT_SEC:-5}" \
      bash "$ROOT_DIR/tools/executor_preflight.sh" 2>&1
  )"; then
    preflight_exit_code=0
  else
    preflight_exit_code=$?
  fi

  preflight_verdict="$(normalize_preflight_verdict "$(extract_field "preflight_verdict" "$preflight_output")")"
  preflight_reason="$(trim_string "$(extract_field "preflight_reason" "$preflight_output")")"
  preflight_reason_code="$(trim_string "$(extract_field "preflight_reason_code" "$preflight_output")")"
  health_status="$(trim_string "$(extract_field "health_status" "$preflight_output")")"
  auth_probe_without_auth_code="$(trim_string "$(extract_field "auth_probe_without_auth_code" "$preflight_output")")"
  auth_probe_with_auth_http_status="$(trim_string "$(extract_field "auth_probe_with_auth_http_status" "$preflight_output")")"
  auth_probe_with_auth_code="$(trim_string "$(extract_field "auth_probe_with_auth_code" "$preflight_output")")"
  preflight_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$preflight_output")")"
  preflight_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$preflight_output")")"
  preflight_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$preflight_output")")"
  preflight_executor_submit_verify_strict_raw="$(trim_string "$(extract_field "executor_submit_verify_strict" "$preflight_output")")"
  if ! preflight_executor_submit_verify_strict="$(extract_bool_field_strict "executor_submit_verify_strict" "$preflight_output")"; then
    input_errors+=("preflight helper executor_submit_verify_strict must be boolean token, got: ${preflight_executor_submit_verify_strict_raw:-<empty>}")
    preflight_executor_submit_verify_strict="unknown"
  fi
  preflight_executor_submit_verify_configured_raw="$(trim_string "$(extract_field "executor_submit_verify_configured" "$preflight_output")")"
  if ! preflight_executor_submit_verify_configured="$(extract_bool_field_strict "executor_submit_verify_configured" "$preflight_output")"; then
    input_errors+=("preflight helper executor_submit_verify_configured must be boolean token, got: ${preflight_executor_submit_verify_configured_raw:-<empty>}")
    preflight_executor_submit_verify_configured="unknown"
  fi
  preflight_executor_submit_verify_fallback_configured_raw="$(trim_string "$(extract_field "executor_submit_verify_fallback_configured" "$preflight_output")")"
  if ! preflight_executor_submit_verify_fallback_configured="$(extract_bool_field_strict "executor_submit_verify_fallback_configured" "$preflight_output")"; then
    input_errors+=("preflight helper executor_submit_verify_fallback_configured must be boolean token, got: ${preflight_executor_submit_verify_fallback_configured_raw:-<empty>}")
    preflight_executor_submit_verify_fallback_configured="unknown"
  fi
  if [[ "$preflight_executor_submit_verify_strict" == "true" && "$preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("preflight helper executor_submit_verify_strict=true requires executor_submit_verify_configured=true")
  fi
  if [[ "$preflight_executor_submit_verify_fallback_configured" == "true" && "$preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("preflight helper executor_submit_verify_fallback_configured=true requires executor_submit_verify_configured=true")
  fi
  if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" && "$preflight_executor_submit_verify_strict" != "true" ]]; then
    input_errors+=("GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true requires preflight_executor_submit_verify_strict=true")
  fi
  if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" && "$preflight_executor_submit_verify_configured" != "true" ]]; then
    input_errors+=("GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true requires preflight_executor_submit_verify_configured=true")
  fi
  preflight_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$preflight_output")")"
  if ! preflight_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$preflight_output")"; then
    input_errors+=("preflight helper artifacts_written must be boolean token, got: ${preflight_artifacts_written_raw:-<empty>}")
    preflight_artifacts_written="unknown"
  elif [[ -n "$preflight_output_dir" && "$preflight_artifacts_written" != "true" ]]; then
    input_errors+=("preflight helper artifacts_written must be true")
  fi
  if [[ "$preflight_verdict" == "UNKNOWN" ]]; then
    preflight_reason="unable to classify preflight helper verdict (exit=$preflight_exit_code)"
    preflight_reason_code="unknown_verdict"
  elif [[ -z "$preflight_reason" ]]; then
    preflight_reason="preflight helper reported $preflight_verdict"
    preflight_reason_code="missing_reason"
  elif [[ -z "$preflight_reason_code" ]]; then
    preflight_reason_code="missing_reason"
  fi
fi

rehearsal_output=""
rehearsal_exit_code=3
rehearsal_verdict="UNKNOWN"
rehearsal_reason="execution devnet rehearsal helper not executed"
rehearsal_reason_code="not_executed"
rehearsal_preflight_verdict=""
overall_go_nogo_verdict=""
overall_go_nogo_reason_code=""
windowed_signoff_verdict=""
windowed_signoff_reason=""
route_fee_signoff_verdict=""
route_fee_signoff_reason=""
route_fee_signoff_reason_code=""
rehearsal_artifact_manifest=""
rehearsal_summary_sha256=""
rehearsal_preflight_sha256=""
rehearsal_go_nogo_sha256=""
rehearsal_tests_sha256=""
rehearsal_artifacts_written="false"
rehearsal_nested_package_bundle_enabled="unknown"
rehearsal_nested_go_nogo_require_jito_rpc_policy="n/a"
rehearsal_nested_go_nogo_require_executor_upstream="n/a"
rehearsal_nested_go_nogo_require_ingestion_grpc="n/a"
rehearsal_nested_go_nogo_require_followlist_activity="n/a"
rehearsal_nested_go_nogo_require_non_bootstrap_signer="n/a"
rehearsal_nested_go_nogo_require_confirmed_execution_sample="n/a"
rehearsal_nested_go_nogo_min_confirmed_orders="n/a"
rehearsal_nested_jito_rpc_policy_verdict="unknown"
rehearsal_nested_jito_rpc_policy_reason_code="n/a"
rehearsal_nested_go_nogo_require_fastlane_disabled="n/a"
rehearsal_nested_fastlane_feature_flag_verdict="unknown"
rehearsal_nested_fastlane_feature_flag_reason_code="n/a"
rehearsal_nested_executor_env_path="n/a"
rehearsal_nested_executor_backend_mode_guard_verdict="unknown"
rehearsal_nested_executor_backend_mode_guard_reason_code="n/a"
rehearsal_nested_executor_upstream_endpoint_guard_verdict="unknown"
rehearsal_nested_executor_upstream_endpoint_guard_reason_code="n/a"
rehearsal_nested_ingestion_grpc_guard_verdict="unknown"
rehearsal_nested_ingestion_grpc_guard_reason_code="n/a"
rehearsal_nested_followlist_activity_guard_verdict="unknown"
rehearsal_nested_followlist_activity_guard_reason_code="n/a"
rehearsal_nested_non_bootstrap_signer_guard_verdict="unknown"
rehearsal_nested_non_bootstrap_signer_guard_reason_code="n/a"
rehearsal_nested_confirmed_execution_sample_guard_verdict="unknown"
rehearsal_nested_confirmed_execution_sample_guard_reason_code="n/a"
tests_run=""
tests_failed=""
if [[ "$executor_rollout_run_rehearsal_norm" != "true" ]]; then
  rehearsal_output=$'devnet_rehearsal_verdict: SKIP\ndevnet_rehearsal_reason: stage_disabled\ndevnet_rehearsal_reason_code: stage_disabled\nartifacts_written: false\npackage_bundle_enabled: false'
  rehearsal_exit_code=0
  rehearsal_verdict="SKIP"
  rehearsal_reason="devnet rehearsal stage disabled by EXECUTOR_ROLLOUT_RUN_REHEARSAL=false"
  rehearsal_reason_code="stage_disabled"
  rehearsal_artifacts_written="n/a"
  rehearsal_nested_package_bundle_enabled="n/a"
  rehearsal_nested_go_nogo_require_jito_rpc_policy="n/a"
  rehearsal_nested_go_nogo_require_executor_upstream="n/a"
  rehearsal_nested_go_nogo_require_ingestion_grpc="n/a"
  rehearsal_nested_go_nogo_require_followlist_activity="n/a"
  rehearsal_nested_go_nogo_require_non_bootstrap_signer="n/a"
  rehearsal_nested_go_nogo_require_confirmed_execution_sample="n/a"
  rehearsal_nested_go_nogo_min_confirmed_orders="n/a"
  rehearsal_nested_jito_rpc_policy_verdict="n/a"
  rehearsal_nested_jito_rpc_policy_reason_code="n/a"
  rehearsal_nested_go_nogo_require_fastlane_disabled="n/a"
  rehearsal_nested_fastlane_feature_flag_verdict="n/a"
  rehearsal_nested_fastlane_feature_flag_reason_code="n/a"
  rehearsal_nested_executor_env_path="n/a"
  rehearsal_nested_executor_backend_mode_guard_verdict="n/a"
  rehearsal_nested_executor_backend_mode_guard_reason_code="n/a"
  rehearsal_nested_executor_upstream_endpoint_guard_verdict="n/a"
  rehearsal_nested_executor_upstream_endpoint_guard_reason_code="n/a"
  rehearsal_nested_ingestion_grpc_guard_verdict="n/a"
  rehearsal_nested_ingestion_grpc_guard_reason_code="n/a"
  rehearsal_nested_followlist_activity_guard_verdict="n/a"
  rehearsal_nested_followlist_activity_guard_reason_code="n/a"
  rehearsal_nested_non_bootstrap_signer_guard_verdict="n/a"
  rehearsal_nested_non_bootstrap_signer_guard_reason_code="n/a"
  rehearsal_nested_confirmed_execution_sample_guard_verdict="n/a"
  rehearsal_nested_confirmed_execution_sample_guard_reason_code="n/a"
elif ((${#input_errors[@]} > 0)); then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="${input_errors[0]}"
  rehearsal_reason_code="input_error"
elif [[ ! "$WINDOW_HOURS" =~ ^[0-9]+$ || ! "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="invalid rehearsal window arguments"
  rehearsal_reason_code="input_error"
elif [[ ! -f "$CONFIG_PATH" ]]; then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="config file not found: $CONFIG_PATH"
  rehearsal_reason_code="input_error"
else
  if rehearsal_output="$(
    PATH="$PATH" \
      DB_PATH="${DB_PATH:-}" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_DIR="$rehearsal_output_dir" \
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
      GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
      GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
      WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
      WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
      ROUTE_FEE_SIGNOFF_REQUIRED="$route_fee_signoff_required_norm" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode_norm" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
      EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
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
  rehearsal_preflight_verdict="$(trim_string "$(extract_field "preflight_verdict" "$rehearsal_output")")"
  overall_go_nogo_verdict="$(trim_string "$(extract_field "overall_go_nogo_verdict" "$rehearsal_output")")"
  overall_go_nogo_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$rehearsal_output")")"
  windowed_signoff_verdict="$(trim_string "$(extract_field "windowed_signoff_verdict" "$rehearsal_output")")"
  windowed_signoff_reason="$(trim_string "$(extract_field "windowed_signoff_reason" "$rehearsal_output")")"
  route_fee_signoff_verdict="$(trim_string "$(extract_field "route_fee_signoff_verdict" "$rehearsal_output")")"
  route_fee_signoff_reason="$(trim_string "$(extract_field "route_fee_signoff_reason" "$rehearsal_output")")"
  route_fee_signoff_reason_code="$(trim_string "$(extract_field "route_fee_signoff_reason_code" "$rehearsal_output")")"
  rehearsal_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rehearsal_output")")"
  rehearsal_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$rehearsal_output")")"
  rehearsal_preflight_sha256="$(trim_string "$(extract_field "preflight_sha256" "$rehearsal_output")")"
  rehearsal_go_nogo_sha256="$(trim_string "$(extract_field "go_nogo_sha256" "$rehearsal_output")")"
  rehearsal_tests_sha256="$(trim_string "$(extract_field "tests_sha256" "$rehearsal_output")")"
  rehearsal_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rehearsal_output")")"
  if ! rehearsal_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal artifacts_written must be boolean token, got: ${rehearsal_artifacts_written_raw:-<empty>}")
    rehearsal_artifacts_written="unknown"
  elif [[ -n "$rehearsal_output_dir" && "$rehearsal_artifacts_written" != "true" ]]; then
    input_errors+=("nested devnet rehearsal artifacts_written must be true")
  fi
  rehearsal_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$rehearsal_output")")"
  if ! rehearsal_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal package_bundle_enabled must be boolean token, got: ${rehearsal_nested_package_bundle_enabled_raw:-<empty>}")
    rehearsal_nested_package_bundle_enabled="unknown"
  elif [[ "$rehearsal_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested devnet rehearsal helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
  rehearsal_nested_go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_executor_upstream must be boolean token, got: ${rehearsal_nested_go_nogo_require_executor_upstream_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_executor_upstream="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_executor_upstream" != "$go_nogo_require_executor_upstream_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_executor_upstream mismatch: nested=${rehearsal_nested_go_nogo_require_executor_upstream} expected=${go_nogo_require_executor_upstream_norm}")
  fi
  rehearsal_nested_go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_ingestion_grpc must be boolean token, got: ${rehearsal_nested_go_nogo_require_ingestion_grpc_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_ingestion_grpc="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_ingestion_grpc" != "$go_nogo_require_ingestion_grpc_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_ingestion_grpc mismatch: nested=${rehearsal_nested_go_nogo_require_ingestion_grpc} expected=${go_nogo_require_ingestion_grpc_norm}")
  fi
  rehearsal_nested_go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "go_nogo_require_followlist_activity" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_followlist_activity="$(extract_bool_field_strict "go_nogo_require_followlist_activity" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_followlist_activity must be boolean token, got: ${rehearsal_nested_go_nogo_require_followlist_activity_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_followlist_activity="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_followlist_activity" != "$go_nogo_require_followlist_activity_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_followlist_activity mismatch: nested=${rehearsal_nested_go_nogo_require_followlist_activity} expected=${go_nogo_require_followlist_activity_norm}")
  fi
  rehearsal_nested_go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_non_bootstrap_signer must be boolean token, got: ${rehearsal_nested_go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_non_bootstrap_signer="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_non_bootstrap_signer" != "$go_nogo_require_non_bootstrap_signer_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_non_bootstrap_signer mismatch: nested=${rehearsal_nested_go_nogo_require_non_bootstrap_signer} expected=${go_nogo_require_non_bootstrap_signer_norm}")
  fi
  rehearsal_nested_go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "go_nogo_require_confirmed_execution_sample" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "go_nogo_require_confirmed_execution_sample" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_confirmed_execution_sample must be boolean token, got: ${rehearsal_nested_go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_confirmed_execution_sample="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_confirmed_execution_sample mismatch: nested=${rehearsal_nested_go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
  fi
  rehearsal_nested_go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "go_nogo_min_confirmed_orders" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$rehearsal_nested_go_nogo_min_confirmed_orders_raw")"; then
    input_errors+=("nested devnet rehearsal go_nogo_min_confirmed_orders must be an integer >= 1, got: ${rehearsal_nested_go_nogo_min_confirmed_orders_raw:-<empty>}")
    rehearsal_nested_go_nogo_min_confirmed_orders="0"
  elif [[ "$rehearsal_nested_go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_min_confirmed_orders mismatch: nested=${rehearsal_nested_go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
  fi
  rehearsal_nested_go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "go_nogo_require_jito_rpc_policy" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "go_nogo_require_jito_rpc_policy" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_jito_rpc_policy must be boolean token, got: ${rehearsal_nested_go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_jito_rpc_policy="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_jito_rpc_policy" != "$go_nogo_require_jito_rpc_policy_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_jito_rpc_policy mismatch: nested=${rehearsal_nested_go_nogo_require_jito_rpc_policy} expected=${go_nogo_require_jito_rpc_policy_norm}")
  fi
  rehearsal_nested_jito_rpc_policy_verdict_raw="$(trim_string "$(extract_field "jito_rpc_policy_verdict" "$rehearsal_output")")"
  rehearsal_nested_jito_rpc_policy_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_jito_rpc_policy_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_jito_rpc_policy_verdict="$(normalize_gate_verdict "$rehearsal_nested_jito_rpc_policy_verdict_raw")"
  if [[ -z "$rehearsal_nested_jito_rpc_policy_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal jito_rpc_policy_verdict must be non-empty")
    rehearsal_nested_jito_rpc_policy_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_jito_rpc_policy_verdict_raw_upper" != "PASS" && "$rehearsal_nested_jito_rpc_policy_verdict_raw_upper" != "WARN" && "$rehearsal_nested_jito_rpc_policy_verdict_raw_upper" != "NO_DATA" && "$rehearsal_nested_jito_rpc_policy_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal jito_rpc_policy_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${rehearsal_nested_jito_rpc_policy_verdict_raw})")
    rehearsal_nested_jito_rpc_policy_verdict="UNKNOWN"
  fi
  rehearsal_nested_jito_rpc_policy_reason_code="$(trim_string "$(extract_field "jito_rpc_policy_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_jito_rpc_policy_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal jito_rpc_policy_reason_code must be non-empty")
    rehearsal_nested_jito_rpc_policy_reason_code="n/a"
  fi
  rehearsal_nested_go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "go_nogo_require_fastlane_disabled" "$rehearsal_output")")"
  if ! rehearsal_nested_go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "go_nogo_require_fastlane_disabled" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal go_nogo_require_fastlane_disabled must be boolean token, got: ${rehearsal_nested_go_nogo_require_fastlane_disabled_raw:-<empty>}")
    rehearsal_nested_go_nogo_require_fastlane_disabled="unknown"
  elif [[ "$rehearsal_nested_go_nogo_require_fastlane_disabled" != "$go_nogo_require_fastlane_disabled_norm" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_require_fastlane_disabled mismatch: nested=${rehearsal_nested_go_nogo_require_fastlane_disabled} expected=${go_nogo_require_fastlane_disabled_norm}")
  fi
  rehearsal_nested_fastlane_feature_flag_verdict_raw="$(trim_string "$(extract_field "fastlane_feature_flag_verdict" "$rehearsal_output")")"
  rehearsal_nested_fastlane_feature_flag_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_fastlane_feature_flag_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_fastlane_feature_flag_verdict="$(normalize_gate_verdict "$rehearsal_nested_fastlane_feature_flag_verdict_raw")"
  if [[ -z "$rehearsal_nested_fastlane_feature_flag_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal fastlane_feature_flag_verdict must be non-empty")
    rehearsal_nested_fastlane_feature_flag_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_fastlane_feature_flag_verdict_raw_upper" != "PASS" && "$rehearsal_nested_fastlane_feature_flag_verdict_raw_upper" != "WARN" && "$rehearsal_nested_fastlane_feature_flag_verdict_raw_upper" != "NO_DATA" && "$rehearsal_nested_fastlane_feature_flag_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal fastlane_feature_flag_verdict must be one of PASS,WARN,NO_DATA,SKIP (got: ${rehearsal_nested_fastlane_feature_flag_verdict_raw})")
    rehearsal_nested_fastlane_feature_flag_verdict="UNKNOWN"
  fi
  rehearsal_nested_fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "fastlane_feature_flag_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_fastlane_feature_flag_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal fastlane_feature_flag_reason_code must be non-empty")
    rehearsal_nested_fastlane_feature_flag_reason_code="n/a"
  fi
  rehearsal_nested_executor_env_path="$(trim_string "$(extract_field "executor_env_path" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_executor_env_path" ]]; then
    input_errors+=("nested devnet rehearsal executor_env_path must be non-empty")
    rehearsal_nested_executor_env_path="n/a"
  elif [[ "$rehearsal_nested_executor_env_path" != "$EXECUTOR_ENV_PATH" ]]; then
    input_errors+=("nested devnet rehearsal executor_env_path mismatch: nested=${rehearsal_nested_executor_env_path} expected=${EXECUTOR_ENV_PATH}")
  fi
  rehearsal_nested_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_executor_backend_mode_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_executor_backend_mode_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_executor_backend_mode_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_backend_mode_guard_verdict must be non-empty")
    rehearsal_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_executor_backend_mode_guard_verdict_raw})")
    rehearsal_nested_executor_backend_mode_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "go_nogo_executor_backend_mode_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_executor_backend_mode_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_backend_mode_guard_reason_code must be non-empty")
    rehearsal_nested_executor_backend_mode_guard_reason_code="n/a"
  fi
  rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_executor_upstream_endpoint_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_upstream_endpoint_guard_verdict must be non-empty")
    rehearsal_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_executor_upstream_endpoint_guard_verdict_raw})")
    rehearsal_nested_executor_upstream_endpoint_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "go_nogo_executor_upstream_endpoint_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_executor_upstream_endpoint_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_executor_upstream_endpoint_guard_reason_code must be non-empty")
    rehearsal_nested_executor_upstream_endpoint_guard_reason_code="n/a"
  fi
  rehearsal_nested_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_ingestion_grpc_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_ingestion_grpc_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_ingestion_grpc_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_ingestion_grpc_guard_verdict must be non-empty")
    rehearsal_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_ingestion_grpc_guard_verdict_raw})")
    rehearsal_nested_ingestion_grpc_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "go_nogo_ingestion_grpc_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_ingestion_grpc_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_ingestion_grpc_guard_reason_code must be non-empty")
    rehearsal_nested_ingestion_grpc_guard_reason_code="n/a"
  fi
  rehearsal_nested_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_followlist_activity_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_followlist_activity_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_followlist_activity_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_followlist_activity_guard_verdict must be non-empty")
    rehearsal_nested_followlist_activity_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_followlist_activity_guard_verdict_raw})")
    rehearsal_nested_followlist_activity_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "go_nogo_followlist_activity_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_followlist_activity_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_followlist_activity_guard_reason_code must be non-empty")
    rehearsal_nested_followlist_activity_guard_reason_code="n/a"
  fi
  rehearsal_nested_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_non_bootstrap_signer_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_non_bootstrap_signer_guard_verdict must be non-empty")
    rehearsal_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_non_bootstrap_signer_guard_verdict_raw})")
    rehearsal_nested_non_bootstrap_signer_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "go_nogo_non_bootstrap_signer_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_non_bootstrap_signer_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_non_bootstrap_signer_guard_reason_code must be non-empty")
    rehearsal_nested_non_bootstrap_signer_guard_reason_code="n/a"
  fi
  rehearsal_nested_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "go_nogo_confirmed_execution_sample_guard_verdict" "$rehearsal_output")")"
  rehearsal_nested_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  rehearsal_nested_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw")"
  if [[ -z "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_confirmed_execution_sample_guard_verdict must be non-empty")
    rehearsal_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  elif [[ "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$rehearsal_nested_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${rehearsal_nested_confirmed_execution_sample_guard_verdict_raw})")
    rehearsal_nested_confirmed_execution_sample_guard_verdict="UNKNOWN"
  fi
  rehearsal_nested_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "go_nogo_confirmed_execution_sample_guard_reason_code" "$rehearsal_output")")"
  if [[ -z "$rehearsal_nested_confirmed_execution_sample_guard_reason_code" ]]; then
    input_errors+=("nested devnet rehearsal go_nogo_confirmed_execution_sample_guard_reason_code must be non-empty")
    rehearsal_nested_confirmed_execution_sample_guard_reason_code="n/a"
  fi
  if [[ "$go_nogo_require_executor_upstream_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_executor_backend_mode_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
    if [[ "$rehearsal_nested_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_executor_upstream_endpoint_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
    fi
  else
    if [[ "$rehearsal_nested_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_executor_backend_mode_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${rehearsal_nested_executor_backend_mode_guard_verdict})")
    fi
    if [[ "$rehearsal_nested_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_executor_upstream_endpoint_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${rehearsal_nested_executor_upstream_endpoint_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
    fi
  else
    if [[ "$rehearsal_nested_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${rehearsal_nested_ingestion_grpc_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_followlist_activity_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
    fi
  else
    if [[ "$rehearsal_nested_followlist_activity_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${rehearsal_nested_followlist_activity_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
    fi
  else
    if [[ "$rehearsal_nested_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${rehearsal_nested_non_bootstrap_signer_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
    fi
  else
    if [[ "$rehearsal_nested_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal go_nogo_confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${rehearsal_nested_confirmed_execution_sample_guard_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_jito_rpc_policy_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_jito_rpc_policy_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal jito_rpc_policy_verdict cannot be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=true")
    fi
  else
    if [[ "$rehearsal_nested_jito_rpc_policy_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal jito_rpc_policy_verdict must be SKIP when GO_NOGO_REQUIRE_JITO_RPC_POLICY=false (got: ${rehearsal_nested_jito_rpc_policy_verdict})")
    fi
  fi
  if [[ "$go_nogo_require_fastlane_disabled_norm" == "true" ]]; then
    if [[ "$rehearsal_nested_fastlane_feature_flag_verdict" == "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal fastlane_feature_flag_verdict cannot be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=true")
    fi
  else
    if [[ "$rehearsal_nested_fastlane_feature_flag_verdict" != "SKIP" ]]; then
      input_errors+=("nested devnet rehearsal fastlane_feature_flag_verdict must be SKIP when GO_NOGO_REQUIRE_FASTLANE_DISABLED=false (got: ${rehearsal_nested_fastlane_feature_flag_verdict})")
    fi
  fi
  tests_run="$(trim_string "$(extract_field "tests_run" "$rehearsal_output")")"
  tests_failed="$(trim_string "$(extract_field "tests_failed" "$rehearsal_output")")"

  if [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
    rehearsal_reason="unable to classify rehearsal helper verdict (exit=$rehearsal_exit_code)"
    rehearsal_reason_code="unknown_verdict"
  elif [[ -z "$rehearsal_reason" ]]; then
    rehearsal_reason="rehearsal helper reported $rehearsal_verdict"
    rehearsal_reason_code="missing_reason"
  elif [[ -z "$rehearsal_reason_code" ]]; then
    rehearsal_reason_code="missing_reason"
  fi
fi

executor_rollout_verdict="NO_GO"
executor_rollout_reason="unrecognized rollout gate state"
executor_rollout_reason_code="unrecognized_state"
if ((${#input_errors[@]} > 0)); then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="${input_errors[0]}"
  executor_rollout_reason_code="input_error"
elif [[ "$executor_rollout_run_rotation_norm" == "true" && "$rotation_verdict" == "FAIL" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="signer rotation readiness failed: ${rotation_reason:-n/a}"
  executor_rollout_reason_code="rotation_fail"
elif [[ "$executor_rollout_run_rotation_norm" == "true" && "$rotation_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="signer rotation readiness verdict unknown; fail-closed"
  executor_rollout_reason_code="rotation_unknown"
elif [[ "$executor_rollout_run_preflight_norm" == "true" && "$preflight_verdict" == "FAIL" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="executor preflight failed: ${preflight_reason:-n/a}"
  executor_rollout_reason_code="preflight_fail"
elif [[ "$executor_rollout_run_preflight_norm" == "true" && "$preflight_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="executor preflight verdict unknown; fail-closed"
  executor_rollout_reason_code="preflight_unknown"
elif [[ "$executor_rollout_run_rehearsal_norm" == "true" && "$rehearsal_verdict" == "NO_GO" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="devnet rehearsal returned NO_GO: ${rehearsal_reason:-n/a}"
  executor_rollout_reason_code="rehearsal_no_go"
elif [[ "$executor_rollout_run_rehearsal_norm" == "true" && "$rehearsal_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="devnet rehearsal verdict unknown; fail-closed"
  executor_rollout_reason_code="rehearsal_unknown"
elif [[ "$executor_rollout_run_rotation_norm" == "true" && "$rotation_verdict" == "WARN" ]]; then
  executor_rollout_verdict="HOLD"
  executor_rollout_reason="signer rotation readiness returned WARN: ${rotation_reason:-n/a}"
  executor_rollout_reason_code="rotation_warn"
elif [[ "$executor_rollout_run_preflight_norm" == "true" && "$preflight_verdict" == "SKIP" ]]; then
  executor_rollout_verdict="HOLD"
  executor_rollout_reason="executor preflight returned SKIP: ${preflight_reason:-n/a}"
  executor_rollout_reason_code="preflight_skip"
elif [[ "$executor_rollout_run_rehearsal_norm" == "true" && "$rehearsal_verdict" == "HOLD" ]]; then
  executor_rollout_verdict="HOLD"
  executor_rollout_reason="devnet rehearsal returned HOLD: ${rehearsal_reason:-n/a}"
  executor_rollout_reason_code="rehearsal_hold"
elif [[ ( "$executor_rollout_run_rotation_norm" != "true" || "$rotation_verdict" == "PASS" ) && ( "$executor_rollout_run_preflight_norm" != "true" || "$preflight_verdict" == "PASS" ) && ( "$executor_rollout_run_rehearsal_norm" != "true" || "$rehearsal_verdict" == "GO" ) ]]; then
  executor_rollout_verdict="GO"
  executor_rollout_reason="enabled rollout stages passed"
  executor_rollout_reason_code="gates_pass"
fi

artifacts_written="false"
if [[ -n "$OUTPUT_DIR" ]]; then
  artifacts_written="true"
fi
package_bundle_artifacts_written="false"
package_bundle_exit_code="n/a"
package_bundle_error="n/a"
package_bundle_path="n/a"
package_bundle_sha256="n/a"
package_bundle_sha256_path="n/a"
package_bundle_contents_manifest="n/a"
package_bundle_file_count="n/a"

summary_output="$(cat <<EOF_SUMMARY
=== Executor Rollout Evidence Summary ===
utc_now: $timestamp_utc
executor_env: $EXECUTOR_ENV_PATH
adapter_env: $ADAPTER_ENV_PATH
config: $CONFIG_PATH
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES
executor_rollout_profile: $EXECUTOR_ROLLOUT_PROFILE
executor_rollout_run_rotation: $executor_rollout_run_rotation_norm
executor_rollout_run_preflight: $executor_rollout_run_preflight_norm
executor_rollout_run_rehearsal: $executor_rollout_run_rehearsal_norm
go_nogo_require_executor_upstream: $go_nogo_require_executor_upstream_norm
go_nogo_require_ingestion_grpc: $go_nogo_require_ingestion_grpc_norm
go_nogo_require_followlist_activity: $go_nogo_require_followlist_activity_norm
go_nogo_require_non_bootstrap_signer: $go_nogo_require_non_bootstrap_signer_norm
go_nogo_require_submit_verify_strict: $go_nogo_require_submit_verify_strict_norm
go_nogo_require_confirmed_execution_sample: $go_nogo_require_confirmed_execution_sample_norm
go_nogo_min_confirmed_orders: $go_nogo_min_confirmed_orders_norm
executor_env_path: $EXECUTOR_ENV_PATH

rotation_readiness_verdict: $rotation_verdict
rotation_readiness_reason: ${rotation_reason:-n/a}
rotation_exit_code: $rotation_exit_code
rotation_artifact_report: ${rotation_artifact_report:-n/a}
rotation_artifact_manifest: ${rotation_artifact_manifest:-n/a}
rotation_report_sha256: ${rotation_report_sha256:-n/a}
rotation_artifacts_written: $rotation_artifacts_written

preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
preflight_reason_code: ${preflight_reason_code:-n/a}
preflight_exit_code: $preflight_exit_code
health_status: ${health_status:-n/a}
auth_probe_without_auth_code: ${auth_probe_without_auth_code:-n/a}
auth_probe_with_auth_http_status: ${auth_probe_with_auth_http_status:-n/a}
auth_probe_with_auth_code: ${auth_probe_with_auth_code:-n/a}
preflight_artifact_summary: ${preflight_artifact_summary:-n/a}
preflight_artifact_manifest: ${preflight_artifact_manifest:-n/a}
preflight_summary_sha256: ${preflight_summary_sha256:-n/a}
preflight_artifacts_written: $preflight_artifacts_written
preflight_executor_submit_verify_strict: ${preflight_executor_submit_verify_strict:-n/a}
preflight_executor_submit_verify_configured: ${preflight_executor_submit_verify_configured:-n/a}
preflight_executor_submit_verify_fallback_configured: ${preflight_executor_submit_verify_fallback_configured:-n/a}

devnet_rehearsal_verdict: $rehearsal_verdict
devnet_rehearsal_reason: ${rehearsal_reason:-n/a}
devnet_rehearsal_reason_code: ${rehearsal_reason_code:-n/a}
devnet_rehearsal_exit_code: $rehearsal_exit_code
rehearsal_preflight_verdict: ${rehearsal_preflight_verdict:-n/a}
overall_go_nogo_verdict: ${overall_go_nogo_verdict:-n/a}
overall_go_nogo_reason_code: ${overall_go_nogo_reason_code:-n/a}
windowed_signoff_verdict: ${windowed_signoff_verdict:-n/a}
windowed_signoff_reason: ${windowed_signoff_reason:-n/a}
route_fee_signoff_verdict: ${route_fee_signoff_verdict:-n/a}
route_fee_signoff_reason: ${route_fee_signoff_reason:-n/a}
route_fee_signoff_reason_code: ${route_fee_signoff_reason_code:-n/a}
tests_run: ${tests_run:-n/a}
tests_failed: ${tests_failed:-n/a}
rehearsal_artifact_manifest: ${rehearsal_artifact_manifest:-n/a}
rehearsal_summary_sha256: ${rehearsal_summary_sha256:-n/a}
rehearsal_preflight_sha256: ${rehearsal_preflight_sha256:-n/a}
rehearsal_go_nogo_sha256: ${rehearsal_go_nogo_sha256:-n/a}
rehearsal_tests_sha256: ${rehearsal_tests_sha256:-n/a}
rehearsal_artifacts_written: $rehearsal_artifacts_written
rehearsal_nested_package_bundle_enabled: ${rehearsal_nested_package_bundle_enabled:-unknown}
rehearsal_nested_go_nogo_require_jito_rpc_policy: ${rehearsal_nested_go_nogo_require_jito_rpc_policy:-n/a}
rehearsal_nested_go_nogo_require_executor_upstream: ${rehearsal_nested_go_nogo_require_executor_upstream:-n/a}
rehearsal_nested_go_nogo_require_ingestion_grpc: ${rehearsal_nested_go_nogo_require_ingestion_grpc:-n/a}
rehearsal_nested_go_nogo_require_followlist_activity: ${rehearsal_nested_go_nogo_require_followlist_activity:-n/a}
rehearsal_nested_go_nogo_require_non_bootstrap_signer: ${rehearsal_nested_go_nogo_require_non_bootstrap_signer:-n/a}
rehearsal_nested_go_nogo_require_confirmed_execution_sample: ${rehearsal_nested_go_nogo_require_confirmed_execution_sample:-n/a}
rehearsal_nested_go_nogo_min_confirmed_orders: ${rehearsal_nested_go_nogo_min_confirmed_orders:-n/a}
rehearsal_nested_jito_rpc_policy_verdict: ${rehearsal_nested_jito_rpc_policy_verdict:-unknown}
rehearsal_nested_jito_rpc_policy_reason_code: ${rehearsal_nested_jito_rpc_policy_reason_code:-n/a}
rehearsal_nested_go_nogo_require_fastlane_disabled: ${rehearsal_nested_go_nogo_require_fastlane_disabled:-n/a}
rehearsal_nested_fastlane_feature_flag_verdict: ${rehearsal_nested_fastlane_feature_flag_verdict:-unknown}
rehearsal_nested_fastlane_feature_flag_reason_code: ${rehearsal_nested_fastlane_feature_flag_reason_code:-n/a}
rehearsal_nested_executor_env_path: ${rehearsal_nested_executor_env_path:-n/a}
rehearsal_nested_executor_backend_mode_guard_verdict: ${rehearsal_nested_executor_backend_mode_guard_verdict:-unknown}
rehearsal_nested_executor_backend_mode_guard_reason_code: ${rehearsal_nested_executor_backend_mode_guard_reason_code:-n/a}
rehearsal_nested_executor_upstream_endpoint_guard_verdict: ${rehearsal_nested_executor_upstream_endpoint_guard_verdict:-unknown}
rehearsal_nested_executor_upstream_endpoint_guard_reason_code: ${rehearsal_nested_executor_upstream_endpoint_guard_reason_code:-n/a}
rehearsal_nested_ingestion_grpc_guard_verdict: ${rehearsal_nested_ingestion_grpc_guard_verdict:-unknown}
rehearsal_nested_ingestion_grpc_guard_reason_code: ${rehearsal_nested_ingestion_grpc_guard_reason_code:-n/a}
rehearsal_nested_followlist_activity_guard_verdict: ${rehearsal_nested_followlist_activity_guard_verdict:-unknown}
rehearsal_nested_followlist_activity_guard_reason_code: ${rehearsal_nested_followlist_activity_guard_reason_code:-n/a}
rehearsal_nested_non_bootstrap_signer_guard_verdict: ${rehearsal_nested_non_bootstrap_signer_guard_verdict:-unknown}
rehearsal_nested_non_bootstrap_signer_guard_reason_code: ${rehearsal_nested_non_bootstrap_signer_guard_reason_code:-n/a}
rehearsal_nested_confirmed_execution_sample_guard_verdict: ${rehearsal_nested_confirmed_execution_sample_guard_verdict:-unknown}
rehearsal_nested_confirmed_execution_sample_guard_reason_code: ${rehearsal_nested_confirmed_execution_sample_guard_reason_code:-n/a}
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}

input_error_count: ${#input_errors[@]}

executor_rollout_verdict: $executor_rollout_verdict
executor_rollout_reason: $executor_rollout_reason
executor_rollout_reason_code: $executor_rollout_reason_code
artifacts_written: $artifacts_written
EOF_SUMMARY
)"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/executor_rollout_evidence_summary_${timestamp_compact}.txt"
  rotation_capture_path="$OUTPUT_DIR/executor_signer_rotation_captured_${timestamp_compact}.txt"
  preflight_capture_path="$OUTPUT_DIR/executor_preflight_captured_${timestamp_compact}.txt"
  rehearsal_capture_path="$OUTPUT_DIR/execution_devnet_rehearsal_captured_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/executor_rollout_evidence_manifest_${timestamp_compact}.txt"

  printf '%s\n' "$summary_output" >"$summary_path"
  printf '%s\n' "$rotation_output" >"$rotation_capture_path"
  printf '%s\n' "$preflight_output" >"$preflight_capture_path"
  printf '%s\n' "$rehearsal_output" >"$rehearsal_capture_path"

  run_package_bundle_once() {
    local package_bundle_output=""
    if package_bundle_output="$(
      OUTPUT_DIR="$PACKAGE_BUNDLE_OUTPUT_DIR" \
        BUNDLE_LABEL="$PACKAGE_BUNDLE_LABEL" \
        bash "$ROOT_DIR/tools/evidence_bundle_pack.sh" "$OUTPUT_DIR" 2>&1
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
    run_package_bundle_once
  fi

  cat >>"$summary_path" <<EOF
package_bundle_artifacts_written: $package_bundle_artifacts_written
package_bundle_exit_code: $package_bundle_exit_code
package_bundle_error: $package_bundle_error
EOF

  summary_sha256="$(sha256_file_value "$summary_path")"
  rotation_capture_sha256="$(sha256_file_value "$rotation_capture_path")"
  preflight_capture_sha256="$(sha256_file_value "$preflight_capture_path")"
  rehearsal_capture_sha256="$(sha256_file_value "$rehearsal_capture_path")"

  cat >"$manifest_path" <<EOF_MANIFEST
summary_sha256: $summary_sha256
rotation_capture_sha256: $rotation_capture_sha256
preflight_capture_sha256: $preflight_capture_sha256
rehearsal_capture_sha256: $rehearsal_capture_sha256
EOF_MANIFEST
  manifest_sha256="$(sha256_file_value "$manifest_path")"

  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_rotation_capture: $rotation_capture_path"
  echo "artifact_preflight_capture: $preflight_capture_path"
  echo "artifact_rehearsal_capture: $rehearsal_capture_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "rotation_capture_sha256: $rotation_capture_sha256"
  echo "preflight_capture_sha256: $preflight_capture_sha256"
  echo "rehearsal_capture_sha256: $rehearsal_capture_sha256"
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
fi

case "$executor_rollout_verdict" in
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
