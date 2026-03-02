#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-60}"
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
ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-false}"
ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="${REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED:-false}"
REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-adapter_rollout_evidence}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_DIR}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

declare -a input_errors=()
if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  input_errors+=("window hours must be an integer (got: $WINDOW_HOURS)")
fi
if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  input_errors+=("risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)")
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

parse_rollout_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_rollout_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_rollout_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_rollout_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_rollout_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_rollout_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_rollout_bool_setting_into "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" route_fee_signoff_go_nogo_test_mode_norm
parse_rollout_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" "$REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" rehearsal_route_fee_signoff_required_norm
parse_rollout_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" rehearsal_route_fee_signoff_go_nogo_test_mode_norm
parse_rollout_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm
if [[ "$package_bundle_enabled_norm" == "true" && -z "$OUTPUT_DIR" ]]; then
  input_errors+=("PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set")
fi

rotation_output_dir=""
rehearsal_output_dir=""
route_fee_signoff_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  rotation_output_dir="$OUTPUT_DIR/rotation"
  rehearsal_output_dir="$OUTPUT_DIR/rehearsal"
  route_fee_signoff_output_dir="$OUTPUT_DIR/route_fee_signoff"
  mkdir -p "$rotation_output_dir" "$rehearsal_output_dir" "$route_fee_signoff_output_dir"
fi

rotation_output=""
rotation_exit_code=3
rotation_verdict="UNKNOWN"
rotation_reason="rotation helper not executed"
rotation_artifact_report=""
rotation_artifact_manifest=""
rotation_report_sha256=""
rotation_artifacts_written="false"
if ((${#input_errors[@]} == 0)) && [[ -f "$ADAPTER_ENV_PATH" ]]; then
  if rotation_output="$(
    ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
      OUTPUT_DIR="$rotation_output_dir" \
      bash "$ROOT_DIR/tools/adapter_secret_rotation_report.sh" 2>&1
  )"; then
    rotation_exit_code=0
  else
    rotation_exit_code=$?
  fi

  rotation_verdict="$(normalize_rotation_verdict "$(extract_field "rotation_readiness_verdict" "$rotation_output")")"
  rotation_reason="$(extract_field "rotation_readiness_reason" "$rotation_output")"
  rotation_artifact_report="$(trim_string "$(extract_field "artifact_report" "$rotation_output")")"
  rotation_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rotation_output")")"
  rotation_report_sha256="$(trim_string "$(extract_field "report_sha256" "$rotation_output")")"
  rotation_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rotation_output")")"
  if ! rotation_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rotation_output")"; then
    input_errors+=("rotation helper artifacts_written must be boolean token, got: ${rotation_artifacts_written_raw:-<empty>}")
    rotation_artifacts_written="unknown"
  fi
  rotation_first_error="$(printf '%s\n' "$rotation_output" | awk '
    /^--- errors ---$/ {in_errors=1; next}
    /^--- / && in_errors {exit}
    in_errors && NF {print; exit}
  ')"
  rotation_first_warning="$(printf '%s\n' "$rotation_output" | awk '
    /^--- warnings ---$/ {in_warnings=1; next}
    /^--- / && in_warnings {exit}
    in_warnings && NF {print; exit}
  ')"
  if [[ "$rotation_verdict" == "FAIL" ]]; then
    rotation_reason="$(trim_string "${rotation_first_error:-${rotation_reason:-rotation helper reported FAIL}}")"
  elif [[ "$rotation_verdict" == "WARN" ]]; then
    rotation_reason="$(trim_string "${rotation_first_warning:-${rotation_reason:-rotation helper reported WARN}}")"
  elif [[ "$rotation_verdict" == "UNKNOWN" ]]; then
    rotation_reason="unable to classify rotation helper verdict (exit=$rotation_exit_code)"
  elif [[ -z "${rotation_reason:-}" ]]; then
    rotation_reason="rotation helper PASS"
  fi
elif ((${#input_errors[@]} == 0)); then
  rotation_exit_code=1
  rotation_verdict="FAIL"
  rotation_reason="adapter env file not found: $ADAPTER_ENV_PATH"
fi

rehearsal_output=""
rehearsal_exit_code=3
rehearsal_verdict="UNKNOWN"
rehearsal_reason="execution devnet rehearsal helper not executed"
preflight_verdict=""
go_nogo_verdict=""
go_nogo_reason_code=""
go_nogo_require_jito_rpc_policy=""
jito_rpc_policy_verdict=""
jito_rpc_policy_reason=""
jito_rpc_policy_reason_code=""
go_nogo_require_fastlane_disabled=""
submit_fastlane_enabled=""
fastlane_feature_flag_verdict=""
fastlane_feature_flag_reason=""
fastlane_feature_flag_reason_code=""
rehearsal_reason_code=""
dynamic_cu_hint_source_reason_code=""
dynamic_cu_policy_verdict=""
dynamic_cu_policy_reason=""
dynamic_tip_policy_verdict=""
dynamic_tip_policy_reason=""
windowed_signoff_required=""
windowed_signoff_windows_csv=""
windowed_signoff_require_dynamic_hint_source_pass=""
windowed_signoff_require_dynamic_tip_policy_pass=""
windowed_signoff_verdict=""
windowed_signoff_reason=""
primary_route=""
fallback_route=""
primary_attempted_orders=""
primary_success_rate_pct=""
primary_timeout_rate_pct=""
fallback_attempted_orders=""
fallback_success_rate_pct=""
fallback_timeout_rate_pct=""
confirmed_orders_total=""
fee_consistency_missing_coverage_rows=""
fee_consistency_mismatch_rows=""
fallback_used_events=""
hint_mismatch_events=""
go_nogo_artifact_manifest=""
go_nogo_calibration_sha256=""
go_nogo_snapshot_sha256=""
go_nogo_preflight_sha256=""
go_nogo_summary_sha256=""
go_nogo_artifacts_written="false"
rehearsal_artifact_manifest=""
rehearsal_summary_sha256=""
rehearsal_preflight_sha256=""
rehearsal_go_nogo_sha256=""
rehearsal_tests_sha256=""
rehearsal_artifacts_written="false"
rehearsal_nested_package_bundle_enabled="unknown"
windowed_signoff_artifact_manifest=""
windowed_signoff_summary_sha256=""
windowed_signoff_artifacts_written="false"
rehearsal_route_fee_signoff_required=""
rehearsal_route_fee_signoff_windows_csv=""
rehearsal_route_fee_signoff_verdict=""
rehearsal_route_fee_signoff_reason=""
rehearsal_route_fee_signoff_reason_code=""
rehearsal_route_fee_signoff_exit_code=""
rehearsal_route_fee_signoff_artifact_manifest=""
rehearsal_route_fee_signoff_summary_sha256=""
rehearsal_route_fee_signoff_artifacts_written="false"
rehearsal_route_fee_primary_route_stable=""
rehearsal_route_fee_stable_primary_route=""
rehearsal_route_fee_fallback_route_stable=""
rehearsal_route_fee_stable_fallback_route=""
rehearsal_route_fee_route_profile_pass_count=""
rehearsal_route_fee_fee_decomposition_pass_count=""
rehearsal_route_fee_window_count=""
tests_run=""
tests_failed=""
if ((${#input_errors[@]} > 0)); then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="${input_errors[0]}"
  rehearsal_reason_code="input_error"
elif [[ ! "$WINDOW_HOURS" =~ ^[0-9]+$ || ! "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="invalid rehearsal window arguments"
elif [[ ! -f "$CONFIG_PATH" ]]; then
  rehearsal_exit_code=3
  rehearsal_verdict="NO_GO"
  rehearsal_reason="config file not found: $CONFIG_PATH"
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
      WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
      WINDOWED_SIGNOFF_REQUIRED="$windowed_signoff_required_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
      WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
      ROUTE_FEE_SIGNOFF_REQUIRED="$rehearsal_route_fee_signoff_required_norm" \
      ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$rehearsal_route_fee_signoff_go_nogo_test_mode_norm" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
      ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
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
  preflight_verdict="$(trim_string "$(extract_field "preflight_verdict" "$rehearsal_output")")"
  go_nogo_verdict="$(trim_string "$(extract_field "overall_go_nogo_verdict" "$rehearsal_output")")"
  go_nogo_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$rehearsal_output")")"
  go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "go_nogo_require_jito_rpc_policy" "$rehearsal_output")")"
  if ! go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "go_nogo_require_jito_rpc_policy" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal go_nogo_require_jito_rpc_policy must be boolean token, got: ${go_nogo_require_jito_rpc_policy_raw:-<empty>}")
    go_nogo_require_jito_rpc_policy="unknown"
  fi
  jito_rpc_policy_verdict="$(normalize_gate_verdict "$(extract_field "jito_rpc_policy_verdict" "$rehearsal_output")")"
  jito_rpc_policy_reason="$(trim_string "$(extract_field "jito_rpc_policy_reason" "$rehearsal_output")")"
  jito_rpc_policy_reason_code="$(trim_string "$(extract_field "jito_rpc_policy_reason_code" "$rehearsal_output")")"
  go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "go_nogo_require_fastlane_disabled" "$rehearsal_output")")"
  if ! go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "go_nogo_require_fastlane_disabled" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal go_nogo_require_fastlane_disabled must be boolean token, got: ${go_nogo_require_fastlane_disabled_raw:-<empty>}")
    go_nogo_require_fastlane_disabled="unknown"
  fi
  submit_fastlane_enabled_raw="$(trim_string "$(extract_field "submit_fastlane_enabled" "$rehearsal_output")")"
  if ! submit_fastlane_enabled="$(extract_bool_field_strict "submit_fastlane_enabled" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal submit_fastlane_enabled must be boolean token, got: ${submit_fastlane_enabled_raw:-<empty>}")
    submit_fastlane_enabled="unknown"
  fi
  fastlane_feature_flag_verdict="$(normalize_gate_verdict "$(extract_field "fastlane_feature_flag_verdict" "$rehearsal_output")")"
  fastlane_feature_flag_reason="$(trim_string "$(extract_field "fastlane_feature_flag_reason" "$rehearsal_output")")"
  fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "fastlane_feature_flag_reason_code" "$rehearsal_output")")"
  dynamic_cu_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_policy_verdict" "$rehearsal_output")")"
  dynamic_cu_policy_reason="$(trim_string "$(extract_field "dynamic_cu_policy_reason" "$rehearsal_output")")"
  dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$rehearsal_output")")"
  dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$rehearsal_output")")"
  windowed_signoff_required="$(trim_string "$(extract_field "windowed_signoff_required" "$rehearsal_output")")"
  windowed_signoff_windows_csv="$(trim_string "$(extract_field "windowed_signoff_windows_csv" "$rehearsal_output")")"
  windowed_signoff_require_dynamic_hint_source_pass="$(trim_string "$(extract_field "windowed_signoff_require_dynamic_hint_source_pass" "$rehearsal_output")")"
  windowed_signoff_require_dynamic_tip_policy_pass="$(trim_string "$(extract_field "windowed_signoff_require_dynamic_tip_policy_pass" "$rehearsal_output")")"
  windowed_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "windowed_signoff_verdict" "$rehearsal_output")")"
  windowed_signoff_reason="$(trim_string "$(extract_field "windowed_signoff_reason" "$rehearsal_output")")"
  dynamic_cu_hint_api_total="$(trim_string "$(extract_field "dynamic_cu_hint_api_total" "$rehearsal_output")")"
  dynamic_cu_hint_rpc_total="$(trim_string "$(extract_field "dynamic_cu_hint_rpc_total" "$rehearsal_output")")"
  dynamic_cu_hint_api_configured="$(trim_string "$(extract_field "dynamic_cu_hint_api_configured" "$rehearsal_output")")"
  dynamic_cu_hint_source_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_hint_source_verdict" "$rehearsal_output")")"
  dynamic_cu_hint_source_reason="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason" "$rehearsal_output")")"
  dynamic_cu_hint_source_reason_code="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason_code" "$rehearsal_output")")"
  primary_route="$(trim_string "$(extract_field "primary_route" "$rehearsal_output")")"
  fallback_route="$(trim_string "$(extract_field "fallback_route" "$rehearsal_output")")"
  primary_attempted_orders="$(trim_string "$(extract_field "primary_attempted_orders" "$rehearsal_output")")"
  primary_success_rate_pct="$(trim_string "$(extract_field "primary_success_rate_pct" "$rehearsal_output")")"
  primary_timeout_rate_pct="$(trim_string "$(extract_field "primary_timeout_rate_pct" "$rehearsal_output")")"
  fallback_attempted_orders="$(trim_string "$(extract_field "fallback_attempted_orders" "$rehearsal_output")")"
  fallback_success_rate_pct="$(trim_string "$(extract_field "fallback_success_rate_pct" "$rehearsal_output")")"
  fallback_timeout_rate_pct="$(trim_string "$(extract_field "fallback_timeout_rate_pct" "$rehearsal_output")")"
  confirmed_orders_total="$(trim_string "$(extract_field "confirmed_orders_total" "$rehearsal_output")")"
  fee_consistency_missing_coverage_rows="$(trim_string "$(extract_field "fee_consistency_missing_coverage_rows" "$rehearsal_output")")"
  fee_consistency_mismatch_rows="$(trim_string "$(extract_field "fee_consistency_mismatch_rows" "$rehearsal_output")")"
  fallback_used_events="$(trim_string "$(extract_field "fallback_used_events" "$rehearsal_output")")"
  hint_mismatch_events="$(trim_string "$(extract_field "hint_mismatch_events" "$rehearsal_output")")"
  go_nogo_artifact_manifest="$(trim_string "$(extract_field "go_nogo_artifact_manifest" "$rehearsal_output")")"
  go_nogo_calibration_sha256="$(trim_string "$(extract_field "go_nogo_calibration_sha256" "$rehearsal_output")")"
  go_nogo_snapshot_sha256="$(trim_string "$(extract_field "go_nogo_snapshot_sha256" "$rehearsal_output")")"
  go_nogo_preflight_sha256="$(trim_string "$(extract_field "go_nogo_preflight_sha256" "$rehearsal_output")")"
  go_nogo_summary_sha256="$(trim_string "$(extract_field "go_nogo_summary_sha256" "$rehearsal_output")")"
  go_nogo_artifacts_written_raw="$(trim_string "$(extract_field "go_nogo_artifacts_written" "$rehearsal_output")")"
  if ! go_nogo_artifacts_written="$(extract_bool_field_strict "go_nogo_artifacts_written" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal go_nogo_artifacts_written must be boolean token, got: ${go_nogo_artifacts_written_raw:-<empty>}")
    go_nogo_artifacts_written="unknown"
  fi
  rehearsal_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rehearsal_output")")"
  rehearsal_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$rehearsal_output")")"
  rehearsal_preflight_sha256="$(trim_string "$(extract_field "preflight_sha256" "$rehearsal_output")")"
  rehearsal_go_nogo_sha256="$(trim_string "$(extract_field "go_nogo_sha256" "$rehearsal_output")")"
  rehearsal_tests_sha256="$(trim_string "$(extract_field "tests_sha256" "$rehearsal_output")")"
  rehearsal_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rehearsal_output")")"
  if ! rehearsal_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal artifacts_written must be boolean token, got: ${rehearsal_artifacts_written_raw:-<empty>}")
    rehearsal_artifacts_written="unknown"
  fi
  rehearsal_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$rehearsal_output")")"
  if ! rehearsal_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$rehearsal_output")"; then
    input_errors+=("nested devnet rehearsal package_bundle_enabled must be boolean token, got: ${rehearsal_nested_package_bundle_enabled_raw:-<empty>}")
    rehearsal_nested_package_bundle_enabled="unknown"
  elif [[ "$rehearsal_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested devnet rehearsal helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
  windowed_signoff_artifact_manifest="$(trim_string "$(extract_field "windowed_signoff_artifact_manifest" "$rehearsal_output")")"
  windowed_signoff_summary_sha256="$(trim_string "$(extract_field "windowed_signoff_summary_sha256" "$rehearsal_output")")"
  windowed_signoff_artifacts_written_raw="$(trim_string "$(extract_field "windowed_signoff_artifacts_written" "$rehearsal_output")")"
  if ! windowed_signoff_artifacts_written="$(extract_bool_field_strict "windowed_signoff_artifacts_written" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal windowed_signoff_artifacts_written must be boolean token, got: ${windowed_signoff_artifacts_written_raw:-<empty>}")
    windowed_signoff_artifacts_written="unknown"
  fi
  rehearsal_route_fee_signoff_required_raw="$(trim_string "$(extract_field "route_fee_signoff_required" "$rehearsal_output")")"
  if ! rehearsal_route_fee_signoff_required="$(extract_bool_field_strict "route_fee_signoff_required" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal route_fee_signoff_required must be boolean token, got: ${rehearsal_route_fee_signoff_required_raw:-<empty>}")
    rehearsal_route_fee_signoff_required="unknown"
  fi
  rehearsal_route_fee_signoff_windows_csv="$(trim_string "$(extract_field "route_fee_signoff_windows_csv" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "route_fee_signoff_verdict" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_reason="$(trim_string "$(extract_field "route_fee_signoff_reason" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_reason_code="$(trim_string "$(extract_field "route_fee_signoff_reason_code" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_exit_code="$(trim_string "$(extract_field "route_fee_signoff_exit_code" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_artifact_manifest="$(trim_string "$(extract_field "route_fee_signoff_artifact_manifest" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_summary_sha256="$(trim_string "$(extract_field "route_fee_signoff_summary_sha256" "$rehearsal_output")")"
  rehearsal_route_fee_signoff_artifacts_written_raw="$(trim_string "$(extract_field "route_fee_signoff_artifacts_written" "$rehearsal_output")")"
  if ! rehearsal_route_fee_signoff_artifacts_written="$(extract_bool_field_strict "route_fee_signoff_artifacts_written" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal route_fee_signoff_artifacts_written must be boolean token, got: ${rehearsal_route_fee_signoff_artifacts_written_raw:-<empty>}")
    rehearsal_route_fee_signoff_artifacts_written="unknown"
  fi
  rehearsal_route_fee_primary_route_stable_raw="$(trim_string "$(extract_field "route_fee_primary_route_stable" "$rehearsal_output")")"
  if ! rehearsal_route_fee_primary_route_stable="$(extract_bool_field_strict "route_fee_primary_route_stable" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal route_fee_primary_route_stable must be boolean token, got: ${rehearsal_route_fee_primary_route_stable_raw:-<empty>}")
    rehearsal_route_fee_primary_route_stable="unknown"
  fi
  rehearsal_route_fee_stable_primary_route="$(trim_string "$(extract_field "route_fee_stable_primary_route" "$rehearsal_output")")"
  rehearsal_route_fee_fallback_route_stable_raw="$(trim_string "$(extract_field "route_fee_fallback_route_stable" "$rehearsal_output")")"
  if ! rehearsal_route_fee_fallback_route_stable="$(extract_bool_field_strict "route_fee_fallback_route_stable" "$rehearsal_output")"; then
    input_errors+=("nested rehearsal route_fee_fallback_route_stable must be boolean token, got: ${rehearsal_route_fee_fallback_route_stable_raw:-<empty>}")
    rehearsal_route_fee_fallback_route_stable="unknown"
  fi
  rehearsal_route_fee_stable_fallback_route="$(trim_string "$(extract_field "route_fee_stable_fallback_route" "$rehearsal_output")")"
  rehearsal_route_fee_route_profile_pass_count="$(trim_string "$(extract_field "route_fee_route_profile_pass_count" "$rehearsal_output")")"
  rehearsal_route_fee_fee_decomposition_pass_count="$(trim_string "$(extract_field "route_fee_fee_decomposition_pass_count" "$rehearsal_output")")"
  rehearsal_route_fee_window_count="$(trim_string "$(extract_field "route_fee_window_count" "$rehearsal_output")")"
  tests_run="$(trim_string "$(extract_field "tests_run" "$rehearsal_output")")"
  tests_failed="$(trim_string "$(extract_field "tests_failed" "$rehearsal_output")")"
  if [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
    rehearsal_reason="unable to classify rehearsal verdict (exit=$rehearsal_exit_code)"
  elif [[ -z "$rehearsal_reason" ]]; then
    rehearsal_reason="execution devnet rehearsal reported $rehearsal_verdict"
  fi
fi

route_fee_signoff_required="$route_fee_signoff_required_norm"
route_fee_signoff_go_nogo_test_mode="$route_fee_signoff_go_nogo_test_mode_norm"
route_fee_signoff_go_nogo_test_fee_override="$(trim_string "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE")"
route_fee_signoff_go_nogo_test_route_override="$(trim_string "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE")"
route_fee_signoff_test_verdict_override_raw="$(trim_string "$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE")"
route_fee_signoff_output=""
route_fee_signoff_exit_code=3
route_fee_signoff_verdict="UNKNOWN"
route_fee_signoff_reason="execution route/fee signoff helper not executed"
route_fee_signoff_reason_code="not_executed"
route_fee_signoff_windows_csv=""
route_fee_signoff_artifact_manifest=""
route_fee_signoff_summary_sha256=""
route_fee_signoff_artifacts_written="false"
route_fee_signoff_nested_package_bundle_enabled="unknown"
route_fee_primary_route_stable=""
route_fee_stable_primary_route=""
route_fee_fallback_route_stable=""
route_fee_stable_fallback_route=""
route_fee_route_profile_pass_count=""
route_fee_fee_decomposition_pass_count=""
route_fee_window_count=""
if ((${#input_errors[@]} > 0)); then
  route_fee_signoff_exit_code=3
  route_fee_signoff_verdict="UNKNOWN"
  route_fee_signoff_reason="skipped due input validation errors"
  route_fee_signoff_reason_code="input_error"
elif [[ ! "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  route_fee_signoff_exit_code=3
  route_fee_signoff_verdict="NO_GO"
  route_fee_signoff_reason="invalid risk events window for route/fee signoff"
  route_fee_signoff_reason_code="input_error"
elif [[ ! -f "$CONFIG_PATH" ]]; then
  route_fee_signoff_exit_code=3
  route_fee_signoff_verdict="NO_GO"
  route_fee_signoff_reason="config file not found: $CONFIG_PATH"
  route_fee_signoff_reason_code="input_error"
else
  if route_fee_signoff_output="$(
    PATH="$PATH" \
      DB_PATH="${DB_PATH:-}" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_DIR="$route_fee_signoff_output_dir" \
      GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$route_fee_signoff_go_nogo_test_fee_override" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$route_fee_signoff_go_nogo_test_route_override" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
      PACKAGE_BUNDLE_ENABLED="false" \
      bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    route_fee_signoff_exit_code=0
  else
    route_fee_signoff_exit_code=$?
  fi

  route_fee_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "signoff_verdict" "$route_fee_signoff_output")")"
  route_fee_signoff_reason="$(trim_string "$(extract_field "signoff_reason" "$route_fee_signoff_output")")"
  route_fee_signoff_reason_code="$(trim_string "$(extract_field "signoff_reason_code" "$route_fee_signoff_output")")"
  route_fee_signoff_windows_csv="$(trim_string "$(extract_field "windows_csv" "$route_fee_signoff_output")")"
  route_fee_signoff_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$route_fee_signoff_output")")"
  route_fee_signoff_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$route_fee_signoff_output")")"
  route_fee_signoff_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$route_fee_signoff_output")")"
  if ! route_fee_signoff_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$route_fee_signoff_output")"; then
    input_errors+=("nested route/fee signoff artifacts_written must be boolean token, got: ${route_fee_signoff_artifacts_written_raw:-<empty>}")
    route_fee_signoff_artifacts_written="unknown"
  fi
  route_fee_signoff_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$route_fee_signoff_output")")"
  if ! route_fee_signoff_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$route_fee_signoff_output")"; then
    input_errors+=("nested route/fee signoff package_bundle_enabled must be boolean token, got: ${route_fee_signoff_nested_package_bundle_enabled_raw:-<empty>}")
    route_fee_signoff_nested_package_bundle_enabled="unknown"
  elif [[ "$route_fee_signoff_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested route/fee signoff helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
  route_fee_primary_route_stable_raw="$(trim_string "$(extract_field "primary_route_stable" "$route_fee_signoff_output")")"
  if ! route_fee_primary_route_stable="$(extract_bool_field_strict "primary_route_stable" "$route_fee_signoff_output")"; then
    input_errors+=("nested route/fee signoff primary_route_stable must be boolean token, got: ${route_fee_primary_route_stable_raw:-<empty>}")
    route_fee_primary_route_stable="unknown"
  fi
  route_fee_stable_primary_route="$(trim_string "$(extract_field "stable_primary_route" "$route_fee_signoff_output")")"
  route_fee_fallback_route_stable_raw="$(trim_string "$(extract_field "fallback_route_stable" "$route_fee_signoff_output")")"
  if ! route_fee_fallback_route_stable="$(extract_bool_field_strict "fallback_route_stable" "$route_fee_signoff_output")"; then
    input_errors+=("nested route/fee signoff fallback_route_stable must be boolean token, got: ${route_fee_fallback_route_stable_raw:-<empty>}")
    route_fee_fallback_route_stable="unknown"
  fi
  route_fee_stable_fallback_route="$(trim_string "$(extract_field "stable_fallback_route" "$route_fee_signoff_output")")"
  route_fee_route_profile_pass_count="$(trim_string "$(extract_field "route_profile_pass_count" "$route_fee_signoff_output")")"
  route_fee_fee_decomposition_pass_count="$(trim_string "$(extract_field "fee_decomposition_pass_count" "$route_fee_signoff_output")")"
  route_fee_window_count="$(trim_string "$(extract_field "window_count" "$route_fee_signoff_output")")"
  if [[ "$route_fee_signoff_verdict" == "UNKNOWN" ]]; then
    route_fee_signoff_reason="unable to classify route/fee signoff verdict (exit=$route_fee_signoff_exit_code)"
    route_fee_signoff_reason_code="unknown_verdict"
  elif [[ -z "$route_fee_signoff_reason" ]]; then
    route_fee_signoff_reason="execution route/fee signoff helper reported $route_fee_signoff_verdict"
    route_fee_signoff_reason_code="missing_reason"
  fi
fi

if [[ -n "$route_fee_signoff_test_verdict_override_raw" ]]; then
  if [[ "$route_fee_signoff_go_nogo_test_mode" == "true" ]]; then
    route_fee_signoff_verdict="$(normalize_go_nogo_verdict "$route_fee_signoff_test_verdict_override_raw")"
    route_fee_signoff_reason="test override active (ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE=${route_fee_signoff_test_verdict_override_raw})"
    route_fee_signoff_reason_code="test_override"
  else
    input_errors+=("ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE requires ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE=true")
  fi
fi

adapter_rollout_verdict="NO_GO"
adapter_rollout_reason="unrecognized rollout gate state"
adapter_rollout_reason_code="unrecognized_state"
if ((${#input_errors[@]} > 0)); then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="${input_errors[0]}"
  adapter_rollout_reason_code="input_error"
elif [[ "$rotation_verdict" == "FAIL" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="rotation readiness failed: ${rotation_reason:-n/a}"
  adapter_rollout_reason_code="rotation_fail"
elif [[ "$rotation_verdict" == "UNKNOWN" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="rotation readiness verdict unknown; fail-closed"
  adapter_rollout_reason_code="rotation_unknown"
elif [[ "$rehearsal_verdict" == "NO_GO" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="devnet rehearsal returned NO_GO: ${rehearsal_reason:-n/a}"
  adapter_rollout_reason_code="rehearsal_no_go"
elif [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="devnet rehearsal verdict unknown; fail-closed"
  adapter_rollout_reason_code="rehearsal_unknown"
elif [[ "$route_fee_signoff_required" == "true" && "$route_fee_signoff_verdict" == "UNKNOWN" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="required route/fee signoff verdict unknown; fail-closed"
  adapter_rollout_reason_code="route_fee_signoff_unknown"
elif [[ "$route_fee_signoff_required" == "true" && "$route_fee_signoff_verdict" == "NO_GO" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="required route/fee signoff returned NO_GO: ${route_fee_signoff_reason:-n/a}"
  adapter_rollout_reason_code="route_fee_signoff_no_go"
elif [[ "$rotation_verdict" == "WARN" || "$rehearsal_verdict" == "HOLD" || ( "$route_fee_signoff_required" == "true" && "$route_fee_signoff_verdict" == "HOLD" ) ]]; then
  adapter_rollout_verdict="HOLD"
  if [[ "$rotation_verdict" == "WARN" ]]; then
    adapter_rollout_reason="rotation readiness returned WARN: ${rotation_reason:-n/a}"
    adapter_rollout_reason_code="rotation_warn"
  elif [[ "$rehearsal_verdict" == "HOLD" ]]; then
    adapter_rollout_reason="devnet rehearsal returned HOLD: ${rehearsal_reason:-n/a}"
    adapter_rollout_reason_code="rehearsal_hold"
  else
    adapter_rollout_reason="required route/fee signoff returned HOLD: ${route_fee_signoff_reason:-n/a}"
    adapter_rollout_reason_code="route_fee_signoff_hold"
  fi
elif [[ "$rotation_verdict" == "PASS" && "$rehearsal_verdict" == "GO" && ( "$route_fee_signoff_required" != "true" || "$route_fee_signoff_verdict" == "GO" ) ]]; then
  adapter_rollout_verdict="GO"
  if [[ "$route_fee_signoff_required" == "true" ]]; then
    adapter_rollout_reason="rotation readiness, devnet rehearsal, and required route/fee signoff gates passed"
    adapter_rollout_reason_code="gates_pass_with_route_fee"
  else
    adapter_rollout_reason="rotation readiness and devnet rehearsal gates passed"
    adapter_rollout_reason_code="gates_pass"
  fi
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

summary_output="$(cat <<EOF
=== Adapter Rollout Evidence Summary ===
utc_now: $timestamp_utc
adapter_env: $ADAPTER_ENV_PATH
config: $CONFIG_PATH
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES

rotation_readiness_verdict: $rotation_verdict
rotation_readiness_reason: ${rotation_reason:-n/a}
rotation_exit_code: $rotation_exit_code
rotation_artifact_report: ${rotation_artifact_report:-n/a}
rotation_artifact_manifest: ${rotation_artifact_manifest:-n/a}
rotation_report_sha256: ${rotation_report_sha256:-n/a}
rotation_artifacts_written: $rotation_artifacts_written

devnet_rehearsal_verdict: $rehearsal_verdict
devnet_rehearsal_reason: ${rehearsal_reason:-n/a}
devnet_rehearsal_reason_code: ${rehearsal_reason_code:-n/a}
devnet_rehearsal_exit_code: $rehearsal_exit_code
preflight_verdict: ${preflight_verdict:-unknown}
overall_go_nogo_verdict: ${go_nogo_verdict:-unknown}
overall_go_nogo_reason_code: ${go_nogo_reason_code:-n/a}
go_nogo_require_jito_rpc_policy: ${go_nogo_require_jito_rpc_policy:-false}
jito_rpc_policy_verdict: ${jito_rpc_policy_verdict:-unknown}
jito_rpc_policy_reason: ${jito_rpc_policy_reason:-n/a}
jito_rpc_policy_reason_code: ${jito_rpc_policy_reason_code:-n/a}
go_nogo_require_fastlane_disabled: ${go_nogo_require_fastlane_disabled:-false}
submit_fastlane_enabled: ${submit_fastlane_enabled:-false}
fastlane_feature_flag_verdict: ${fastlane_feature_flag_verdict:-unknown}
fastlane_feature_flag_reason: ${fastlane_feature_flag_reason:-n/a}
fastlane_feature_flag_reason_code: ${fastlane_feature_flag_reason_code:-n/a}
route_fee_signoff_required: $route_fee_signoff_required
route_fee_signoff_verdict: ${route_fee_signoff_verdict:-unknown}
route_fee_signoff_reason: ${route_fee_signoff_reason:-n/a}
route_fee_signoff_reason_code: ${route_fee_signoff_reason_code:-n/a}
route_fee_signoff_exit_code: ${route_fee_signoff_exit_code:-3}
route_fee_signoff_windows_csv: ${route_fee_signoff_windows_csv:-n/a}
route_fee_signoff_artifact_manifest: ${route_fee_signoff_artifact_manifest:-n/a}
route_fee_signoff_summary_sha256: ${route_fee_signoff_summary_sha256:-n/a}
route_fee_signoff_artifacts_written: $route_fee_signoff_artifacts_written
route_fee_signoff_nested_package_bundle_enabled: ${route_fee_signoff_nested_package_bundle_enabled:-unknown}
route_fee_primary_route_stable: ${route_fee_primary_route_stable:-false}
route_fee_stable_primary_route: ${route_fee_stable_primary_route:-n/a}
route_fee_fallback_route_stable: ${route_fee_fallback_route_stable:-false}
route_fee_stable_fallback_route: ${route_fee_stable_fallback_route:-n/a}
route_fee_route_profile_pass_count: ${route_fee_route_profile_pass_count:-n/a}
route_fee_fee_decomposition_pass_count: ${route_fee_fee_decomposition_pass_count:-n/a}
route_fee_window_count: ${route_fee_window_count:-n/a}
dynamic_cu_policy_verdict: ${dynamic_cu_policy_verdict:-unknown}
dynamic_cu_policy_reason: ${dynamic_cu_policy_reason:-n/a}
dynamic_tip_policy_verdict: ${dynamic_tip_policy_verdict:-unknown}
dynamic_tip_policy_reason: ${dynamic_tip_policy_reason:-n/a}
windowed_signoff_required: ${windowed_signoff_required:-false}
windowed_signoff_windows_csv: ${windowed_signoff_windows_csv:-n/a}
windowed_signoff_require_dynamic_hint_source_pass: ${windowed_signoff_require_dynamic_hint_source_pass:-false}
windowed_signoff_require_dynamic_tip_policy_pass: ${windowed_signoff_require_dynamic_tip_policy_pass:-false}
windowed_signoff_verdict: ${windowed_signoff_verdict:-unknown}
windowed_signoff_reason: ${windowed_signoff_reason:-n/a}
dynamic_cu_hint_api_total: ${dynamic_cu_hint_api_total:-n/a}
dynamic_cu_hint_rpc_total: ${dynamic_cu_hint_rpc_total:-n/a}
dynamic_cu_hint_api_configured: ${dynamic_cu_hint_api_configured:-false}
dynamic_cu_hint_source_verdict: ${dynamic_cu_hint_source_verdict:-unknown}
dynamic_cu_hint_source_reason: ${dynamic_cu_hint_source_reason:-n/a}
dynamic_cu_hint_source_reason_code: ${dynamic_cu_hint_source_reason_code:-n/a}
primary_route: ${primary_route:-n/a}
fallback_route: ${fallback_route:-n/a}
primary_attempted_orders: ${primary_attempted_orders:-n/a}
primary_success_rate_pct: ${primary_success_rate_pct:-n/a}
primary_timeout_rate_pct: ${primary_timeout_rate_pct:-n/a}
fallback_attempted_orders: ${fallback_attempted_orders:-n/a}
fallback_success_rate_pct: ${fallback_success_rate_pct:-n/a}
fallback_timeout_rate_pct: ${fallback_timeout_rate_pct:-n/a}
confirmed_orders_total: ${confirmed_orders_total:-n/a}
fee_consistency_missing_coverage_rows: ${fee_consistency_missing_coverage_rows:-n/a}
fee_consistency_mismatch_rows: ${fee_consistency_mismatch_rows:-n/a}
fallback_used_events: ${fallback_used_events:-n/a}
hint_mismatch_events: ${hint_mismatch_events:-n/a}
go_nogo_artifact_manifest: ${go_nogo_artifact_manifest:-n/a}
go_nogo_calibration_sha256: ${go_nogo_calibration_sha256:-n/a}
go_nogo_snapshot_sha256: ${go_nogo_snapshot_sha256:-n/a}
go_nogo_preflight_sha256: ${go_nogo_preflight_sha256:-n/a}
go_nogo_summary_sha256: ${go_nogo_summary_sha256:-n/a}
go_nogo_artifacts_written: $go_nogo_artifacts_written
rehearsal_artifact_manifest: ${rehearsal_artifact_manifest:-n/a}
rehearsal_summary_sha256: ${rehearsal_summary_sha256:-n/a}
rehearsal_preflight_sha256: ${rehearsal_preflight_sha256:-n/a}
rehearsal_go_nogo_sha256: ${rehearsal_go_nogo_sha256:-n/a}
rehearsal_tests_sha256: ${rehearsal_tests_sha256:-n/a}
rehearsal_artifacts_written: $rehearsal_artifacts_written
rehearsal_nested_package_bundle_enabled: ${rehearsal_nested_package_bundle_enabled:-unknown}
windowed_signoff_artifact_manifest: ${windowed_signoff_artifact_manifest:-n/a}
windowed_signoff_summary_sha256: ${windowed_signoff_summary_sha256:-n/a}
windowed_signoff_artifacts_written: $windowed_signoff_artifacts_written
rehearsal_route_fee_signoff_required: ${rehearsal_route_fee_signoff_required:-false}
rehearsal_route_fee_signoff_windows_csv: ${rehearsal_route_fee_signoff_windows_csv:-n/a}
rehearsal_route_fee_signoff_verdict: ${rehearsal_route_fee_signoff_verdict:-unknown}
rehearsal_route_fee_signoff_reason: ${rehearsal_route_fee_signoff_reason:-n/a}
rehearsal_route_fee_signoff_reason_code: ${rehearsal_route_fee_signoff_reason_code:-n/a}
rehearsal_route_fee_signoff_exit_code: ${rehearsal_route_fee_signoff_exit_code:-3}
rehearsal_route_fee_signoff_artifact_manifest: ${rehearsal_route_fee_signoff_artifact_manifest:-n/a}
rehearsal_route_fee_signoff_summary_sha256: ${rehearsal_route_fee_signoff_summary_sha256:-n/a}
rehearsal_route_fee_signoff_artifacts_written: ${rehearsal_route_fee_signoff_artifacts_written:-false}
rehearsal_route_fee_primary_route_stable: ${rehearsal_route_fee_primary_route_stable:-false}
rehearsal_route_fee_stable_primary_route: ${rehearsal_route_fee_stable_primary_route:-n/a}
rehearsal_route_fee_fallback_route_stable: ${rehearsal_route_fee_fallback_route_stable:-false}
rehearsal_route_fee_stable_fallback_route: ${rehearsal_route_fee_stable_fallback_route:-n/a}
rehearsal_route_fee_route_profile_pass_count: ${rehearsal_route_fee_route_profile_pass_count:-n/a}
rehearsal_route_fee_fee_decomposition_pass_count: ${rehearsal_route_fee_fee_decomposition_pass_count:-n/a}
rehearsal_route_fee_window_count: ${rehearsal_route_fee_window_count:-n/a}
tests_run: ${tests_run:-unknown}
tests_failed: ${tests_failed:-unknown}
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}
input_error_count: ${#input_errors[@]}

adapter_rollout_verdict: $adapter_rollout_verdict
adapter_rollout_reason: $adapter_rollout_reason
adapter_rollout_reason_code: $adapter_rollout_reason_code
artifacts_written: $artifacts_written
EOF
)"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/adapter_rollout_evidence_summary_${timestamp_compact}.txt"
  rotation_capture_path="$OUTPUT_DIR/adapter_secret_rotation_captured_${timestamp_compact}.txt"
  rehearsal_capture_path="$OUTPUT_DIR/execution_devnet_rehearsal_captured_${timestamp_compact}.txt"
  route_fee_signoff_capture_path="$OUTPUT_DIR/execution_route_fee_signoff_captured_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/adapter_rollout_evidence_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" >"$summary_path"
  printf '%s\n' "$rotation_output" >"$rotation_capture_path"
  printf '%s\n' "$rehearsal_output" >"$rehearsal_capture_path"
  printf '%s\n' "$route_fee_signoff_output" >"$route_fee_signoff_capture_path"

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
  rehearsal_capture_sha256="$(sha256_file_value "$rehearsal_capture_path")"
  route_fee_signoff_capture_sha256="$(sha256_file_value "$route_fee_signoff_capture_path")"

  cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
rotation_capture_sha256: $rotation_capture_sha256
rehearsal_capture_sha256: $rehearsal_capture_sha256
route_fee_signoff_capture_sha256: $route_fee_signoff_capture_sha256
EOF
  manifest_sha256="$(sha256_file_value "$manifest_path")"

  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_rotation_capture: $rotation_capture_path"
  echo "artifact_rehearsal_capture: $rehearsal_capture_path"
  echo "artifact_route_fee_signoff_capture: $route_fee_signoff_capture_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "rotation_capture_sha256: $rotation_capture_sha256"
  echo "rehearsal_capture_sha256: $rehearsal_capture_sha256"
  echo "route_fee_signoff_capture_sha256: $route_fee_signoff_capture_sha256"
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

case "$adapter_rollout_verdict" in
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
