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
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_server_rollout}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_ROOT}"

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

parse_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_bool_setting_into "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" route_fee_signoff_go_nogo_test_mode_norm
parse_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" "$REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" rehearsal_route_fee_signoff_required_norm
parse_bool_setting_into "REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" rehearsal_route_fee_signoff_go_nogo_test_mode_norm
parse_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm

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

rehearsal_output=""
rehearsal_exit_code=3
rehearsal_verdict="UNKNOWN"
rehearsal_reason="not executed"
rehearsal_reason_code="not_executed"
rehearsal_summary_sha256="n/a"
rehearsal_capture="$step_root/rehearsal_capture_${now_compact}.txt"

executor_final_output=""
executor_final_exit_code=3
executor_final_verdict="UNKNOWN"
executor_final_reason="not executed"
executor_final_reason_code="not_executed"
executor_final_summary_sha256="n/a"
executor_final_capture="$step_root/executor_final_capture_${now_compact}.txt"

adapter_final_output=""
adapter_final_exit_code=3
adapter_final_verdict="UNKNOWN"
adapter_final_reason="not executed"
adapter_final_reason_code="not_executed"
adapter_final_summary_sha256="n/a"
adapter_final_capture="$step_root/adapter_final_capture_${now_compact}.txt"

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

  go_nogo_output_dir="$step_root/go_nogo"
  mkdir -p "$go_nogo_output_dir"
  if go_nogo_output="$(
    DB_PATH="${DB_PATH:-}" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_DIR="$go_nogo_output_dir" \
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

  rehearsal_output_dir="$step_root/rehearsal"
  mkdir -p "$rehearsal_output_dir"
  if rehearsal_output="$(
    DB_PATH="${DB_PATH:-}" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_DIR="$rehearsal_output_dir" \
      RUN_TESTS="$run_tests_norm" \
      DEVNET_REHEARSAL_TEST_MODE="$devnet_rehearsal_test_mode_norm" \
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
fi

printf '%s\n' "$preflight_output" >"$preflight_capture"
printf '%s\n' "$calibration_output" >"$calibration_capture"
printf '%s\n' "$go_nogo_output" >"$go_nogo_capture"
printf '%s\n' "$rehearsal_output" >"$rehearsal_capture"
printf '%s\n' "$executor_final_output" >"$executor_final_capture"
printf '%s\n' "$adapter_final_output" >"$adapter_final_capture"

preflight_capture_sha256="$(sha256_file_value "$preflight_capture")"
calibration_capture_sha256="$(sha256_file_value "$calibration_capture")"
go_nogo_capture_sha256="$(sha256_file_value "$go_nogo_capture")"
rehearsal_capture_sha256="$(sha256_file_value "$rehearsal_capture")"
executor_final_capture_sha256="$(sha256_file_value "$executor_final_capture")"
adapter_final_capture_sha256="$(sha256_file_value "$adapter_final_capture")"

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

  if [[ "$overall_go_nogo_verdict" == "NO_GO" || "$overall_go_nogo_verdict" == "UNKNOWN" ]]; then
    set_no_go "go/no-go stage not GO (${overall_go_nogo_verdict}): ${overall_go_nogo_reason:-n/a}" "go_nogo_not_go"
  elif [[ "$overall_go_nogo_verdict" == "HOLD" ]]; then
    set_hold_if_go "go/no-go stage HOLD: ${overall_go_nogo_reason:-n/a}" "go_nogo_hold"
  fi

  if [[ "$rehearsal_verdict" == "NO_GO" || "$rehearsal_verdict" == "UNKNOWN" ]]; then
    set_no_go "devnet rehearsal not GO (${rehearsal_verdict}): ${rehearsal_reason:-n/a}" "rehearsal_not_go"
  elif [[ "$rehearsal_verdict" == "HOLD" ]]; then
    set_hold_if_go "devnet rehearsal HOLD: ${rehearsal_reason:-n/a}" "rehearsal_hold"
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
route_fee_signoff_required: $route_fee_signoff_required_norm
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
rehearsal_route_fee_signoff_required: $rehearsal_route_fee_signoff_required_norm
rehearsal_route_fee_signoff_windows_csv: $REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV
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
go_nogo_summary_sha256: ${go_nogo_summary_sha256:-n/a}
rehearsal_exit_code: $rehearsal_exit_code
rehearsal_verdict: $rehearsal_verdict
rehearsal_reason: ${rehearsal_reason:-n/a}
rehearsal_reason_code: ${rehearsal_reason_code:-n/a}
rehearsal_summary_sha256: ${rehearsal_summary_sha256:-n/a}
executor_final_exit_code: $executor_final_exit_code
executor_final_verdict: $executor_final_verdict
executor_final_reason: ${executor_final_reason:-n/a}
executor_final_reason_code: ${executor_final_reason_code:-n/a}
executor_final_summary_sha256: ${executor_final_summary_sha256:-n/a}
adapter_final_exit_code: $adapter_final_exit_code
adapter_final_verdict: $adapter_final_verdict
adapter_final_reason: ${adapter_final_reason:-n/a}
adapter_final_reason_code: ${adapter_final_reason_code:-n/a}
adapter_final_summary_sha256: ${adapter_final_summary_sha256:-n/a}
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
echo "preflight_capture_sha256: $preflight_capture_sha256"
echo "calibration_capture_sha256: $calibration_capture_sha256"
echo "go_nogo_capture_sha256: $go_nogo_capture_sha256"
echo "rehearsal_capture_sha256: $rehearsal_capture_sha256"
echo "executor_final_capture_sha256: $executor_final_capture_sha256"
echo "adapter_final_capture_sha256: $adapter_final_capture_sha256"

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
    package_bundle_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$package_bundle_output")")"
    package_bundle_path="$(trim_string "$(extract_field "bundle_path" "$package_bundle_output")")"
    package_bundle_sha256="$(trim_string "$(extract_field "bundle_sha256" "$package_bundle_output")")"
    package_bundle_sha256_path="$(trim_string "$(extract_field "bundle_sha256_path" "$package_bundle_output")")"
    package_bundle_contents_manifest="$(trim_string "$(extract_field "contents_manifest" "$package_bundle_output")")"
    package_bundle_file_count="$(trim_string "$(extract_field "file_count" "$package_bundle_output")")"
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
