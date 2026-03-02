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
OUTPUT_ROOT="${OUTPUT_ROOT:-state/executor-final-$(date -u +"%Y%m%dT%H%M%SZ")}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-executor_final_evidence}"
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

parse_final_bool_setting_into() {
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

parse_final_bool_setting_into "RUN_TESTS" "$RUN_TESTS" run_tests_norm
parse_final_bool_setting_into "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE" devnet_rehearsal_test_mode_norm
parse_final_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_final_bool_setting_into "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED" windowed_signoff_required_norm
parse_final_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass_norm
parse_final_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass_norm
parse_final_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy_norm
parse_final_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled_norm
parse_final_bool_setting_into "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED" route_fee_signoff_required_norm
parse_final_bool_setting_into "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" route_fee_signoff_go_nogo_test_mode_norm
parse_final_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm

mkdir -p "$OUTPUT_ROOT"
rollout_output_dir="$OUTPUT_ROOT/rollout"
mkdir -p "$rollout_output_dir"

rollout_output=""
rollout_exit_code=3
rollout_verdict="UNKNOWN"
rollout_reason="executor rollout helper not executed"
rollout_reason_code="not_executed"
rollout_artifacts_written="false"
rollout_artifact_summary="n/a"
rollout_artifact_manifest="n/a"
rollout_summary_sha256="n/a"
rollout_nested_package_bundle_enabled="unknown"
if ((${#input_errors[@]} == 0)); then
  if rollout_output="$(
    EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      OUTPUT_DIR="$rollout_output_dir" \
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
      bash "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    rollout_exit_code=0
  else
    rollout_exit_code=$?
  fi
  rollout_verdict="$(normalize_go_nogo_verdict "$(extract_field "executor_rollout_verdict" "$rollout_output")")"
  rollout_reason="$(trim_string "$(extract_field "executor_rollout_reason" "$rollout_output")")"
  rollout_reason_code="$(trim_string "$(extract_field "executor_rollout_reason_code" "$rollout_output")")"
  rollout_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$rollout_output")")"
  if ! rollout_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$rollout_output")"; then
    input_errors+=("nested executor rollout artifacts_written must be boolean token, got: ${rollout_artifacts_written_raw:-<empty>}")
    rollout_artifacts_written="unknown"
  fi
  rollout_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$rollout_output")")"
  rollout_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rollout_output")")"
  rollout_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$rollout_output")")"
  rollout_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$rollout_output")")"
  if ! rollout_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$rollout_output")"; then
    input_errors+=("nested executor rollout package_bundle_enabled must be boolean token, got: ${rollout_nested_package_bundle_enabled_raw:-<empty>}")
    rollout_nested_package_bundle_enabled="unknown"
  elif [[ "$rollout_nested_package_bundle_enabled" != "false" ]]; then
    input_errors+=("nested executor rollout helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi

  if [[ "$rollout_verdict" == "UNKNOWN" ]]; then
    rollout_reason="unable to classify executor rollout verdict (exit=$rollout_exit_code)"
    rollout_reason_code="unknown_verdict"
  elif [[ -z "$rollout_reason" ]]; then
    rollout_reason="executor rollout helper reported $rollout_verdict"
    rollout_reason_code="missing_reason"
  fi
  if [[ -z "$rollout_reason_code" ]]; then
    rollout_reason_code="n/a"
  fi
elif ((${#input_errors[@]} > 0)); then
  rollout_exit_code=3
  rollout_verdict="NO_GO"
  rollout_reason="${input_errors[0]}"
  rollout_reason_code="input_error"
fi

if ((${#input_errors[@]} > 0)); then
  rollout_verdict="NO_GO"
  rollout_reason="${input_errors[0]}"
  rollout_reason_code="input_error"
fi

rollout_capture_path="$OUTPUT_ROOT/executor_rollout_evidence_captured_${timestamp_compact}.txt"
printf '%s\n' "$rollout_output" >"$rollout_capture_path"
rollout_capture_sha256="$(sha256_file_value "$rollout_capture_path")"

rollout_artifact_summary_sha256="n/a"
if [[ -n "$rollout_artifact_summary" && "$rollout_artifact_summary" != "n/a" && -f "$rollout_artifact_summary" ]]; then
  rollout_artifact_summary_sha256="$(sha256_file_value "$rollout_artifact_summary")"
fi

rollout_artifact_manifest_sha256="n/a"
if [[ -n "$rollout_artifact_manifest" && "$rollout_artifact_manifest" != "n/a" && -f "$rollout_artifact_manifest" ]]; then
  rollout_artifact_manifest_sha256="$(sha256_file_value "$rollout_artifact_manifest")"
fi

summary_output="=== Executor Final Evidence Package ===
utc_now: $timestamp_utc
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES
executor_env: $EXECUTOR_ENV_PATH
adapter_env: $ADAPTER_ENV_PATH
config: $CONFIG_PATH
output_root: $OUTPUT_ROOT
rollout_output_dir: $rollout_output_dir
run_tests: $RUN_TESTS
devnet_rehearsal_test_mode: $devnet_rehearsal_test_mode_norm
go_nogo_test_mode: $go_nogo_test_mode_norm
windowed_signoff_required: $windowed_signoff_required_norm
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass_norm
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass_norm
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy_norm
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled_norm
route_fee_signoff_required: $route_fee_signoff_required_norm
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: $PACKAGE_BUNDLE_OUTPUT_DIR
input_error_count: ${#input_errors[@]}
rollout_exit_code: $rollout_exit_code
rollout_verdict: $rollout_verdict
rollout_reason: ${rollout_reason:-n/a}
rollout_reason_code: ${rollout_reason_code:-n/a}
rollout_artifacts_written: $rollout_artifacts_written
rollout_artifact_summary: ${rollout_artifact_summary:-n/a}
rollout_artifact_manifest: ${rollout_artifact_manifest:-n/a}
rollout_summary_sha256: ${rollout_summary_sha256:-n/a}
rollout_artifact_summary_sha256: $rollout_artifact_summary_sha256
rollout_artifact_manifest_sha256: $rollout_artifact_manifest_sha256
rollout_nested_package_bundle_enabled: ${rollout_nested_package_bundle_enabled:-unknown}
final_executor_package_verdict: $rollout_verdict
final_executor_package_reason: ${rollout_reason:-n/a}
final_executor_package_reason_code: ${rollout_reason_code:-n/a}
artifacts_written: true"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

summary_path="$OUTPUT_ROOT/executor_final_evidence_summary_${timestamp_compact}.txt"
manifest_path="$OUTPUT_ROOT/executor_final_evidence_manifest_${timestamp_compact}.txt"
printf '%s\n' "$summary_output" >"$summary_path"

echo
echo "artifacts_written: true"
echo "artifact_summary: $summary_path"
echo "artifact_rollout_capture: $rollout_capture_path"
echo "artifact_manifest: $manifest_path"
echo "rollout_capture_sha256: $rollout_capture_sha256"

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
  # First pass resolves bundle status used by summary artifact.
  run_package_bundle_once
fi

cat >>"$summary_path" <<EOF
package_bundle_artifacts_written: $package_bundle_artifacts_written
package_bundle_exit_code: $package_bundle_exit_code
package_bundle_error: $package_bundle_error
EOF
summary_sha256="$(sha256_file_value "$summary_path")"
cat >"$manifest_path" <<EOF_MANIFEST
summary_sha256: $summary_sha256
rollout_capture_sha256: $rollout_capture_sha256
rollout_summary_sha256: ${rollout_summary_sha256:-n/a}
rollout_artifact_summary_sha256: $rollout_artifact_summary_sha256
rollout_artifact_manifest_sha256: $rollout_artifact_manifest_sha256
EOF_MANIFEST

manifest_sha256="$(sha256_file_value "$manifest_path")"

if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
  # Second pass packages finalized summary+manifest payload.
  run_package_bundle_once
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

case "$rollout_verdict" in
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
