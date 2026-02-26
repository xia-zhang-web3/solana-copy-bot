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
ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-false}"
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

parse_bool_token_strict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    0|false|no|off)
      printf 'false'
      ;;
    *)
      return 1
      ;;
  esac
}

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
if [[ -f "$EXECUTOR_ENV_PATH" ]]; then
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
  rotation_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$rotation_output")")"
  if [[ "$rotation_verdict" == "UNKNOWN" ]]; then
    rotation_reason="unable to classify rotation helper verdict (exit=$rotation_exit_code)"
  elif [[ -z "$rotation_reason" ]]; then
    rotation_reason="rotation helper reported $rotation_verdict"
  fi
else
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
if [[ -f "$CONFIG_PATH" && -f "$EXECUTOR_ENV_PATH" && -f "$ADAPTER_ENV_PATH" ]]; then
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
  preflight_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$preflight_output")")"
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
tests_run=""
tests_failed=""
if [[ ! "$WINDOW_HOURS" =~ ^[0-9]+$ || ! "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
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
  rehearsal_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$rehearsal_output")")"
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
elif [[ "$rotation_verdict" == "FAIL" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="signer rotation readiness failed: ${rotation_reason:-n/a}"
  executor_rollout_reason_code="rotation_fail"
elif [[ "$rotation_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="signer rotation readiness verdict unknown; fail-closed"
  executor_rollout_reason_code="rotation_unknown"
elif [[ "$preflight_verdict" == "FAIL" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="executor preflight failed: ${preflight_reason:-n/a}"
  executor_rollout_reason_code="preflight_fail"
elif [[ "$preflight_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="executor preflight verdict unknown; fail-closed"
  executor_rollout_reason_code="preflight_unknown"
elif [[ "$rehearsal_verdict" == "NO_GO" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="devnet rehearsal returned NO_GO: ${rehearsal_reason:-n/a}"
  executor_rollout_reason_code="rehearsal_no_go"
elif [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
  executor_rollout_verdict="NO_GO"
  executor_rollout_reason="devnet rehearsal verdict unknown; fail-closed"
  executor_rollout_reason_code="rehearsal_unknown"
elif [[ "$rotation_verdict" == "WARN" || "$preflight_verdict" == "SKIP" || "$rehearsal_verdict" == "HOLD" ]]; then
  executor_rollout_verdict="HOLD"
  if [[ "$rotation_verdict" == "WARN" ]]; then
    executor_rollout_reason="signer rotation readiness returned WARN: ${rotation_reason:-n/a}"
    executor_rollout_reason_code="rotation_warn"
  elif [[ "$preflight_verdict" == "SKIP" ]]; then
    executor_rollout_reason="executor preflight returned SKIP: ${preflight_reason:-n/a}"
    executor_rollout_reason_code="preflight_skip"
  else
    executor_rollout_reason="devnet rehearsal returned HOLD: ${rehearsal_reason:-n/a}"
    executor_rollout_reason_code="rehearsal_hold"
  fi
elif [[ "$rotation_verdict" == "PASS" && "$preflight_verdict" == "PASS" && "$rehearsal_verdict" == "GO" ]]; then
  executor_rollout_verdict="GO"
  executor_rollout_reason="signer rotation readiness, executor preflight, and devnet rehearsal gates passed"
  executor_rollout_reason_code="gates_pass"
fi

artifacts_written="false"
if [[ -n "$OUTPUT_DIR" ]]; then
  artifacts_written="true"
fi

summary_output="$(cat <<EOF_SUMMARY
=== Executor Rollout Evidence Summary ===
utc_now: $timestamp_utc
executor_env: $EXECUTOR_ENV_PATH
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
