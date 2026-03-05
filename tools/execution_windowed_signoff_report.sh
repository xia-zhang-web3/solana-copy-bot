#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOWS_CSV="${1:-1,6,24}"
RISK_EVENTS_MINUTES="${2:-60}"
SERVICE="${SERVICE:-solana-copy-bot}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-false}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-false}"
GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="${GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM:-true}"
GO_NOGO_REQUIRE_INGESTION_GRPC="${GO_NOGO_REQUIRE_INGESTION_GRPC:-false}"
GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="${GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER:-false}"
EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-false}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_windowed_signoff}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_DIR}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

declare -a input_errors=()
declare -a windows=()

parse_windowed_bool_setting_into() {
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

parse_windowed_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy
parse_windowed_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled
parse_windowed_bool_setting_into "GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" "$GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" go_nogo_require_executor_upstream
parse_windowed_bool_setting_into "GO_NOGO_REQUIRE_INGESTION_GRPC" "$GO_NOGO_REQUIRE_INGESTION_GRPC" go_nogo_require_ingestion_grpc
parse_windowed_bool_setting_into "GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" "$GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" go_nogo_require_non_bootstrap_signer
parse_windowed_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" windowed_signoff_require_dynamic_hint_source_pass
parse_windowed_bool_setting_into "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" windowed_signoff_require_dynamic_tip_policy_pass
parse_windowed_bool_setting_into "GO_NOGO_TEST_MODE" "${GO_NOGO_TEST_MODE:-false}" go_nogo_test_mode_norm
parse_windowed_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm

if [[ "$package_bundle_enabled_norm" == "true" && -z "$OUTPUT_DIR" ]]; then
  input_errors+=("PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set")
fi

if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  input_errors+=("risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)")
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  input_errors+=("config file not found: $CONFIG_PATH")
fi

contains_window() {
  local needle="$1"
  local item
  for item in "${windows[@]-}"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

IFS=',' read -ra raw_windows <<< "$WINDOWS_CSV"
for raw_token in "${raw_windows[@]-}"; do
  token="$(trim_string "$raw_token")"
  if [[ -z "$token" ]]; then
    continue
  fi
  if ! [[ "$token" =~ ^[0-9]+$ ]]; then
    input_errors+=("window token must be an integer (got: $token)")
    continue
  fi
  if [[ "$token" == "0" ]]; then
    input_errors+=("window token must be > 0 (got: $token)")
    continue
  fi
  if ! contains_window "$token"; then
    windows+=("$token")
  fi
done

if ((${#windows[@]} == 0)); then
  input_errors+=("no valid windows parsed from WINDOWS_CSV=$WINDOWS_CSV")
fi

declare -a window_ids=()
declare -a window_go_nogo_exit_codes=()
declare -a window_overall_verdicts=()
declare -a window_overall_reason_codes=()
declare -a window_fee_verdicts=()
declare -a window_route_verdicts=()
declare -a window_primary_routes=()
declare -a window_fallback_routes=()
declare -a window_confirmed_orders=()
declare -a window_fee_missing=()
declare -a window_fee_mismatch=()
declare -a window_fallback_used=()
declare -a window_hint_mismatch=()
declare -a window_dynamic_policy_config_enabled=()
declare -a window_dynamic_hint_source_verdicts=()
declare -a window_dynamic_hint_source_reasons=()
declare -a window_dynamic_tip_policy_config_enabled=()
declare -a window_dynamic_tip_policy_verdicts=()
declare -a window_dynamic_tip_policy_reasons=()
declare -a window_jito_rpc_policy_verdicts=()
declare -a window_jito_rpc_policy_reasons=()
declare -a window_jito_rpc_policy_reason_codes=()
declare -a window_fastlane_feature_flag_verdicts=()
declare -a window_fastlane_feature_flag_reasons=()
declare -a window_fastlane_feature_flag_reason_codes=()
declare -a window_executor_backend_mode_guard_verdicts=()
declare -a window_executor_backend_mode_guard_reason_codes=()
declare -a window_executor_upstream_endpoint_guard_verdicts=()
declare -a window_executor_upstream_endpoint_guard_reason_codes=()
declare -a window_go_nogo_require_ingestion_grpc=()
declare -a window_ingestion_grpc_guard_verdicts=()
declare -a window_ingestion_grpc_guard_reason_codes=()
declare -a window_go_nogo_require_non_bootstrap_signer=()
declare -a window_non_bootstrap_signer_guard_verdicts=()
declare -a window_non_bootstrap_signer_guard_reason_codes=()
declare -a window_go_nogo_artifacts_written=()
declare -a window_go_nogo_nested_package_bundle_enabled=()
declare -a window_go_nogo_artifact_manifests=()
declare -a window_go_nogo_calibration_sha256=()
declare -a window_go_nogo_snapshot_sha256=()
declare -a window_go_nogo_preflight_sha256=()
declare -a window_go_nogo_summary_sha256=()
declare -a window_capture_paths=()
declare -a window_capture_sha256=()

route_is_value() {
  local route="$(trim_string "$1")"
  if [[ -z "$route" ]]; then
    return 1
  fi
  case "$route" in
    n/a|N/A|unknown|UNKNOWN|"<none>")
      return 1
      ;;
  esac
  return 0
}

window_total=0
window_pass_count=0
window_unknown_count=0
window_hard_block_count=0
first_unknown_reason=""
first_non_pass_reason=""

primary_route_stable="true"
fallback_route_stable="true"
primary_route_seen="false"
fallback_route_seen="false"
stable_primary_route=""
stable_fallback_route=""

if ((${#input_errors[@]} == 0)); then
  for window_hours in "${windows[@]}"; do
    window_total=$((window_total + 1))

    go_nogo_output_dir=""
    if [[ -n "$OUTPUT_DIR" ]]; then
      go_nogo_output_dir="$OUTPUT_DIR/window_${window_hours}h/go_nogo"
      mkdir -p "$go_nogo_output_dir"
    fi

    go_nogo_output=""
    go_nogo_exit_code=0
    if go_nogo_output="$(
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled" \
      GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream" \
      GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc" \
      GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer" \
      EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
      GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
      PACKAGE_BUNDLE_ENABLED="false" \
      OUTPUT_DIR="$go_nogo_output_dir" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" "$window_hours" "$RISK_EVENTS_MINUTES" 2>&1
    )"; then
      go_nogo_exit_code=0
    else
      go_nogo_exit_code=$?
    fi

    overall_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
    overall_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$go_nogo_output")")"
    fee_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$go_nogo_output")")"
    route_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$go_nogo_output")")"
    primary_route="$(trim_string "$(extract_field "primary_route" "$go_nogo_output")")"
    fallback_route="$(trim_string "$(extract_field "fallback_route" "$go_nogo_output")")"
    confirmed_orders_total="$(trim_string "$(extract_field "confirmed_orders_total" "$go_nogo_output")")"
    fee_missing_rows="$(trim_string "$(extract_field "fee_consistency_missing_coverage_rows" "$go_nogo_output")")"
    fee_mismatch_rows="$(trim_string "$(extract_field "fee_consistency_mismatch_rows" "$go_nogo_output")")"
    fallback_used_events="$(trim_string "$(extract_field "fallback_used_events" "$go_nogo_output")")"
    hint_mismatch_events="$(trim_string "$(extract_field "hint_mismatch_events" "$go_nogo_output")")"
    go_nogo_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$go_nogo_output")")"
    if ! go_nogo_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go artifacts_written must be boolean token, got: ${go_nogo_artifacts_written_raw:-<empty>}")
      go_nogo_artifacts_written="unknown"
    elif [[ -n "$go_nogo_output_dir" && "$go_nogo_artifacts_written" != "true" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go artifacts_written must be true")
    fi
    go_nogo_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$go_nogo_output")")"
    if ! go_nogo_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go package_bundle_enabled must be boolean token, got: ${go_nogo_nested_package_bundle_enabled_raw:-<empty>}")
      go_nogo_nested_package_bundle_enabled="unknown"
    elif [[ "$go_nogo_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
    go_nogo_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$go_nogo_output")")"
    go_nogo_calibration_sha256="$(trim_string "$(extract_field "calibration_sha256" "$go_nogo_output")")"
    go_nogo_snapshot_sha256="$(trim_string "$(extract_field "snapshot_sha256" "$go_nogo_output")")"
    go_nogo_preflight_sha256="$(trim_string "$(extract_field "preflight_sha256" "$go_nogo_output")")"
    go_nogo_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$go_nogo_output")")"
    dynamic_policy_config_enabled_raw="$(trim_string "$(extract_field "dynamic_cu_policy_config_enabled" "$go_nogo_output")")"
    if ! dynamic_policy_config_enabled="$(extract_bool_field_strict "dynamic_cu_policy_config_enabled" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go dynamic_cu_policy_config_enabled must be boolean token, got: ${dynamic_policy_config_enabled_raw:-<empty>}")
      dynamic_policy_config_enabled="unknown"
    fi
    dynamic_hint_source_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_hint_source_verdict" "$go_nogo_output")")"
    dynamic_hint_source_reason="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason" "$go_nogo_output")")"
    dynamic_tip_policy_config_enabled_raw="$(trim_string "$(extract_field "dynamic_tip_policy_config_enabled" "$go_nogo_output")")"
    if ! dynamic_tip_policy_config_enabled="$(extract_bool_field_strict "dynamic_tip_policy_config_enabled" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go dynamic_tip_policy_config_enabled must be boolean token, got: ${dynamic_tip_policy_config_enabled_raw:-<empty>}")
      dynamic_tip_policy_config_enabled="unknown"
    fi
    dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$go_nogo_output")")"
    dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$go_nogo_output")")"
    jito_rpc_policy_verdict="$(normalize_gate_verdict "$(extract_field "jito_rpc_policy_verdict" "$go_nogo_output")")"
    jito_rpc_policy_reason="$(trim_string "$(extract_field "jito_rpc_policy_reason" "$go_nogo_output")")"
    jito_rpc_policy_reason_code="$(trim_string "$(extract_field "jito_rpc_policy_reason_code" "$go_nogo_output")")"
    fastlane_feature_flag_verdict="$(normalize_gate_verdict "$(extract_field "fastlane_feature_flag_verdict" "$go_nogo_output")")"
    fastlane_feature_flag_reason="$(trim_string "$(extract_field "fastlane_feature_flag_reason" "$go_nogo_output")")"
    fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "fastlane_feature_flag_reason_code" "$go_nogo_output")")"
    executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "executor_backend_mode_guard_verdict" "$go_nogo_output")")"
    executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$executor_backend_mode_guard_verdict_raw")"
    if [[ -z "$executor_backend_mode_guard_verdict_raw" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_backend_mode_guard_verdict must be non-empty")
      executor_backend_mode_guard_verdict="UNKNOWN"
    elif [[ "$executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_backend_mode_guard_verdict_raw})")
      executor_backend_mode_guard_verdict="UNKNOWN"
    fi
    executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "executor_backend_mode_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$executor_backend_mode_guard_reason_code" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_backend_mode_guard_reason_code must be non-empty")
      executor_backend_mode_guard_reason_code="n/a"
    fi
    executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_verdict" "$go_nogo_output")")"
    executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$executor_upstream_endpoint_guard_verdict_raw")"
    if [[ -z "$executor_upstream_endpoint_guard_verdict_raw" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_upstream_endpoint_guard_verdict must be non-empty")
      executor_upstream_endpoint_guard_verdict="UNKNOWN"
    elif [[ "$executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${executor_upstream_endpoint_guard_verdict_raw})")
      executor_upstream_endpoint_guard_verdict="UNKNOWN"
    fi
    executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$executor_upstream_endpoint_guard_reason_code" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go executor_upstream_endpoint_guard_reason_code must be non-empty")
      executor_upstream_endpoint_guard_reason_code="n/a"
    fi
    go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$go_nogo_output")")"
    if ! go_nogo_require_ingestion_grpc_nested="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go go_nogo_require_ingestion_grpc must be boolean token, got: ${go_nogo_require_ingestion_grpc_raw:-<empty>}")
      go_nogo_require_ingestion_grpc_nested="unknown"
    elif [[ "$go_nogo_require_ingestion_grpc_nested" != "$go_nogo_require_ingestion_grpc" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go go_nogo_require_ingestion_grpc mismatch: nested=${go_nogo_require_ingestion_grpc_nested} expected=${go_nogo_require_ingestion_grpc}")
    fi
    ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "ingestion_grpc_guard_verdict" "$go_nogo_output")")"
    ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$ingestion_grpc_guard_verdict_raw")"
    if [[ -z "$ingestion_grpc_guard_verdict_raw" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go ingestion_grpc_guard_verdict must be non-empty")
      ingestion_grpc_guard_verdict="UNKNOWN"
    elif [[ "$ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${ingestion_grpc_guard_verdict_raw})")
      ingestion_grpc_guard_verdict="UNKNOWN"
    fi
    ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "ingestion_grpc_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$ingestion_grpc_guard_reason_code" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go ingestion_grpc_guard_reason_code must be non-empty")
      ingestion_grpc_guard_reason_code="n/a"
    fi
    go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")")"
    if ! go_nogo_require_non_bootstrap_signer_nested="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go go_nogo_require_non_bootstrap_signer must be boolean token, got: ${go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
      go_nogo_require_non_bootstrap_signer_nested="unknown"
    elif [[ "$go_nogo_require_non_bootstrap_signer_nested" != "$go_nogo_require_non_bootstrap_signer" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go go_nogo_require_non_bootstrap_signer mismatch: nested=${go_nogo_require_non_bootstrap_signer_nested} expected=${go_nogo_require_non_bootstrap_signer}")
    fi
    non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "non_bootstrap_signer_guard_verdict" "$go_nogo_output")")"
    non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
    non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$non_bootstrap_signer_guard_verdict_raw")"
    if [[ -z "$non_bootstrap_signer_guard_verdict_raw" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go non_bootstrap_signer_guard_verdict must be non-empty")
      non_bootstrap_signer_guard_verdict="UNKNOWN"
    elif [[ "$non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${non_bootstrap_signer_guard_verdict_raw})")
      non_bootstrap_signer_guard_verdict="UNKNOWN"
    fi
    non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "non_bootstrap_signer_guard_reason_code" "$go_nogo_output")")"
    if [[ -z "$non_bootstrap_signer_guard_reason_code" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go non_bootstrap_signer_guard_reason_code must be non-empty")
      non_bootstrap_signer_guard_reason_code="n/a"
    fi
    if [[ "$go_nogo_require_executor_upstream" == "true" ]]; then
      if [[ "$executor_backend_mode_guard_verdict" == "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go executor_backend_mode_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
      if [[ "$executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go executor_upstream_endpoint_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
      fi
    else
      if [[ "$executor_backend_mode_guard_verdict" != "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go executor_backend_mode_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${executor_backend_mode_guard_verdict})")
      fi
      if [[ "$executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go executor_upstream_endpoint_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${executor_upstream_endpoint_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_ingestion_grpc" == "true" ]]; then
      if [[ "$ingestion_grpc_guard_verdict" == "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
      fi
    else
      if [[ "$ingestion_grpc_guard_verdict" != "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${ingestion_grpc_guard_verdict})")
      fi
    fi
    if [[ "$go_nogo_require_non_bootstrap_signer" == "true" ]]; then
      if [[ "$non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
      fi
    else
      if [[ "$non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
        input_errors+=("window ${window_hours}h nested go/no-go non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${non_bootstrap_signer_guard_verdict})")
      fi
    fi
    if [[ -z "$dynamic_hint_source_reason" ]]; then
      dynamic_hint_source_reason="n/a"
    fi
    if [[ -z "$dynamic_tip_policy_reason" ]]; then
      dynamic_tip_policy_reason="n/a"
    fi
    if [[ -z "$jito_rpc_policy_reason" ]]; then
      jito_rpc_policy_reason="n/a"
    fi
    if [[ -z "$jito_rpc_policy_reason_code" ]]; then
      jito_rpc_policy_reason_code="n/a"
    fi
    if [[ -z "$fastlane_feature_flag_reason" ]]; then
      fastlane_feature_flag_reason="n/a"
    fi
    if [[ -z "$fastlane_feature_flag_reason_code" ]]; then
      fastlane_feature_flag_reason_code="n/a"
    fi

    capture_path=""
    capture_sha256="n/a"
    if [[ -n "$OUTPUT_DIR" ]]; then
      capture_path="$OUTPUT_DIR/window_${window_hours}h/execution_go_nogo_captured_${timestamp_compact}.txt"
      printf '%s\n' "$go_nogo_output" > "$capture_path"
      capture_sha256="$(sha256_file_value "$capture_path")"
    fi

    window_ids+=("$window_hours")
    window_go_nogo_exit_codes+=("$go_nogo_exit_code")
    window_overall_verdicts+=("$overall_verdict")
    window_overall_reason_codes+=("${overall_reason_code:-n/a}")
    window_fee_verdicts+=("$fee_verdict")
    window_route_verdicts+=("$route_verdict")
    window_primary_routes+=("$primary_route")
    window_fallback_routes+=("$fallback_route")
    window_confirmed_orders+=("${confirmed_orders_total:-n/a}")
    window_fee_missing+=("${fee_missing_rows:-n/a}")
    window_fee_mismatch+=("${fee_mismatch_rows:-n/a}")
    window_fallback_used+=("${fallback_used_events:-n/a}")
    window_hint_mismatch+=("${hint_mismatch_events:-n/a}")
    window_dynamic_policy_config_enabled+=("$dynamic_policy_config_enabled")
    window_dynamic_hint_source_verdicts+=("$dynamic_hint_source_verdict")
    window_dynamic_hint_source_reasons+=("$dynamic_hint_source_reason")
    window_dynamic_tip_policy_config_enabled+=("$dynamic_tip_policy_config_enabled")
    window_dynamic_tip_policy_verdicts+=("$dynamic_tip_policy_verdict")
    window_dynamic_tip_policy_reasons+=("$dynamic_tip_policy_reason")
    window_jito_rpc_policy_verdicts+=("$jito_rpc_policy_verdict")
    window_jito_rpc_policy_reasons+=("$jito_rpc_policy_reason")
    window_jito_rpc_policy_reason_codes+=("$jito_rpc_policy_reason_code")
    window_fastlane_feature_flag_verdicts+=("$fastlane_feature_flag_verdict")
    window_fastlane_feature_flag_reasons+=("$fastlane_feature_flag_reason")
    window_fastlane_feature_flag_reason_codes+=("$fastlane_feature_flag_reason_code")
    window_executor_backend_mode_guard_verdicts+=("$executor_backend_mode_guard_verdict")
    window_executor_backend_mode_guard_reason_codes+=("$executor_backend_mode_guard_reason_code")
    window_executor_upstream_endpoint_guard_verdicts+=("$executor_upstream_endpoint_guard_verdict")
    window_executor_upstream_endpoint_guard_reason_codes+=("$executor_upstream_endpoint_guard_reason_code")
    window_go_nogo_require_ingestion_grpc+=("$go_nogo_require_ingestion_grpc_nested")
    window_ingestion_grpc_guard_verdicts+=("$ingestion_grpc_guard_verdict")
    window_ingestion_grpc_guard_reason_codes+=("$ingestion_grpc_guard_reason_code")
    window_go_nogo_require_non_bootstrap_signer+=("$go_nogo_require_non_bootstrap_signer_nested")
    window_non_bootstrap_signer_guard_verdicts+=("$non_bootstrap_signer_guard_verdict")
    window_non_bootstrap_signer_guard_reason_codes+=("$non_bootstrap_signer_guard_reason_code")
    window_go_nogo_artifacts_written+=("$go_nogo_artifacts_written")
    window_go_nogo_nested_package_bundle_enabled+=("$go_nogo_nested_package_bundle_enabled")
    window_go_nogo_artifact_manifests+=("${go_nogo_artifact_manifest:-n/a}")
    window_go_nogo_calibration_sha256+=("${go_nogo_calibration_sha256:-n/a}")
    window_go_nogo_snapshot_sha256+=("${go_nogo_snapshot_sha256:-n/a}")
    window_go_nogo_preflight_sha256+=("${go_nogo_preflight_sha256:-n/a}")
    window_go_nogo_summary_sha256+=("${go_nogo_summary_sha256:-n/a}")
    window_capture_paths+=("${capture_path:-n/a}")
    window_capture_sha256+=("${capture_sha256:-n/a}")

    if [[ "$overall_verdict" == "UNKNOWN" || "$fee_verdict" == "UNKNOWN" || "$route_verdict" == "UNKNOWN" ]]; then
      window_unknown_count=$((window_unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window=${window_hours}h produced UNKNOWN verdict fields (overall=${overall_verdict}, fee=${fee_verdict}, route=${route_verdict}, exit_code=${go_nogo_exit_code})"
      fi
      continue
    fi
    if [[ "$windowed_signoff_require_dynamic_hint_source_pass" == "true" && "$dynamic_policy_config_enabled" == "true" && "$dynamic_hint_source_verdict" == "UNKNOWN" ]]; then
      window_unknown_count=$((window_unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window=${window_hours}h dynamic hint source verdict unknown while required (exit_code=${go_nogo_exit_code})"
      fi
      continue
    fi
    if [[ "$windowed_signoff_require_dynamic_tip_policy_pass" == "true" && "$dynamic_tip_policy_config_enabled" == "true" && "$dynamic_tip_policy_verdict" == "UNKNOWN" ]]; then
      window_unknown_count=$((window_unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window=${window_hours}h dynamic tip policy verdict unknown while required (exit_code=${go_nogo_exit_code})"
      fi
      continue
    fi

    if [[ "$overall_verdict" != "GO" || "$go_nogo_exit_code" -ne 0 ]]; then
      if [[ "$overall_verdict" == "NO_GO" || "$go_nogo_exit_code" -ne 0 ]]; then
        window_hard_block_count=$((window_hard_block_count + 1))
      fi
      if [[ -z "$first_non_pass_reason" ]]; then
        first_non_pass_reason="window=${window_hours}h blocked by overall go/no-go state (overall=${overall_verdict}, exit_code=${go_nogo_exit_code})"
      fi
      continue
    fi

    if [[ "$fee_verdict" != "PASS" || "$route_verdict" != "PASS" ]]; then
      window_unknown_count=$((window_unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window=${window_hours}h produced inconsistent GO state with non-PASS gates (fee=${fee_verdict}, route=${route_verdict})"
      fi
      continue
    fi
    if [[ "$windowed_signoff_require_dynamic_hint_source_pass" == "true" && "$dynamic_policy_config_enabled" == "true" && "$dynamic_hint_source_verdict" != "PASS" ]]; then
      if [[ -z "$first_non_pass_reason" ]]; then
        first_non_pass_reason="window=${window_hours}h dynamic hint source gate not PASS (verdict=${dynamic_hint_source_verdict}, reason=${dynamic_hint_source_reason})"
      fi
      continue
    fi
    if [[ "$windowed_signoff_require_dynamic_tip_policy_pass" == "true" && "$dynamic_tip_policy_config_enabled" == "true" && "$dynamic_tip_policy_verdict" != "PASS" ]]; then
      if [[ -z "$first_non_pass_reason" ]]; then
        first_non_pass_reason="window=${window_hours}h dynamic tip policy gate not PASS (verdict=${dynamic_tip_policy_verdict}, reason=${dynamic_tip_policy_reason})"
      fi
      continue
    fi
    window_pass_count=$((window_pass_count + 1))

    if route_is_value "$primary_route"; then
      if [[ "$primary_route_seen" == "false" ]]; then
        primary_route_seen="true"
        stable_primary_route="$primary_route"
      elif [[ "$stable_primary_route" != "$primary_route" ]]; then
        primary_route_stable="false"
      fi
    fi

    if route_is_value "$fallback_route"; then
      if [[ "$fallback_route_seen" == "false" ]]; then
        fallback_route_seen="true"
        stable_fallback_route="$fallback_route"
      elif [[ "$stable_fallback_route" != "$fallback_route" ]]; then
        fallback_route_stable="false"
      fi
    fi
  done
fi

if [[ "$primary_route_seen" == "false" ]]; then
  stable_primary_route="n/a"
fi
if [[ "$fallback_route_seen" == "false" ]]; then
  stable_fallback_route="n/a"
fi

artifacts_written="false"
if [[ -n "$OUTPUT_DIR" ]]; then
  artifacts_written="true"
fi

signoff_verdict="NO_GO"
signoff_reason="unrecognized signoff gate state"
signoff_reason_code="unrecognized_state"
if ((${#input_errors[@]} > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${input_errors[0]}"
  signoff_reason_code="input_error"
elif ((window_unknown_count > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_unknown_reason:-unknown window verdict state}"
  signoff_reason_code="window_unknown"
elif ((window_hard_block_count > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_non_pass_reason:-at least one window is blocked by overall go/no-go state}"
  signoff_reason_code="window_hard_block"
elif ((window_pass_count == window_total)) && [[ "$primary_route_stable" == "true" ]] && [[ "$fallback_route_stable" == "true" ]]; then
  signoff_verdict="GO"
  signoff_reason="all windows GO with PASS fee/route gates and stable primary/fallback routes"
  signoff_reason_code="all_windows_pass_stable_routes"
else
  signoff_verdict="HOLD"
  if [[ "$primary_route_stable" != "true" ]]; then
    signoff_reason="primary route is not stable across windows"
    signoff_reason_code="primary_route_unstable"
  elif [[ "$fallback_route_stable" != "true" ]]; then
    signoff_reason="fallback route is not stable across windows"
    signoff_reason_code="fallback_route_unstable"
  else
    signoff_reason="${first_non_pass_reason:-at least one window did not pass fee/route readiness gates}"
    signoff_reason_code="window_not_pass"
  fi
fi

summary_output="$(cat <<EOF
=== Execution Windowed Signoff Summary ===
utc_now: $timestamp_utc
config: $CONFIG_PATH
service: $SERVICE
windows_csv: $WINDOWS_CSV
risk_events_minutes: $RISK_EVENTS_MINUTES
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled
go_nogo_require_executor_upstream: $go_nogo_require_executor_upstream
go_nogo_require_ingestion_grpc: $go_nogo_require_ingestion_grpc
go_nogo_require_non_bootstrap_signer: $go_nogo_require_non_bootstrap_signer
executor_env_path: $EXECUTOR_ENV_PATH
go_nogo_test_mode: $go_nogo_test_mode_norm
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}
window_count: $window_total
window_pass_count: $window_pass_count
window_unknown_count: $window_unknown_count
window_hard_block_count: $window_hard_block_count
primary_route_stable: $primary_route_stable
stable_primary_route: $stable_primary_route
fallback_route_stable: $fallback_route_stable
stable_fallback_route: $stable_fallback_route
input_error_count: ${#input_errors[@]}
artifacts_written: $artifacts_written
EOF
)"

for idx in "${!window_ids[@]}"; do
  window_id="${window_ids[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_exit_code: ${window_go_nogo_exit_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_verdict: ${window_overall_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_reason_code: ${window_overall_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_decomposition_verdict: ${window_fee_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_route_profile_verdict: ${window_route_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_primary_route: ${window_primary_routes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fallback_route: ${window_fallback_routes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_confirmed_orders_total: ${window_confirmed_orders[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_consistency_missing_coverage_rows: ${window_fee_missing[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_consistency_mismatch_rows: ${window_fee_mismatch[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fallback_used_events: ${window_fallback_used[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_hint_mismatch_events: ${window_hint_mismatch[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_cu_policy_config_enabled: ${window_dynamic_policy_config_enabled[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_cu_hint_source_verdict: ${window_dynamic_hint_source_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_cu_hint_source_reason: ${window_dynamic_hint_source_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_tip_policy_config_enabled: ${window_dynamic_tip_policy_config_enabled[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_tip_policy_verdict: ${window_dynamic_tip_policy_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_dynamic_tip_policy_reason: ${window_dynamic_tip_policy_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_jito_rpc_policy_verdict: ${window_jito_rpc_policy_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_jito_rpc_policy_reason: ${window_jito_rpc_policy_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_jito_rpc_policy_reason_code: ${window_jito_rpc_policy_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fastlane_feature_flag_verdict: ${window_fastlane_feature_flag_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fastlane_feature_flag_reason: ${window_fastlane_feature_flag_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fastlane_feature_flag_reason_code: ${window_fastlane_feature_flag_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_executor_backend_mode_guard_verdict: ${window_executor_backend_mode_guard_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_executor_backend_mode_guard_reason_code: ${window_executor_backend_mode_guard_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_executor_upstream_endpoint_guard_verdict: ${window_executor_upstream_endpoint_guard_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_executor_upstream_endpoint_guard_reason_code: ${window_executor_upstream_endpoint_guard_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_require_ingestion_grpc: ${window_go_nogo_require_ingestion_grpc[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_ingestion_grpc_guard_verdict: ${window_ingestion_grpc_guard_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_ingestion_grpc_guard_reason_code: ${window_ingestion_grpc_guard_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_require_non_bootstrap_signer: ${window_go_nogo_require_non_bootstrap_signer[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_non_bootstrap_signer_guard_verdict: ${window_non_bootstrap_signer_guard_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_non_bootstrap_signer_guard_reason_code: ${window_non_bootstrap_signer_guard_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_artifacts_written: ${window_go_nogo_artifacts_written[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_nested_package_bundle_enabled: ${window_go_nogo_nested_package_bundle_enabled[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_artifact_manifest: ${window_go_nogo_artifact_manifests[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_calibration_sha256: ${window_go_nogo_calibration_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_snapshot_sha256: ${window_go_nogo_snapshot_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_preflight_sha256: ${window_go_nogo_preflight_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_summary_sha256: ${window_go_nogo_summary_sha256[$idx]}"
done

summary_output+=$'\n'
summary_output+=$'\n'"signoff_verdict: $signoff_verdict"
summary_output+=$'\n'"signoff_reason: $signoff_reason"
summary_output+=$'\n'"signoff_reason_code: $signoff_reason_code"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

package_bundle_artifacts_written="false"
package_bundle_exit_code="n/a"
package_bundle_error="n/a"
package_bundle_path="n/a"
package_bundle_sha256="n/a"
package_bundle_sha256_path="n/a"
package_bundle_contents_manifest="n/a"
package_bundle_file_count="n/a"
summary_sha256="n/a"
manifest_sha256="n/a"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/execution_windowed_signoff_summary_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_windowed_signoff_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" > "$summary_path"

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

  {
    echo "summary_sha256: $summary_sha256"
    for idx in "${!window_ids[@]}"; do
      window_id="${window_ids[$idx]}"
      echo "window_${window_id}h_capture_path: ${window_capture_paths[$idx]}"
      echo "window_${window_id}h_capture_sha256: ${window_capture_sha256[$idx]}"
    done
  } > "$manifest_path"
  manifest_sha256="$(sha256_file_value "$manifest_path")"

  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "manifest_sha256: $manifest_sha256"
  for idx in "${!window_ids[@]}"; do
    window_id="${window_ids[$idx]}"
    echo "window_${window_id}h_capture_path: ${window_capture_paths[$idx]}"
    echo "window_${window_id}h_capture_sha256: ${window_capture_sha256[$idx]}"
  done
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

case "$signoff_verdict" in
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
