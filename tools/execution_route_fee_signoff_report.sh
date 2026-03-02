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
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_route_fee_signoff}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_DIR}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
route_fee_signoff_test_verdict_override_raw="$(trim_string "$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE")"
route_fee_signoff_test_verdict_override_norm="$(normalize_go_nogo_verdict "$route_fee_signoff_test_verdict_override_raw")"

declare -a input_errors=()

parse_signoff_bool_setting_into() {
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

parse_signoff_bool_setting_into "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY" go_nogo_require_jito_rpc_policy
parse_signoff_bool_setting_into "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED" go_nogo_require_fastlane_disabled
parse_signoff_bool_setting_into "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE" go_nogo_test_mode_norm
parse_signoff_bool_setting_into "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED" package_bundle_enabled_norm

declare -a windows=()

if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  input_errors+=("risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)")
fi

if [[ -n "$route_fee_signoff_test_verdict_override_raw" && "$route_fee_signoff_test_verdict_override_norm" == "UNKNOWN" ]]; then
  input_errors+=("route fee signoff test verdict override must be GO, HOLD, or NO_GO (got: $route_fee_signoff_test_verdict_override_raw)")
fi
if [[ -n "$route_fee_signoff_test_verdict_override_raw" && "$go_nogo_test_mode_norm" != "true" ]]; then
  input_errors+=("route fee signoff test verdict override requires GO_NOGO_TEST_MODE=true")
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  input_errors+=("config file not found: $CONFIG_PATH")
fi
if [[ "$package_bundle_enabled_norm" == "true" && -z "$OUTPUT_DIR" ]]; then
  input_errors+=("PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set")
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

route_is_value() {
  local route
  route="$(trim_string "$1")"
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

manifest_entry_path() {
  local path="$1"
  if [[ -z "$path" || "$path" == "n/a" ]]; then
    printf 'n/a'
    return
  fi
  if [[ -n "$OUTPUT_DIR" && "$path" == "$OUTPUT_DIR/"* ]]; then
    printf '%s' "${path#"$OUTPUT_DIR"/}"
    return
  fi
  printf '%s' "$path"
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
declare -a window_calibration_exit_codes=()
declare -a window_overall_go_nogo_verdicts=()
declare -a window_overall_go_nogo_reasons=()
declare -a window_overall_go_nogo_reason_codes=()
declare -a window_route_profile_verdicts=()
declare -a window_route_profile_reasons=()
declare -a window_fee_decomposition_verdicts=()
declare -a window_fee_decomposition_reasons=()
declare -a window_calibration_route_profile_verdicts=()
declare -a window_calibration_route_profile_reasons=()
declare -a window_calibration_fee_decomposition_verdicts=()
declare -a window_calibration_fee_decomposition_reasons=()
declare -a window_fee_verdict_parity=()
declare -a window_route_verdict_parity=()
declare -a window_primary_routes=()
declare -a window_fallback_routes=()
declare -a window_go_nogo_capture_paths=()
declare -a window_go_nogo_capture_sha256=()
declare -a window_calibration_capture_paths=()
declare -a window_calibration_capture_sha256=()
declare -a window_go_nogo_artifact_manifests=()
declare -a window_go_nogo_nested_package_bundle_enabled=()
declare -a window_go_nogo_summary_sha256=()
declare -a window_go_nogo_calibration_sha256=()

window_total=0
go_nogo_go_count=0
go_nogo_hold_count=0
go_nogo_no_go_count=0
unknown_count=0
route_profile_pass_count=0
fee_decomposition_pass_count=0

first_unknown_reason=""
first_non_pass_reason=""
first_hard_block_reason=""
first_go_nogo_hold_reason=""

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
    calibration_output_dir=""
    if [[ -n "$OUTPUT_DIR" ]]; then
      go_nogo_output_dir="$OUTPUT_DIR/window_${window_hours}h/go_nogo"
      calibration_output_dir="$OUTPUT_DIR/window_${window_hours}h/calibration"
      mkdir -p "$go_nogo_output_dir" "$calibration_output_dir"
    fi

    go_nogo_output=""
    go_nogo_exit_code=0
    if go_nogo_output="$(
      CONFIG_PATH="$CONFIG_PATH" \
      SERVICE="$SERVICE" \
      GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy" \
      GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled" \
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

    db_path_window="$(trim_string "$(extract_field "db" "$go_nogo_output")")"
    calibration_output=""
    calibration_exit_code=0
    if calibration_output="$(
      DB_PATH="${db_path_window:-${DB_PATH:-}}" \
      CONFIG_PATH="$CONFIG_PATH" \
      bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" "$window_hours" 2>&1
    )"; then
      calibration_exit_code=0
    else
      calibration_exit_code=$?
    fi

    overall_go_nogo_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
    overall_go_nogo_reason="$(trim_string "$(extract_field "overall_go_nogo_reason" "$go_nogo_output")")"
    overall_go_nogo_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$go_nogo_output")")"
    route_profile_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$go_nogo_output")")"
    route_profile_reason="$(trim_string "$(extract_field "route_profile_reason" "$go_nogo_output")")"
    fee_decomposition_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$go_nogo_output")")"
    fee_decomposition_reason="$(trim_string "$(extract_field "fee_decomposition_reason" "$go_nogo_output")")"
    primary_route="$(trim_string "$(extract_field "primary_route" "$go_nogo_output")")"
    fallback_route="$(trim_string "$(extract_field "fallback_route" "$go_nogo_output")")"
    go_nogo_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$go_nogo_output")")"
    go_nogo_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$go_nogo_output")")"
    if ! go_nogo_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$go_nogo_output")"; then
      input_errors+=("window ${window_hours}h nested go/no-go package_bundle_enabled must be boolean token, got: ${go_nogo_nested_package_bundle_enabled_raw:-<empty>}")
      go_nogo_nested_package_bundle_enabled="unknown"
    elif [[ "$go_nogo_nested_package_bundle_enabled" != "false" ]]; then
      input_errors+=("window ${window_hours}h nested go/no-go helper must run with PACKAGE_BUNDLE_ENABLED=false")
    fi
    go_nogo_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$go_nogo_output")")"
    go_nogo_calibration_sha256="$(trim_string "$(extract_field "calibration_sha256" "$go_nogo_output")")"

    calibration_route_profile_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$calibration_output")")"
    calibration_route_profile_reason="$(trim_string "$(extract_field "route_profile_reason" "$calibration_output")")"
    calibration_fee_decomposition_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$calibration_output")")"
    calibration_fee_decomposition_reason="$(trim_string "$(extract_field "fee_decomposition_reason" "$calibration_output")")"

    fee_verdict_parity="true"
    if [[ "$fee_decomposition_verdict" != "$calibration_fee_decomposition_verdict" ]]; then
      fee_verdict_parity="false"
    fi

    route_verdict_parity="true"
    if [[ "$route_profile_verdict" != "$calibration_route_profile_verdict" ]]; then
      route_verdict_parity="false"
    fi

    if [[ -z "$route_profile_reason" ]]; then
      route_profile_reason="n/a"
    fi
    if [[ -z "$overall_go_nogo_reason" ]]; then
      overall_go_nogo_reason="n/a"
    fi
    if [[ -z "$overall_go_nogo_reason_code" ]]; then
      overall_go_nogo_reason_code="n/a"
    fi
    if [[ -z "$fee_decomposition_reason" ]]; then
      fee_decomposition_reason="n/a"
    fi
    if [[ -z "$calibration_route_profile_reason" ]]; then
      calibration_route_profile_reason="n/a"
    fi
    if [[ -z "$calibration_fee_decomposition_reason" ]]; then
      calibration_fee_decomposition_reason="n/a"
    fi

    go_nogo_capture_path="n/a"
    go_nogo_capture_sha256="n/a"
    calibration_capture_path="n/a"
    calibration_capture_sha256="n/a"
    if [[ -n "$OUTPUT_DIR" ]]; then
      go_nogo_capture_path="$OUTPUT_DIR/window_${window_hours}h/execution_go_nogo_captured_${timestamp_compact}.txt"
      calibration_capture_path="$OUTPUT_DIR/window_${window_hours}h/execution_fee_calibration_captured_${timestamp_compact}.txt"
      printf '%s\n' "$go_nogo_output" >"$go_nogo_capture_path"
      printf '%s\n' "$calibration_output" >"$calibration_capture_path"
      go_nogo_capture_sha256="$(sha256_file_value "$go_nogo_capture_path")"
      calibration_capture_sha256="$(sha256_file_value "$calibration_capture_path")"
    fi

    window_ids+=("$window_hours")
    window_go_nogo_exit_codes+=("$go_nogo_exit_code")
    window_calibration_exit_codes+=("$calibration_exit_code")
    window_overall_go_nogo_verdicts+=("$overall_go_nogo_verdict")
    window_overall_go_nogo_reasons+=("$overall_go_nogo_reason")
    window_overall_go_nogo_reason_codes+=("$overall_go_nogo_reason_code")
    window_route_profile_verdicts+=("$route_profile_verdict")
    window_route_profile_reasons+=("$route_profile_reason")
    window_fee_decomposition_verdicts+=("$fee_decomposition_verdict")
    window_fee_decomposition_reasons+=("$fee_decomposition_reason")
    window_calibration_route_profile_verdicts+=("$calibration_route_profile_verdict")
    window_calibration_route_profile_reasons+=("$calibration_route_profile_reason")
    window_calibration_fee_decomposition_verdicts+=("$calibration_fee_decomposition_verdict")
    window_calibration_fee_decomposition_reasons+=("$calibration_fee_decomposition_reason")
    window_fee_verdict_parity+=("$fee_verdict_parity")
    window_route_verdict_parity+=("$route_verdict_parity")
    window_primary_routes+=("${primary_route:-n/a}")
    window_fallback_routes+=("${fallback_route:-n/a}")
    window_go_nogo_capture_paths+=("$go_nogo_capture_path")
    window_go_nogo_capture_sha256+=("$go_nogo_capture_sha256")
    window_calibration_capture_paths+=("$calibration_capture_path")
    window_calibration_capture_sha256+=("$calibration_capture_sha256")
    window_go_nogo_artifact_manifests+=("${go_nogo_artifact_manifest:-n/a}")
    window_go_nogo_nested_package_bundle_enabled+=("${go_nogo_nested_package_bundle_enabled:-unknown}")
    window_go_nogo_summary_sha256+=("${go_nogo_summary_sha256:-n/a}")
    window_go_nogo_calibration_sha256+=("${go_nogo_calibration_sha256:-n/a}")

    if route_is_value "$primary_route"; then
      if [[ "$primary_route_seen" == "false" ]]; then
        primary_route_seen="true"
        stable_primary_route="$primary_route"
      elif [[ "$primary_route" != "$stable_primary_route" ]]; then
        primary_route_stable="false"
      fi
    fi

    if route_is_value "$fallback_route"; then
      if [[ "$fallback_route_seen" == "false" ]]; then
        fallback_route_seen="true"
        stable_fallback_route="$fallback_route"
      elif [[ "$fallback_route" != "$stable_fallback_route" ]]; then
        fallback_route_stable="false"
      fi
    fi

    case "$overall_go_nogo_verdict" in
      GO) go_nogo_go_count=$((go_nogo_go_count + 1)) ;;
      HOLD) go_nogo_hold_count=$((go_nogo_hold_count + 1)) ;;
      NO_GO) go_nogo_no_go_count=$((go_nogo_no_go_count + 1)) ;;
      *)
        unknown_count=$((unknown_count + 1))
        if [[ -z "$first_unknown_reason" ]]; then
          first_unknown_reason="window ${window_hours}h has unknown go/no-go verdict"
        fi
        ;;
    esac

    if [[ "$overall_go_nogo_verdict" == "HOLD" && -z "$first_go_nogo_hold_reason" ]]; then
      first_go_nogo_hold_reason="window ${window_hours}h nested go/no-go verdict is HOLD: ${overall_go_nogo_reason:-n/a}"
    fi

    if [[ "$route_profile_verdict" == "PASS" && "$route_verdict_parity" == "true" ]]; then
      route_profile_pass_count=$((route_profile_pass_count + 1))
    elif [[ "$route_profile_verdict" == "UNKNOWN" || "$calibration_route_profile_verdict" == "UNKNOWN" || "$route_verdict_parity" == "false" ]]; then
      unknown_count=$((unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window ${window_hours}h route-profile verdict could not be classified or mismatched between go/no-go and calibration"
      fi
    elif [[ -z "$first_non_pass_reason" ]]; then
      first_non_pass_reason="window ${window_hours}h route-profile verdict is ${route_profile_verdict}"
    fi

    if [[ "$fee_decomposition_verdict" == "PASS" && "$fee_verdict_parity" == "true" ]]; then
      fee_decomposition_pass_count=$((fee_decomposition_pass_count + 1))
    elif [[ "$fee_decomposition_verdict" == "UNKNOWN" || "$calibration_fee_decomposition_verdict" == "UNKNOWN" || "$fee_verdict_parity" == "false" ]]; then
      unknown_count=$((unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window ${window_hours}h fee-decomposition verdict could not be classified or mismatched between go/no-go and calibration"
      fi
    elif [[ -z "$first_non_pass_reason" ]]; then
      first_non_pass_reason="window ${window_hours}h fee-decomposition verdict is ${fee_decomposition_verdict}"
    fi

    if (( go_nogo_exit_code != 0 )) && [[ "$overall_go_nogo_verdict" == "GO" ]]; then
      unknown_count=$((unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window ${window_hours}h go/no-go exited ${go_nogo_exit_code} with GO verdict"
      fi
    fi

    if (( calibration_exit_code != 0 )); then
      unknown_count=$((unknown_count + 1))
      if [[ -z "$first_unknown_reason" ]]; then
        first_unknown_reason="window ${window_hours}h calibration exited ${calibration_exit_code}"
      fi
    fi

    if [[ "$overall_go_nogo_verdict" == "NO_GO" ]] && [[ -z "$first_hard_block_reason" ]]; then
      first_hard_block_reason="window ${window_hours}h nested go/no-go verdict is NO_GO: ${overall_go_nogo_reason:-n/a}"
    fi
  done
fi

if [[ "$primary_route_seen" == "false" ]]; then
  stable_primary_route="n/a"
fi
if [[ "$fallback_route_seen" == "false" ]]; then
  stable_fallback_route="n/a"
fi

signoff_verdict="NO_GO"
signoff_reason="unrecognized route/fee signoff state"
signoff_reason_code="unrecognized_state"

if ((${#input_errors[@]} > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${input_errors[0]}"
  signoff_reason_code="input_error"
elif (( unknown_count > 0 )); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_unknown_reason:-unknown verdict state detected}"
  signoff_reason_code="window_unknown"
elif (( go_nogo_no_go_count > 0 )); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_hard_block_reason:-at least one window returned NO_GO}"
  signoff_reason_code="window_hard_block"
elif (( window_total > 0 )) && (( go_nogo_go_count == window_total )) && (( route_profile_pass_count == window_total )) && (( fee_decomposition_pass_count == window_total )) && [[ "$primary_route_stable" == "true" && "$fallback_route_stable" == "true" ]]; then
  signoff_verdict="GO"
  signoff_reason="all windows GO with PASS route-profile/fee-decomposition verdicts and stable primary/fallback routes"
  signoff_reason_code="all_windows_pass_stable_routes"
elif [[ "$primary_route_stable" != "true" || "$fallback_route_stable" != "true" ]]; then
  signoff_verdict="HOLD"
  if [[ -n "$first_non_pass_reason" ]]; then
    signoff_reason="primary/fallback route changed across windows and at least one window is not PASS: ${first_non_pass_reason}"
    signoff_reason_code="routes_unstable_window_not_pass"
  else
    signoff_reason="primary/fallback route changed across windows before full route/fee signoff closure"
    signoff_reason_code="routes_unstable"
  fi
elif (( go_nogo_hold_count > 0 )); then
  signoff_verdict="HOLD"
  signoff_reason="${first_go_nogo_hold_reason:-at least one window nested go/no-go verdict is HOLD}"
  signoff_reason_code="nested_go_nogo_hold"
else
  signoff_verdict="HOLD"
  signoff_reason="${first_non_pass_reason:-at least one window is not yet PASS for route-profile or fee-decomposition signoff}"
  signoff_reason_code="window_not_pass"
fi

if [[ -n "$route_fee_signoff_test_verdict_override_raw" && "$route_fee_signoff_test_verdict_override_norm" != "UNKNOWN" && "$go_nogo_test_mode_norm" == "true" && ${#input_errors[@]} -eq 0 ]]; then
  signoff_verdict="$route_fee_signoff_test_verdict_override_norm"
  signoff_reason="route/fee signoff test override applied"
  signoff_reason_code="test_override"
fi

summary_output="=== Execution Route/Fee Signoff Summary ===
timestamp_utc: $timestamp_utc
service: $SERVICE
config_path: $CONFIG_PATH
windows_csv: $WINDOWS_CSV
risk_events_minutes: $RISK_EVENTS_MINUTES
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled
go_nogo_test_mode: $go_nogo_test_mode_norm
route_fee_signoff_test_verdict_override: ${route_fee_signoff_test_verdict_override_raw:-n/a}
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}
window_count: ${#window_ids[@]}
go_nogo_go_count: $go_nogo_go_count
go_nogo_hold_count: $go_nogo_hold_count
go_nogo_no_go_count: $go_nogo_no_go_count
route_profile_pass_count: $route_profile_pass_count
fee_decomposition_pass_count: $fee_decomposition_pass_count
primary_route_stable: $primary_route_stable
stable_primary_route: $stable_primary_route
fallback_route_stable: $fallback_route_stable
stable_fallback_route: $stable_fallback_route
unknown_count: $unknown_count
signoff_verdict: $signoff_verdict
signoff_reason: $signoff_reason
signoff_reason_code: $signoff_reason_code"

for idx in "${!window_ids[@]}"; do
  window_id="${window_ids[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_exit_code: ${window_go_nogo_exit_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_exit_code: ${window_calibration_exit_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_verdict: ${window_overall_go_nogo_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_reason: ${window_overall_go_nogo_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_reason_code: ${window_overall_go_nogo_reason_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_route_profile_verdict: ${window_route_profile_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_route_profile_reason: ${window_route_profile_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_decomposition_verdict: ${window_fee_decomposition_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_decomposition_reason: ${window_fee_decomposition_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_route_profile_verdict: ${window_calibration_route_profile_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_route_profile_reason: ${window_calibration_route_profile_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_fee_decomposition_verdict: ${window_calibration_fee_decomposition_verdicts[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_fee_decomposition_reason: ${window_calibration_fee_decomposition_reasons[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_route_verdict_parity: ${window_route_verdict_parity[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fee_verdict_parity: ${window_fee_verdict_parity[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_primary_route: ${window_primary_routes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_fallback_route: ${window_fallback_routes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_artifact_manifest: ${window_go_nogo_artifact_manifests[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_nested_package_bundle_enabled: ${window_go_nogo_nested_package_bundle_enabled[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_summary_sha256: ${window_go_nogo_summary_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_calibration_sha256: ${window_go_nogo_calibration_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_capture_path: ${window_go_nogo_capture_paths[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_capture_sha256: ${window_go_nogo_capture_sha256[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_capture_path: ${window_calibration_capture_paths[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_calibration_capture_sha256: ${window_calibration_capture_sha256[$idx]}"
done

if ((${#input_errors[@]} > 0)); then
  for error in "${input_errors[@]}"; do
    summary_output+=$'\n'"input_error: $error"
  done
fi

artifacts_written="false"
summary_path="n/a"
manifest_path="n/a"
summary_sha256="n/a"
manifest_sha256="n/a"
package_bundle_artifacts_written="false"
package_bundle_exit_code="n/a"
package_bundle_error="n/a"
package_bundle_path="n/a"
package_bundle_sha256="n/a"
package_bundle_sha256_path="n/a"
package_bundle_contents_manifest="n/a"
package_bundle_file_count="n/a"
if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/execution_route_fee_signoff_summary_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_route_fee_signoff_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" >"$summary_path"

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
    summary_entry_path="$(manifest_entry_path "$summary_path")"
    printf '%s  %s\n' "$summary_sha256" "$summary_entry_path"
    for idx in "${!window_ids[@]}"; do
      capture_path="${window_go_nogo_capture_paths[$idx]}"
      if [[ "$capture_path" != "n/a" ]]; then
        capture_sha="$(sha256_file_value "$capture_path")"
        capture_entry_path="$(manifest_entry_path "$capture_path")"
        printf '%s  %s\n' "$capture_sha" "$capture_entry_path"
      fi
      calibration_path="${window_calibration_capture_paths[$idx]}"
      if [[ "$calibration_path" != "n/a" ]]; then
        calibration_sha="$(sha256_file_value "$calibration_path")"
        calibration_entry_path="$(manifest_entry_path "$calibration_path")"
        printf '%s  %s\n' "$calibration_sha" "$calibration_entry_path"
      fi
    done
  } >"$manifest_path"
  manifest_sha256="$(sha256_file_value "$manifest_path")"
  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi
  artifacts_written="true"
fi

printf '%s\n' "$summary_output"
echo "artifacts_written: $artifacts_written"
echo "artifact_summary: $summary_path"
echo "artifact_manifest: $manifest_path"
echo "summary_sha256: $summary_sha256"
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
