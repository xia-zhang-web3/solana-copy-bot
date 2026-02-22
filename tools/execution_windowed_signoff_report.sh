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
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-false}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
windowed_signoff_require_dynamic_hint_source_pass="$(normalize_bool_token "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS")"
windowed_signoff_require_dynamic_tip_policy_pass="$(normalize_bool_token "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS")"

declare -a input_errors=()
declare -a windows=()

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
for raw_token in "${raw_windows[@]}"; do
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
      GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
      OUTPUT_DIR="$go_nogo_output_dir" \
      bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" "$window_hours" "$RISK_EVENTS_MINUTES" 2>&1
    )"; then
      go_nogo_exit_code=0
    else
      go_nogo_exit_code=$?
    fi

    overall_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
    fee_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$go_nogo_output")")"
    route_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$go_nogo_output")")"
    primary_route="$(trim_string "$(extract_field "primary_route" "$go_nogo_output")")"
    fallback_route="$(trim_string "$(extract_field "fallback_route" "$go_nogo_output")")"
    confirmed_orders_total="$(trim_string "$(extract_field "confirmed_orders_total" "$go_nogo_output")")"
    fee_missing_rows="$(trim_string "$(extract_field "fee_consistency_missing_coverage_rows" "$go_nogo_output")")"
    fee_mismatch_rows="$(trim_string "$(extract_field "fee_consistency_mismatch_rows" "$go_nogo_output")")"
    fallback_used_events="$(trim_string "$(extract_field "fallback_used_events" "$go_nogo_output")")"
    hint_mismatch_events="$(trim_string "$(extract_field "hint_mismatch_events" "$go_nogo_output")")"
    dynamic_policy_config_enabled="$(normalize_bool_token "$(extract_field "dynamic_cu_policy_config_enabled" "$go_nogo_output")")"
    dynamic_hint_source_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_hint_source_verdict" "$go_nogo_output")")"
    dynamic_hint_source_reason="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason" "$go_nogo_output")")"
    dynamic_tip_policy_config_enabled="$(normalize_bool_token "$(extract_field "dynamic_tip_policy_config_enabled" "$go_nogo_output")")"
    dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$go_nogo_output")")"
    dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$go_nogo_output")")"
    if [[ -z "$dynamic_hint_source_reason" ]]; then
      dynamic_hint_source_reason="n/a"
    fi
    if [[ -z "$dynamic_tip_policy_reason" ]]; then
      dynamic_tip_policy_reason="n/a"
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

signoff_verdict="NO_GO"
signoff_reason="unrecognized signoff gate state"
if ((${#input_errors[@]} > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${input_errors[0]}"
elif ((window_unknown_count > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_unknown_reason:-unknown window verdict state}"
elif ((window_hard_block_count > 0)); then
  signoff_verdict="NO_GO"
  signoff_reason="${first_non_pass_reason:-at least one window is blocked by overall go/no-go state}"
elif ((window_pass_count == window_total)) && [[ "$primary_route_stable" == "true" ]] && [[ "$fallback_route_stable" == "true" ]]; then
  signoff_verdict="GO"
  signoff_reason="all windows GO with PASS fee/route gates and stable primary/fallback routes"
else
  signoff_verdict="HOLD"
  if [[ "$primary_route_stable" != "true" ]]; then
    signoff_reason="primary route is not stable across windows"
  elif [[ "$fallback_route_stable" != "true" ]]; then
    signoff_reason="fallback route is not stable across windows"
  else
    signoff_reason="${first_non_pass_reason:-at least one window did not pass fee/route readiness gates}"
  fi
fi

summary_output="$(cat <<EOF
=== Execution Windowed Signoff Summary ===
utc_now: $timestamp_utc
config: $CONFIG_PATH
service: $SERVICE
windows_csv: $WINDOWS_CSV
risk_events_minutes: $RISK_EVENTS_MINUTES
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass
window_count: $window_total
window_pass_count: $window_pass_count
window_unknown_count: $window_unknown_count
window_hard_block_count: $window_hard_block_count
primary_route_stable: $primary_route_stable
stable_primary_route: $stable_primary_route
fallback_route_stable: $fallback_route_stable
stable_fallback_route: $stable_fallback_route
input_error_count: ${#input_errors[@]}
EOF
)"

for idx in "${!window_ids[@]}"; do
  window_id="${window_ids[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_go_nogo_exit_code: ${window_go_nogo_exit_codes[$idx]}"
  summary_output+=$'\n'"window_${window_id}h_overall_go_nogo_verdict: ${window_overall_verdicts[$idx]}"
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
done

summary_output+=$'\n'
summary_output+=$'\n'"signoff_verdict: $signoff_verdict"
summary_output+=$'\n'"signoff_reason: $signoff_reason"

echo "$summary_output"
if ((${#input_errors[@]} > 0)); then
  for input_error in "${input_errors[@]}"; do
    echo "input_error: $input_error"
  done
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/execution_windowed_signoff_summary_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_windowed_signoff_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" > "$summary_path"
  summary_sha256="$(sha256_file_value "$summary_path")"

  {
    echo "summary_sha256: $summary_sha256"
    for idx in "${!window_ids[@]}"; do
      window_id="${window_ids[$idx]}"
      echo "window_${window_id}h_capture_path: ${window_capture_paths[$idx]}"
      echo "window_${window_id}h_capture_sha256: ${window_capture_sha256[$idx]}"
    done
  } > "$manifest_path"

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  for idx in "${!window_ids[@]}"; do
    window_id="${window_ids[$idx]}"
    echo "window_${window_id}h_capture_path: ${window_capture_paths[$idx]}"
    echo "window_${window_id}h_capture_sha256: ${window_capture_sha256[$idx]}"
  done
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
