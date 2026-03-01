#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-60}"
SERVICE="${SERVICE:-solana-copy-bot}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_go_nogo}"
PACKAGE_BUNDLE_OUTPUT_DIR="${PACKAGE_BUNDLE_OUTPUT_DIR:-$OUTPUT_DIR}"

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  echo "window hours must be an integer (got: $WINDOW_HOURS)" >&2
  exit 1
fi

if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  echo "risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

go_nogo_require_jito_rpc_policy_raw="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-false}"
go_nogo_require_fastlane_disabled_raw="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-false}"
go_nogo_test_mode_raw="${GO_NOGO_TEST_MODE:-false}"
if ! go_nogo_require_jito_rpc_policy="$(parse_bool_token_strict "$go_nogo_require_jito_rpc_policy_raw")"; then
  echo "GO_NOGO_REQUIRE_JITO_RPC_POLICY must be a boolean token (true/false/1/0/yes/no/on/off), got: ${go_nogo_require_jito_rpc_policy_raw}" >&2
  exit 1
fi
if ! go_nogo_require_fastlane_disabled="$(parse_bool_token_strict "$go_nogo_require_fastlane_disabled_raw")"; then
  echo "GO_NOGO_REQUIRE_FASTLANE_DISABLED must be a boolean token (true/false/1/0/yes/no/on/off), got: ${go_nogo_require_fastlane_disabled_raw}" >&2
  exit 1
fi
if ! go_nogo_test_mode="$(parse_bool_token_strict "$go_nogo_test_mode_raw")"; then
  echo "GO_NOGO_TEST_MODE must be a boolean token (true/false/1/0/yes/no/on/off), got: ${go_nogo_test_mode_raw}" >&2
  exit 1
fi
if ! package_bundle_enabled="$(parse_bool_token_strict "$PACKAGE_BUNDLE_ENABLED")"; then
  echo "PACKAGE_BUNDLE_ENABLED must be a boolean token (true/false/1/0/yes/no/on/off), got: ${PACKAGE_BUNDLE_ENABLED}" >&2
  exit 1
fi
if [[ "$package_bundle_enabled" == "true" && -z "$OUTPUT_DIR" ]]; then
  echo "PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set" >&2
  exit 1
fi

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

cfg_or_env_bool() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local fallback="${4:-false}"
  local raw=""
  local source_desc=""
  if [[ -n "${!env_name+x}" ]]; then
    raw="${!env_name}"
    source_desc="env ${env_name}"
  else
    raw="$(cfg_value "$section" "$key")"
    source_desc="config [${section}].${key}"
  fi
  raw="$(trim_string "$raw")"
  if [[ -z "$raw" ]]; then
    raw="$fallback"
    source_desc="${source_desc} (fallback)"
  fi
  local normalized=""
  if ! normalized="$(normalize_bool_token "$raw" 2>/dev/null)"; then
    echo "invalid boolean setting for ${source_desc}: expected true/false/1/0/yes/no/on/off, got: ${raw}" >&2
    return 1
  fi
  printf '%s' "$normalized"
}

cfg_or_env_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local fallback="${4:-}"
  local raw=""
  if [[ -n "${!env_name+x}" ]]; then
    raw="${!env_name}"
  else
    raw="$(cfg_value "$section" "$key")"
  fi
  raw="$(trim_string "$raw")"
  if [[ -z "$raw" ]]; then
    raw="$fallback"
  fi
  printf '%s' "$raw"
}

sum_route_map_values() {
  local raw_map="$1"
  python3 - "$raw_map" <<'PY'
import json
import sys

raw = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
if not raw:
    print(0)
    raise SystemExit(0)

try:
    payload = json.loads(raw)
except Exception:
    print(0)
    raise SystemExit(0)

if not isinstance(payload, dict):
    print(0)
    raise SystemExit(0)

total = 0
for value in payload.values():
    if isinstance(value, bool):
        total += int(value)
    elif isinstance(value, int):
        total += value
    elif isinstance(value, float):
        total += int(value)
print(total)
PY
}

normalize_route_token() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    ""|"n/a"|"unknown"|"<none>")
      printf ''
      ;;
    *)
      printf '%s' "$raw"
      ;;
  esac
}

dynamic_cu_policy_config_enabled="$(cfg_or_env_bool execution submit_dynamic_cu_price_enabled SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED false)"
dynamic_tip_policy_config_enabled="$(cfg_or_env_bool execution submit_dynamic_tip_lamports_enabled SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_ENABLED false)"
dynamic_cu_hint_api_primary_url="$(cfg_or_env_string execution submit_dynamic_cu_price_api_primary_url SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_PRIMARY_URL "")"
execution_mode_for_go_nogo="$(trim_string "$(cfg_or_env_string execution mode SOLANA_COPY_BOT_EXECUTION_MODE "paper")")"
if [[ -z "$execution_mode_for_go_nogo" ]]; then
  execution_mode_for_go_nogo="paper"
fi
submit_fastlane_enabled="$(cfg_or_env_bool execution submit_fastlane_enabled SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED false)"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

calibration_output="$(
  DB_PATH="${DB_PATH:-}" CONFIG_PATH="$CONFIG_PATH" \
    bash "$ROOT_DIR/tools/execution_fee_calibration_report.sh" "$WINDOW_HOURS"
)"
preflight_output=""
preflight_exit_code=0
if preflight_output="$(
  CONFIG_PATH="$CONFIG_PATH" \
    bash "$ROOT_DIR/tools/execution_adapter_preflight.sh" 2>&1
)"; then
  preflight_exit_code=0
else
  preflight_exit_code=$?
fi
snapshot_output="$(
  PATH="${PATH}" DB_PATH="${DB_PATH:-}" CONFIG_PATH="$CONFIG_PATH" SERVICE="$SERVICE" \
    bash "$ROOT_DIR/tools/runtime_snapshot.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES"
)"

db_path="$(first_non_empty "$(extract_field "db" "$calibration_output")" "$(extract_field "db" "$snapshot_output")")"
preflight_verdict="$(normalize_preflight_verdict "$(extract_field "preflight_verdict" "$preflight_output")")"
preflight_reason="$(extract_field "preflight_reason" "$preflight_output")"
preflight_error_count="$(extract_field "error_count" "$preflight_output")"
preflight_first_error="$(printf '%s\n' "$preflight_output" | awk -F': ' '$1=="error" {print substr($0, index($0, ": ") + 2); exit}')"
if [[ "$preflight_verdict" == "FAIL" ]]; then
  if [[ -n "${preflight_first_error:-}" ]]; then
    preflight_reason="$preflight_first_error"
  elif [[ -z "${preflight_reason:-}" ]]; then
    preflight_reason="adapter preflight returned FAIL"
  fi
elif [[ "$preflight_verdict" == "UNKNOWN" && "$preflight_exit_code" -ne 0 ]]; then
  preflight_reason="adapter preflight exited with code $preflight_exit_code without recognizable verdict"
elif [[ -z "${preflight_reason:-}" ]]; then
  preflight_reason="n/a"
fi
fee_decomposition_verdict="$(normalize_gate_verdict "$(extract_field "fee_decomposition_verdict" "$calibration_output")")"
fee_decomposition_reason="$(extract_field "fee_decomposition_reason" "$calibration_output")"
route_profile_verdict="$(normalize_gate_verdict "$(extract_field "route_profile_verdict" "$calibration_output")")"
route_profile_reason="$(extract_field "route_profile_reason" "$calibration_output")"
recommended_route_order_csv="$(extract_field "recommended_route_order_csv" "$calibration_output")"
adapter_mode_strict_policy_echo="$(extract_field "adapter_mode_strict_policy_echo" "$calibration_output")"
confirmed_orders_total="$(extract_field "confirmed_orders_total" "$calibration_output")"
fee_consistency_missing_coverage_rows="$(extract_field "fee_consistency_missing_coverage_rows" "$calibration_output")"
fee_consistency_mismatch_rows="$(extract_field "fee_consistency_mismatch_rows" "$calibration_output")"
fallback_used_events="$(extract_field "fallback_used_events" "$calibration_output")"
hint_mismatch_events="$(extract_field "hint_mismatch_events" "$calibration_output")"
allowlisted_route_count="$(extract_field "allowlisted_route_count" "$calibration_output")"
primary_route="$(extract_field "primary_route" "$calibration_output")"
fallback_route="$(extract_field "fallback_route" "$calibration_output")"
primary_attempted_orders="$(extract_field "primary_attempted_orders" "$calibration_output")"
primary_success_rate_pct="$(extract_field "primary_success_rate_pct" "$calibration_output")"
primary_timeout_rate_pct="$(extract_field "primary_timeout_rate_pct" "$calibration_output")"
fallback_attempted_orders="$(extract_field "fallback_attempted_orders" "$calibration_output")"
fallback_success_rate_pct="$(extract_field "fallback_success_rate_pct" "$calibration_output")"
fallback_timeout_rate_pct="$(extract_field "fallback_timeout_rate_pct" "$calibration_output")"
ingestion_lag_ms_p95="$(extract_field "ingestion_lag_ms_p95" "$snapshot_output")"
ingestion_lag_ms_p99="$(extract_field "ingestion_lag_ms_p99" "$snapshot_output")"
parse_rejected_total="$(extract_field "parse_rejected_total" "$snapshot_output")"
parse_rejected_by_reason="$(extract_field "parse_rejected_by_reason" "$snapshot_output")"
parse_fallback_by_reason="$(extract_field "parse_fallback_by_reason" "$snapshot_output")"
replaced_ratio_last_interval="$(extract_field "replaced_ratio_last_interval" "$snapshot_output")"
execution_batch_sample_available="$(extract_field "execution_batch_sample_available" "$snapshot_output")"
submit_attempted_by_route="$(extract_field "submit_attempted_by_route" "$snapshot_output")"
submit_retry_scheduled_by_route="$(extract_field "submit_retry_scheduled_by_route" "$snapshot_output")"
submit_failed_by_route="$(extract_field "submit_failed_by_route" "$snapshot_output")"
submit_dynamic_cu_policy_enabled_by_route="$(extract_field "submit_dynamic_cu_policy_enabled_by_route" "$snapshot_output")"
submit_dynamic_cu_hint_used_by_route="$(extract_field "submit_dynamic_cu_hint_used_by_route" "$snapshot_output")"
submit_dynamic_cu_hint_api_by_route="$(extract_field "submit_dynamic_cu_hint_api_by_route" "$snapshot_output")"
submit_dynamic_cu_hint_rpc_by_route="$(extract_field "submit_dynamic_cu_hint_rpc_by_route" "$snapshot_output")"
submit_dynamic_cu_price_applied_by_route="$(extract_field "submit_dynamic_cu_price_applied_by_route" "$snapshot_output")"
submit_dynamic_cu_static_fallback_by_route="$(extract_field "submit_dynamic_cu_static_fallback_by_route" "$snapshot_output")"
submit_dynamic_tip_policy_enabled_by_route="$(extract_field "submit_dynamic_tip_policy_enabled_by_route" "$snapshot_output")"
submit_dynamic_tip_applied_by_route="$(extract_field "submit_dynamic_tip_applied_by_route" "$snapshot_output")"
submit_dynamic_tip_static_floor_by_route="$(extract_field "submit_dynamic_tip_static_floor_by_route" "$snapshot_output")"
dynamic_cu_hint_api_configured="false"
if [[ -n "$dynamic_cu_hint_api_primary_url" ]]; then
  dynamic_cu_hint_api_configured="true"
fi

submit_dynamic_cu_policy_enabled_total="$(sum_route_map_values "${submit_dynamic_cu_policy_enabled_by_route:-}")"
submit_dynamic_cu_hint_used_total="$(sum_route_map_values "${submit_dynamic_cu_hint_used_by_route:-}")"
submit_dynamic_cu_hint_api_total="$(sum_route_map_values "${submit_dynamic_cu_hint_api_by_route:-}")"
submit_dynamic_cu_hint_rpc_total="$(sum_route_map_values "${submit_dynamic_cu_hint_rpc_by_route:-}")"
submit_dynamic_cu_price_applied_total="$(sum_route_map_values "${submit_dynamic_cu_price_applied_by_route:-}")"
submit_dynamic_cu_static_fallback_total="$(sum_route_map_values "${submit_dynamic_cu_static_fallback_by_route:-}")"
submit_dynamic_tip_policy_enabled_total="$(sum_route_map_values "${submit_dynamic_tip_policy_enabled_by_route:-}")"
submit_dynamic_tip_applied_total="$(sum_route_map_values "${submit_dynamic_tip_applied_by_route:-}")"
submit_dynamic_tip_static_floor_total="$(sum_route_map_values "${submit_dynamic_tip_static_floor_by_route:-}")"
if ! execution_batch_sample_available_normalized="$(normalize_bool_token "${execution_batch_sample_available:-false}")"; then
  echo "execution_batch_sample_available from runtime snapshot must be a boolean token (true/false/1/0/yes/no/on/off), got: ${execution_batch_sample_available:-}" >&2
  exit 1
fi

dynamic_cu_policy_verdict="SKIP"
dynamic_cu_policy_reason="dynamic CU-price policy disabled in execution config"
if [[ "$dynamic_cu_policy_config_enabled" == "true" ]]; then
  if [[ "$execution_batch_sample_available_normalized" != "true" ]]; then
    dynamic_cu_policy_verdict="NO_DATA"
    dynamic_cu_policy_reason="no execution batch sample available in runtime snapshot window"
  elif (( submit_dynamic_cu_policy_enabled_total == 0 )); then
    dynamic_cu_policy_verdict="WARN"
    dynamic_cu_policy_reason="policy enabled but no dynamic CU-price submit attempts observed"
  elif (( submit_dynamic_cu_price_applied_total > 0 )); then
    dynamic_cu_policy_verdict="PASS"
    dynamic_cu_policy_reason="dynamic CU-price applied on at least one submit attempt"
  elif (( submit_dynamic_cu_hint_used_total > 0 )); then
    dynamic_cu_policy_verdict="WARN"
    dynamic_cu_policy_reason="priority-fee hints observed but all attempts stayed on static CU-price floor"
  else
    dynamic_cu_policy_verdict="WARN"
    dynamic_cu_policy_reason="no priority-fee hints observed; submits used static CU-price fallback only"
  fi
fi

dynamic_cu_hint_source_verdict="SKIP"
dynamic_cu_hint_source_reason="dynamic CU-price policy disabled in execution config"
dynamic_cu_hint_source_reason_code="policy_disabled"
if [[ "$dynamic_cu_policy_config_enabled" == "true" ]]; then
  if [[ "$execution_batch_sample_available_normalized" != "true" ]]; then
    dynamic_cu_hint_source_verdict="NO_DATA"
    dynamic_cu_hint_source_reason="no execution batch sample available in runtime snapshot window"
    dynamic_cu_hint_source_reason_code="no_execution_batch_sample"
  elif (( submit_dynamic_cu_hint_used_total == 0 )); then
    dynamic_cu_hint_source_verdict="WARN"
    dynamic_cu_hint_source_reason="no dynamic CU-price hints observed in submit attempts"
    dynamic_cu_hint_source_reason_code="hint_not_used"
  elif [[ "$dynamic_cu_hint_api_configured" == "true" ]]; then
    if (( submit_dynamic_cu_hint_api_total > 0 )); then
      dynamic_cu_hint_source_verdict="PASS"
      dynamic_cu_hint_source_reason="external Priority Fee API hints observed"
      dynamic_cu_hint_source_reason_code="api_hints_observed"
    elif (( submit_dynamic_cu_hint_rpc_total > 0 )); then
      dynamic_cu_hint_source_verdict="WARN"
      dynamic_cu_hint_source_reason="external Priority Fee API configured but only RPC fallback hints observed"
      dynamic_cu_hint_source_reason_code="api_configured_rpc_only"
    else
      dynamic_cu_hint_source_verdict="WARN"
      dynamic_cu_hint_source_reason="dynamic CU-price hints observed but source split counters are missing"
      dynamic_cu_hint_source_reason_code="source_split_missing"
    fi
  else
    if (( submit_dynamic_cu_hint_rpc_total > 0 )); then
      dynamic_cu_hint_source_verdict="PASS"
      dynamic_cu_hint_source_reason="RPC priority-fee hints observed (external API not configured)"
      dynamic_cu_hint_source_reason_code="rpc_hints_observed"
    elif (( submit_dynamic_cu_hint_api_total > 0 )); then
      dynamic_cu_hint_source_verdict="WARN"
      dynamic_cu_hint_source_reason="API hint counters observed while external Priority Fee API is not configured"
      dynamic_cu_hint_source_reason_code="api_counters_without_api_config"
    else
      dynamic_cu_hint_source_verdict="WARN"
      dynamic_cu_hint_source_reason="hint source split counters are missing despite hint usage"
      dynamic_cu_hint_source_reason_code="source_split_missing_no_api"
    fi
  fi
fi

dynamic_tip_policy_verdict="SKIP"
dynamic_tip_policy_reason="dynamic tip policy disabled in execution config"
if [[ "$dynamic_tip_policy_config_enabled" == "true" ]]; then
  if [[ "$execution_batch_sample_available_normalized" != "true" ]]; then
    dynamic_tip_policy_verdict="NO_DATA"
    dynamic_tip_policy_reason="no execution batch sample available in runtime snapshot window"
  elif (( submit_dynamic_tip_policy_enabled_total == 0 )); then
    dynamic_tip_policy_verdict="WARN"
    dynamic_tip_policy_reason="policy enabled but no dynamic tip submit attempts observed"
  elif (( submit_dynamic_tip_applied_total > 0 )); then
    dynamic_tip_policy_verdict="PASS"
    dynamic_tip_policy_reason="dynamic tip applied on at least one submit attempt"
  elif (( submit_dynamic_tip_static_floor_total > 0 )); then
    dynamic_tip_policy_verdict="WARN"
    dynamic_tip_policy_reason="dynamic tip policy active but all attempts stayed on static tip floor"
  else
    dynamic_tip_policy_verdict="WARN"
    dynamic_tip_policy_reason="dynamic tip policy active but no tip-path evidence observed"
  fi
fi

jito_rpc_policy_verdict="SKIP"
jito_rpc_policy_reason="strict jito->rpc policy gate disabled"
jito_rpc_policy_reason_code="gate_disabled"
if [[ "$go_nogo_require_jito_rpc_policy" == "true" ]]; then
  if [[ "$execution_mode_for_go_nogo" != "adapter_submit_confirm" ]]; then
    jito_rpc_policy_verdict="SKIP"
    jito_rpc_policy_reason="strict jito->rpc policy gate requires adapter_submit_confirm mode"
    jito_rpc_policy_reason_code="requires_adapter_mode"
  elif [[ "$route_profile_verdict" == "UNKNOWN" ]]; then
    jito_rpc_policy_verdict="UNKNOWN"
    jito_rpc_policy_reason="route profile verdict unknown; unable to classify strict jito->rpc policy gate"
    jito_rpc_policy_reason_code="route_profile_unknown"
  elif [[ "$route_profile_verdict" == "NO_DATA" ]]; then
    jito_rpc_policy_verdict="NO_DATA"
    jito_rpc_policy_reason="route profile has no data; strict jito->rpc policy gate cannot be evaluated"
    jito_rpc_policy_reason_code="route_profile_no_data"
  elif [[ "$route_profile_verdict" != "PASS" ]]; then
    jito_rpc_policy_verdict="WARN"
    jito_rpc_policy_reason="route profile gate is not PASS (${route_profile_verdict}); strict jito->rpc target not met"
    jito_rpc_policy_reason_code="route_profile_not_pass"
  else
    primary_route_normalized="$(normalize_route_token "${primary_route:-}")"
    fallback_route_normalized="$(normalize_route_token "${fallback_route:-}")"
    if [[ "$primary_route_normalized" == "jito" && "$fallback_route_normalized" == "rpc" ]]; then
      jito_rpc_policy_verdict="PASS"
      jito_rpc_policy_reason="route profile confirms jito primary with rpc fallback"
      jito_rpc_policy_reason_code="target_met"
    else
      jito_rpc_policy_verdict="WARN"
      jito_rpc_policy_reason="route profile primary/fallback mismatch for strict jito->rpc target (observed primary=${primary_route_normalized:-<none>}, fallback=${fallback_route_normalized:-<none>})"
      jito_rpc_policy_reason_code="target_mismatch"
    fi
  fi
fi

fastlane_feature_flag_verdict="SKIP"
fastlane_feature_flag_reason="strict fastlane-disabled gate disabled"
fastlane_feature_flag_reason_code="gate_disabled"
if [[ "$go_nogo_require_fastlane_disabled" == "true" ]]; then
  if [[ "$execution_mode_for_go_nogo" != "adapter_submit_confirm" ]]; then
    fastlane_feature_flag_verdict="SKIP"
    fastlane_feature_flag_reason="strict fastlane-disabled gate requires adapter_submit_confirm mode"
    fastlane_feature_flag_reason_code="requires_adapter_mode"
  elif [[ "$submit_fastlane_enabled" == "true" ]]; then
    fastlane_feature_flag_verdict="WARN"
    fastlane_feature_flag_reason="execution.submit_fastlane_enabled=true violates strict fastlane-disabled gate"
    fastlane_feature_flag_reason_code="fastlane_enabled"
  else
    fastlane_feature_flag_verdict="PASS"
    fastlane_feature_flag_reason="execution.submit_fastlane_enabled=false satisfies strict fastlane-disabled gate"
    fastlane_feature_flag_reason_code="fastlane_disabled"
  fi
fi

# Test-only overrides for smoke validation of verdict precedence branches.
if [[ "$go_nogo_test_mode" == "true" ]]; then
  if [[ -n "${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" ]]; then
    fee_decomposition_verdict="$(normalize_gate_verdict "$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE")"
  fi
  if [[ -n "${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" ]]; then
    route_profile_verdict="$(normalize_gate_verdict "$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE")"
  fi
fi

overall_go_nogo_verdict="HOLD"
overall_go_nogo_reason="readiness gates are not in final pass state yet"
overall_go_nogo_reason_code="readiness_not_final"
if [[ "$preflight_verdict" == "FAIL" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="adapter preflight failed: ${preflight_reason:-unknown preflight failure}"
  overall_go_nogo_reason_code="preflight_fail"
elif [[ "$preflight_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify adapter preflight verdict; fail-closed"
  overall_go_nogo_reason_code="preflight_unknown"
elif [[ "$go_nogo_require_jito_rpc_policy" == "true" && "$jito_rpc_policy_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify strict jito->rpc policy gate verdict; fail-closed"
  overall_go_nogo_reason_code="jito_policy_unknown"
elif [[ "$go_nogo_require_fastlane_disabled" == "true" && "$fastlane_feature_flag_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify strict fastlane-disabled gate verdict; fail-closed"
  overall_go_nogo_reason_code="fastlane_policy_unknown"
elif [[ "$preflight_verdict" == "PASS" && "$fee_decomposition_verdict" == "PASS" && "$route_profile_verdict" == "PASS" && ( "$go_nogo_require_jito_rpc_policy" != "true" || "$jito_rpc_policy_verdict" == "PASS" ) && ( "$go_nogo_require_fastlane_disabled" != "true" || "$fastlane_feature_flag_verdict" == "PASS" ) ]]; then
  overall_go_nogo_verdict="GO"
  overall_go_nogo_reason="adapter preflight, fee decomposition and route profile readiness gates are PASS"
  overall_go_nogo_reason_code="all_required_gates_pass"
elif [[ "$fee_decomposition_verdict" == "UNKNOWN" || "$route_profile_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify readiness gate verdicts from tool output"
  overall_go_nogo_reason_code="readiness_gate_unknown"
elif [[ "$go_nogo_require_jito_rpc_policy" == "true" && "$jito_rpc_policy_verdict" == "WARN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="strict jito->rpc policy gate not PASS: ${jito_rpc_policy_reason:-n/a}"
  overall_go_nogo_reason_code="jito_policy_not_pass"
elif [[ "$go_nogo_require_fastlane_disabled" == "true" && "$fastlane_feature_flag_verdict" == "WARN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="strict fastlane-disabled gate not PASS: ${fastlane_feature_flag_reason:-n/a}"
  overall_go_nogo_reason_code="fastlane_policy_not_pass"
elif [[ "$fee_decomposition_verdict" == "WARN" || "$route_profile_verdict" == "WARN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="at least one readiness gate is WARN; rollout escalation required before live enable"
  overall_go_nogo_reason_code="readiness_gate_warn"
elif [[ "$go_nogo_require_jito_rpc_policy" == "true" && ( "$jito_rpc_policy_verdict" == "NO_DATA" || "$jito_rpc_policy_verdict" == "SKIP" ) ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="strict jito->rpc policy gate lacks conclusive evidence: ${jito_rpc_policy_reason:-n/a}"
  overall_go_nogo_reason_code="jito_policy_inconclusive"
elif [[ "$go_nogo_require_fastlane_disabled" == "true" && "$fastlane_feature_flag_verdict" == "SKIP" ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="strict fastlane-disabled gate lacks conclusive evidence: ${fastlane_feature_flag_reason:-n/a}"
  overall_go_nogo_reason_code="fastlane_policy_inconclusive"
elif [[ "$fee_decomposition_verdict" == "NO_DATA" || "$route_profile_verdict" == "NO_DATA" ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="insufficient execution evidence in selected time window"
  overall_go_nogo_reason_code="readiness_gate_no_data"
elif [[ "$fee_decomposition_verdict" == "SKIP" || "$route_profile_verdict" == "SKIP" ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="execution mode is not adapter_submit_confirm; live readiness gates skipped"
  overall_go_nogo_reason_code="readiness_gate_skip"
else
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unrecognized go/no-go gate state; fail-closed"
  overall_go_nogo_reason_code="unrecognized_state"
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
=== Execution Go/No-Go Summary ===
utc_now: $timestamp_utc
config: $CONFIG_PATH
db: ${db_path:-unknown}
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES

fee_decomposition_verdict: $fee_decomposition_verdict
fee_decomposition_reason: ${fee_decomposition_reason:-n/a}
route_profile_verdict: $route_profile_verdict
route_profile_reason: ${route_profile_reason:-n/a}
recommended_route_order_csv: ${recommended_route_order_csv:-n/a}
adapter_mode_strict_policy_echo: ${adapter_mode_strict_policy_echo:-n/a}
confirmed_orders_total: ${confirmed_orders_total:-n/a}
fee_consistency_missing_coverage_rows: ${fee_consistency_missing_coverage_rows:-n/a}
fee_consistency_mismatch_rows: ${fee_consistency_mismatch_rows:-n/a}
fallback_used_events: ${fallback_used_events:-n/a}
hint_mismatch_events: ${hint_mismatch_events:-n/a}
allowlisted_route_count: ${allowlisted_route_count:-n/a}
primary_route: ${primary_route:-n/a}
fallback_route: ${fallback_route:-n/a}
primary_attempted_orders: ${primary_attempted_orders:-n/a}
primary_success_rate_pct: ${primary_success_rate_pct:-n/a}
primary_timeout_rate_pct: ${primary_timeout_rate_pct:-n/a}
fallback_attempted_orders: ${fallback_attempted_orders:-n/a}
fallback_success_rate_pct: ${fallback_success_rate_pct:-n/a}
fallback_timeout_rate_pct: ${fallback_timeout_rate_pct:-n/a}
preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
preflight_error_count: ${preflight_error_count:-0}

ingestion_lag_ms_p95: ${ingestion_lag_ms_p95:-n/a}
ingestion_lag_ms_p99: ${ingestion_lag_ms_p99:-n/a}
parse_rejected_total: ${parse_rejected_total:-n/a}
parse_rejected_by_reason: ${parse_rejected_by_reason:-{}}
parse_fallback_by_reason: ${parse_fallback_by_reason:-{}}
replaced_ratio_last_interval: ${replaced_ratio_last_interval:-n/a}
execution_batch_sample_available: ${execution_batch_sample_available_normalized:-false}
submit_attempted_by_route: ${submit_attempted_by_route:-{}}
submit_retry_scheduled_by_route: ${submit_retry_scheduled_by_route:-{}}
submit_failed_by_route: ${submit_failed_by_route:-{}}
submit_dynamic_cu_policy_enabled_by_route: ${submit_dynamic_cu_policy_enabled_by_route:-{}}
submit_dynamic_cu_hint_used_by_route: ${submit_dynamic_cu_hint_used_by_route:-{}}
submit_dynamic_cu_hint_api_by_route: ${submit_dynamic_cu_hint_api_by_route:-{}}
submit_dynamic_cu_hint_rpc_by_route: ${submit_dynamic_cu_hint_rpc_by_route:-{}}
submit_dynamic_cu_price_applied_by_route: ${submit_dynamic_cu_price_applied_by_route:-{}}
submit_dynamic_cu_static_fallback_by_route: ${submit_dynamic_cu_static_fallback_by_route:-{}}
submit_dynamic_tip_policy_enabled_by_route: ${submit_dynamic_tip_policy_enabled_by_route:-{}}
submit_dynamic_tip_applied_by_route: ${submit_dynamic_tip_applied_by_route:-{}}
submit_dynamic_tip_static_floor_by_route: ${submit_dynamic_tip_static_floor_by_route:-{}}
dynamic_cu_policy_config_enabled: $dynamic_cu_policy_config_enabled
dynamic_cu_policy_enabled_total: ${submit_dynamic_cu_policy_enabled_total:-0}
dynamic_cu_hint_used_total: ${submit_dynamic_cu_hint_used_total:-0}
dynamic_cu_hint_api_total: ${submit_dynamic_cu_hint_api_total:-0}
dynamic_cu_hint_rpc_total: ${submit_dynamic_cu_hint_rpc_total:-0}
dynamic_cu_hint_api_configured: ${dynamic_cu_hint_api_configured:-false}
dynamic_cu_hint_source_verdict: $dynamic_cu_hint_source_verdict
dynamic_cu_hint_source_reason: $dynamic_cu_hint_source_reason
dynamic_cu_hint_source_reason_code: $dynamic_cu_hint_source_reason_code
dynamic_cu_price_applied_total: ${submit_dynamic_cu_price_applied_total:-0}
dynamic_cu_static_fallback_total: ${submit_dynamic_cu_static_fallback_total:-0}
dynamic_cu_policy_verdict: $dynamic_cu_policy_verdict
dynamic_cu_policy_reason: $dynamic_cu_policy_reason
dynamic_tip_policy_config_enabled: $dynamic_tip_policy_config_enabled
dynamic_tip_policy_enabled_total: ${submit_dynamic_tip_policy_enabled_total:-0}
dynamic_tip_applied_total: ${submit_dynamic_tip_applied_total:-0}
dynamic_tip_static_floor_total: ${submit_dynamic_tip_static_floor_total:-0}
dynamic_tip_policy_verdict: $dynamic_tip_policy_verdict
dynamic_tip_policy_reason: $dynamic_tip_policy_reason
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy
jito_rpc_policy_verdict: $jito_rpc_policy_verdict
jito_rpc_policy_reason: $jito_rpc_policy_reason
jito_rpc_policy_reason_code: $jito_rpc_policy_reason_code
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled
submit_fastlane_enabled: $submit_fastlane_enabled
fastlane_feature_flag_verdict: $fastlane_feature_flag_verdict
fastlane_feature_flag_reason: $fastlane_feature_flag_reason
fastlane_feature_flag_reason_code: $fastlane_feature_flag_reason_code
package_bundle_enabled: $package_bundle_enabled
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}

overall_go_nogo_verdict: $overall_go_nogo_verdict
overall_go_nogo_reason: $overall_go_nogo_reason
overall_go_nogo_reason_code: $overall_go_nogo_reason_code
artifacts_written: $artifacts_written
EOF
)"

echo "$summary_output"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  calibration_path="$OUTPUT_DIR/execution_fee_calibration_${timestamp_compact}.txt"
  snapshot_path="$OUTPUT_DIR/runtime_snapshot_${timestamp_compact}.txt"
  preflight_path="$OUTPUT_DIR/execution_adapter_preflight_${timestamp_compact}.txt"
  summary_path="$OUTPUT_DIR/execution_go_nogo_summary_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_go_nogo_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$calibration_output" > "$calibration_path"
  printf '%s\n' "$snapshot_output" > "$snapshot_path"
  printf '%s\n' "$preflight_output" > "$preflight_path"
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

  if [[ "$package_bundle_enabled" == "true" ]]; then
    run_package_bundle_once
  fi

  cat >>"$summary_path" <<EOF
package_bundle_artifacts_written: $package_bundle_artifacts_written
package_bundle_exit_code: $package_bundle_exit_code
package_bundle_error: $package_bundle_error
EOF

  calibration_sha256="$(sha256_file_value "$calibration_path")"
  snapshot_sha256="$(sha256_file_value "$snapshot_path")"
  preflight_sha256="$(sha256_file_value "$preflight_path")"
  summary_sha256="$(sha256_file_value "$summary_path")"
  cat >"$manifest_path" <<EOF
calibration_sha256: $calibration_sha256
snapshot_sha256: $snapshot_sha256
preflight_sha256: $preflight_sha256
summary_sha256: $summary_sha256
EOF
  manifest_sha256="$(sha256_file_value "$manifest_path")"

  if [[ "$package_bundle_enabled" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi

  echo
  echo "artifacts_written: true"
  echo "artifact_calibration: $calibration_path"
  echo "artifact_snapshot: $snapshot_path"
  echo "artifact_preflight: $preflight_path"
  echo "artifact_summary: $summary_path"
  echo "artifact_manifest: $manifest_path"
  echo "calibration_sha256: $calibration_sha256"
  echo "snapshot_sha256: $snapshot_sha256"
  echo "preflight_sha256: $preflight_sha256"
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

  if [[ "$package_bundle_enabled" == "true" && "$package_bundle_artifacts_written" != "true" ]]; then
    exit 1
  fi
fi
