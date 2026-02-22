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
RUN_TESTS="${RUN_TESTS:-true}"
# Test-only override: when true, allows GO with RUN_TESTS=false.
DEVNET_REHEARSAL_TEST_MODE="${DEVNET_REHEARSAL_TEST_MODE:-false}"
WINDOWED_SIGNOFF_WINDOWS_CSV="${WINDOWED_SIGNOFF_WINDOWS_CSV:-1,6,24}"
WINDOWED_SIGNOFF_REQUIRED="${WINDOWED_SIGNOFF_REQUIRED:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-false}"

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

cfg_or_env_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  if [[ -n "${!env_name+x}" ]]; then
    printf "%s" "${!env_name}"
    return
  fi
  cfg_value "$section" "$key"
}

redacted_endpoint_label() {
  local endpoint="$1"
  endpoint="$(trim_string "$endpoint")"
  if [[ -z "$endpoint" ]]; then
    printf "missing"
    return
  fi
  python3 - "$endpoint" <<'PY'
import sys
from urllib.parse import urlsplit

raw = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
if not raw:
    print("missing")
    raise SystemExit(0)
try:
    parsed = urlsplit(raw)
except Exception:
    print("invalid_endpoint")
    raise SystemExit(0)

scheme = (parsed.scheme or "").lower()
hostname = parsed.hostname or ""
if scheme not in {"http", "https"} or not hostname:
    print("invalid_endpoint")
    raise SystemExit(0)
if parsed.port is None:
    print(f"{scheme}://{hostname}")
else:
    print(f"{scheme}://{hostname}:{parsed.port}")
PY
}

validate_devnet_rpc_url() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import sys
from urllib.parse import urlsplit

raw = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
if not raw:
    print("missing")
    raise SystemExit(1)
try:
    parsed = urlsplit(raw)
except Exception as exc:
    print(f"invalid url parse: {exc}")
    raise SystemExit(1)

scheme = (parsed.scheme or "").lower()
hostname = parsed.hostname or ""
if scheme not in {"http", "https"}:
    print(f"unsupported scheme: {parsed.scheme or '<none>'} (expected http|https)")
    raise SystemExit(1)
if not hostname:
    print("host is missing")
    raise SystemExit(1)
if "replace_me" in raw.lower():
    print("placeholder value detected (REPLACE_ME)")
    raise SystemExit(1)
print("ok")
PY
}

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

run_tests_norm="$(normalize_bool_token "$RUN_TESTS")"
test_mode_norm="$(normalize_bool_token "$DEVNET_REHEARSAL_TEST_MODE")"
windowed_signoff_required_norm="$(normalize_bool_token "$WINDOWED_SIGNOFF_REQUIRED")"
windowed_signoff_require_dynamic_hint_source_pass_norm="$(normalize_bool_token "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS")"

execution_enabled_raw="$(cfg_or_env_string execution enabled SOLANA_COPY_BOT_EXECUTION_ENABLED)"
execution_enabled="$(normalize_bool_token "${execution_enabled_raw:-false}")"
execution_mode="$(trim_string "$(cfg_or_env_string execution mode SOLANA_COPY_BOT_EXECUTION_MODE)")"
if [[ -z "$execution_mode" ]]; then
  execution_mode="paper"
fi
devnet_rpc_url="$(trim_string "$(cfg_or_env_string execution rpc_devnet_http_url SOLANA_COPY_BOT_EXECUTION_RPC_DEVNET_HTTP_URL)")"
devnet_rpc_label="$(redacted_endpoint_label "$devnet_rpc_url")"

declare -a config_errors=()
if [[ "$execution_enabled" != "true" ]]; then
  config_errors+=("execution.enabled must be true for devnet rehearsal")
fi
if [[ "$execution_mode" != "adapter_submit_confirm" ]]; then
  config_errors+=("execution.mode must be adapter_submit_confirm for Stage C.5 rehearsal (got: ${execution_mode:-<empty>})")
fi
if ! devnet_rpc_check="$(validate_devnet_rpc_url "$devnet_rpc_url" 2>&1)"; then
  config_errors+=("execution.rpc_devnet_http_url invalid: $devnet_rpc_check")
fi

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
preflight_verdict="$(extract_field "preflight_verdict" "$preflight_output")"
preflight_verdict="$(trim_string "${preflight_verdict:-UNKNOWN}")"
if [[ -z "$preflight_verdict" ]]; then
  preflight_verdict="UNKNOWN"
fi
preflight_reason="$(extract_field "preflight_reason" "$preflight_output")"
preflight_reason="$(trim_string "${preflight_reason:-}")"
if [[ "$preflight_verdict" == "UNKNOWN" && "$preflight_exit_code" -ne 0 && -z "$preflight_reason" ]]; then
  preflight_reason="adapter preflight exited with code $preflight_exit_code without recognizable verdict"
fi

go_nogo_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  go_nogo_output_dir="$OUTPUT_DIR/go_nogo"
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
  bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
)"; then
  go_nogo_exit_code=0
else
  go_nogo_exit_code=$?
fi
go_nogo_nested_capture_path=""
if [[ -n "$go_nogo_output_dir" ]]; then
  go_nogo_nested_capture_path="$go_nogo_output_dir/execution_go_nogo_captured_${timestamp_compact}.txt"
  printf '%s\n' "$go_nogo_output" > "$go_nogo_nested_capture_path"
fi

windowed_signoff_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  windowed_signoff_output_dir="$OUTPUT_DIR/windowed_signoff"
  mkdir -p "$windowed_signoff_output_dir"
fi

windowed_signoff_output=""
windowed_signoff_exit_code=0
if windowed_signoff_output="$(
  CONFIG_PATH="$CONFIG_PATH" \
  SERVICE="$SERVICE" \
  GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}" \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
  GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
  WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
  OUTPUT_DIR="$windowed_signoff_output_dir" \
  bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" "$WINDOWED_SIGNOFF_WINDOWS_CSV" "$RISK_EVENTS_MINUTES" 2>&1
)"; then
  windowed_signoff_exit_code=0
else
  windowed_signoff_exit_code=$?
fi
windowed_signoff_nested_capture_path=""
if [[ -n "$windowed_signoff_output_dir" ]]; then
  windowed_signoff_nested_capture_path="$windowed_signoff_output_dir/execution_windowed_signoff_captured_${timestamp_compact}.txt"
  printf '%s\n' "$windowed_signoff_output" > "$windowed_signoff_nested_capture_path"
fi

overall_go_nogo_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
overall_go_nogo_reason="$(trim_string "$(extract_field "overall_go_nogo_reason" "$go_nogo_output")")"
dynamic_cu_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_policy_verdict" "$go_nogo_output")")"
dynamic_cu_policy_reason="$(trim_string "$(extract_field "dynamic_cu_policy_reason" "$go_nogo_output")")"
dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$go_nogo_output")")"
dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$go_nogo_output")")"
dynamic_cu_hint_api_total="$(trim_string "$(extract_field "dynamic_cu_hint_api_total" "$go_nogo_output")")"
dynamic_cu_hint_rpc_total="$(trim_string "$(extract_field "dynamic_cu_hint_rpc_total" "$go_nogo_output")")"
dynamic_cu_hint_api_configured="$(trim_string "$(extract_field "dynamic_cu_hint_api_configured" "$go_nogo_output")")"
dynamic_cu_hint_source_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_hint_source_verdict" "$go_nogo_output")")"
dynamic_cu_hint_source_reason="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason" "$go_nogo_output")")"
primary_route="$(trim_string "$(extract_field "primary_route" "$go_nogo_output")")"
fallback_route="$(trim_string "$(extract_field "fallback_route" "$go_nogo_output")")"
primary_attempted_orders="$(trim_string "$(extract_field "primary_attempted_orders" "$go_nogo_output")")"
primary_success_rate_pct="$(trim_string "$(extract_field "primary_success_rate_pct" "$go_nogo_output")")"
primary_timeout_rate_pct="$(trim_string "$(extract_field "primary_timeout_rate_pct" "$go_nogo_output")")"
fallback_attempted_orders="$(trim_string "$(extract_field "fallback_attempted_orders" "$go_nogo_output")")"
fallback_success_rate_pct="$(trim_string "$(extract_field "fallback_success_rate_pct" "$go_nogo_output")")"
fallback_timeout_rate_pct="$(trim_string "$(extract_field "fallback_timeout_rate_pct" "$go_nogo_output")")"
confirmed_orders_total="$(trim_string "$(extract_field "confirmed_orders_total" "$go_nogo_output")")"
fee_consistency_missing_coverage_rows="$(trim_string "$(extract_field "fee_consistency_missing_coverage_rows" "$go_nogo_output")")"
fee_consistency_mismatch_rows="$(trim_string "$(extract_field "fee_consistency_mismatch_rows" "$go_nogo_output")")"
fallback_used_events="$(trim_string "$(extract_field "fallback_used_events" "$go_nogo_output")")"
hint_mismatch_events="$(trim_string "$(extract_field "hint_mismatch_events" "$go_nogo_output")")"
go_nogo_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$go_nogo_output")")"
go_nogo_calibration_sha256="$(trim_string "$(extract_field "calibration_sha256" "$go_nogo_output")")"
go_nogo_snapshot_sha256="$(trim_string "$(extract_field "snapshot_sha256" "$go_nogo_output")")"
go_nogo_preflight_sha256="$(trim_string "$(extract_field "preflight_sha256" "$go_nogo_output")")"
go_nogo_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$go_nogo_output")")"
windowed_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "signoff_verdict" "$windowed_signoff_output")")"
windowed_signoff_reason="$(trim_string "$(extract_field "signoff_reason" "$windowed_signoff_output")")"
windowed_signoff_require_dynamic_hint_source_pass="$(normalize_bool_token "$(extract_field "windowed_signoff_require_dynamic_hint_source_pass" "$windowed_signoff_output")")"
windowed_signoff_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$windowed_signoff_output")")"
windowed_signoff_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$windowed_signoff_output")")"
if [[ "$overall_go_nogo_verdict" == "UNKNOWN" && "$go_nogo_exit_code" -ne 0 && -z "$overall_go_nogo_reason" ]]; then
  overall_go_nogo_reason="execution_go_nogo_report exited with code $go_nogo_exit_code"
fi
if [[ "$windowed_signoff_verdict" == "UNKNOWN" && "$windowed_signoff_exit_code" -ne 0 && -z "$windowed_signoff_reason" ]]; then
  windowed_signoff_reason="execution_windowed_signoff_report exited with code $windowed_signoff_exit_code"
fi
if [[ -z "$dynamic_cu_policy_reason" ]]; then
  dynamic_cu_policy_reason="n/a"
fi
if [[ -z "$dynamic_tip_policy_reason" ]]; then
  dynamic_tip_policy_reason="n/a"
fi
if [[ -z "$dynamic_cu_hint_source_reason" ]]; then
  dynamic_cu_hint_source_reason="n/a"
fi
if [[ -z "$windowed_signoff_reason" ]]; then
  windowed_signoff_reason="n/a"
fi

tests_total=0
tests_failed=0
tests_run="false"
test_log=""
test_log_path="$(mktemp)"
trap 'rm -f "$test_log_path"' EXIT
if [[ "$run_tests_norm" == "true" ]]; then
  tests_run="true"
  test_commands=(
    "cargo test -p copybot-app -q risk_guard_infra_blocks_when_parser_stall_detected"
    "cargo test -p copybot-app -q risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold"
    "cargo test -p copybot-app -q risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary"
    "cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price"
    "cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing"
    "cargo test -p copybot-storage -q live_unrealized_pnl_sol_ignores_micro_swap_outlier_price"
    "cargo test -p copybot-storage -q live_unrealized_pnl_sol_counts_missing_when_only_micro_quotes_exist"
    "cargo test -p copybot-execution -q adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject"
    "cargo test -p copybot-execution -q adapter_intent_simulator_redacts_endpoint_on_retryable_send_error"
  )
  for test_cmd in "${test_commands[@]}"; do
    tests_total=$((tests_total + 1))
    test_log+=$'\n'"[test] $test_cmd"$'\n'
    if eval "$test_cmd" >>"$test_log_path" 2>&1; then
      test_log+="result: PASS"$'\n'
    else
      tests_failed=$((tests_failed + 1))
      test_log+="result: FAIL"$'\n'
    fi
  done
else
  test_log="tests skipped (RUN_TESTS=false)"
fi

devnet_rehearsal_verdict="GO"
devnet_rehearsal_reason="all Stage C.5 gates passed"
if ((${#config_errors[@]} > 0)); then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="${config_errors[0]}"
elif [[ "$preflight_verdict" != "PASS" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="adapter preflight not PASS (${preflight_verdict}): ${preflight_reason:-n/a}"
elif [[ "$overall_go_nogo_verdict" == "UNKNOWN" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="go/no-go verdict unknown: ${overall_go_nogo_reason:-n/a}"
elif [[ "$overall_go_nogo_verdict" == "NO_GO" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="${overall_go_nogo_reason:-go/no-go returned NO_GO}"
elif [[ "$overall_go_nogo_verdict" == "HOLD" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="${overall_go_nogo_reason:-go/no-go returned HOLD}"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "UNKNOWN" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="windowed signoff verdict unknown: ${windowed_signoff_reason:-n/a}"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "NO_GO" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="windowed signoff returned NO_GO: ${windowed_signoff_reason:-n/a}"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "HOLD" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="windowed signoff returned HOLD: ${windowed_signoff_reason:-n/a}"
elif [[ "$tests_run" != "true" && "$test_mode_norm" != "true" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="targeted regression tests were skipped (RUN_TESTS=false)"
elif ((tests_failed > 0)); then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="targeted regression tests failed: ${tests_failed}/${tests_total}"
elif [[ "$tests_run" != "true" && "$test_mode_norm" == "true" ]]; then
  devnet_rehearsal_verdict="GO"
  devnet_rehearsal_reason="test mode override active (RUN_TESTS=false, DEVNET_REHEARSAL_TEST_MODE=true)"
fi

summary_output="$(cat <<EOF
=== Execution Devnet Rehearsal ===
utc_now: $timestamp_utc
config: $CONFIG_PATH
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES

execution_enabled: $execution_enabled
execution_mode: $execution_mode
rpc_devnet_http_url: $devnet_rpc_label
config_error_count: ${#config_errors[@]}
preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
overall_go_nogo_verdict: $overall_go_nogo_verdict
overall_go_nogo_reason: ${overall_go_nogo_reason:-n/a}
dynamic_cu_policy_verdict: $dynamic_cu_policy_verdict
dynamic_cu_policy_reason: $dynamic_cu_policy_reason
dynamic_tip_policy_verdict: $dynamic_tip_policy_verdict
dynamic_tip_policy_reason: $dynamic_tip_policy_reason
dynamic_cu_hint_api_total: ${dynamic_cu_hint_api_total:-n/a}
dynamic_cu_hint_rpc_total: ${dynamic_cu_hint_rpc_total:-n/a}
dynamic_cu_hint_api_configured: ${dynamic_cu_hint_api_configured:-false}
dynamic_cu_hint_source_verdict: ${dynamic_cu_hint_source_verdict:-unknown}
dynamic_cu_hint_source_reason: ${dynamic_cu_hint_source_reason:-n/a}
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
windowed_signoff_required: $windowed_signoff_required_norm
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass
windowed_signoff_exit_code: $windowed_signoff_exit_code
windowed_signoff_verdict: ${windowed_signoff_verdict:-unknown}
windowed_signoff_reason: ${windowed_signoff_reason:-n/a}
windowed_signoff_artifact_manifest: ${windowed_signoff_artifact_manifest:-n/a}
windowed_signoff_summary_sha256: ${windowed_signoff_summary_sha256:-n/a}
tests_run: $tests_run
tests_total: $tests_total
tests_failed: $tests_failed
devnet_rehearsal_verdict: $devnet_rehearsal_verdict
devnet_rehearsal_reason: $devnet_rehearsal_reason
EOF
)"

echo "$summary_output"
if ((${#config_errors[@]} > 0)); then
  for error_line in "${config_errors[@]}"; do
    echo "config_error: $error_line"
  done
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/execution_devnet_rehearsal_summary_${timestamp_compact}.txt"
  preflight_path="$OUTPUT_DIR/execution_devnet_rehearsal_preflight_${timestamp_compact}.txt"
  go_nogo_path="$OUTPUT_DIR/execution_devnet_rehearsal_go_nogo_${timestamp_compact}.txt"
  windowed_signoff_path="$OUTPUT_DIR/execution_devnet_rehearsal_windowed_signoff_${timestamp_compact}.txt"
  tests_path="$OUTPUT_DIR/execution_devnet_rehearsal_tests_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_devnet_rehearsal_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" > "$summary_path"
  printf '%s\n' "$preflight_output" > "$preflight_path"
  printf '%s\n' "$go_nogo_output" > "$go_nogo_path"
  printf '%s\n' "$windowed_signoff_output" > "$windowed_signoff_path"
  printf '%s\n' "$test_log" > "$tests_path"

  summary_sha256="$(sha256_file_value "$summary_path")"
  preflight_sha256="$(sha256_file_value "$preflight_path")"
  go_nogo_sha256="$(sha256_file_value "$go_nogo_path")"
  windowed_signoff_sha256="$(sha256_file_value "$windowed_signoff_path")"
  tests_sha256="$(sha256_file_value "$tests_path")"
  if [[ -n "$go_nogo_nested_capture_path" ]]; then
    go_nogo_nested_capture_sha256="$(sha256_file_value "$go_nogo_nested_capture_path")"
  else
    go_nogo_nested_capture_sha256="n/a"
  fi
  if [[ -n "$windowed_signoff_nested_capture_path" ]]; then
    windowed_signoff_nested_capture_sha256="$(sha256_file_value "$windowed_signoff_nested_capture_path")"
  else
    windowed_signoff_nested_capture_sha256="n/a"
  fi
  cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
preflight_sha256: $preflight_sha256
go_nogo_sha256: $go_nogo_sha256
windowed_signoff_sha256: $windowed_signoff_sha256
tests_sha256: $tests_sha256
go_nogo_nested_capture_sha256: $go_nogo_nested_capture_sha256
windowed_signoff_nested_capture_sha256: $windowed_signoff_nested_capture_sha256
EOF

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_preflight: $preflight_path"
  echo "artifact_go_nogo: $go_nogo_path"
  echo "artifact_windowed_signoff: $windowed_signoff_path"
  echo "artifact_tests: $tests_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "preflight_sha256: $preflight_sha256"
  echo "go_nogo_sha256: $go_nogo_sha256"
  echo "windowed_signoff_sha256: $windowed_signoff_sha256"
  echo "tests_sha256: $tests_sha256"
  if [[ -n "$go_nogo_nested_capture_path" ]]; then
    echo "artifact_go_nogo_nested_capture: $go_nogo_nested_capture_path"
    echo "go_nogo_nested_capture_sha256: $go_nogo_nested_capture_sha256"
  fi
  if [[ -n "$windowed_signoff_nested_capture_path" ]]; then
    echo "artifact_windowed_signoff_nested_capture: $windowed_signoff_nested_capture_path"
    echo "windowed_signoff_nested_capture_sha256: $windowed_signoff_nested_capture_sha256"
  fi
fi

case "$devnet_rehearsal_verdict" in
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
