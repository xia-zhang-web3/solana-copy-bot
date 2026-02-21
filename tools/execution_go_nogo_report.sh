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
ingestion_lag_ms_p95="$(extract_field "ingestion_lag_ms_p95" "$snapshot_output")"
ingestion_lag_ms_p99="$(extract_field "ingestion_lag_ms_p99" "$snapshot_output")"
parse_rejected_total="$(extract_field "parse_rejected_total" "$snapshot_output")"
parse_rejected_by_reason="$(extract_field "parse_rejected_by_reason" "$snapshot_output")"
parse_fallback_by_reason="$(extract_field "parse_fallback_by_reason" "$snapshot_output")"
replaced_ratio_last_interval="$(extract_field "replaced_ratio_last_interval" "$snapshot_output")"

# Test-only overrides for smoke validation of verdict precedence branches.
go_nogo_test_mode="$(normalize_bool_token "${GO_NOGO_TEST_MODE:-false}")"
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
if [[ "$preflight_verdict" == "FAIL" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="adapter preflight failed: ${preflight_reason:-unknown preflight failure}"
elif [[ "$preflight_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify adapter preflight verdict; fail-closed"
elif [[ "$preflight_verdict" == "PASS" && "$fee_decomposition_verdict" == "PASS" && "$route_profile_verdict" == "PASS" ]]; then
  overall_go_nogo_verdict="GO"
  overall_go_nogo_reason="adapter preflight, fee decomposition and route profile readiness gates are PASS"
elif [[ "$fee_decomposition_verdict" == "UNKNOWN" || "$route_profile_verdict" == "UNKNOWN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unable to classify readiness gate verdicts from tool output"
elif [[ "$fee_decomposition_verdict" == "WARN" || "$route_profile_verdict" == "WARN" ]]; then
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="at least one readiness gate is WARN; rollout escalation required before live enable"
elif [[ "$fee_decomposition_verdict" == "NO_DATA" || "$route_profile_verdict" == "NO_DATA" ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="insufficient execution evidence in selected time window"
elif [[ "$fee_decomposition_verdict" == "SKIP" || "$route_profile_verdict" == "SKIP" ]]; then
  overall_go_nogo_verdict="HOLD"
  overall_go_nogo_reason="execution mode is not adapter_submit_confirm; live readiness gates skipped"
else
  overall_go_nogo_verdict="NO_GO"
  overall_go_nogo_reason="unrecognized go/no-go gate state; fail-closed"
fi

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
preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
preflight_error_count: ${preflight_error_count:-0}

ingestion_lag_ms_p95: ${ingestion_lag_ms_p95:-n/a}
ingestion_lag_ms_p99: ${ingestion_lag_ms_p99:-n/a}
parse_rejected_total: ${parse_rejected_total:-n/a}
parse_rejected_by_reason: ${parse_rejected_by_reason:-{}}
parse_fallback_by_reason: ${parse_fallback_by_reason:-{}}
replaced_ratio_last_interval: ${replaced_ratio_last_interval:-n/a}

overall_go_nogo_verdict: $overall_go_nogo_verdict
overall_go_nogo_reason: $overall_go_nogo_reason
EOF
)"

echo "$summary_output"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  calibration_path="$OUTPUT_DIR/execution_fee_calibration_${timestamp_compact}.txt"
  snapshot_path="$OUTPUT_DIR/runtime_snapshot_${timestamp_compact}.txt"
  preflight_path="$OUTPUT_DIR/execution_adapter_preflight_${timestamp_compact}.txt"
  summary_path="$OUTPUT_DIR/execution_go_nogo_summary_${timestamp_compact}.txt"
  printf '%s\n' "$calibration_output" > "$calibration_path"
  printf '%s\n' "$snapshot_output" > "$snapshot_path"
  printf '%s\n' "$preflight_output" > "$preflight_path"
  printf '%s\n' "$summary_output" > "$summary_path"
  echo
  echo "artifacts_written: true"
  echo "artifact_calibration: $calibration_path"
  echo "artifact_snapshot: $snapshot_path"
  echo "artifact_preflight: $preflight_path"
  echo "artifact_summary: $summary_path"
fi
