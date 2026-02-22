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

rotation_output_dir=""
rehearsal_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  rotation_output_dir="$OUTPUT_DIR/rotation"
  rehearsal_output_dir="$OUTPUT_DIR/rehearsal"
  mkdir -p "$rotation_output_dir" "$rehearsal_output_dir"
fi

rotation_output=""
rotation_exit_code=3
rotation_verdict="UNKNOWN"
rotation_reason="rotation helper not executed"
if [[ -f "$ADAPTER_ENV_PATH" ]]; then
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
else
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
dynamic_cu_policy_verdict=""
dynamic_cu_policy_reason=""
dynamic_tip_policy_verdict=""
dynamic_tip_policy_reason=""
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
tests_run=""
tests_failed=""
if [[ ! "$WINDOW_HOURS" =~ ^[0-9]+$ || ! "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
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
      RUN_TESTS="$RUN_TESTS" \
      DEVNET_REHEARSAL_TEST_MODE="$DEVNET_REHEARSAL_TEST_MODE" \
      GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}" \
      GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
      GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
      bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    rehearsal_exit_code=0
  else
    rehearsal_exit_code=$?
  fi

  rehearsal_verdict="$(normalize_rehearsal_verdict "$(extract_field "devnet_rehearsal_verdict" "$rehearsal_output")")"
  rehearsal_reason="$(trim_string "$(extract_field "devnet_rehearsal_reason" "$rehearsal_output")")"
  preflight_verdict="$(trim_string "$(extract_field "preflight_verdict" "$rehearsal_output")")"
  go_nogo_verdict="$(trim_string "$(extract_field "overall_go_nogo_verdict" "$rehearsal_output")")"
  dynamic_cu_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_policy_verdict" "$rehearsal_output")")"
  dynamic_cu_policy_reason="$(trim_string "$(extract_field "dynamic_cu_policy_reason" "$rehearsal_output")")"
  dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$rehearsal_output")")"
  dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$rehearsal_output")")"
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
  tests_run="$(trim_string "$(extract_field "tests_run" "$rehearsal_output")")"
  tests_failed="$(trim_string "$(extract_field "tests_failed" "$rehearsal_output")")"
  if [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
    rehearsal_reason="unable to classify rehearsal verdict (exit=$rehearsal_exit_code)"
  elif [[ -z "$rehearsal_reason" ]]; then
    rehearsal_reason="execution devnet rehearsal reported $rehearsal_verdict"
  fi
fi

adapter_rollout_verdict="NO_GO"
adapter_rollout_reason="unrecognized rollout gate state"
if ((${#input_errors[@]} > 0)); then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="${input_errors[0]}"
elif [[ "$rotation_verdict" == "FAIL" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="rotation readiness failed: ${rotation_reason:-n/a}"
elif [[ "$rotation_verdict" == "UNKNOWN" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="rotation readiness verdict unknown; fail-closed"
elif [[ "$rehearsal_verdict" == "NO_GO" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="devnet rehearsal returned NO_GO: ${rehearsal_reason:-n/a}"
elif [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
  adapter_rollout_verdict="NO_GO"
  adapter_rollout_reason="devnet rehearsal verdict unknown; fail-closed"
elif [[ "$rotation_verdict" == "WARN" || "$rehearsal_verdict" == "HOLD" ]]; then
  adapter_rollout_verdict="HOLD"
  if [[ "$rotation_verdict" == "WARN" ]]; then
    adapter_rollout_reason="rotation readiness returned WARN: ${rotation_reason:-n/a}"
  else
    adapter_rollout_reason="devnet rehearsal returned HOLD: ${rehearsal_reason:-n/a}"
  fi
elif [[ "$rotation_verdict" == "PASS" && "$rehearsal_verdict" == "GO" ]]; then
  adapter_rollout_verdict="GO"
  adapter_rollout_reason="rotation readiness and devnet rehearsal gates passed"
fi

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

devnet_rehearsal_verdict: $rehearsal_verdict
devnet_rehearsal_reason: ${rehearsal_reason:-n/a}
devnet_rehearsal_exit_code: $rehearsal_exit_code
preflight_verdict: ${preflight_verdict:-unknown}
overall_go_nogo_verdict: ${go_nogo_verdict:-unknown}
dynamic_cu_policy_verdict: ${dynamic_cu_policy_verdict:-unknown}
dynamic_cu_policy_reason: ${dynamic_cu_policy_reason:-n/a}
dynamic_tip_policy_verdict: ${dynamic_tip_policy_verdict:-unknown}
dynamic_tip_policy_reason: ${dynamic_tip_policy_reason:-n/a}
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
tests_run: ${tests_run:-unknown}
tests_failed: ${tests_failed:-unknown}
input_error_count: ${#input_errors[@]}

adapter_rollout_verdict: $adapter_rollout_verdict
adapter_rollout_reason: $adapter_rollout_reason
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
  manifest_path="$OUTPUT_DIR/adapter_rollout_evidence_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" >"$summary_path"
  printf '%s\n' "$rotation_output" >"$rotation_capture_path"
  printf '%s\n' "$rehearsal_output" >"$rehearsal_capture_path"

  summary_sha256="$(sha256_file_value "$summary_path")"
  rotation_capture_sha256="$(sha256_file_value "$rotation_capture_path")"
  rehearsal_capture_sha256="$(sha256_file_value "$rehearsal_capture_path")"
  cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
rotation_capture_sha256: $rotation_capture_sha256
rehearsal_capture_sha256: $rehearsal_capture_sha256
EOF

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_rotation_capture: $rotation_capture_path"
  echo "artifact_rehearsal_capture: $rehearsal_capture_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "rotation_capture_sha256: $rotation_capture_sha256"
  echo "rehearsal_capture_sha256: $rehearsal_capture_sha256"
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
