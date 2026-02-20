#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-60}"
ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-/etc/solana-copy-bot/adapter.env}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
SERVICE="${SERVICE:-solana-copy-bot}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
RUN_TESTS="${RUN_TESTS:-true}"
DEVNET_REHEARSAL_TEST_MODE="${DEVNET_REHEARSAL_TEST_MODE:-false}"

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]]; then
  echo "window hours must be an integer (got: $WINDOW_HOURS)" >&2
  exit 1
fi

if ! [[ "$RISK_EVENTS_MINUTES" =~ ^[0-9]+$ ]]; then
  echo "risk events minutes must be an integer (got: $RISK_EVENTS_MINUTES)" >&2
  exit 1
fi

if [[ ! -f "$ADAPTER_ENV_PATH" ]]; then
  echo "adapter env file not found: $ADAPTER_ENV_PATH" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

extract_field() {
  local key="$1"
  local text="$2"
  printf '%s\n' "$text" | awk -F': ' -v key="$key" '
    $1 == key {
      print substr($0, index($0, ": ") + 2)
      exit
    }
  '
}

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

normalize_rotation_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
  PASS | WARN | FAIL)
    printf '%s' "$raw"
    ;;
  *)
    printf 'UNKNOWN'
    ;;
  esac
}

normalize_rehearsal_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
  GO | HOLD | NO_GO)
    printf '%s' "$raw"
    ;;
  *)
    printf 'UNKNOWN'
    ;;
  esac
}

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

rotation_output_dir=""
rehearsal_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  rotation_output_dir="$OUTPUT_DIR/rotation"
  rehearsal_output_dir="$OUTPUT_DIR/rehearsal"
  mkdir -p "$rotation_output_dir" "$rehearsal_output_dir"
fi

rotation_output=""
rotation_exit_code=0
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
rotation_first_error="$(printf '%s\n' "$rotation_output" | awk -F': ' '$1=="error" {print substr($0, index($0, ": ") + 2); exit}')"
rotation_first_warning="$(printf '%s\n' "$rotation_output" | awk -F': ' '$1=="warning" {print substr($0, index($0, ": ") + 2); exit}')"
if [[ "$rotation_verdict" == "FAIL" ]]; then
  rotation_reason="$(trim_string "${rotation_first_error:-${rotation_reason:-rotation helper reported FAIL}}")"
elif [[ "$rotation_verdict" == "WARN" ]]; then
  rotation_reason="$(trim_string "${rotation_first_warning:-${rotation_reason:-rotation helper reported WARN}}")"
elif [[ "$rotation_verdict" == "UNKNOWN" ]]; then
  rotation_reason="unable to classify rotation helper verdict (exit=$rotation_exit_code)"
elif [[ -z "${rotation_reason:-}" ]]; then
  rotation_reason="rotation helper PASS"
fi

rehearsal_output=""
rehearsal_exit_code=0
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
tests_run="$(trim_string "$(extract_field "tests_run" "$rehearsal_output")")"
tests_failed="$(trim_string "$(extract_field "tests_failed" "$rehearsal_output")")"
if [[ "$rehearsal_verdict" == "UNKNOWN" ]]; then
  rehearsal_reason="unable to classify rehearsal verdict (exit=$rehearsal_exit_code)"
elif [[ -z "$rehearsal_reason" ]]; then
  rehearsal_reason="execution devnet rehearsal reported $rehearsal_verdict"
fi

adapter_rollout_verdict="NO_GO"
adapter_rollout_reason="unrecognized rollout gate state"
if [[ "$rotation_verdict" == "FAIL" ]]; then
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
tests_run: ${tests_run:-unknown}
tests_failed: ${tests_failed:-unknown}

adapter_rollout_verdict: $adapter_rollout_verdict
adapter_rollout_reason: $adapter_rollout_reason
EOF
)"

echo "$summary_output"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/adapter_rollout_evidence_summary_${timestamp_compact}.txt"
  rotation_capture_path="$OUTPUT_DIR/adapter_secret_rotation_captured_${timestamp_compact}.txt"
  rehearsal_capture_path="$OUTPUT_DIR/execution_devnet_rehearsal_captured_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" >"$summary_path"
  printf '%s\n' "$rotation_output" >"$rotation_capture_path"
  printf '%s\n' "$rehearsal_output" >"$rehearsal_capture_path"
  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_rotation_capture: $rotation_capture_path"
  echo "artifact_rehearsal_capture: $rehearsal_capture_path"
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
