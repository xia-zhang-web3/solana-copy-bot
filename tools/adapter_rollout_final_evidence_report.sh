#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOW_HOURS="${1:-24}"
RISK_EVENTS_MINUTES="${2:-60}"
ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-/etc/solana-copy-bot/adapter.env}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/live.toml}}"
SERVICE="${SERVICE:-solana-copy-bot}"
OUTPUT_ROOT="${OUTPUT_ROOT:-state/adapter-rollout-final-$(date -u +"%Y%m%dT%H%M%SZ")}"

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
REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="${REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED:-true}"
REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"

ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"

REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-$GO_NOGO_TEST_MODE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE}"
REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

mkdir -p "$OUTPUT_ROOT"
rollout_output_dir="$OUTPUT_ROOT/rollout"
mkdir -p "$rollout_output_dir"

rollout_output=""
rollout_exit_code=3
if rollout_output="$(
  ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
    CONFIG_PATH="$CONFIG_PATH" \
    SERVICE="$SERVICE" \
    OUTPUT_DIR="$rollout_output_dir" \
    RUN_TESTS="$RUN_TESTS" \
    DEVNET_REHEARSAL_TEST_MODE="$DEVNET_REHEARSAL_TEST_MODE" \
    GO_NOGO_TEST_MODE="$GO_NOGO_TEST_MODE" \
    GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
    GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
    WINDOWED_SIGNOFF_REQUIRED="$WINDOWED_SIGNOFF_REQUIRED" \
    WINDOWED_SIGNOFF_WINDOWS_CSV="$WINDOWED_SIGNOFF_WINDOWS_CSV" \
    WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" \
    WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" \
    GO_NOGO_REQUIRE_JITO_RPC_POLICY="$GO_NOGO_REQUIRE_JITO_RPC_POLICY" \
    GO_NOGO_REQUIRE_FASTLANE_DISABLED="$GO_NOGO_REQUIRE_FASTLANE_DISABLED" \
    ROUTE_FEE_SIGNOFF_REQUIRED="$ROUTE_FEE_SIGNOFF_REQUIRED" \
    ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
    ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" \
    ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
    ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
    ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED="$REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV="$REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
    REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$REHEARSAL_ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
    bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" "$WINDOW_HOURS" "$RISK_EVENTS_MINUTES" 2>&1
)"; then
  rollout_exit_code=0
else
  rollout_exit_code=$?
fi

rollout_capture_path="$OUTPUT_ROOT/adapter_rollout_evidence_captured_${timestamp_compact}.txt"
printf '%s\n' "$rollout_output" >"$rollout_capture_path"
rollout_capture_sha256="$(sha256_file_value "$rollout_capture_path")"

rollout_verdict="$(normalize_go_nogo_verdict "$(extract_field "adapter_rollout_verdict" "$rollout_output")")"
rollout_reason="$(trim_string "$(extract_field "adapter_rollout_reason" "$rollout_output")")"
rollout_reason_code="$(trim_string "$(extract_field "adapter_rollout_reason_code" "$rollout_output")")"
rollout_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$rollout_output")")"
rollout_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$rollout_output")")"
rollout_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$rollout_output")")"
rollout_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$rollout_output")")"

if [[ "$rollout_verdict" == "UNKNOWN" ]]; then
  rollout_reason="unable to classify rollout verdict (exit=$rollout_exit_code)"
  rollout_reason_code="unknown_verdict"
elif [[ -z "$rollout_reason" ]]; then
  rollout_reason="rollout helper reported $rollout_verdict"
  rollout_reason_code="missing_reason"
fi
if [[ -z "$rollout_reason_code" ]]; then
  rollout_reason_code="n/a"
fi

rollout_artifact_summary_sha256="n/a"
if [[ -n "$rollout_artifact_summary" && "$rollout_artifact_summary" != "n/a" && -f "$rollout_artifact_summary" ]]; then
  rollout_artifact_summary_sha256="$(sha256_file_value "$rollout_artifact_summary")"
fi

rollout_artifact_manifest_sha256="n/a"
if [[ -n "$rollout_artifact_manifest" && "$rollout_artifact_manifest" != "n/a" && -f "$rollout_artifact_manifest" ]]; then
  rollout_artifact_manifest_sha256="$(sha256_file_value "$rollout_artifact_manifest")"
fi

summary_output="=== Adapter Rollout Final Evidence Package ===
utc_now: $timestamp_utc
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES
adapter_env: $ADAPTER_ENV_PATH
config: $CONFIG_PATH
output_root: $OUTPUT_ROOT
rollout_output_dir: $rollout_output_dir
run_tests: $RUN_TESTS
devnet_rehearsal_test_mode: $DEVNET_REHEARSAL_TEST_MODE
go_nogo_test_mode: $GO_NOGO_TEST_MODE
windowed_signoff_required: $WINDOWED_SIGNOFF_REQUIRED
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
windowed_signoff_require_dynamic_hint_source_pass: $WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS
windowed_signoff_require_dynamic_tip_policy_pass: $WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS
go_nogo_require_jito_rpc_policy: $GO_NOGO_REQUIRE_JITO_RPC_POLICY
go_nogo_require_fastlane_disabled: $GO_NOGO_REQUIRE_FASTLANE_DISABLED
route_fee_signoff_required: $ROUTE_FEE_SIGNOFF_REQUIRED
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
rehearsal_route_fee_signoff_required: $REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED
rehearsal_route_fee_signoff_windows_csv: $REHEARSAL_ROUTE_FEE_SIGNOFF_WINDOWS_CSV
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
final_rollout_package_verdict: $rollout_verdict
final_rollout_package_reason: ${rollout_reason:-n/a}
final_rollout_package_reason_code: ${rollout_reason_code:-n/a}
artifacts_written: true"

echo "$summary_output"

summary_path="$OUTPUT_ROOT/adapter_rollout_final_evidence_summary_${timestamp_compact}.txt"
manifest_path="$OUTPUT_ROOT/adapter_rollout_final_evidence_manifest_${timestamp_compact}.txt"
printf '%s\n' "$summary_output" >"$summary_path"
summary_sha256="$(sha256_file_value "$summary_path")"
cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
rollout_capture_sha256: $rollout_capture_sha256
rollout_summary_sha256: ${rollout_summary_sha256:-n/a}
rollout_artifact_summary_sha256: $rollout_artifact_summary_sha256
rollout_artifact_manifest_sha256: $rollout_artifact_manifest_sha256
EOF
manifest_sha256="$(sha256_file_value "$manifest_path")"

echo
echo "artifacts_written: true"
echo "artifact_summary: $summary_path"
echo "artifact_rollout_capture: $rollout_capture_path"
echo "artifact_manifest: $manifest_path"
echo "summary_sha256: $summary_sha256"
echo "rollout_capture_sha256: $rollout_capture_sha256"
echo "manifest_sha256: $manifest_sha256"

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
