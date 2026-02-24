#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

WINDOWS_CSV="${1:-1,6,24}"
RISK_EVENTS_MINUTES="${2:-60}"
SERVICE="${SERVICE:-solana-copy-bot}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/live.toml}}"
OUTPUT_ROOT="${OUTPUT_ROOT:-state/route-fee-signoff-final-$(date -u +"%Y%m%dT%H%M%SZ")}"

GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-true}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-true}"
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}"
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
go_nogo_require_jito_rpc_policy="$(normalize_bool_token "$GO_NOGO_REQUIRE_JITO_RPC_POLICY")"
go_nogo_require_fastlane_disabled="$(normalize_bool_token "$GO_NOGO_REQUIRE_FASTLANE_DISABLED")"

mkdir -p "$OUTPUT_ROOT"
signoff_output_dir="$OUTPUT_ROOT/route_fee_signoff"
mkdir -p "$signoff_output_dir"

signoff_output=""
signoff_exit_code=3
if signoff_output="$(
  CONFIG_PATH="$CONFIG_PATH" \
    SERVICE="$SERVICE" \
    OUTPUT_DIR="$signoff_output_dir" \
    GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy" \
    GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled" \
    GO_NOGO_TEST_MODE="$GO_NOGO_TEST_MODE" \
    GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$GO_NOGO_TEST_FEE_VERDICT_OVERRIDE" \
    GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE" \
    ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE" \
    bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "$WINDOWS_CSV" "$RISK_EVENTS_MINUTES" 2>&1
)"; then
  signoff_exit_code=0
else
  signoff_exit_code=$?
fi

signoff_capture_path="$OUTPUT_ROOT/execution_route_fee_signoff_captured_${timestamp_compact}.txt"
printf '%s\n' "$signoff_output" >"$signoff_capture_path"
signoff_capture_sha256="$(sha256_file_value "$signoff_capture_path")"

window_count="$(trim_string "$(extract_field "window_count" "$signoff_output")")"
go_nogo_go_count="$(trim_string "$(extract_field "go_nogo_go_count" "$signoff_output")")"
route_profile_pass_count="$(trim_string "$(extract_field "route_profile_pass_count" "$signoff_output")")"
fee_decomposition_pass_count="$(trim_string "$(extract_field "fee_decomposition_pass_count" "$signoff_output")")"
primary_route_stable="$(trim_string "$(extract_field "primary_route_stable" "$signoff_output")")"
stable_primary_route="$(trim_string "$(extract_field "stable_primary_route" "$signoff_output")")"
fallback_route_stable="$(trim_string "$(extract_field "fallback_route_stable" "$signoff_output")")"
stable_fallback_route="$(trim_string "$(extract_field "stable_fallback_route" "$signoff_output")")"

signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "signoff_verdict" "$signoff_output")")"
signoff_reason="$(trim_string "$(extract_field "signoff_reason" "$signoff_output")")"
signoff_reason_code="$(trim_string "$(extract_field "signoff_reason_code" "$signoff_output")")"
signoff_artifacts_written="$(normalize_bool_token "$(extract_field "artifacts_written" "$signoff_output")")"
signoff_artifact_summary="$(trim_string "$(extract_field "artifact_summary" "$signoff_output")")"
signoff_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$signoff_output")")"
signoff_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$signoff_output")")"

if [[ "$signoff_verdict" == "UNKNOWN" ]]; then
  signoff_reason="unable to classify route/fee signoff verdict (exit=$signoff_exit_code)"
  signoff_reason_code="unknown_verdict"
elif [[ -z "$signoff_reason" ]]; then
  signoff_reason="route/fee signoff helper reported $signoff_verdict"
  signoff_reason_code="missing_reason"
fi
if [[ -z "$signoff_reason_code" ]]; then
  signoff_reason_code="n/a"
fi

signoff_artifact_summary_sha256="n/a"
if [[ -n "$signoff_artifact_summary" && "$signoff_artifact_summary" != "n/a" && -f "$signoff_artifact_summary" ]]; then
  signoff_artifact_summary_sha256="$(sha256_file_value "$signoff_artifact_summary")"
fi

signoff_artifact_manifest_sha256="n/a"
if [[ -n "$signoff_artifact_manifest" && "$signoff_artifact_manifest" != "n/a" && -f "$signoff_artifact_manifest" ]]; then
  signoff_artifact_manifest_sha256="$(sha256_file_value "$signoff_artifact_manifest")"
fi

summary_output="=== Execution Route/Fee Final Evidence Package ===
utc_now: $timestamp_utc
service: $SERVICE
config: $CONFIG_PATH
windows_csv: $WINDOWS_CSV
risk_events_minutes: $RISK_EVENTS_MINUTES
output_root: $OUTPUT_ROOT
signoff_output_dir: $signoff_output_dir
go_nogo_require_jito_rpc_policy: $go_nogo_require_jito_rpc_policy
go_nogo_require_fastlane_disabled: $go_nogo_require_fastlane_disabled
go_nogo_test_mode: $GO_NOGO_TEST_MODE
route_fee_signoff_test_verdict_override: ${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-n/a}
signoff_exit_code: $signoff_exit_code
window_count: ${window_count:-n/a}
go_nogo_go_count: ${go_nogo_go_count:-n/a}
route_profile_pass_count: ${route_profile_pass_count:-n/a}
fee_decomposition_pass_count: ${fee_decomposition_pass_count:-n/a}
primary_route_stable: ${primary_route_stable:-n/a}
stable_primary_route: ${stable_primary_route:-n/a}
fallback_route_stable: ${fallback_route_stable:-n/a}
stable_fallback_route: ${stable_fallback_route:-n/a}
signoff_verdict: $signoff_verdict
signoff_reason: ${signoff_reason:-n/a}
signoff_reason_code: ${signoff_reason_code:-n/a}
signoff_artifacts_written: $signoff_artifacts_written
signoff_artifact_summary: ${signoff_artifact_summary:-n/a}
signoff_artifact_manifest: ${signoff_artifact_manifest:-n/a}
signoff_summary_sha256: ${signoff_summary_sha256:-n/a}
signoff_artifact_summary_sha256: $signoff_artifact_summary_sha256
signoff_artifact_manifest_sha256: $signoff_artifact_manifest_sha256
final_route_fee_package_verdict: $signoff_verdict
final_route_fee_package_reason: ${signoff_reason:-n/a}
final_route_fee_package_reason_code: ${signoff_reason_code:-n/a}
artifacts_written: true"

echo "$summary_output"

summary_path="$OUTPUT_ROOT/execution_route_fee_final_evidence_summary_${timestamp_compact}.txt"
manifest_path="$OUTPUT_ROOT/execution_route_fee_final_evidence_manifest_${timestamp_compact}.txt"
printf '%s\n' "$summary_output" >"$summary_path"
summary_sha256="$(sha256_file_value "$summary_path")"
cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
signoff_capture_sha256: $signoff_capture_sha256
signoff_summary_sha256: ${signoff_summary_sha256:-n/a}
signoff_artifact_summary_sha256: $signoff_artifact_summary_sha256
signoff_artifact_manifest_sha256: $signoff_artifact_manifest_sha256
EOF
manifest_sha256="$(sha256_file_value "$manifest_path")"

echo
echo "artifacts_written: true"
echo "artifact_summary: $summary_path"
echo "artifact_signoff_capture: $signoff_capture_path"
echo "artifact_manifest: $manifest_path"
echo "summary_sha256: $summary_sha256"
echo "signoff_capture_sha256: $signoff_capture_sha256"
echo "manifest_sha256: $manifest_sha256"

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
