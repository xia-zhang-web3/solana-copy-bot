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
DEVNET_REHEARSAL_PROFILE="${DEVNET_REHEARSAL_PROFILE:-full}"
WINDOWED_SIGNOFF_WINDOWS_CSV="${WINDOWED_SIGNOFF_WINDOWS_CSV:-1,6,24}"
WINDOWED_SIGNOFF_REQUIRED="${WINDOWED_SIGNOFF_REQUIRED:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS:-false}"
WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="${WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS:-false}"
GO_NOGO_REQUIRE_JITO_RPC_POLICY="${GO_NOGO_REQUIRE_JITO_RPC_POLICY:-false}"
GO_NOGO_REQUIRE_FASTLANE_DISABLED="${GO_NOGO_REQUIRE_FASTLANE_DISABLED:-false}"
GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="${GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM:-true}"
GO_NOGO_REQUIRE_INGESTION_GRPC="${GO_NOGO_REQUIRE_INGESTION_GRPC:-false}"
GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="${GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY:-false}"
GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="${GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER:-false}"
GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="${GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT:-false}"
GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="${GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE:-false}"
GO_NOGO_MIN_CONFIRMED_ORDERS="${GO_NOGO_MIN_CONFIRMED_ORDERS:-1}"
GO_NOGO_TEST_MODE="${GO_NOGO_TEST_MODE:-false}"
EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"
ROUTE_FEE_SIGNOFF_WINDOWS_CSV="${ROUTE_FEE_SIGNOFF_WINDOWS_CSV:-1,6,24}"
ROUTE_FEE_SIGNOFF_REQUIRED="${ROUTE_FEE_SIGNOFF_REQUIRED:-false}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE:-${GO_NOGO_TEST_MODE:-false}}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}}"
ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}}"
ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE="${ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE:-}"
PACKAGE_BUNDLE_ENABLED="${PACKAGE_BUNDLE_ENABLED:-false}"
PACKAGE_BUNDLE_LABEL="${PACKAGE_BUNDLE_LABEL:-execution_devnet_rehearsal}"
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

parse_rehearsal_bool_setting() {
  local setting_name="$1"
  local raw_value="$2"
  local parsed_value=""
  if ! parsed_value="$(parse_bool_token_strict "$raw_value")"; then
    echo "${setting_name} must be a boolean token (true/false/1/0/yes/no/on/off), got: ${raw_value}" >&2
    exit 1
  fi
  printf '%s' "$parsed_value"
}

parse_rehearsal_positive_u64_setting() {
  local setting_name="$1"
  local raw_value="$2"
  local parsed_value=""
  if ! parsed_value="$(parse_positive_u64_token_strict "$raw_value")"; then
    echo "${setting_name} must be an integer >= 1, got: ${raw_value}" >&2
    exit 1
  fi
  printf '%s' "$parsed_value"
}

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

devnet_rehearsal_run_windowed_signoff_raw="true"
devnet_rehearsal_run_route_fee_signoff_raw="true"
case "$DEVNET_REHEARSAL_PROFILE" in
  full)
    ;;
  core_only)
    devnet_rehearsal_run_windowed_signoff_raw="false"
    devnet_rehearsal_run_route_fee_signoff_raw="false"
    ;;
  *)
    echo "DEVNET_REHEARSAL_PROFILE must be one of: full,core_only (got: $DEVNET_REHEARSAL_PROFILE)" >&2
    exit 1
    ;;
esac
if [[ -n "${DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF+x}" ]]; then
  devnet_rehearsal_run_windowed_signoff_raw="${DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF}"
fi
if [[ -n "${DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF+x}" ]]; then
  devnet_rehearsal_run_route_fee_signoff_raw="${DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF}"
fi

run_tests_norm="$(parse_rehearsal_bool_setting "RUN_TESTS" "$RUN_TESTS")"
test_mode_norm="$(parse_rehearsal_bool_setting "DEVNET_REHEARSAL_TEST_MODE" "$DEVNET_REHEARSAL_TEST_MODE")"
windowed_signoff_required_norm="$(parse_rehearsal_bool_setting "WINDOWED_SIGNOFF_REQUIRED" "$WINDOWED_SIGNOFF_REQUIRED")"
windowed_signoff_require_dynamic_hint_source_pass_norm="$(parse_rehearsal_bool_setting "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS")"
windowed_signoff_require_dynamic_tip_policy_pass_norm="$(parse_rehearsal_bool_setting "WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS" "$WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS")"
devnet_rehearsal_run_windowed_signoff_norm="$(parse_rehearsal_bool_setting "DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF" "$devnet_rehearsal_run_windowed_signoff_raw")"
devnet_rehearsal_run_route_fee_signoff_norm="$(parse_rehearsal_bool_setting "DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF" "$devnet_rehearsal_run_route_fee_signoff_raw")"
go_nogo_require_jito_rpc_policy_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_JITO_RPC_POLICY" "$GO_NOGO_REQUIRE_JITO_RPC_POLICY")"
go_nogo_require_fastlane_disabled_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_FASTLANE_DISABLED" "$GO_NOGO_REQUIRE_FASTLANE_DISABLED")"
go_nogo_require_executor_upstream_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM" "$GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM")"
go_nogo_require_ingestion_grpc_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_INGESTION_GRPC" "$GO_NOGO_REQUIRE_INGESTION_GRPC")"
go_nogo_require_followlist_activity_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY" "$GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY")"
go_nogo_require_non_bootstrap_signer_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER" "$GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER")"
go_nogo_require_submit_verify_strict_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT" "$GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT")"
go_nogo_require_confirmed_execution_sample_norm="$(parse_rehearsal_bool_setting "GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE" "$GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE")"
go_nogo_min_confirmed_orders_norm="$(parse_rehearsal_positive_u64_setting "GO_NOGO_MIN_CONFIRMED_ORDERS" "$GO_NOGO_MIN_CONFIRMED_ORDERS")"
go_nogo_test_mode_norm="$(parse_rehearsal_bool_setting "GO_NOGO_TEST_MODE" "$GO_NOGO_TEST_MODE")"
route_fee_signoff_required_norm="$(parse_rehearsal_bool_setting "ROUTE_FEE_SIGNOFF_REQUIRED" "$ROUTE_FEE_SIGNOFF_REQUIRED")"
route_fee_signoff_go_nogo_test_mode_norm="$(parse_rehearsal_bool_setting "ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE" "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE")"
package_bundle_enabled_norm="$(parse_rehearsal_bool_setting "PACKAGE_BUNDLE_ENABLED" "$PACKAGE_BUNDLE_ENABLED")"
route_fee_signoff_go_nogo_test_fee_override="$(trim_string "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_FEE_VERDICT_OVERRIDE")"
route_fee_signoff_go_nogo_test_route_override="$(trim_string "$ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE")"
route_fee_signoff_test_verdict_override_raw="$(trim_string "$ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE")"
if [[ "$package_bundle_enabled_norm" == "true" && -z "$OUTPUT_DIR" ]]; then
  echo "PACKAGE_BUNDLE_ENABLED=true requires OUTPUT_DIR to be set" >&2
  exit 1
fi

execution_enabled_raw="$(cfg_or_env_string execution enabled SOLANA_COPY_BOT_EXECUTION_ENABLED)"
execution_enabled="$(parse_rehearsal_bool_setting "SOLANA_COPY_BOT_EXECUTION_ENABLED" "${execution_enabled_raw:-false}")"
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
if [[ "$windowed_signoff_required_norm" == "true" && "$devnet_rehearsal_run_windowed_signoff_norm" != "true" ]]; then
  config_errors+=("WINDOWED_SIGNOFF_REQUIRED=true requires DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=true")
fi
if [[ "$route_fee_signoff_required_norm" == "true" && "$devnet_rehearsal_run_route_fee_signoff_norm" != "true" ]]; then
  config_errors+=("ROUTE_FEE_SIGNOFF_REQUIRED=true requires DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=true")
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
  GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
  GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
  GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream_norm" \
  GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
  GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
  GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
  GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
  GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
  GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
  GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
  EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
  GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
  PACKAGE_BUNDLE_ENABLED="false" \
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
if [[ "$devnet_rehearsal_run_windowed_signoff_norm" == "true" ]]; then
  if windowed_signoff_output="$(
    CONFIG_PATH="$CONFIG_PATH" \
    SERVICE="$SERVICE" \
    GO_NOGO_TEST_MODE="$go_nogo_test_mode_norm" \
    GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="${GO_NOGO_TEST_FEE_VERDICT_OVERRIDE:-}" \
    GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="${GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE:-}" \
    GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
    GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
    GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream_norm" \
    GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
    GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
    GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
    GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
    GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
    GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
    EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
    WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS="$windowed_signoff_require_dynamic_hint_source_pass_norm" \
    WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS="$windowed_signoff_require_dynamic_tip_policy_pass_norm" \
    PACKAGE_BUNDLE_ENABLED="false" \
    OUTPUT_DIR="$windowed_signoff_output_dir" \
    bash "$ROOT_DIR/tools/execution_windowed_signoff_report.sh" "$WINDOWED_SIGNOFF_WINDOWS_CSV" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    windowed_signoff_exit_code=0
  else
    windowed_signoff_exit_code=$?
  fi
else
  windowed_signoff_exit_code=0
  windowed_signoff_output="$(cat <<EOF
signoff_verdict: SKIP
signoff_reason: windowed signoff stage disabled via DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=false
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass_norm
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass_norm
artifact_manifest: n/a
summary_sha256: n/a
artifacts_written: false
package_bundle_enabled: false
EOF
)"
fi
windowed_signoff_nested_capture_path=""
if [[ -n "$windowed_signoff_output_dir" ]]; then
  windowed_signoff_nested_capture_path="$windowed_signoff_output_dir/execution_windowed_signoff_captured_${timestamp_compact}.txt"
  printf '%s\n' "$windowed_signoff_output" > "$windowed_signoff_nested_capture_path"
fi

route_fee_signoff_output_dir=""
if [[ -n "$OUTPUT_DIR" ]]; then
  route_fee_signoff_output_dir="$OUTPUT_DIR/route_fee_signoff"
  mkdir -p "$route_fee_signoff_output_dir"
fi

route_fee_signoff_output=""
route_fee_signoff_exit_code=0
if [[ "$devnet_rehearsal_run_route_fee_signoff_norm" == "true" ]]; then
  if route_fee_signoff_output="$(
    DB_PATH="${DB_PATH:-}" \
    CONFIG_PATH="$CONFIG_PATH" \
    SERVICE="$SERVICE" \
    GO_NOGO_REQUIRE_JITO_RPC_POLICY="$go_nogo_require_jito_rpc_policy_norm" \
    GO_NOGO_REQUIRE_FASTLANE_DISABLED="$go_nogo_require_fastlane_disabled_norm" \
    GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$go_nogo_require_executor_upstream_norm" \
    GO_NOGO_REQUIRE_INGESTION_GRPC="$go_nogo_require_ingestion_grpc_norm" \
    GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY="$go_nogo_require_followlist_activity_norm" \
    GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER="$go_nogo_require_non_bootstrap_signer_norm" \
    GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT="$go_nogo_require_submit_verify_strict_norm" \
    GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE="$go_nogo_require_confirmed_execution_sample_norm" \
    GO_NOGO_MIN_CONFIRMED_ORDERS="$go_nogo_min_confirmed_orders_norm" \
    EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
    GO_NOGO_TEST_MODE="$route_fee_signoff_go_nogo_test_mode_norm" \
    GO_NOGO_TEST_FEE_VERDICT_OVERRIDE="$route_fee_signoff_go_nogo_test_fee_override" \
    GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE="$route_fee_signoff_go_nogo_test_route_override" \
    PACKAGE_BUNDLE_ENABLED="false" \
    OUTPUT_DIR="$route_fee_signoff_output_dir" \
    bash "$ROOT_DIR/tools/execution_route_fee_signoff_report.sh" "$ROUTE_FEE_SIGNOFF_WINDOWS_CSV" "$RISK_EVENTS_MINUTES" 2>&1
  )"; then
    route_fee_signoff_exit_code=0
  else
    route_fee_signoff_exit_code=$?
  fi
else
  route_fee_signoff_exit_code=0
  route_fee_signoff_output="$(cat <<EOF
signoff_verdict: SKIP
signoff_reason: route/fee signoff stage disabled via DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=false
signoff_reason_code: stage_disabled
windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
artifact_manifest: n/a
summary_sha256: n/a
artifacts_written: false
package_bundle_enabled: false
primary_route_stable: false
stable_primary_route: n/a
fallback_route_stable: false
stable_fallback_route: n/a
route_profile_pass_count: n/a
fee_decomposition_pass_count: n/a
window_count: n/a
EOF
)"
fi
route_fee_signoff_nested_capture_path=""
if [[ -n "$route_fee_signoff_output_dir" ]]; then
  route_fee_signoff_nested_capture_path="$route_fee_signoff_output_dir/execution_route_fee_signoff_captured_${timestamp_compact}.txt"
  printf '%s\n' "$route_fee_signoff_output" > "$route_fee_signoff_nested_capture_path"
fi

go_nogo_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$go_nogo_output")")"
if ! go_nogo_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go package_bundle_enabled must be boolean token, got: ${go_nogo_nested_package_bundle_enabled_raw:-<empty>}")
  go_nogo_nested_package_bundle_enabled="unknown"
elif [[ "$go_nogo_nested_package_bundle_enabled" != "false" ]]; then
  config_errors+=("nested go/no-go helper must run with PACKAGE_BUNDLE_ENABLED=false")
fi

windowed_signoff_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$windowed_signoff_output")")"
if [[ "$devnet_rehearsal_run_windowed_signoff_norm" == "true" ]]; then
  if ! windowed_signoff_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$windowed_signoff_output")"; then
    config_errors+=("nested windowed signoff package_bundle_enabled must be boolean token, got: ${windowed_signoff_nested_package_bundle_enabled_raw:-<empty>}")
    windowed_signoff_nested_package_bundle_enabled="unknown"
  elif [[ "$windowed_signoff_nested_package_bundle_enabled" != "false" ]]; then
    config_errors+=("nested windowed signoff helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
else
  windowed_signoff_nested_package_bundle_enabled="n/a"
fi

route_fee_signoff_nested_package_bundle_enabled_raw="$(trim_string "$(extract_field "package_bundle_enabled" "$route_fee_signoff_output")")"
if [[ "$devnet_rehearsal_run_route_fee_signoff_norm" == "true" ]]; then
  if ! route_fee_signoff_nested_package_bundle_enabled="$(extract_bool_field_strict "package_bundle_enabled" "$route_fee_signoff_output")"; then
    config_errors+=("nested route/fee signoff package_bundle_enabled must be boolean token, got: ${route_fee_signoff_nested_package_bundle_enabled_raw:-<empty>}")
    route_fee_signoff_nested_package_bundle_enabled="unknown"
  elif [[ "$route_fee_signoff_nested_package_bundle_enabled" != "false" ]]; then
    config_errors+=("nested route/fee signoff helper must run with PACKAGE_BUNDLE_ENABLED=false")
  fi
else
  route_fee_signoff_nested_package_bundle_enabled="n/a"
fi

go_nogo_executor_backend_mode_guard_verdict="unknown"
go_nogo_executor_backend_mode_guard_reason_code="n/a"
go_nogo_executor_upstream_endpoint_guard_verdict="unknown"
go_nogo_executor_upstream_endpoint_guard_reason_code="n/a"
go_nogo_ingestion_grpc_guard_verdict="unknown"
go_nogo_ingestion_grpc_guard_reason_code="n/a"
go_nogo_followlist_activity_guard_verdict="unknown"
go_nogo_followlist_activity_guard_reason_code="n/a"
go_nogo_non_bootstrap_signer_guard_verdict="unknown"
go_nogo_non_bootstrap_signer_guard_reason_code="n/a"
go_nogo_submit_verify_guard_verdict="unknown"
go_nogo_submit_verify_guard_reason_code="n/a"
go_nogo_confirmed_execution_sample_guard_verdict="unknown"
go_nogo_confirmed_execution_sample_guard_reason_code="n/a"

overall_go_nogo_verdict="$(normalize_go_nogo_verdict "$(extract_field "overall_go_nogo_verdict" "$go_nogo_output")")"
overall_go_nogo_reason="$(trim_string "$(extract_field "overall_go_nogo_reason" "$go_nogo_output")")"
overall_go_nogo_reason_code="$(trim_string "$(extract_field "overall_go_nogo_reason_code" "$go_nogo_output")")"
dynamic_cu_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_policy_verdict" "$go_nogo_output")")"
dynamic_cu_policy_reason="$(trim_string "$(extract_field "dynamic_cu_policy_reason" "$go_nogo_output")")"
dynamic_tip_policy_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_tip_policy_verdict" "$go_nogo_output")")"
dynamic_tip_policy_reason="$(trim_string "$(extract_field "dynamic_tip_policy_reason" "$go_nogo_output")")"
dynamic_cu_hint_api_total="$(trim_string "$(extract_field "dynamic_cu_hint_api_total" "$go_nogo_output")")"
dynamic_cu_hint_rpc_total="$(trim_string "$(extract_field "dynamic_cu_hint_rpc_total" "$go_nogo_output")")"
dynamic_cu_hint_api_configured="$(trim_string "$(extract_field "dynamic_cu_hint_api_configured" "$go_nogo_output")")"
dynamic_cu_hint_source_verdict="$(normalize_gate_verdict "$(extract_field "dynamic_cu_hint_source_verdict" "$go_nogo_output")")"
dynamic_cu_hint_source_reason="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason" "$go_nogo_output")")"
dynamic_cu_hint_source_reason_code="$(trim_string "$(extract_field "dynamic_cu_hint_source_reason_code" "$go_nogo_output")")"
go_nogo_require_jito_rpc_policy_raw="$(trim_string "$(extract_field "go_nogo_require_jito_rpc_policy" "$go_nogo_output")")"
if ! go_nogo_require_jito_rpc_policy="$(extract_bool_field_strict "go_nogo_require_jito_rpc_policy" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_jito_rpc_policy must be boolean token, got: ${go_nogo_require_jito_rpc_policy_raw:-<empty>}")
  go_nogo_require_jito_rpc_policy="unknown"
fi
jito_rpc_policy_verdict="$(normalize_gate_verdict "$(extract_field "jito_rpc_policy_verdict" "$go_nogo_output")")"
jito_rpc_policy_reason="$(trim_string "$(extract_field "jito_rpc_policy_reason" "$go_nogo_output")")"
jito_rpc_policy_reason_code="$(trim_string "$(extract_field "jito_rpc_policy_reason_code" "$go_nogo_output")")"
go_nogo_require_fastlane_disabled_raw="$(trim_string "$(extract_field "go_nogo_require_fastlane_disabled" "$go_nogo_output")")"
if ! go_nogo_require_fastlane_disabled="$(extract_bool_field_strict "go_nogo_require_fastlane_disabled" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_fastlane_disabled must be boolean token, got: ${go_nogo_require_fastlane_disabled_raw:-<empty>}")
  go_nogo_require_fastlane_disabled="unknown"
fi
go_nogo_require_executor_upstream_raw="$(trim_string "$(extract_field "go_nogo_require_executor_upstream" "$go_nogo_output")")"
if ! go_nogo_require_executor_upstream="$(extract_bool_field_strict "go_nogo_require_executor_upstream" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_executor_upstream must be boolean token, got: ${go_nogo_require_executor_upstream_raw:-<empty>}")
  go_nogo_require_executor_upstream="unknown"
fi
go_nogo_require_ingestion_grpc_raw="$(trim_string "$(extract_field "go_nogo_require_ingestion_grpc" "$go_nogo_output")")"
if ! go_nogo_require_ingestion_grpc="$(extract_bool_field_strict "go_nogo_require_ingestion_grpc" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_ingestion_grpc must be boolean token, got: ${go_nogo_require_ingestion_grpc_raw:-<empty>}")
  go_nogo_require_ingestion_grpc="unknown"
fi
go_nogo_require_followlist_activity_raw="$(trim_string "$(extract_field "go_nogo_require_followlist_activity" "$go_nogo_output")")"
if ! go_nogo_require_followlist_activity="$(extract_bool_field_strict "go_nogo_require_followlist_activity" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_followlist_activity must be boolean token, got: ${go_nogo_require_followlist_activity_raw:-<empty>}")
  go_nogo_require_followlist_activity="unknown"
fi
go_nogo_require_non_bootstrap_signer_raw="$(trim_string "$(extract_field "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")")"
if ! go_nogo_require_non_bootstrap_signer="$(extract_bool_field_strict "go_nogo_require_non_bootstrap_signer" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_non_bootstrap_signer must be boolean token, got: ${go_nogo_require_non_bootstrap_signer_raw:-<empty>}")
  go_nogo_require_non_bootstrap_signer="unknown"
fi
go_nogo_require_submit_verify_strict_raw="$(trim_string "$(extract_field "go_nogo_require_submit_verify_strict" "$go_nogo_output")")"
if ! go_nogo_require_submit_verify_strict="$(extract_bool_field_strict "go_nogo_require_submit_verify_strict" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_submit_verify_strict must be boolean token, got: ${go_nogo_require_submit_verify_strict_raw:-<empty>}")
  go_nogo_require_submit_verify_strict="unknown"
fi
go_nogo_require_confirmed_execution_sample_raw="$(trim_string "$(extract_field "go_nogo_require_confirmed_execution_sample" "$go_nogo_output")")"
if ! go_nogo_require_confirmed_execution_sample="$(extract_bool_field_strict "go_nogo_require_confirmed_execution_sample" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go go_nogo_require_confirmed_execution_sample must be boolean token, got: ${go_nogo_require_confirmed_execution_sample_raw:-<empty>}")
  go_nogo_require_confirmed_execution_sample="unknown"
fi
go_nogo_min_confirmed_orders_raw="$(trim_string "$(extract_field "go_nogo_min_confirmed_orders" "$go_nogo_output")")"
if ! go_nogo_min_confirmed_orders="$(parse_positive_u64_token_strict "$go_nogo_min_confirmed_orders_raw")"; then
  config_errors+=("nested go/no-go go_nogo_min_confirmed_orders must be an integer >= 1, got: ${go_nogo_min_confirmed_orders_raw:-<empty>}")
  go_nogo_min_confirmed_orders="0"
fi
go_nogo_executor_backend_mode_guard_verdict_raw="$(trim_string "$(extract_field "executor_backend_mode_guard_verdict" "$go_nogo_output")")"
go_nogo_executor_backend_mode_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_executor_backend_mode_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_executor_backend_mode_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_executor_backend_mode_guard_verdict_raw")"
if [[ -z "$go_nogo_executor_backend_mode_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go executor_backend_mode_guard_verdict must be non-empty")
  go_nogo_executor_backend_mode_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_executor_backend_mode_guard_verdict_raw_upper" != "PASS" && "$go_nogo_executor_backend_mode_guard_verdict_raw_upper" != "WARN" && "$go_nogo_executor_backend_mode_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_executor_backend_mode_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go executor_backend_mode_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_executor_backend_mode_guard_verdict_raw})")
  go_nogo_executor_backend_mode_guard_verdict="UNKNOWN"
fi
go_nogo_executor_backend_mode_guard_reason_code="$(trim_string "$(extract_field "executor_backend_mode_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_executor_backend_mode_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go executor_backend_mode_guard_reason_code must be non-empty")
  go_nogo_executor_backend_mode_guard_reason_code="n/a"
fi
go_nogo_executor_upstream_endpoint_guard_verdict_raw="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_verdict" "$go_nogo_output")")"
go_nogo_executor_upstream_endpoint_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_executor_upstream_endpoint_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_executor_upstream_endpoint_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_executor_upstream_endpoint_guard_verdict_raw")"
if [[ -z "$go_nogo_executor_upstream_endpoint_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict must be non-empty")
  go_nogo_executor_upstream_endpoint_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_executor_upstream_endpoint_guard_verdict_raw_upper" != "PASS" && "$go_nogo_executor_upstream_endpoint_guard_verdict_raw_upper" != "WARN" && "$go_nogo_executor_upstream_endpoint_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_executor_upstream_endpoint_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_executor_upstream_endpoint_guard_verdict_raw})")
  go_nogo_executor_upstream_endpoint_guard_verdict="UNKNOWN"
fi
go_nogo_executor_upstream_endpoint_guard_reason_code="$(trim_string "$(extract_field "executor_upstream_endpoint_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_executor_upstream_endpoint_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go executor_upstream_endpoint_guard_reason_code must be non-empty")
  go_nogo_executor_upstream_endpoint_guard_reason_code="n/a"
fi
go_nogo_ingestion_grpc_guard_verdict_raw="$(trim_string "$(extract_field "ingestion_grpc_guard_verdict" "$go_nogo_output")")"
go_nogo_ingestion_grpc_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_ingestion_grpc_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_ingestion_grpc_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_ingestion_grpc_guard_verdict_raw")"
if [[ -z "$go_nogo_ingestion_grpc_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go ingestion_grpc_guard_verdict must be non-empty")
  go_nogo_ingestion_grpc_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_ingestion_grpc_guard_verdict_raw_upper" != "PASS" && "$go_nogo_ingestion_grpc_guard_verdict_raw_upper" != "WARN" && "$go_nogo_ingestion_grpc_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_ingestion_grpc_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go ingestion_grpc_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_ingestion_grpc_guard_verdict_raw})")
  go_nogo_ingestion_grpc_guard_verdict="UNKNOWN"
fi
go_nogo_ingestion_grpc_guard_reason_code="$(trim_string "$(extract_field "ingestion_grpc_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_ingestion_grpc_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go ingestion_grpc_guard_reason_code must be non-empty")
  go_nogo_ingestion_grpc_guard_reason_code="n/a"
fi
go_nogo_followlist_activity_guard_verdict_raw="$(trim_string "$(extract_field "followlist_activity_guard_verdict" "$go_nogo_output")")"
go_nogo_followlist_activity_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_followlist_activity_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_followlist_activity_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_followlist_activity_guard_verdict_raw")"
if [[ -z "$go_nogo_followlist_activity_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go followlist_activity_guard_verdict must be non-empty")
  go_nogo_followlist_activity_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_followlist_activity_guard_verdict_raw_upper" != "PASS" && "$go_nogo_followlist_activity_guard_verdict_raw_upper" != "WARN" && "$go_nogo_followlist_activity_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_followlist_activity_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go followlist_activity_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_followlist_activity_guard_verdict_raw})")
  go_nogo_followlist_activity_guard_verdict="UNKNOWN"
fi
go_nogo_followlist_activity_guard_reason_code="$(trim_string "$(extract_field "followlist_activity_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_followlist_activity_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go followlist_activity_guard_reason_code must be non-empty")
  go_nogo_followlist_activity_guard_reason_code="n/a"
fi
go_nogo_non_bootstrap_signer_guard_verdict_raw="$(trim_string "$(extract_field "non_bootstrap_signer_guard_verdict" "$go_nogo_output")")"
go_nogo_non_bootstrap_signer_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_non_bootstrap_signer_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_non_bootstrap_signer_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_non_bootstrap_signer_guard_verdict_raw")"
if [[ -z "$go_nogo_non_bootstrap_signer_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be non-empty")
  go_nogo_non_bootstrap_signer_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_non_bootstrap_signer_guard_verdict_raw_upper" != "PASS" && "$go_nogo_non_bootstrap_signer_guard_verdict_raw_upper" != "WARN" && "$go_nogo_non_bootstrap_signer_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_non_bootstrap_signer_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_non_bootstrap_signer_guard_verdict_raw})")
  go_nogo_non_bootstrap_signer_guard_verdict="UNKNOWN"
fi
go_nogo_non_bootstrap_signer_guard_reason_code="$(trim_string "$(extract_field "non_bootstrap_signer_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_non_bootstrap_signer_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go non_bootstrap_signer_guard_reason_code must be non-empty")
  go_nogo_non_bootstrap_signer_guard_reason_code="n/a"
fi
go_nogo_submit_verify_guard_verdict_raw="$(trim_string "$(extract_field "submit_verify_guard_verdict" "$go_nogo_output")")"
go_nogo_submit_verify_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_submit_verify_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_submit_verify_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_submit_verify_guard_verdict_raw")"
if [[ -z "$go_nogo_submit_verify_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go submit_verify_guard_verdict must be non-empty")
  go_nogo_submit_verify_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_submit_verify_guard_verdict_raw_upper" != "PASS" && "$go_nogo_submit_verify_guard_verdict_raw_upper" != "WARN" && "$go_nogo_submit_verify_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_submit_verify_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go submit_verify_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_submit_verify_guard_verdict_raw})")
  go_nogo_submit_verify_guard_verdict="UNKNOWN"
fi
go_nogo_submit_verify_guard_reason_code="$(trim_string "$(extract_field "submit_verify_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_submit_verify_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go submit_verify_guard_reason_code must be non-empty")
  go_nogo_submit_verify_guard_reason_code="n/a"
fi
go_nogo_confirmed_execution_sample_guard_verdict_raw="$(trim_string "$(extract_field "confirmed_execution_sample_guard_verdict" "$go_nogo_output")")"
go_nogo_confirmed_execution_sample_guard_verdict_raw_upper="$(printf '%s' "$go_nogo_confirmed_execution_sample_guard_verdict_raw" | tr '[:lower:]' '[:upper:]')"
go_nogo_confirmed_execution_sample_guard_verdict="$(normalize_strict_guard_verdict "$go_nogo_confirmed_execution_sample_guard_verdict_raw")"
if [[ -z "$go_nogo_confirmed_execution_sample_guard_verdict_raw" ]]; then
  config_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be non-empty")
  go_nogo_confirmed_execution_sample_guard_verdict="UNKNOWN"
elif [[ "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "PASS" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "WARN" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "UNKNOWN" && "$go_nogo_confirmed_execution_sample_guard_verdict_raw_upper" != "SKIP" ]]; then
  config_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${go_nogo_confirmed_execution_sample_guard_verdict_raw})")
  go_nogo_confirmed_execution_sample_guard_verdict="UNKNOWN"
fi
go_nogo_confirmed_execution_sample_guard_reason_code="$(trim_string "$(extract_field "confirmed_execution_sample_guard_reason_code" "$go_nogo_output")")"
if [[ -z "$go_nogo_confirmed_execution_sample_guard_reason_code" ]]; then
  config_errors+=("nested go/no-go confirmed_execution_sample_guard_reason_code must be non-empty")
  go_nogo_confirmed_execution_sample_guard_reason_code="n/a"
fi
if [[ "$go_nogo_require_executor_upstream_norm" == "true" ]]; then
  if [[ "$go_nogo_executor_backend_mode_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go executor_backend_mode_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
  fi
  if [[ "$go_nogo_executor_upstream_endpoint_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true")
  fi
else
  if [[ "$go_nogo_executor_backend_mode_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go executor_backend_mode_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${go_nogo_executor_backend_mode_guard_verdict})")
  fi
  if [[ "$go_nogo_executor_upstream_endpoint_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go executor_upstream_endpoint_guard_verdict must be SKIP when GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false (got: ${go_nogo_executor_upstream_endpoint_guard_verdict})")
  fi
fi
if [[ "$go_nogo_require_ingestion_grpc_norm" == "true" ]]; then
  if [[ "$go_nogo_ingestion_grpc_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go ingestion_grpc_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=true")
  fi
else
  if [[ "$go_nogo_ingestion_grpc_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go ingestion_grpc_guard_verdict must be SKIP when GO_NOGO_REQUIRE_INGESTION_GRPC=false (got: ${go_nogo_ingestion_grpc_guard_verdict})")
  fi
fi
if [[ "$go_nogo_require_followlist_activity_norm" == "true" ]]; then
  if [[ "$go_nogo_followlist_activity_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go followlist_activity_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=true")
  fi
else
  if [[ "$go_nogo_followlist_activity_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go followlist_activity_guard_verdict must be SKIP when GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY=false (got: ${go_nogo_followlist_activity_guard_verdict})")
  fi
fi
if [[ "$go_nogo_require_non_bootstrap_signer_norm" == "true" ]]; then
  if [[ "$go_nogo_non_bootstrap_signer_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=true")
  fi
else
  if [[ "$go_nogo_non_bootstrap_signer_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go non_bootstrap_signer_guard_verdict must be SKIP when GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER=false (got: ${go_nogo_non_bootstrap_signer_guard_verdict})")
  fi
fi
if [[ "$go_nogo_require_submit_verify_strict_norm" == "true" ]]; then
  if [[ "$go_nogo_submit_verify_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go submit_verify_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true")
  fi
else
  if [[ "$go_nogo_submit_verify_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go submit_verify_guard_verdict must be SKIP when GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=false (got: ${go_nogo_submit_verify_guard_verdict})")
  fi
fi
if [[ "$go_nogo_require_confirmed_execution_sample" != "$go_nogo_require_confirmed_execution_sample_norm" ]]; then
  config_errors+=("nested go/no-go go_nogo_require_confirmed_execution_sample mismatch: nested=${go_nogo_require_confirmed_execution_sample} expected=${go_nogo_require_confirmed_execution_sample_norm}")
fi
if [[ "$go_nogo_min_confirmed_orders" != "$go_nogo_min_confirmed_orders_norm" ]]; then
  config_errors+=("nested go/no-go go_nogo_min_confirmed_orders mismatch: nested=${go_nogo_min_confirmed_orders} expected=${go_nogo_min_confirmed_orders_norm}")
fi
if [[ "$go_nogo_require_confirmed_execution_sample_norm" == "true" ]]; then
  if [[ "$go_nogo_confirmed_execution_sample_guard_verdict" == "SKIP" ]]; then
    config_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict cannot be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=true")
  fi
else
  if [[ "$go_nogo_confirmed_execution_sample_guard_verdict" != "SKIP" ]]; then
    config_errors+=("nested go/no-go confirmed_execution_sample_guard_verdict must be SKIP when GO_NOGO_REQUIRE_CONFIRMED_EXECUTION_SAMPLE=false (got: ${go_nogo_confirmed_execution_sample_guard_verdict})")
  fi
fi
submit_fastlane_enabled_raw="$(trim_string "$(extract_field "submit_fastlane_enabled" "$go_nogo_output")")"
if ! submit_fastlane_enabled="$(extract_bool_field_strict "submit_fastlane_enabled" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go submit_fastlane_enabled must be boolean token, got: ${submit_fastlane_enabled_raw:-<empty>}")
  submit_fastlane_enabled="unknown"
fi
fastlane_feature_flag_verdict="$(normalize_gate_verdict "$(extract_field "fastlane_feature_flag_verdict" "$go_nogo_output")")"
fastlane_feature_flag_reason="$(trim_string "$(extract_field "fastlane_feature_flag_reason" "$go_nogo_output")")"
fastlane_feature_flag_reason_code="$(trim_string "$(extract_field "fastlane_feature_flag_reason_code" "$go_nogo_output")")"
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
go_nogo_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$go_nogo_output")")"
if ! go_nogo_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$go_nogo_output")"; then
  config_errors+=("nested go/no-go artifacts_written must be boolean token, got: ${go_nogo_artifacts_written_raw:-<empty>}")
  go_nogo_artifacts_written="unknown"
elif [[ -n "$go_nogo_output_dir" && "$go_nogo_artifacts_written" != "true" ]]; then
  config_errors+=("nested go/no-go artifacts_written must be true")
fi
windowed_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "signoff_verdict" "$windowed_signoff_output")")"
windowed_signoff_reason="$(trim_string "$(extract_field "signoff_reason" "$windowed_signoff_output")")"
if [[ "$devnet_rehearsal_run_windowed_signoff_norm" == "true" ]]; then
  windowed_signoff_require_dynamic_hint_source_pass_raw="$(trim_string "$(extract_field "windowed_signoff_require_dynamic_hint_source_pass" "$windowed_signoff_output")")"
  if ! windowed_signoff_require_dynamic_hint_source_pass="$(extract_bool_field_strict "windowed_signoff_require_dynamic_hint_source_pass" "$windowed_signoff_output")"; then
    config_errors+=("nested windowed signoff windowed_signoff_require_dynamic_hint_source_pass must be boolean token, got: ${windowed_signoff_require_dynamic_hint_source_pass_raw:-<empty>}")
    windowed_signoff_require_dynamic_hint_source_pass="unknown"
  fi
  windowed_signoff_require_dynamic_tip_policy_pass_raw="$(trim_string "$(extract_field "windowed_signoff_require_dynamic_tip_policy_pass" "$windowed_signoff_output")")"
  if ! windowed_signoff_require_dynamic_tip_policy_pass="$(extract_bool_field_strict "windowed_signoff_require_dynamic_tip_policy_pass" "$windowed_signoff_output")"; then
    config_errors+=("nested windowed signoff windowed_signoff_require_dynamic_tip_policy_pass must be boolean token, got: ${windowed_signoff_require_dynamic_tip_policy_pass_raw:-<empty>}")
    windowed_signoff_require_dynamic_tip_policy_pass="unknown"
  fi
  windowed_signoff_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$windowed_signoff_output")")"
  windowed_signoff_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$windowed_signoff_output")")"
  windowed_signoff_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$windowed_signoff_output")")"
  if ! windowed_signoff_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$windowed_signoff_output")"; then
    config_errors+=("nested windowed signoff artifacts_written must be boolean token, got: ${windowed_signoff_artifacts_written_raw:-<empty>}")
    windowed_signoff_artifacts_written="unknown"
  elif [[ -n "$windowed_signoff_output_dir" && "$windowed_signoff_artifacts_written" != "true" ]]; then
    config_errors+=("nested windowed signoff artifacts_written must be true")
  fi
else
  windowed_signoff_verdict="SKIP"
  windowed_signoff_reason="windowed signoff stage disabled via DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=false"
  windowed_signoff_require_dynamic_hint_source_pass="$windowed_signoff_require_dynamic_hint_source_pass_norm"
  windowed_signoff_require_dynamic_tip_policy_pass="$windowed_signoff_require_dynamic_tip_policy_pass_norm"
  windowed_signoff_artifact_manifest="n/a"
  windowed_signoff_summary_sha256="n/a"
  windowed_signoff_artifacts_written="n/a"
fi
route_fee_signoff_verdict="$(normalize_go_nogo_verdict "$(extract_field "signoff_verdict" "$route_fee_signoff_output")")"
route_fee_signoff_reason="$(trim_string "$(extract_field "signoff_reason" "$route_fee_signoff_output")")"
route_fee_signoff_reason_code="$(trim_string "$(extract_field "signoff_reason_code" "$route_fee_signoff_output")")"
route_fee_signoff_windows_csv="$(trim_string "$(extract_field "windows_csv" "$route_fee_signoff_output")")"
if [[ "$devnet_rehearsal_run_route_fee_signoff_norm" == "true" ]]; then
  route_fee_signoff_artifact_manifest="$(trim_string "$(extract_field "artifact_manifest" "$route_fee_signoff_output")")"
  route_fee_signoff_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$route_fee_signoff_output")")"
  route_fee_signoff_artifacts_written_raw="$(trim_string "$(extract_field "artifacts_written" "$route_fee_signoff_output")")"
  if ! route_fee_signoff_artifacts_written="$(extract_bool_field_strict "artifacts_written" "$route_fee_signoff_output")"; then
    config_errors+=("nested route/fee signoff artifacts_written must be boolean token, got: ${route_fee_signoff_artifacts_written_raw:-<empty>}")
    route_fee_signoff_artifacts_written="unknown"
  elif [[ -n "$route_fee_signoff_output_dir" && "$route_fee_signoff_artifacts_written" != "true" ]]; then
    config_errors+=("nested route/fee signoff artifacts_written must be true")
  fi
  route_fee_primary_route_stable_raw="$(trim_string "$(extract_field "primary_route_stable" "$route_fee_signoff_output")")"
  if ! route_fee_primary_route_stable="$(extract_bool_field_strict "primary_route_stable" "$route_fee_signoff_output")"; then
    config_errors+=("nested route/fee signoff primary_route_stable must be boolean token, got: ${route_fee_primary_route_stable_raw:-<empty>}")
    route_fee_primary_route_stable="unknown"
  fi
  route_fee_stable_primary_route="$(trim_string "$(extract_field "stable_primary_route" "$route_fee_signoff_output")")"
  route_fee_fallback_route_stable_raw="$(trim_string "$(extract_field "fallback_route_stable" "$route_fee_signoff_output")")"
  if ! route_fee_fallback_route_stable="$(extract_bool_field_strict "fallback_route_stable" "$route_fee_signoff_output")"; then
    config_errors+=("nested route/fee signoff fallback_route_stable must be boolean token, got: ${route_fee_fallback_route_stable_raw:-<empty>}")
    route_fee_fallback_route_stable="unknown"
  fi
  route_fee_stable_fallback_route="$(trim_string "$(extract_field "stable_fallback_route" "$route_fee_signoff_output")")"
  route_fee_route_profile_pass_count="$(trim_string "$(extract_field "route_profile_pass_count" "$route_fee_signoff_output")")"
  route_fee_fee_decomposition_pass_count="$(trim_string "$(extract_field "fee_decomposition_pass_count" "$route_fee_signoff_output")")"
  route_fee_window_count="$(trim_string "$(extract_field "window_count" "$route_fee_signoff_output")")"
else
  route_fee_signoff_verdict="SKIP"
  route_fee_signoff_reason="route/fee signoff stage disabled via DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=false"
  route_fee_signoff_reason_code="stage_disabled"
  route_fee_signoff_artifact_manifest="n/a"
  route_fee_signoff_summary_sha256="n/a"
  route_fee_signoff_artifacts_written="n/a"
  route_fee_primary_route_stable="n/a"
  route_fee_stable_primary_route="n/a"
  route_fee_fallback_route_stable="n/a"
  route_fee_stable_fallback_route="n/a"
  route_fee_route_profile_pass_count="n/a"
  route_fee_fee_decomposition_pass_count="n/a"
  route_fee_window_count="n/a"
fi
if [[ "$overall_go_nogo_verdict" == "UNKNOWN" && "$go_nogo_exit_code" -ne 0 && -z "$overall_go_nogo_reason" ]]; then
  overall_go_nogo_reason="execution_go_nogo_report exited with code $go_nogo_exit_code"
fi
if [[ "$windowed_signoff_verdict" == "UNKNOWN" && "$windowed_signoff_exit_code" -ne 0 && -z "$windowed_signoff_reason" ]]; then
  windowed_signoff_reason="execution_windowed_signoff_report exited with code $windowed_signoff_exit_code"
fi
if [[ "$route_fee_signoff_verdict" == "UNKNOWN" && "$route_fee_signoff_exit_code" -ne 0 && -z "$route_fee_signoff_reason" ]]; then
  route_fee_signoff_reason="execution_route_fee_signoff_report exited with code $route_fee_signoff_exit_code"
fi
if [[ -n "$route_fee_signoff_test_verdict_override_raw" ]]; then
  if [[ "$route_fee_signoff_go_nogo_test_mode_norm" == "true" ]]; then
    route_fee_signoff_verdict="$(normalize_go_nogo_verdict "$route_fee_signoff_test_verdict_override_raw")"
    route_fee_signoff_reason="test override active (ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE=${route_fee_signoff_test_verdict_override_raw})"
    route_fee_signoff_reason_code="test_override"
  else
    config_errors+=("ROUTE_FEE_SIGNOFF_TEST_VERDICT_OVERRIDE requires ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE=true")
  fi
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
if [[ -z "${dynamic_cu_hint_source_reason_code:-}" ]]; then
  dynamic_cu_hint_source_reason_code="n/a"
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
if [[ -z "$windowed_signoff_reason" ]]; then
  windowed_signoff_reason="n/a"
fi
if [[ -z "$route_fee_signoff_reason" ]]; then
  route_fee_signoff_reason="n/a"
fi
if [[ -z "${route_fee_signoff_reason_code:-}" ]]; then
  route_fee_signoff_reason_code="n/a"
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
devnet_rehearsal_reason_code="all_gates_passed"
if ((${#config_errors[@]} > 0)); then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="${config_errors[0]}"
  devnet_rehearsal_reason_code="config_error"
elif [[ "$preflight_verdict" != "PASS" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="adapter preflight not PASS (${preflight_verdict}): ${preflight_reason:-n/a}"
  devnet_rehearsal_reason_code="preflight_not_pass"
elif [[ "$overall_go_nogo_verdict" == "UNKNOWN" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="go/no-go verdict unknown: ${overall_go_nogo_reason:-n/a}"
  devnet_rehearsal_reason_code="go_nogo_unknown"
elif [[ "$overall_go_nogo_verdict" == "NO_GO" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="${overall_go_nogo_reason:-go/no-go returned NO_GO}"
  devnet_rehearsal_reason_code="go_nogo_no_go"
elif [[ "$overall_go_nogo_verdict" == "HOLD" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="${overall_go_nogo_reason:-go/no-go returned HOLD}"
  devnet_rehearsal_reason_code="go_nogo_hold"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "UNKNOWN" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="windowed signoff verdict unknown: ${windowed_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="windowed_signoff_unknown"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "NO_GO" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="windowed signoff returned NO_GO: ${windowed_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="windowed_signoff_no_go"
elif [[ "$windowed_signoff_required_norm" == "true" && "$windowed_signoff_verdict" == "HOLD" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="windowed signoff returned HOLD: ${windowed_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="windowed_signoff_hold"
elif [[ "$route_fee_signoff_required_norm" == "true" && "$route_fee_signoff_verdict" == "UNKNOWN" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="route/fee signoff verdict unknown: ${route_fee_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="route_fee_signoff_unknown"
elif [[ "$route_fee_signoff_required_norm" == "true" && "$route_fee_signoff_verdict" == "NO_GO" ]]; then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="route/fee signoff returned NO_GO: ${route_fee_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="route_fee_signoff_no_go"
elif [[ "$route_fee_signoff_required_norm" == "true" && "$route_fee_signoff_verdict" == "HOLD" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="route/fee signoff returned HOLD: ${route_fee_signoff_reason:-n/a}"
  devnet_rehearsal_reason_code="route_fee_signoff_hold"
elif [[ "$tests_run" != "true" && "$test_mode_norm" != "true" ]]; then
  devnet_rehearsal_verdict="HOLD"
  devnet_rehearsal_reason="targeted regression tests were skipped (RUN_TESTS=false)"
  devnet_rehearsal_reason_code="tests_skipped"
elif ((tests_failed > 0)); then
  devnet_rehearsal_verdict="NO_GO"
  devnet_rehearsal_reason="targeted regression tests failed: ${tests_failed}/${tests_total}"
  devnet_rehearsal_reason_code="tests_failed"
elif [[ "$tests_run" != "true" && "$test_mode_norm" == "true" ]]; then
  devnet_rehearsal_verdict="GO"
  devnet_rehearsal_reason="test mode override active (RUN_TESTS=false, DEVNET_REHEARSAL_TEST_MODE=true)"
  devnet_rehearsal_reason_code="test_mode_override"
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
=== Execution Devnet Rehearsal ===
utc_now: $timestamp_utc
config: $CONFIG_PATH
service: $SERVICE
window_hours: $WINDOW_HOURS
risk_events_minutes: $RISK_EVENTS_MINUTES
devnet_rehearsal_profile: $DEVNET_REHEARSAL_PROFILE
devnet_rehearsal_run_windowed_signoff: $devnet_rehearsal_run_windowed_signoff_norm
devnet_rehearsal_run_route_fee_signoff: $devnet_rehearsal_run_route_fee_signoff_norm

execution_enabled: $execution_enabled
execution_mode: $execution_mode
rpc_devnet_http_url: $devnet_rpc_label
config_error_count: ${#config_errors[@]}
preflight_verdict: $preflight_verdict
preflight_reason: ${preflight_reason:-n/a}
overall_go_nogo_verdict: $overall_go_nogo_verdict
overall_go_nogo_reason: ${overall_go_nogo_reason:-n/a}
overall_go_nogo_reason_code: ${overall_go_nogo_reason_code:-n/a}
dynamic_cu_policy_verdict: $dynamic_cu_policy_verdict
dynamic_cu_policy_reason: $dynamic_cu_policy_reason
dynamic_tip_policy_verdict: $dynamic_tip_policy_verdict
dynamic_tip_policy_reason: $dynamic_tip_policy_reason
dynamic_cu_hint_api_total: ${dynamic_cu_hint_api_total:-n/a}
dynamic_cu_hint_rpc_total: ${dynamic_cu_hint_rpc_total:-n/a}
dynamic_cu_hint_api_configured: ${dynamic_cu_hint_api_configured:-false}
dynamic_cu_hint_source_verdict: ${dynamic_cu_hint_source_verdict:-unknown}
dynamic_cu_hint_source_reason: ${dynamic_cu_hint_source_reason:-n/a}
dynamic_cu_hint_source_reason_code: ${dynamic_cu_hint_source_reason_code:-n/a}
go_nogo_require_jito_rpc_policy: ${go_nogo_require_jito_rpc_policy:-false}
jito_rpc_policy_verdict: ${jito_rpc_policy_verdict:-unknown}
jito_rpc_policy_reason: ${jito_rpc_policy_reason:-n/a}
jito_rpc_policy_reason_code: ${jito_rpc_policy_reason_code:-n/a}
go_nogo_require_fastlane_disabled: ${go_nogo_require_fastlane_disabled:-false}
go_nogo_require_executor_upstream: ${go_nogo_require_executor_upstream:-false}
go_nogo_require_ingestion_grpc: ${go_nogo_require_ingestion_grpc:-false}
go_nogo_require_followlist_activity: ${go_nogo_require_followlist_activity:-false}
go_nogo_require_non_bootstrap_signer: ${go_nogo_require_non_bootstrap_signer:-false}
go_nogo_require_submit_verify_strict: ${go_nogo_require_submit_verify_strict:-false}
go_nogo_require_confirmed_execution_sample: ${go_nogo_require_confirmed_execution_sample:-false}
go_nogo_min_confirmed_orders: ${go_nogo_min_confirmed_orders:-0}
executor_env_path: $EXECUTOR_ENV_PATH
go_nogo_executor_backend_mode_guard_verdict: ${go_nogo_executor_backend_mode_guard_verdict:-unknown}
go_nogo_executor_backend_mode_guard_reason_code: ${go_nogo_executor_backend_mode_guard_reason_code:-n/a}
go_nogo_executor_upstream_endpoint_guard_verdict: ${go_nogo_executor_upstream_endpoint_guard_verdict:-unknown}
go_nogo_executor_upstream_endpoint_guard_reason_code: ${go_nogo_executor_upstream_endpoint_guard_reason_code:-n/a}
go_nogo_ingestion_grpc_guard_verdict: ${go_nogo_ingestion_grpc_guard_verdict:-unknown}
go_nogo_ingestion_grpc_guard_reason_code: ${go_nogo_ingestion_grpc_guard_reason_code:-n/a}
go_nogo_followlist_activity_guard_verdict: ${go_nogo_followlist_activity_guard_verdict:-unknown}
go_nogo_followlist_activity_guard_reason_code: ${go_nogo_followlist_activity_guard_reason_code:-n/a}
go_nogo_confirmed_execution_sample_guard_verdict: ${go_nogo_confirmed_execution_sample_guard_verdict:-unknown}
go_nogo_confirmed_execution_sample_guard_reason_code: ${go_nogo_confirmed_execution_sample_guard_reason_code:-n/a}
go_nogo_non_bootstrap_signer_guard_verdict: ${go_nogo_non_bootstrap_signer_guard_verdict:-unknown}
go_nogo_non_bootstrap_signer_guard_reason_code: ${go_nogo_non_bootstrap_signer_guard_reason_code:-n/a}
go_nogo_submit_verify_guard_verdict: ${go_nogo_submit_verify_guard_verdict:-unknown}
go_nogo_submit_verify_guard_reason_code: ${go_nogo_submit_verify_guard_reason_code:-n/a}
submit_fastlane_enabled: ${submit_fastlane_enabled:-false}
fastlane_feature_flag_verdict: ${fastlane_feature_flag_verdict:-unknown}
fastlane_feature_flag_reason: ${fastlane_feature_flag_reason:-n/a}
fastlane_feature_flag_reason_code: ${fastlane_feature_flag_reason_code:-n/a}
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
go_nogo_artifacts_written: $go_nogo_artifacts_written
go_nogo_nested_package_bundle_enabled: ${go_nogo_nested_package_bundle_enabled:-unknown}
windowed_signoff_required: $windowed_signoff_required_norm
windowed_signoff_windows_csv: $WINDOWED_SIGNOFF_WINDOWS_CSV
windowed_signoff_require_dynamic_hint_source_pass: $windowed_signoff_require_dynamic_hint_source_pass
windowed_signoff_require_dynamic_tip_policy_pass: $windowed_signoff_require_dynamic_tip_policy_pass
windowed_signoff_exit_code: $windowed_signoff_exit_code
windowed_signoff_verdict: ${windowed_signoff_verdict:-unknown}
windowed_signoff_reason: ${windowed_signoff_reason:-n/a}
windowed_signoff_artifact_manifest: ${windowed_signoff_artifact_manifest:-n/a}
windowed_signoff_summary_sha256: ${windowed_signoff_summary_sha256:-n/a}
windowed_signoff_artifacts_written: $windowed_signoff_artifacts_written
windowed_signoff_nested_package_bundle_enabled: ${windowed_signoff_nested_package_bundle_enabled:-unknown}
route_fee_signoff_required: $route_fee_signoff_required_norm
route_fee_signoff_windows_csv: $ROUTE_FEE_SIGNOFF_WINDOWS_CSV
route_fee_signoff_exit_code: $route_fee_signoff_exit_code
route_fee_signoff_verdict: ${route_fee_signoff_verdict:-unknown}
route_fee_signoff_reason: ${route_fee_signoff_reason:-n/a}
route_fee_signoff_reason_code: ${route_fee_signoff_reason_code:-n/a}
route_fee_signoff_artifact_manifest: ${route_fee_signoff_artifact_manifest:-n/a}
route_fee_signoff_summary_sha256: ${route_fee_signoff_summary_sha256:-n/a}
route_fee_signoff_artifacts_written: $route_fee_signoff_artifacts_written
route_fee_signoff_nested_package_bundle_enabled: ${route_fee_signoff_nested_package_bundle_enabled:-unknown}
route_fee_primary_route_stable: ${route_fee_primary_route_stable:-false}
route_fee_stable_primary_route: ${route_fee_stable_primary_route:-n/a}
route_fee_fallback_route_stable: ${route_fee_fallback_route_stable:-false}
route_fee_stable_fallback_route: ${route_fee_stable_fallback_route:-n/a}
route_fee_route_profile_pass_count: ${route_fee_route_profile_pass_count:-n/a}
route_fee_fee_decomposition_pass_count: ${route_fee_fee_decomposition_pass_count:-n/a}
route_fee_window_count: ${route_fee_window_count:-n/a}
tests_run: $tests_run
tests_total: $tests_total
tests_failed: $tests_failed
package_bundle_enabled: $package_bundle_enabled_norm
package_bundle_label: $PACKAGE_BUNDLE_LABEL
package_bundle_output_dir: ${PACKAGE_BUNDLE_OUTPUT_DIR:-n/a}
devnet_rehearsal_verdict: $devnet_rehearsal_verdict
devnet_rehearsal_reason: $devnet_rehearsal_reason
devnet_rehearsal_reason_code: $devnet_rehearsal_reason_code
artifacts_written: $artifacts_written
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
  route_fee_signoff_path="$OUTPUT_DIR/execution_devnet_rehearsal_route_fee_signoff_${timestamp_compact}.txt"
  tests_path="$OUTPUT_DIR/execution_devnet_rehearsal_tests_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/execution_devnet_rehearsal_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary_output" > "$summary_path"
  printf '%s\n' "$preflight_output" > "$preflight_path"
  printf '%s\n' "$go_nogo_output" > "$go_nogo_path"
  printf '%s\n' "$windowed_signoff_output" > "$windowed_signoff_path"
  printf '%s\n' "$route_fee_signoff_output" > "$route_fee_signoff_path"
  printf '%s\n' "$test_log" > "$tests_path"

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
  preflight_sha256="$(sha256_file_value "$preflight_path")"
  go_nogo_sha256="$(sha256_file_value "$go_nogo_path")"
  windowed_signoff_sha256="$(sha256_file_value "$windowed_signoff_path")"
  route_fee_signoff_sha256="$(sha256_file_value "$route_fee_signoff_path")"
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
  if [[ -n "$route_fee_signoff_nested_capture_path" ]]; then
    route_fee_signoff_nested_capture_sha256="$(sha256_file_value "$route_fee_signoff_nested_capture_path")"
  else
    route_fee_signoff_nested_capture_sha256="n/a"
  fi
  cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
preflight_sha256: $preflight_sha256
go_nogo_sha256: $go_nogo_sha256
windowed_signoff_sha256: $windowed_signoff_sha256
route_fee_signoff_sha256: $route_fee_signoff_sha256
tests_sha256: $tests_sha256
go_nogo_nested_capture_sha256: $go_nogo_nested_capture_sha256
windowed_signoff_nested_capture_sha256: $windowed_signoff_nested_capture_sha256
route_fee_signoff_nested_capture_sha256: $route_fee_signoff_nested_capture_sha256
EOF
  manifest_sha256="$(sha256_file_value "$manifest_path")"

  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" == "true" ]]; then
    run_package_bundle_once
  fi

  echo
  echo "artifacts_written: true"
  echo "artifact_summary: $summary_path"
  echo "artifact_preflight: $preflight_path"
  echo "artifact_go_nogo: $go_nogo_path"
  echo "artifact_windowed_signoff: $windowed_signoff_path"
  echo "artifact_route_fee_signoff: $route_fee_signoff_path"
  echo "artifact_tests: $tests_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "preflight_sha256: $preflight_sha256"
  echo "go_nogo_sha256: $go_nogo_sha256"
  echo "windowed_signoff_sha256: $windowed_signoff_sha256"
  echo "route_fee_signoff_sha256: $route_fee_signoff_sha256"
  echo "tests_sha256: $tests_sha256"
  echo "manifest_sha256: $manifest_sha256"
  echo "package_bundle_artifacts_written: $package_bundle_artifacts_written"
  echo "package_bundle_exit_code: $package_bundle_exit_code"
  echo "package_bundle_error: $package_bundle_error"
  echo "package_bundle_path: $package_bundle_path"
  echo "package_bundle_sha256: $package_bundle_sha256"
  echo "package_bundle_sha256_path: $package_bundle_sha256_path"
  echo "package_bundle_contents_manifest: $package_bundle_contents_manifest"
  echo "package_bundle_file_count: $package_bundle_file_count"
  if [[ -n "$go_nogo_nested_capture_path" ]]; then
    echo "artifact_go_nogo_nested_capture: $go_nogo_nested_capture_path"
    echo "go_nogo_nested_capture_sha256: $go_nogo_nested_capture_sha256"
  fi
  if [[ -n "$windowed_signoff_nested_capture_path" ]]; then
    echo "artifact_windowed_signoff_nested_capture: $windowed_signoff_nested_capture_path"
    echo "windowed_signoff_nested_capture_sha256: $windowed_signoff_nested_capture_sha256"
  fi
  if [[ -n "$route_fee_signoff_nested_capture_path" ]]; then
    echo "artifact_route_fee_signoff_nested_capture: $route_fee_signoff_nested_capture_path"
    echo "route_fee_signoff_nested_capture_sha256: $route_fee_signoff_nested_capture_sha256"
  fi

  if [[ "$package_bundle_enabled_norm" == "true" && "$package_bundle_artifacts_written" != "true" ]]; then
    exit 3
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
