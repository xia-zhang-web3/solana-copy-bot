#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/tools/lib/common.sh"

usage() {
  cat <<'USAGE'
usage:
  tools/refactor_phase_gate.sh baseline --output-dir <dir> [--fixture-dir <dir>]
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 1
fi

phase="$1"
shift

output_dir=""
fixture_dir=""

phase_gate_require_executor_upstream_raw="${REFACTOR_PHASE_GATE_REQUIRE_EXECUTOR_UPSTREAM:-true}"
phase_gate_require_ingestion_grpc_raw="${REFACTOR_PHASE_GATE_REQUIRE_INGESTION_GRPC:-true}"
phase_gate_ingestion_source="$(trim_string "${REFACTOR_PHASE_GATE_INGESTION_SOURCE:-yellowstone_grpc}")"
phase_gate_ingestion_source="$(printf '%s' "$phase_gate_ingestion_source" | tr '[:upper:]' '[:lower:]')"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    --fixture-dir)
      fixture_dir="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$phase" != "baseline" ]]; then
  echo "unsupported phase gate: $phase" >&2
  exit 1
fi

if [[ -z "$output_dir" ]]; then
  echo "--output-dir is required" >&2
  exit 1
fi

if [[ -z "$fixture_dir" ]]; then
  fixture_dir="$output_dir/fixture"
fi

if [[ -z "$phase_gate_ingestion_source" ]]; then
  echo "REFACTOR_PHASE_GATE_INGESTION_SOURCE must be non-empty" >&2
  exit 1
fi

if ! phase_gate_require_executor_upstream="$(parse_bool_token_strict "$phase_gate_require_executor_upstream_raw")"; then
  echo "REFACTOR_PHASE_GATE_REQUIRE_EXECUTOR_UPSTREAM must be a boolean token (got: ${phase_gate_require_executor_upstream_raw:-<empty>})" >&2
  exit 1
fi
if ! phase_gate_require_ingestion_grpc="$(parse_bool_token_strict "$phase_gate_require_ingestion_grpc_raw")"; then
  echo "REFACTOR_PHASE_GATE_REQUIRE_INGESTION_GRPC must be a boolean token (got: ${phase_gate_require_ingestion_grpc_raw:-<empty>})" >&2
  exit 1
fi

raw_dir="$output_dir/raw"
norm_dir="$output_dir/normalized"
mkdir -p "$raw_dir" "$norm_dir"

extract_trimmed_field() {
  local key="$1"
  local payload="$2"
  trim_string "$(extract_field "$key" "$payload")"
}

validate_bool_field_equals() {
  local stage="$1"
  local key="$2"
  local payload="$3"
  local expected="$4"
  local raw_value=""
  local normalized=""
  raw_value="$(extract_trimmed_field "$key" "$payload")"
  if [[ -z "$raw_value" ]]; then
    phase_gate_errors+=("missing $key in $stage output")
    return
  fi
  if ! normalized="$(parse_bool_token_strict "$raw_value")"; then
    phase_gate_errors+=("invalid boolean $key in $stage output: $raw_value")
    return
  fi
  if [[ "$normalized" != "$expected" ]]; then
    phase_gate_errors+=("$key mismatch in $stage output: expected=$expected got=$normalized")
  fi
}

validate_go_nogo_verdict_is_go() {
  local stage="$1"
  local key="$2"
  local payload="$3"
  local raw_value=""
  local verdict=""
  raw_value="$(extract_trimmed_field "$key" "$payload")"
  if [[ -z "$raw_value" ]]; then
    phase_gate_errors+=("missing $key in $stage output")
    return
  fi
  verdict="$(normalize_go_nogo_verdict "$raw_value")"
  if [[ "$verdict" != "GO" ]]; then
    phase_gate_errors+=("$key must be GO in $stage output, got=$verdict")
  fi
}

validate_strict_guard_verdict() {
  local stage="$1"
  local key="$2"
  local payload="$3"
  local required="$4"
  local raw_value=""
  local verdict=""
  raw_value="$(extract_trimmed_field "$key" "$payload")"
  if [[ -z "$raw_value" ]]; then
    phase_gate_errors+=("missing $key in $stage output")
    return
  fi
  verdict="$(normalize_strict_guard_verdict "$raw_value")"
  if [[ "$verdict" == "UNKNOWN" ]]; then
    phase_gate_errors+=("invalid strict guard verdict for $key in $stage output: $raw_value")
    return
  fi
  if [[ "$required" == "true" ]]; then
    if [[ "$verdict" != "PASS" ]]; then
      phase_gate_errors+=("$key must be PASS in $stage output when required=true, got=$verdict")
    fi
  else
    if [[ "$verdict" != "SKIP" ]]; then
      phase_gate_errors+=("$key must be SKIP in $stage output when required=false, got=$verdict")
    fi
  fi
}

sha256_cmd=()
if command -v sha256sum >/dev/null 2>&1; then
  sha256_cmd=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256_cmd=(shasum -a 256)
else
  echo "missing hash tool: need sha256sum or shasum -a 256" >&2
  exit 1
fi

bash "$ROOT_DIR/tools/refactor_baseline_prepare.sh" "$fixture_dir" >"$raw_dir/prepare.log"

config_path="$fixture_dir/devnet_rehearsal.toml"
adapter_env_path="$fixture_dir/adapter.env"
executor_env_path="$fixture_dir/executor.env"
fake_bin_dir="$fixture_dir/fake-bin"

if ! PATH="$fake_bin_dir:$PATH" \
  GO_NOGO_TEST_MODE=true \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$phase_gate_require_executor_upstream" \
  GO_NOGO_REQUIRE_INGESTION_GRPC="$phase_gate_require_ingestion_grpc" \
  SOLANA_COPY_BOT_INGESTION_SOURCE="$phase_gate_ingestion_source" \
  EXECUTOR_ENV_PATH="$executor_env_path" \
  CONFIG_PATH="$config_path" \
  OUTPUT_DIR="$raw_dir/go_nogo_artifacts" \
  bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 >"$raw_dir/go_nogo_stdout.txt"; then
  cat "$raw_dir/go_nogo_stdout.txt" >&2 || true
  echo "phase-gate error: execution_go_nogo_report.sh failed for stage=go_nogo" >&2
  exit 3
fi

if ! PATH="$fake_bin_dir:$PATH" \
  RUN_TESTS=false \
  DEVNET_REHEARSAL_TEST_MODE=true \
  GO_NOGO_TEST_MODE=true \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$phase_gate_require_executor_upstream" \
  GO_NOGO_REQUIRE_INGESTION_GRPC="$phase_gate_require_ingestion_grpc" \
  SOLANA_COPY_BOT_INGESTION_SOURCE="$phase_gate_ingestion_source" \
  EXECUTOR_ENV_PATH="$executor_env_path" \
  CONFIG_PATH="$config_path" \
  OUTPUT_DIR="$raw_dir/rehearsal_artifacts" \
  bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 >"$raw_dir/rehearsal_stdout.txt"; then
  cat "$raw_dir/rehearsal_stdout.txt" >&2 || true
  echo "phase-gate error: execution_devnet_rehearsal.sh failed for stage=rehearsal" >&2
  exit 3
fi

if ! PATH="$fake_bin_dir:$PATH" \
  ADAPTER_ENV_PATH="$adapter_env_path" \
  RUN_TESTS=false \
  DEVNET_REHEARSAL_TEST_MODE=true \
  GO_NOGO_TEST_MODE=true \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM="$phase_gate_require_executor_upstream" \
  GO_NOGO_REQUIRE_INGESTION_GRPC="$phase_gate_require_ingestion_grpc" \
  SOLANA_COPY_BOT_INGESTION_SOURCE="$phase_gate_ingestion_source" \
  EXECUTOR_ENV_PATH="$executor_env_path" \
  CONFIG_PATH="$config_path" \
  OUTPUT_DIR="$raw_dir/rollout_artifacts" \
  bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 >"$raw_dir/rollout_stdout.txt"; then
  cat "$raw_dir/rollout_stdout.txt" >&2 || true
  echo "phase-gate error: adapter_rollout_evidence_report.sh failed for stage=rollout" >&2
  exit 3
fi

phase_gate_errors=()
go_nogo_output="$(cat "$raw_dir/go_nogo_stdout.txt")"
rehearsal_output="$(cat "$raw_dir/rehearsal_stdout.txt")"
rollout_output="$(cat "$raw_dir/rollout_stdout.txt")"

validate_go_nogo_verdict_is_go "go_nogo" "overall_go_nogo_verdict" "$go_nogo_output"
validate_bool_field_equals "go_nogo" "go_nogo_require_executor_upstream" "$go_nogo_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "go_nogo" "executor_backend_mode_guard_verdict" "$go_nogo_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "go_nogo" "executor_upstream_endpoint_guard_verdict" "$go_nogo_output" "$phase_gate_require_executor_upstream"
validate_bool_field_equals "go_nogo" "go_nogo_require_ingestion_grpc" "$go_nogo_output" "$phase_gate_require_ingestion_grpc"
validate_strict_guard_verdict "go_nogo" "ingestion_grpc_guard_verdict" "$go_nogo_output" "$phase_gate_require_ingestion_grpc"

validate_go_nogo_verdict_is_go "rehearsal" "devnet_rehearsal_verdict" "$rehearsal_output"
validate_bool_field_equals "rehearsal" "go_nogo_require_executor_upstream" "$rehearsal_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "rehearsal" "go_nogo_executor_backend_mode_guard_verdict" "$rehearsal_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "rehearsal" "go_nogo_executor_upstream_endpoint_guard_verdict" "$rehearsal_output" "$phase_gate_require_executor_upstream"
validate_bool_field_equals "rehearsal" "go_nogo_require_ingestion_grpc" "$rehearsal_output" "$phase_gate_require_ingestion_grpc"
validate_strict_guard_verdict "rehearsal" "go_nogo_ingestion_grpc_guard_verdict" "$rehearsal_output" "$phase_gate_require_ingestion_grpc"

validate_go_nogo_verdict_is_go "rollout" "adapter_rollout_verdict" "$rollout_output"
validate_bool_field_equals "rollout" "go_nogo_require_executor_upstream" "$rollout_output" "$phase_gate_require_executor_upstream"
validate_bool_field_equals "rollout" "rehearsal_nested_go_nogo_require_executor_upstream" "$rollout_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "rollout" "rehearsal_nested_executor_backend_mode_guard_verdict" "$rollout_output" "$phase_gate_require_executor_upstream"
validate_strict_guard_verdict "rollout" "rehearsal_nested_executor_upstream_endpoint_guard_verdict" "$rollout_output" "$phase_gate_require_executor_upstream"
validate_bool_field_equals "rollout" "go_nogo_require_ingestion_grpc" "$rollout_output" "$phase_gate_require_ingestion_grpc"
validate_bool_field_equals "rollout" "rehearsal_nested_go_nogo_require_ingestion_grpc" "$rollout_output" "$phase_gate_require_ingestion_grpc"
validate_strict_guard_verdict "rollout" "rehearsal_nested_ingestion_grpc_guard_verdict" "$rollout_output" "$phase_gate_require_ingestion_grpc"

if ((${#phase_gate_errors[@]} > 0)); then
  for phase_gate_error in "${phase_gate_errors[@]}"; do
    echo "phase-gate error: $phase_gate_error" >&2
  done
  exit 3
fi

bash "$ROOT_DIR/tools/refactor_normalize_output.sh" "$raw_dir/go_nogo_stdout.txt" "$norm_dir/go_nogo_normalized.txt"
bash "$ROOT_DIR/tools/refactor_normalize_output.sh" "$raw_dir/rehearsal_stdout.txt" "$norm_dir/rehearsal_normalized.txt"
bash "$ROOT_DIR/tools/refactor_normalize_output.sh" "$raw_dir/rollout_stdout.txt" "$norm_dir/rollout_normalized.txt"

"${sha256_cmd[@]}" "$raw_dir/go_nogo_stdout.txt" \
  "$raw_dir/rehearsal_stdout.txt" \
  "$raw_dir/rollout_stdout.txt" >"$output_dir/orchestrators.raw.sha256"

"${sha256_cmd[@]}" "$norm_dir/go_nogo_normalized.txt" \
  "$norm_dir/rehearsal_normalized.txt" \
  "$norm_dir/rollout_normalized.txt" >"$output_dir/orchestrators.normalized.sha256"
awk '{print $1}' "$output_dir/orchestrators.normalized.sha256" >"$output_dir/orchestrators.normalized.hashes"

cat <<EOF_SUMMARY
phase_gate: baseline
fixture_dir: $fixture_dir
output_dir: $output_dir
raw_checksum_manifest: $output_dir/orchestrators.raw.sha256
normalized_checksum_manifest: $output_dir/orchestrators.normalized.sha256
normalized_hashes_manifest: $output_dir/orchestrators.normalized.hashes
normalized_go_nogo: $norm_dir/go_nogo_normalized.txt
normalized_rehearsal: $norm_dir/rehearsal_normalized.txt
normalized_rollout: $norm_dir/rollout_normalized.txt
go_nogo_require_executor_upstream: $phase_gate_require_executor_upstream
go_nogo_require_ingestion_grpc: $phase_gate_require_ingestion_grpc
ingestion_source: $phase_gate_ingestion_source
EOF_SUMMARY
