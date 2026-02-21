#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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

raw_dir="$output_dir/raw"
norm_dir="$output_dir/normalized"
mkdir -p "$raw_dir" "$norm_dir"

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
fake_bin_dir="$fixture_dir/fake-bin"

PATH="$fake_bin_dir:$PATH" \
GO_NOGO_TEST_MODE=true \
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
CONFIG_PATH="$config_path" \
OUTPUT_DIR="$raw_dir/go_nogo_artifacts" \
bash "$ROOT_DIR/tools/execution_go_nogo_report.sh" 24 60 >"$raw_dir/go_nogo_stdout.txt"

PATH="$fake_bin_dir:$PATH" \
RUN_TESTS=false \
DEVNET_REHEARSAL_TEST_MODE=true \
GO_NOGO_TEST_MODE=true \
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
CONFIG_PATH="$config_path" \
OUTPUT_DIR="$raw_dir/rehearsal_artifacts" \
bash "$ROOT_DIR/tools/execution_devnet_rehearsal.sh" 24 60 >"$raw_dir/rehearsal_stdout.txt"

PATH="$fake_bin_dir:$PATH" \
ADAPTER_ENV_PATH="$adapter_env_path" \
RUN_TESTS=false \
DEVNET_REHEARSAL_TEST_MODE=true \
GO_NOGO_TEST_MODE=true \
GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS \
GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
CONFIG_PATH="$config_path" \
OUTPUT_DIR="$raw_dir/rollout_artifacts" \
bash "$ROOT_DIR/tools/adapter_rollout_evidence_report.sh" 24 60 >"$raw_dir/rollout_stdout.txt"

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
EOF_SUMMARY
