#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_PATH="$ROOT_DIR/ops/server_templates/live.server.toml.example"
RUNBOOK_PATH="$ROOT_DIR/ops/discovery_runtime_restore_runbook.md"

cfg_value() {
  local file_path="$1"
  local section="$2"
  local key="$3"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = substr($0, index($0, "=") + 1)
        sub(/[[:space:]]*#.*/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$file_path"
}

[[ "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill source)" == "helius_rpc" ]] || {
  echo "template recent_raw_gap_fill.source must be helius_rpc" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill helius_http_url)" ]] || {
  echo "template recent_raw_gap_fill.helius_http_url must be explicit" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill output_dir)" ]] || {
  echo "template recent_raw_gap_fill.output_dir must be explicit" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill_helius helius_http_url)" ]] || {
  echo "template recent_raw_gap_fill_helius.helius_http_url must be explicit" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill_helius output_dir)" ]] || {
  echo "template recent_raw_gap_fill_helius.output_dir must be explicit" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation source)" == "quicknode_blocks_rpc" ]] || {
  echo "template program_history_validation.source must be quicknode_blocks_rpc" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" program_history_validation http_url)" ]] || {
  echo "template program_history_validation.http_url must be explicit" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation sampling_segments)" == "8" ]] || {
  echo "template program_history_validation.sampling_segments must be 8" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation max_requests_per_second)" == "100" ]] || {
  echo "template program_history_validation.max_requests_per_second must be 100" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation retry_429_max_attempts)" == "4" ]] || {
  echo "template program_history_validation.retry_429_max_attempts must be 4" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation retry_429_backoff_ms)" == "250" ]] || {
  echo "template program_history_validation.retry_429_backoff_ms must be 250" >&2
  exit 1
}
echo "template recent_raw_gap_fill contract: ok"

python3 - "$RUNBOOK_PATH" <<'PY'
import pathlib
import sys

runbook = pathlib.Path(sys.argv[1]).read_text()
assert "live_runtime_current.db" not in runbook, "runbook still references phantom symlink"
assert "sqlite.path" in runbook, "runbook must archive configured sqlite.path"
assert "recent_raw_gap_fill.helius_http_url" in runbook, "runbook must document gap-fill source contract"
assert "recent_raw_gap_fill_helius.helius_http_url" in runbook, "runbook must document Helius-specific gap-fill source contract"
assert "--helius-http-url" in runbook, "runbook must show explicit gap-fill source usage"
assert "program_history_validation.http_url" in runbook, "runbook must document program-history validation source contract"
assert "discovery_program_history_source_validate" in runbook, "runbook must document the validation bin"
assert "validation-only" in runbook, "runbook must frame program-history validation as validation-only"
assert "not_proven_due_to_budget" in runbook, "runbook must explain budget-exhausted validation outcome"
assert "not_proven_due_to_provider_throttling" in runbook, "runbook must explain provider throttling outcome"
assert "non_viable_source_contract" in runbook, "runbook must distinguish source-contract failure"
assert "--max-slots-to-scan" in runbook, "runbook must show the explicit budget-tuning path"
assert "coverage_method" in runbook, "runbook must document coverage method output"
assert "max_requests_per_second" in runbook, "runbook must document QuickNode rate limiter knobs"
assert "retry_429_backoff_ms" in runbook, "runbook must document 429 retry backoff tuning"
assert "125 req/s" in runbook, "runbook must document the QuickNode throttling contract"
assert "TARGET_DB" in runbook, "runbook must document fresh target creation"
assert "--gap-fill-db-path" in runbook, "runbook must document bounded gap-fill replay"
assert "discovery_raw_gap_fill_helius" in runbook, "runbook must document the Helius-specific bin"
print("runbook operator contract: ok")
PY

echo "discovery_gap_fill_operator_contract_smoke_test: ok"
