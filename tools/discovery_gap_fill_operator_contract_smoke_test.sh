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
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_a_max_slots_to_scan)" == "4096" ]] || {
  echo "template program_history_validation.phase_a_max_slots_to_scan must be 4096" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_a_sampling_segments)" == "8" ]] || {
  echo "template program_history_validation.phase_a_sampling_segments must be 8" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_a_max_blocks_per_window)" == "12" ]] || {
  echo "template program_history_validation.phase_a_max_blocks_per_window must be 12" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_b_max_blocks_to_fetch)" == "1024" ]] || {
  echo "template program_history_validation.phase_b_max_blocks_to_fetch must be 1024" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_b_max_candidate_transactions_to_parse)" == "2048" ]] || {
  echo "template program_history_validation.phase_b_max_candidate_transactions_to_parse must be 2048" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_validation phase_b_parseable_rows_target)" == "1" ]] || {
  echo "template program_history_validation.phase_b_parseable_rows_target must be 1" >&2
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
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill source)" == "quicknode_blocks_rpc" ]] || {
  echo "template program_history_gap_fill.source must be quicknode_blocks_rpc" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill http_url)" ]] || {
  echo "template program_history_gap_fill.http_url must be explicit" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill max_requests_per_second)" == "60" ]] || {
  echo "template program_history_gap_fill.max_requests_per_second must be 60" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill block_batch_size)" == "1005" ]] || {
  echo "template program_history_gap_fill.block_batch_size must be 1005" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill block_fetch_concurrency)" == "12" ]] || {
  echo "template program_history_gap_fill.block_fetch_concurrency must be 12" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill max_slots_to_scan)" == "1200000" ]] || {
  echo "template program_history_gap_fill.max_slots_to_scan must be 1200000" >&2
  exit 1
}
[[ "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill max_slot_batches_per_attempt)" == "256" ]] || {
  echo "template program_history_gap_fill.max_slot_batches_per_attempt must be 256" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" program_history_gap_fill output_dir)" ]] || {
  echo "template program_history_gap_fill.output_dir must be explicit" >&2
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
assert "program_history_gap_fill.http_url" in runbook, "runbook must document program-history gap-fill source contract"
assert "discovery_program_history_source_validate" in runbook, "runbook must document the validation bin"
assert "discovery_raw_gap_fill_program_history" in runbook, "runbook must document the program-history gap-fill bin"
assert "validation-only" in runbook, "runbook must frame program-history validation as validation-only"
assert "--phase phase_a" in runbook, "runbook must document Phase A invocation"
assert "--phase phase_b" in runbook, "runbook must document Phase B invocation"
assert "viable_enough_for_phase_b" in runbook, "runbook must explain Phase A positive semantics"
assert "Phase A" in runbook and "Phase B" in runbook, "runbook must explain both validation phases"
assert "not_proven_due_to_budget" in runbook, "runbook must explain budget-exhausted validation outcome"
assert "not_proven_due_to_phase_b_cost_budget" in runbook, "runbook must explain Phase B cost-budget outcome"
assert "not_proven_due_to_provider_throttling" in runbook, "runbook must explain provider throttling outcome"
assert "non_viable_source_contract" in runbook, "runbook must distinguish source-contract failure"
assert "--max-slots-to-scan" in runbook, "runbook must show the explicit budget-tuning path"
assert "--max-blocks-per-window" in runbook, "runbook must show Phase A block-sampling tuning"
assert "coverage_method" in runbook, "runbook must document coverage method output"
assert "final_source_proof_completed" in runbook, "runbook must document final source proof semantics"
assert "phase_b_max_blocks_to_fetch" in runbook, "runbook must document Phase B block cost budget"
assert "phase_b_max_candidate_transactions_to_parse" in runbook, "runbook must document Phase B candidate parse budget"
assert "phase_b_parseable_rows_target" in runbook, "runbook must document Phase B parseable-row target"
assert "early_stop_reason" in runbook, "runbook must document Phase B early-stop output"
assert "max_requests_per_second" in runbook, "runbook must document QuickNode rate limiter knobs"
assert "retry_429_backoff_ms" in runbook, "runbook must document 429 retry backoff tuning"
assert "125 req/s" in runbook, "runbook must document the QuickNode throttling contract"
assert "TARGET_DB" in runbook, "runbook must document fresh target creation"
assert "--gap-fill-db-path" in runbook, "runbook must document bounded gap-fill replay"
assert "discovery_raw_gap_fill_helius" in runbook, "runbook must document the Helius-specific bin"
assert "replayable_output" in runbook, "runbook must document program-history gap-fill replay safety"
assert "not_proven_due_to_attempt_budget" in runbook, "runbook must document attempt-budget bounded outcome"
assert "max_slot_batches_per_attempt" in runbook, "runbook must document resumable attempt budget"
assert "in_progress" in runbook, "runbook must explain persisted in-progress gap-fill state"
assert "resolved_bounds_reused_from_progress" in runbook, "runbook must document resumed bound reuse telemetry"
assert "dominant_phase" in runbook, "runbook must document throughput dominant-phase telemetry"
assert "attempt_frontier_advanced_slots" in runbook, "runbook must document slot frontier progress telemetry"
assert "attempt_block_fetch_ms" in runbook, "runbook must document block fetch timing telemetry"
assert "attempt_sqlite_stage_ms" in runbook, "runbook must document sqlite staging telemetry"
assert "block_fetch_encoding" in runbook, "runbook must document the lighter block fetch payload contract"
assert "block_fetch_concurrency" in runbook, "runbook must document block fetch parallelism tuning"
assert "--block-fetch-concurrency" in runbook, "runbook must show explicit block fetch concurrency tuning"
assert "program_history_gap_fill_progress_reset_unreadable_state" in runbook, "runbook must explain unreadable progress-state reset behavior"
assert "complete_sufficient_for_healthy_restore" in runbook, "runbook must document healthy program-gap-fill outcome"
assert "complete_but_insufficient_for_healthy_restore" in runbook, "runbook must document incomplete but replayable program-gap-fill outcome"
assert "not_proven_due_to_cost_budget" in runbook, "runbook must document program-gap-fill cost budget outcome"
assert "not_proven_due_to_scan_budget" in runbook, "runbook must document program-gap-fill scan budget outcome"
print("runbook operator contract: ok")
PY

echo "discovery_gap_fill_operator_contract_smoke_test: ok"
