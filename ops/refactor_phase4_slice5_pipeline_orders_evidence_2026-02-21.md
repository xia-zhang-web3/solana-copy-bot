# Refactor Evidence: Phase 4 Slice 5 (`execution.pipeline`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of order lifecycle pipeline methods from `crates/execution/src/lib.rs` into `crates/execution/src/pipeline.rs`.

Extracted runtime methods:
1. `ExecutionRuntime::process_pending_order`
2. `ExecutionRuntime::process_simulated_order`
3. `ExecutionRuntime::process_submitted_order`

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/pipeline.rs`
3. `ops/refactor_phase4_slice5_pipeline_orders_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-execution -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`
5. `cargo test -p copybot-execution -q adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject`
6. `cargo test -p copybot-execution -q adapter_intent_simulator_redacts_endpoint_on_retryable_send_error`
7. `cargo test -p copybot-execution -q parse_adapter_submit_response_rejects_missing_required_policy_echo`
8. `cargo test -p copybot-execution -q parse_adapter_submit_response_returns_retryable_on_retryable_reject`
9. `cargo test -p copybot-execution -q parse_adapter_submit_response_returns_terminal_on_terminal_reject`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/execution/src/lib.rs`: raw `2786`, runtime (excl `#[cfg(test)]`) `663`, cfg-test `2123`
- `crates/execution/src/pipeline.rs`: raw `287`, runtime `287`, cfg-test `0`

## Notes
- Pretrade/simulate/submit lifecycle transitions and risk-event emission paths are unchanged.
- Retryable/terminal submit handling and attempt increment semantics are preserved.
