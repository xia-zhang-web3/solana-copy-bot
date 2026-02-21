# Refactor Evidence: Phase 4 Slice 4 (`execution.confirmation`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of confirmation pipeline core method from `crates/execution/src/lib.rs` into `crates/execution/src/confirmation.rs`.

Extracted runtime method:
1. `ExecutionRuntime::process_submitted_order_by_signature`

Visibility adjustments for cross-module wiring:
1. `SignalResult` changed from private to `pub(crate)`
2. `fee_sol_from_lamports` changed to `pub(crate)`
3. `fallback_price_and_source` changed to `pub(crate)`

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/confirmation.rs`
3. `ops/refactor_phase4_slice4_confirmation_pipeline_evidence_2026-02-21.md`

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
- `crates/execution/src/lib.rs`: raw `3064`, runtime (excl `#[cfg(test)]`) `941`, cfg-test `2123`
- `crates/execution/src/confirmation.rs`: raw `390`, runtime `390`, cfg-test `0`

## Notes
- Confirm outcome handling (confirmed/failed/timeout), retry scheduling, risk-event emission, and fee-source accounting paths are unchanged.
- Targeted simulator and submitter parser guard tests remain green as Phase 4 invariant checks.
