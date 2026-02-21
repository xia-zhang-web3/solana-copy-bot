# Refactor Evidence: Phase 4 Slice 2 (`execution.batch_report`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of batch-report struct and route counter helpers from `crates/execution/src/lib.rs` into `crates/execution/src/batch_report.rs`.

Extracted API:
1. `ExecutionBatchReport` (public re-export remains in `execution::lib`)

Extracted crate-internal helpers:
1. `bump_route_counter`
2. `accumulate_route_sum`

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/batch_report.rs`
3. `ops/refactor_phase4_slice2_batch_report_evidence_2026-02-21.md`

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
- `crates/execution/src/lib.rs`: raw `3509`, runtime (excl `#[cfg(test)]`) `1388`, cfg-test `2121`
- `crates/execution/src/batch_report.rs`: raw `46`, runtime `46`, cfg-test `0`

## Notes
- `ExecutionBatchReport` remains externally available via `pub use crate::batch_report::ExecutionBatchReport;` in `crates/execution/src/lib.rs`.
- Route counter behavior (`saturating_add`, empty-route skip) is unchanged.
