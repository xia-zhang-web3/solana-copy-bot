# Refactor Evidence: Phase 4 Slice 3 (`execution.runtime` routes) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of runtime route-selection helpers from `crates/execution/src/lib.rs` into `crates/execution/src/runtime.rs`.

Extracted runtime methods:
1. `ExecutionRuntime::submit_route_for_attempt`
2. `ExecutionRuntime::route_tip_lamports`

Extracted crate-internal helpers:
1. `build_submit_route_order`
2. `normalize_route_tip_lamports`
3. `normalize_route` (private helper)

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/runtime.rs`
3. `ops/refactor_phase4_slice3_runtime_routes_evidence_2026-02-21.md`

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
- `crates/execution/src/lib.rs`: raw `3439`, runtime (excl `#[cfg(test)]`) `1318`, cfg-test `2121`
- `crates/execution/src/runtime.rs`: raw `76`, runtime `76`, cfg-test `0`

## Notes
- From-config route order and tip normalization semantics are unchanged.
- Fallback route selection (`attempt` index clamped to configured route order) remains unchanged.
