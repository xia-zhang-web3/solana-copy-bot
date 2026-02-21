# Refactor Evidence: Phase 4 Slice 6 (`execution.risk_gates`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of runtime buy-pause and risk-gate methods from `crates/execution/src/lib.rs` into `crates/execution/src/risk_gates.rs`.

Extracted runtime methods:
1. `ExecutionRuntime::should_pause_buy_submission`
2. `ExecutionRuntime::is_pre_submit_status`
3. `ExecutionRuntime::execution_risk_block_reason`
4. `ExecutionRuntime::daily_loss_limit_sol`
5. `ExecutionRuntime::max_drawdown_limit_sol`

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/risk_gates.rs`
3. `ops/refactor_phase4_slice6_risk_gates_evidence_2026-02-21.md`

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
- `crates/execution/src/lib.rs`: raw `2630`, runtime (excl `#[cfg(test)]`) `507`, cfg-test `2123`
- `crates/execution/src/risk_gates.rs`: raw `165`, runtime `165`, cfg-test `0`

## Notes
- Buy pause gating and risk-block reason string formatting remain unchanged.
- Daily-loss and max-drawdown limit derivation logic is preserved exactly.
