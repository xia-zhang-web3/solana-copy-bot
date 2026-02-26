# Executor Phase 2B Slice 103 — Route-Executor Slippage Context Guard (2026-02-26)

## Scope

- enforce submit/simulate action-context shape for slippage expectations at route-executor boundary
- fail-close missing submit expectations and fail-close simulate contamination with submit-only expectations

## Changes

1. Hardened `validate_route_executor_action_context` in `route_executor.rs`:
   - submit now requires `instruction_plan`, `expected_slippage_bps`, `expected_route_slippage_cap_bps`
   - simulate now rejects presence of any submit-only context fields above
2. Added route-executor unit coverage:
   - `route_executor_action_context_rejects_submit_missing_slippage_expectation`
   - `route_executor_action_context_rejects_submit_missing_route_slippage_cap_expectation`
   - `route_executor_action_context_rejects_simulate_with_slippage_expectation`
   - `route_executor_action_context_rejects_simulate_with_route_slippage_cap_expectation`
   - `route_executor_action_context_accepts_submit_with_plan_and_slippage_expectations`
3. Added integration pre-forward coverage in `main.rs`:
   - `execute_route_action_rejects_submit_missing_slippage_expectation_before_forward`
   - `execute_route_action_rejects_submit_missing_route_slippage_cap_expectation_before_forward`
   - `execute_route_action_rejects_simulate_with_slippage_expectation_before_forward`
   - `execute_route_action_rejects_simulate_with_route_slippage_cap_expectation_before_forward`
4. Registered all new guard tests in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_action_context_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_missing_slippage_expectation_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_with_route_slippage_cap_expectation_before_forward` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
