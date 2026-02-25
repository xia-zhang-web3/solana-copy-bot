# Executor Phase 2B Slice 69 — Route Payload Expectations Shape Guard (2026-02-25)

## Scope

- harden route-layer boundary against malformed internal payload expectation wiring
- fail-close action-specific expectation drift before adapter dispatch

## Changes

1. Added `validate_route_executor_payload_expectations_shape(...)` in `route_executor`:
   - required for both actions: `request_id`, `signal_id`, `side`, `token`
   - submit-specific: `client_order_id` expectation is required
   - simulate-specific: `client_order_id` expectation is forbidden
2. Wired shape validation into `execute_route_action(...)` before adapter dispatch.
3. Added unit guards:
   - `route_executor_payload_expectations_shape_rejects_submit_missing_client_order_id`
   - `route_executor_payload_expectations_shape_rejects_simulate_with_client_order_id`
   - `route_executor_payload_expectations_shape_accepts_submit_required_fields`
   - `route_executor_payload_expectations_shape_accepts_simulate_required_fields`
4. Added integration pre-forward guards:
   - `execute_route_action_rejects_submit_missing_client_order_expectation_before_forward`
   - `execute_route_action_rejects_simulate_with_client_order_expectation_before_forward`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_missing_client_order_expectation_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_with_client_order_expectation_before_forward` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
