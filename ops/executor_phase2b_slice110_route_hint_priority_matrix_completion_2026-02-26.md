# Executor Phase 2B Slice 110 — Route-Hint Priority Matrix Completion (2026-02-26)

## Scope

- finalize top-of-chain guard-order determinism for `route_executor`
- prove `payload_route` (`route_hint`) rejects always fire before downstream request-shape/context guards

## Changes

1. Added submit integration priority guards:
   - `execute_route_action_rejects_route_hint_before_payload_shape_on_submit`
   - `execute_route_action_rejects_route_hint_before_deadline_context_on_submit`
2. Added simulate integration priority guards:
   - `execute_route_action_rejects_route_hint_before_payload_shape_on_simulate`
   - `execute_route_action_rejects_route_hint_before_action_context_on_simulate`
3. New tests intentionally combine multiple malformed inputs and assert deterministic first failure:
   - `invalid_request_body`
   - detail contains `route payload hint missing ...` or `route payload mismatch ...`
4. Registered all new guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_route_hint_before_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_payload_shape_before_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
