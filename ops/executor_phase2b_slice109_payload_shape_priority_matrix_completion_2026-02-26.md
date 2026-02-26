# Executor Phase 2B Slice 109 — Payload-Shape Priority Matrix Completion (2026-02-26)

## Scope

- complete pre-policy guard-ordering matrix for `payload_shape`
- prove `payload_shape` remains higher priority than both `deadline_context` and `action_context` for submit and simulate flows

## Changes

1. Added submit integration priority guards in `main.rs`:
   - `execute_route_action_rejects_payload_shape_before_deadline_context_on_submit`
   - `execute_route_action_rejects_payload_shape_before_action_context_on_submit`
2. Added simulate integration priority guards in `main.rs`:
   - `execute_route_action_rejects_payload_shape_before_deadline_context_on_simulate`
   - `execute_route_action_rejects_payload_shape_before_action_context_on_simulate`
3. All new tests intentionally combine multiple malformed conditions and assert deterministic first-failure:
   - `invalid_request_body`
   - detail contains `empty token expectation`
4. Registered all new guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_payload_shape_before_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_deadline_context_before_action_context_on_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
