# Executor Phase 2B Slice 66 — Route-Hint Simulate Integration Coverage (2026-02-25)

## Scope

- close remaining integration coverage gap for route-layer `route_hint` guard on `simulate`
- prove pre-forward reject priority for both mismatch and missing-hint branches

## Changes

1. Added integration test:
   - `execute_route_action_rejects_simulate_route_payload_hint_mismatch_before_forward`
   - asserts terminal `invalid_request_body` and `route payload mismatch` detail
2. Added integration test:
   - `execute_route_action_rejects_simulate_route_payload_hint_missing_before_forward`
   - asserts terminal `invalid_request_body` and missing-hint boundary detail
3. Registered both guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger with coverage-closure entry.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_route_payload_hint_mismatch_before_forward` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_route_payload_hint_missing_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
