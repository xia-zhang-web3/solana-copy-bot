# Executor Phase 2B Slice 67 — Route-Hint Submit Missing Coverage (2026-02-25)

## Scope

- complete route-hint integration matrix for route-executor boundary
- add submit-path explicit guard for missing payload route hint

## Changes

1. Added integration test:
   - `execute_route_action_rejects_submit_route_payload_hint_missing_before_forward`
   - asserts terminal `invalid_request_body` and missing-hint route-executor boundary detail
2. Registered new guard in `tools/executor_contract_smoke_test.sh`.
3. Updated roadmap ledger with matrix-completion entry.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_payload_hint_missing_before_forward` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_route_payload_hint_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
