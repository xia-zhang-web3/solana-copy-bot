# Executor Phase 2B Slice 70 — Payload Expectations Shared-Fields Coverage (2026-02-25)

## Scope

- close remaining unit coverage gap for shared-field branches in route-layer expectations-shape guard
- lock reject messages for missing `request_id/signal_id/side/token`

## Changes

1. Added table-driven unit test in `route_executor`:
   - `route_executor_payload_expectations_shape_rejects_missing_shared_fields`
   - covers missing shared expectations for:
     - `request_id`
     - `signal_id`
     - `side`
     - `token`
2. Assertion contract:
   - terminal reject code: `invalid_request_body`
   - detail contains `missing <field> expectation`
3. Registered guard in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_` — PASS
3. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_rejects_missing_shared_fields` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
