# Executor Phase 2B Slice 71 — Payload Expectations Non-Empty Hardening (2026-02-25)

## Scope

- harden route-layer expectation shape guard against empty/whitespace expectation values
- close branch coverage for empty shared/action expectations

## Changes

1. Updated `validate_route_executor_payload_expectations_shape(...)`:
   - required expectations are now validated as:
     - present (`Some`)
     - non-empty after `trim()`
   - reject contract for empty expectations:
     - code: `invalid_request_body`
     - detail: `route action has empty <field> expectation at route-executor boundary`
2. Added table-driven unit guard:
   - `route_executor_payload_expectations_shape_rejects_empty_shared_fields`
   - covers:
     - `request_id`
     - `signal_id`
     - `side`
     - `token`
     - `client_order_id` (submit path)
3. Added integration pre-forward guards:
   - `execute_route_action_rejects_submit_with_empty_token_expectation_before_forward`
   - `execute_route_action_rejects_simulate_with_empty_request_id_expectation_before_forward`
4. Registered new guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_` — PASS
3. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_rejects_empty_shared_fields` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_with_empty_token_expectation_before_forward` — PASS
5. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_with_empty_request_id_expectation_before_forward` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
