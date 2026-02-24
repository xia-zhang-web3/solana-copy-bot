# Executor Phase 2 Slice 38 Evidence (2026-02-24)

## Scope

Continued request-validation cleanup by composing repeated field-validation sequences into shared chain helpers.

## Implemented

1. `crates/executor/src/request_validation.rs`:
   1. added `validate_simulate_request_basics(...)`,
   2. added `validate_submit_request_identity(...)`.
2. `crates/executor/src/main.rs`:
   1. `/simulate` now calls `validate_simulate_request_basics(...)`,
   2. `/submit` now calls `validate_submit_request_identity(...)`.
3. Added/updated tests in request-validation module:
   1. positive coverage for both chain helpers,
   2. negative coverage for invalid action and empty client_order_id.
4. Updated `tools/executor_contract_smoke_test.sh` with new guard tests for helper chains.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q request_validation_validate_simulate_request_basics_` — PASS
2. `cargo test -p copybot-executor -q request_validation_validate_submit_request_identity_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
