# Executor Phase 2 Slice 27 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving request field validation out of `main.rs` into a dedicated shared module used by both `/simulate` and `/submit` handlers.

## Implemented

1. Added `crates/executor/src/request_validation.rs`:
   1. `RequestValidationError` typed enum.
   2. Shared validators:
      1. `validate_simulate_action`
      2. `validate_simulate_dry_run`
      3. `validate_signal_ts_rfc3339`
      4. `validate_non_empty_request_id`
      5. `validate_non_empty_signal_id`
      6. `validate_non_empty_client_order_id`
2. Updated `crates/executor/src/reject_mapping.rs`:
   1. Added `map_request_validation_error_to_reject`.
   2. Preserved existing reject-code/detail contract:
      1. `invalid_action`
      2. `invalid_dry_run`
      3. `invalid_signal_ts`
      4. `invalid_request_id`
      5. `invalid_signal_id`
      6. `invalid_client_order_id`
3. Updated `crates/executor/src/main.rs`:
   1. `handle_simulate` and `handle_submit` now call shared request validators.
   2. Behavior remains fail-closed and contract-compatible through shared mapping.
4. Updated `tools/executor_contract_smoke_test.sh`:
   1. Added guard coverage for request-validation module tests.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q request_validation_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
