# Executor Phase 2 Slice 28 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving request DTO definitions out of `main.rs` into a dedicated shared module.

## Implemented

1. Added `crates/executor/src/request_types.rs`:
   1. `SimulateRequest`
   2. `SubmitRequest`
   3. `ComputeBudgetRequest`
2. Added module-level serde guard tests:
   1. missing `request_id` in simulate payload fails deserialize,
   2. missing `compute_budget` in submit payload fails deserialize.
3. Updated `crates/executor/src/main.rs`:
   1. wired `mod request_types`,
   2. imported shared request DTOs,
   3. removed local duplicate request DTO definitions.
4. Updated `tools/executor_contract_smoke_test.sh`:
   1. added guard test entries for request-types module deserialization constraints.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q request_types_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
