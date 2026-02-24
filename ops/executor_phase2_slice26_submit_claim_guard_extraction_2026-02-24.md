# Executor Phase 2 Slice 26 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving idempotency submit-claim RAII guard out of `main.rs` into a dedicated module.

## Implemented

1. `crates/executor/src/submit_claim_guard.rs`:
   1. Added shared `SubmitClaimGuard`:
      1. stores `client_order_id` + `request_id`,
      2. calls owner-bound `release_submit_claim` in `Drop`.
   2. Preserved release telemetry semantics:
      1. `Ok(true)` -> silent success,
      2. `Ok(false)` -> warning (`no owner-match row`),
      3. `Err` -> warning with error context.
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod submit_claim_guard`
   2. Rewired submit path to shared guard import.
   3. Removed duplicated inline `SubmitClaimGuard` definition and drop implementation.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q handle_submit_rejects_parallel_duplicate_client_order_id_in_flight` — PASS
2. `cargo test -p copybot-executor -q handle_submit_returns_canonical_cached_response_when_store_conflicts` — PASS
3. `cargo test -p copybot-executor -q handle_submit_returns_cached_response_for_duplicate_client_order_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
