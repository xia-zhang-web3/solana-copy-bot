# Executor Phase 2B Slice 44 — Cleanup Worker Non-Blocking Lock (2026-02-25)

## Scope

- reduce submit-path lock contention risk from background response cleanup worker
- keep cleanup fail-safe while avoiding blocking when idempotency mutex is busy
- add explicit regression coverage for busy-lock skip behavior

## Changes

1. Added non-blocking cleanup entrypoint in idempotency store:
   - `run_response_cleanup_if_due_nonblocking(...) -> Result<bool>`
2. Refactored existing cleanup logic through internal helper:
   - `run_response_cleanup_if_due_internal(..., nonblocking: bool)`
3. Worker switched to non-blocking cleanup call:
   - busy lock now skips tick (debug log), instead of waiting on mutex
4. Existing blocking API (`run_response_cleanup_if_due`) preserved unchanged for current call sites/tests.
5. Added regression test:
   - `response_cleanup_nonblocking_skips_when_mutex_is_busy`
6. Added new guard test to `tools/executor_contract_smoke_test.sh`.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/idempotency.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_nonblocking_skips_when_mutex_is_busy` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
4. `cargo test -p copybot-executor -q validate_response_cleanup_worker_tick_sec_rejects_out_of_range_values` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
