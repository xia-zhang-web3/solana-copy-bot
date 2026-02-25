# Executor Phase 2B Slice 47 — Cleanup Worker `spawn_blocking` Hardening (2026-02-25)

## Scope

- keep background response-cleanup behavior unchanged
- prevent blocking SQLite cleanup work from occupying async runtime worker threads
- add explicit handling for blocking-task join failures

## Changes

1. `idempotency_cleanup_worker` now runs each cleanup tick via:
   - `tokio::task::spawn_blocking(...)`
2. Existing outcome semantics are preserved:
   - `Ok(Ok(true))` -> cleanup ran/throttled
   - `Ok(Ok(false))` -> lock busy skip (debug)
   - `Ok(Err(error))` -> cleanup failure (warn)
3. Added new branch:
   - `Err(join_error)` -> blocking task join failure (warn)

## Files

- `crates/executor/src/idempotency_cleanup_worker.rs`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_worker_initial_delay_sec_from_seed_is_within_tick_bounds` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_nonblocking_skips_when_mutex_is_busy` — PASS
4. `cargo test -p copybot-executor -q validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
