# Executor Phase 2B Slice 46 — Cleanup Worker Initial Jitter (2026-02-25)

## Scope

- reduce synchronized cleanup bursts across multiple executor instances
- keep cleanup cadence deterministic and bounded
- add guard coverage for jitter bounds

## Changes

1. Added deterministic initial-delay jitter for background cleanup worker:
   - `response_cleanup_worker_initial_delay_sec(tick_sec)`
   - first tick delay now in `1..=tick_sec` (instead of always exactly `tick_sec`)
2. Seed uses folded current UNIX nanos + process id to spread startup offsets.
3. Worker startup now logs `tick_sec` and `initial_delay_sec` at debug level.
4. Added pure helper:
   - `response_cleanup_worker_initial_delay_sec_from_seed(tick_sec, seed)`
5. Added unit test:
   - `response_cleanup_worker_initial_delay_sec_from_seed_is_within_tick_bounds`
6. Added new test to `tools/executor_contract_smoke_test.sh`.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/idempotency_cleanup_worker.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_worker_initial_delay_sec_from_seed_is_within_tick_bounds` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_nonblocking_skips_when_mutex_is_busy` — PASS
4. `cargo test -p copybot-executor -q validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
