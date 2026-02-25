# Executor Phase 2B Slice 36 — Cleanup Batching + Delayed First Tick (2026-02-25)

## Scope

- reduce residual SQLite contention risk from response cleanup worker
- avoid immediate cleanup execution on cold start
- preserve response-retention semantics and cadence throttling

## Changes

1. `run_response_cleanup_if_due` now deletes stale responses in bounded batches per run.
2. Batch delete uses `rowid IN (SELECT rowid ... ORDER BY updated_at_utc LIMIT ?)` to keep delete work capped.
3. Background worker now uses `tokio::time::interval_at(now + tick, tick)` so first tick is delayed by one interval.
4. Added bounded-cleanup guard test to prevent regression.

## Files

- `crates/executor/src/idempotency.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_is_bounded_per_run` — PASS
3. `cargo test -p copybot-executor -q stale_cached_response_is_not_cleaned_on_submit_claim_path` — PASS
4. `cargo test -p copybot-executor -q response_cleanup_worker_tick_sec_clamps_bounds` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
