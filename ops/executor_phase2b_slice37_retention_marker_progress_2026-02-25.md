# Executor Phase 2B Slice 37 — Retention Marker Progress Guard (2026-02-25)

## Scope

- close retention-SLA gap in batched response cleanup
- prevent stale backlog starvation when cleanup run hits batch cap

## Changes

1. `delete_stale_cached_responses_in_batches` now returns cleanup completion state.
2. `run_response_cleanup_if_due` updates `response_cleanup_last_unix` only when cleanup run fully drains stale rows.
3. If per-run cap is hit, marker is not advanced; next worker tick continues cleanup without waiting full cleanup interval.
4. Extended bounded-cleanup test to run second cleanup immediately and assert backlog drains to zero.

## Files

- `crates/executor/src/idempotency.rs`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_is_bounded_per_run` — PASS
3. `cargo test -p copybot-executor -q stale_cached_response_is_cleaned_when_response_cleanup_due` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
