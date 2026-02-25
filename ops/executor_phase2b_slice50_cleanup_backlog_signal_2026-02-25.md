# Executor Phase 2B Slice 50 — Cleanup Backlog Pressure Signal (2026-02-25)

## Scope

- improve cleanup observability under sustained stale backlog
- keep cleanup semantics unchanged
- provide operator-visible signal when per-run cap throttles marker advancement

## Changes

1. Added debug log in response cleanup path when `delete_stale_cached_responses_in_batches(...)` returns incomplete cleanup (`cleanup_completed=false`).
2. Log includes:
   - `response_retention_sec`
   - `response_cleanup_batch_size`
   - `response_cleanup_max_batches_per_run`
3. Marker behavior remains unchanged:
   - still not advanced when backlog remains.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/idempotency.rs`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_is_bounded_per_run` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
