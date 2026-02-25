# Executor Phase 2B Slice 38 — Exact-Cap Cleanup Completion Probe (2026-02-25)

## Scope

- fix exact-cap edge case in batched response cleanup completion detection
- keep marker progression accurate when cleanup fully drains at batch boundary
- extend guard coverage for exact-limit completion semantics

## Changes

1. `delete_stale_cached_responses_in_batches` now probes for remaining stale rows after exhausting configured batch limit.
2. If no stale rows remain after exact-cap deletes, function returns `cleanup_completed=true`.
3. `run_response_cleanup_if_due` keeps marker behavior: advance only when `cleanup_completed=true`.
4. Added `response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` guard test.

## Files

- `crates/executor/src/idempotency.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_is_bounded_per_run` — PASS
4. `cargo test -p copybot-executor -q stale_cached_response_is_cleaned_when_response_cleanup_due` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
