# Executor Phase 2B Slice 35 — Idempotency Response Cleanup Off Hot Path (2026-02-25)

## Scope

- move stale cached-response retention cleanup out of submit hot path
- add dedicated background worker for idempotency response cleanup ticks
- keep cadence-throttled/global-marker cleanup semantics unchanged
- preserve submit claim/reclaim correctness and cached-response semantics

## Files

- `crates/executor/src/idempotency.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `crates/executor/src/main.rs`
- `crates/executor/src/submit_handler.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Behavioral Notes

1. `load_cached_or_claim_submit` no longer runs response retention `DELETE` on submit path.
2. New `run_response_cleanup_if_due(response_retention_sec)` executes the same cleanup logic asynchronously.
3. Runtime spawns `spawn_response_cleanup_worker(state.clone())` and cancels it during graceful shutdown.
4. Contract smoke includes `response_cleanup_worker_tick_sec_clamps_bounds`.

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q stale_cached_response_is_cleaned_when_response_cleanup_due` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_worker_tick_sec_clamps_bounds` — PASS
4. `cargo test -p copybot-executor -q claim_flow_returns_claimed_inflight_then_cached` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
