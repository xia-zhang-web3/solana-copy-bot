# Executor Phase 2B Slice 34 Evidence (2026-02-25)

## Scope

1. Added submit-response retention control via `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC` (default `604800`, fail-closed `> 0`).
2. Added cadence-throttled cached-response cleanup in `SubmitIdempotencyStore::load_cached_or_claim_submit`.
3. Added shared cross-process cleanup marker for response cleanup (`executor_runtime_meta.response_cleanup_last_unix`).
4. Preserved claim cleanup/reclaim semantics and submit in-flight behavior.
5. Added guard tests for response-cleanup interval bounds and stale cached response eviction.

## Files

1. `crates/executor/src/idempotency.rs`
2. `crates/executor/src/executor_config_env.rs`
3. `crates/executor/src/main.rs`
4. `crates/executor/src/submit_handler.rs`
5. `tools/executor_contract_smoke_test.sh`
6. `ops/executor_backend_runbook.md`
7. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q idempotency_`
3. `cargo test -p copybot-executor -q stale_cached_response_is_cleaned_when_response_cleanup_due`
4. `bash tools/executor_contract_smoke_test.sh`
