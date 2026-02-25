# Executor Phase 2B Slice 39 — Cleanup Tuning Env Controls (2026-02-25)

## Scope

- expose response cleanup throughput controls via executor env config
- keep cleanup marker semantics from slices 37/38 unchanged
- add fail-closed startup validation for cleanup tuning bounds

## Changes

1. Added configurable env knobs:
   - `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE`
   - `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN`
2. `ExecutorConfig::from_env()` now parses and validates both values:
   - must be `> 0`
   - must fit conversion bounds used at runtime (`i64` / `usize`)
3. Background cleanup worker now passes these config values to `run_response_cleanup_if_due`.
4. Runbook env template updated with both optional knobs.
5. Contract smoke guard list includes config validation test for out-of-range cleanup tuning.

## Files

- `crates/executor/src/main.rs`
- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/idempotency.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `tools/executor_contract_smoke_test.sh`
- `ops/executor_backend_runbook.md`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_response_cleanup_tuning_rejects_out_of_range_values` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
4. `cargo test -p copybot-executor -q response_cleanup_is_bounded_per_run` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
