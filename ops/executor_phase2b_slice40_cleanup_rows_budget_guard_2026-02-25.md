# Executor Phase 2B Slice 40 — Cleanup Rows-Budget Guard (2026-02-25)

## Scope

- close ops-safety residual risk for extreme cleanup tuning
- prevent accidental long-lock cleanup runs from misconfigured batch settings

## Changes

1. Added fail-closed rows-per-run guard in startup validation:
   - `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE * COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN <= 200000`
2. Validation now rejects both multiplication overflow and budget exceed cases.
3. Added unit test for excessive rows-per-run rejection.
4. Added smoke guard registration for new test.
5. Runbook notes combined rows-per-run safety bound.

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ops/executor_backend_runbook.md`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_response_cleanup_tuning_rejects_excessive_rows_per_run` — PASS
3. `cargo test -p copybot-executor -q validate_response_cleanup_tuning_rejects_out_of_range_values` — PASS
4. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
