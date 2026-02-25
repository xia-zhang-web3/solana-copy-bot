# Executor Phase 2B Slice 41 — Cleanup Worker Tick Env Tuning (2026-02-25)

## Scope

- expose explicit cleanup worker tick tuning for ops capacity control
- keep fail-closed validation for out-of-range tick values
- preserve retention-derived tick as default fallback

## Changes

1. Added new optional env:
   - `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC`
2. Startup validation now enforces tick bounds:
   - valid range: `15..=300` seconds
3. Worker now uses configured tick directly from `ExecutorConfig`.
4. Retention-based tick helper remains as default provider when env is omitted.
5. Added unit tests for tick validation accept/reject cases.
6. Updated runbook env template and contract smoke guard list.

## Files

- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ops/executor_backend_runbook.md`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_response_cleanup_worker_tick_sec_rejects_out_of_range_values` — PASS
3. `cargo test -p copybot-executor -q validate_response_cleanup_tuning_rejects_excessive_rows_per_run` — PASS
4. `cargo test -p copybot-executor -q response_cleanup_exact_batch_limit_advances_marker_when_fully_drained` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
