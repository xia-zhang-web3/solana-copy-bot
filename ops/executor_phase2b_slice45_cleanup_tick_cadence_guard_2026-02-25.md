# Executor Phase 2B Slice 45 — Cleanup Tick Cadence Guard (2026-02-25)

## Scope

- harden cleanup cadence configuration to protect response-retention SLA
- prevent worker tick misconfiguration that is slower than retention-derived cleanup interval
- keep fail-closed startup behavior

## Changes

1. Added reusable helper:
   - `response_cleanup_interval_sec_for_retention(response_retention_sec: u64) -> u64`
2. Added startup validation:
   - `validate_response_cleanup_worker_cadence(...)`
   - rejects when `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC` > interval derived from `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC`
3. Wired cadence validation into `ExecutorConfig::from_env` after tick range checks.
4. Added unit guard test:
   - `validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval`
5. Added new guard test to `tools/executor_contract_smoke_test.sh`.
6. Updated roadmap ledger.

## Files

- `crates/executor/src/idempotency.rs`
- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval` — PASS
3. `cargo test -p copybot-executor -q response_cleanup_nonblocking_skips_when_mutex_is_busy` — PASS
4. `cargo test -p copybot-executor -q validate_response_cleanup_worker_tick_sec_rejects_out_of_range_values` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
