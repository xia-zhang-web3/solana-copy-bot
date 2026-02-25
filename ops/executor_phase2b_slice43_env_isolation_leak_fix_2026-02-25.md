# Executor Phase 2B Slice 43 — Env Isolation Leak Fix (2026-02-25)

## Scope

- fix low test-harness leakage in executor env-isolation helper
- ensure newly created `COPYBOT_EXECUTOR_*` vars inside scoped tests are removed after scope
- add regression guard and smoke coverage

## Changes

1. Added shared helper:
   - `clear_copybot_executor_env()`
2. Updated `with_clean_executor_env` lifecycle:
   - clear all `COPYBOT_EXECUTOR_*` before running closure
   - clear all `COPYBOT_EXECUTOR_*` again after closure
   - restore saved snapshot only after second clear
3. Added regression test:
   - `with_clean_executor_env_removes_newly_added_keys_after_scope`
4. Added regression test to `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q with_clean_executor_env_removes_newly_added_keys_after_scope` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_wires_response_cleanup_worker_tick_override` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
