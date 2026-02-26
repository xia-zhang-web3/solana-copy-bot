# Executor Phase 2B Slice 149 — Startup Env Non-UTF8 Fail-Closed Sweep (2026-02-26)

## Scope

- eliminate remaining startup fail-open paths where non-UTF8 env values could silently fallback to defaults

## Changes

1. `executor_config_env.rs`:
   - switched defaulted env reads to explicit `VarError` handling:
     - `COPYBOT_EXECUTOR_BIND_ADDR`
     - `COPYBOT_EXECUTOR_CONTRACT_VERSION`
     - `COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH`
   - semantics:
     - `NotPresent` -> existing default
     - `NotUnicode` -> startup error (fail-closed)
2. `env_parsing.rs`:
   - `parse_u64_env` and `parse_f64_env` now distinguish:
     - `NotPresent` -> default
     - `NotUnicode` -> error
3. Test coverage (`#[cfg(unix)]` integration guards in `executor_config_env.rs`):
   - `executor_config_from_env_rejects_non_utf8_bind_addr_value`
   - `executor_config_from_env_rejects_non_utf8_contract_version_value`
   - `executor_config_from_env_rejects_non_utf8_idempotency_db_path_value`
   - `executor_config_from_env_rejects_non_utf8_request_timeout_value`
   - `executor_config_from_env_rejects_non_utf8_max_notional_sol_value`
4. Contract smoke:
   - registered all new guards in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/env_parsing.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_bind_addr_value` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_contract_version_value` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_idempotency_db_path_value` — PASS
5. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_request_timeout_value` — PASS
6. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_max_notional_sol_value` — PASS
7. `cargo test -p copybot-executor -q` — PASS (549/549)
8. `bash tools/executor_contract_smoke_test.sh` — PASS
