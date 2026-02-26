# Executor Phase 2B Slice 150 — Optional Env NotUnicode Fail-Closed (2026-02-26)

## Scope

- close residual fail-open gap where optional env parsing silently treated non-UTF8 values as absent

## Changes

1. `env_parsing.rs`:
   - `optional_non_empty_env` changed from `Option<String>` to `Result<Option<String>>`
   - semantics:
     - `NotPresent` -> `Ok(None)`
     - present empty/whitespace -> `Ok(None)`
     - `NotUnicode` -> `Err(...)` (fail-closed)
2. Call-site migration:
   - updated all executor call-sites to propagate `Result` via `?`
   - includes:
     - startup auth/secret optional env inputs (`COPYBOT_EXECUTOR_BEARER_TOKEN`, etc.)
     - submit-verify optional env inputs (`COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL`, fallback)
3. Test coverage (`#[cfg(unix)]` integration guards in `executor_config_env.rs`):
   - `executor_config_from_env_rejects_non_utf8_bearer_token_value`
   - `executor_config_from_env_rejects_non_utf8_submit_verify_rpc_url_value`
4. Contract smoke:
   - registered both new tests in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/env_parsing.rs`
- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/submit_verify_config.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_bearer_token_value` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_submit_verify_rpc_url_value` — PASS
4. `cargo test -p copybot-executor -q` — PASS (551/551)
5. `bash tools/executor_contract_smoke_test.sh` — PASS
