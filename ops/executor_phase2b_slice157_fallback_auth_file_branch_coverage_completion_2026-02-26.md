# Executor Phase 2B — Slice 157

Date: 2026-02-26  
Owner: execution-dev

## Scope

- close residual low-gap for strict fallback-topology guard by covering `*_FALLBACK_AUTH_TOKEN_FILE` branches.

## Changes

1. Added test helper in `crates/executor/src/executor_config_env.rs`:
   - `with_temp_secret_file(...)` for deterministic file-backed secret fixtures.
2. Added 4 integration guards for file branches:
   - `executor_config_from_env_rejects_route_specific_upstream_fallback_auth_file_without_fallback_endpoint`
   - `executor_config_from_env_rejects_route_specific_send_rpc_fallback_auth_file_without_fallback_endpoint`
   - `executor_config_from_env_rejects_global_upstream_fallback_auth_file_without_any_fallback_endpoint`
   - `executor_config_from_env_rejects_global_send_rpc_fallback_auth_file_without_any_fallback_endpoint`
3. Registered all 4 tests in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap item 317 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted runs for all 4 new file-branch tests — PASS
3. `cargo test -p copybot-executor -q` — PASS (`570/570`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- Fallback-topology strictness is now fully covered for both token and file branches.
- Residual test-only low-gap from auditor note is closed.
