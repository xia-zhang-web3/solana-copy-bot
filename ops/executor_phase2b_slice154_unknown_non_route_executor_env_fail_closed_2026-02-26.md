# Executor Phase 2B — Slice 154

Date: 2026-02-26  
Owner: execution-dev

## Scope

- close residual fail-open behavior where unknown non-route-scoped `COPYBOT_EXECUTOR_*` keys were silently ignored at startup.

## Changes

1. Added strict startup validator in `crates/executor/src/executor_config_env.rs`:
   - `validate_known_executor_env_keys()`
   - scans `env::vars_os()` and rejects unknown non-route-scoped `COPYBOT_EXECUTOR_*` keys when value is non-empty.
2. Preserved existing behavior boundaries:
   - route-scoped keys stay under `validate_route_scoped_env_targets_allowlist`.
   - `COPYBOT_EXECUTOR_TEST_*` namespace remains allowed for test helpers.
   - empty unknown keys remain ignored (to avoid breaking placeholder-only env stubs).
3. Added integration tests:
   - `executor_config_from_env_rejects_unknown_non_route_scoped_env_key`
   - `executor_config_from_env_ignores_empty_unknown_non_route_scoped_env_key`
4. Added smoke registration:
   - `executor_config_from_env_rejects_unknown_non_route_scoped_env_key`
5. Updated roadmap:
   - `ROAD_TO_PRODUCTION.md` item 314.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_non_route_scoped_env_key` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_ignores_empty_unknown_non_route_scoped_env_key` — PASS
4. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_` — PASS

## Result

- Startup config becomes typo-safe for top-level `COPYBOT_EXECUTOR_*` keys and fails closed instead of silently continuing with incorrect runtime configuration.
