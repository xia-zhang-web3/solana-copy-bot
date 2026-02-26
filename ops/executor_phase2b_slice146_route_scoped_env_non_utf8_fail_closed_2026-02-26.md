# Executor Phase 2B Slice 146 — Route-Scoped Env Non-UTF8 Fail-Closed (2026-02-26)

## Scope

- close low gap in route-scoped env guard: prevent `env::vars()` panic on non-UTF8 env entries and keep startup behavior fail-closed via `Result`

## Changes

1. `executor_config_env.rs`:
   - switched route-scoped env scan from `env::vars()` to `env::vars_os()`
   - added explicit fail-closed errors for:
     - non-UTF8 route-scoped key
     - non-UTF8 route-scoped value
   - retained existing semantics:
     - ignore non route-scoped keys
     - skip empty route-scoped values
     - reject route targets outside allowlist
2. Test coverage:
   - `route_scoped_env_targets_allowlist_rejects_non_utf8_value` (`#[cfg(unix)]`)
   - `route_scoped_env_targets_allowlist_rejects_non_utf8_key` (`#[cfg(unix)]`)
3. Contract smoke:
   - registered both new guard tests in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_rejects_non_utf8_value` — PASS
3. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_rejects_non_utf8_key` — PASS
4. `cargo test -p copybot-executor -q` — PASS (540/540)
5. `bash tools/executor_contract_smoke_test.sh` — PASS
