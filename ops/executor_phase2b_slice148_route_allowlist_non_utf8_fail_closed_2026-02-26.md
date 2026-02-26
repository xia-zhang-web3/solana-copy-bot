# Executor Phase 2B Slice 148 — Route Allowlist Non-UTF8 Fail-Closed (2026-02-26)

## Scope

- close low gap where non-UTF8 `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` value was silently treated as missing/default

## Changes

1. `executor_config_env.rs`:
   - replaced `env::var(...).unwrap_or_else(...)` with explicit `VarError` handling for route allowlist
   - semantics:
     - `NotPresent` -> default `paper,rpc,jito`
     - `NotUnicode` -> startup error (`COPYBOT_EXECUTOR_ROUTE_ALLOWLIST must be valid UTF-8`)
2. Test coverage:
   - added unix integration guard `executor_config_from_env_rejects_non_utf8_route_allowlist_value`
3. Contract smoke:
   - registered new guard test in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q executor_config_from_env_rejects_non_utf8_route_allowlist_value` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
