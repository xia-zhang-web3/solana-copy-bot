# Executor Phase 2B Slice 147 — Route-Scoped Env Unknown-Suffix Fail-Closed (2026-02-26)

## Scope

- close remaining fail-open behavior in route-scoped env validation where typo keys under `COPYBOT_EXECUTOR_ROUTE_...` could be silently ignored

## Changes

1. `executor_config_env.rs`:
   - added explicit non-scoped exemption list: `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST`
   - updated route-scoped validator to fail-close on unknown non-empty route-scoped keys that do not match supported suffixes
   - preserved existing checks:
     - non-UTF8 key/value rejection
     - outside-allowlist route rejection
     - empty values ignored
2. Test coverage:
   - `route_scoped_env_targets_allowlist_rejects_unknown_scoped_key`
   - `route_scoped_env_targets_allowlist_ignores_allowlist_key`
   - `executor_config_from_env_rejects_unknown_route_scoped_env_key`
3. Contract smoke:
   - registered all three new tests in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_rejects_unknown_scoped_key` — PASS
3. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_ignores_allowlist_key` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_route_scoped_env_key` — PASS
5. `cargo test -p copybot-executor -q` — PASS (543/543)
6. `bash tools/executor_contract_smoke_test.sh` — PASS
