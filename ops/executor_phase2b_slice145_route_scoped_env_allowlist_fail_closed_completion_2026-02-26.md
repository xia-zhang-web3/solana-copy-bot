# Executor Phase 2B Slice 145 — Route-Scoped Env Allowlist Fail-Closed Completion (2026-02-26)

## Scope

- close residual audit gap around `L-8` realism: make allowlist/backend consistency guard reachable in real env path (not only synthetic unit maps)

## Changes

1. `executor_config_env.rs`:
   - added route-scoped env parser for keys:
     - `COPYBOT_EXECUTOR_ROUTE_<ROUTE>_<SUFFIX>`
   - recognized suffix set mirrors runtime route config surface (`SUBMIT_URL`, `SIMULATE_URL`, `SEND_RPC_URL`, auth token/file variants, fallback variants)
   - parser now chooses **longest matching suffix** to avoid ambiguous short-suffix capture
2. Added startup guard:
   - `validate_route_scoped_env_targets_allowlist(&route_allowlist)`
   - called immediately after allowlist parse/validation in `from_env()`
   - rejects non-empty route-scoped env keys targeting routes not present in allowlist
3. Test coverage:
   - parser extraction and non-scoped ignore tests
   - allowlist target validator reject/accept tests
   - integration-like env test:
     - `executor_config_from_env_rejects_route_scoped_env_outside_allowlist`
4. Contract smoke:
   - registered all new tests in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q parse_route_scoped_env_key_extracts_route_and_suffix` — PASS
3. `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_rejects_outside_route` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_route_scoped_env_outside_allowlist` — PASS
5. `cargo test -p copybot-executor -q` — PASS (538/538)
6. `bash tools/executor_contract_smoke_test.sh` — PASS
