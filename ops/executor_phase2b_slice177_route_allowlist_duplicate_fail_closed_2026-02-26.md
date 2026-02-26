# Executor Phase 2B — Slice 177

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- tighten startup route allowlist parsing to fail closed on duplicate entries (including case-insensitive duplicates after normalization).

## Changes

1. Runtime hardening (`crates/executor/src/route_allowlist.rs`):
   - `parse_route_allowlist(...)` now rejects duplicate routes:
     - before: duplicate entries silently deduplicated by `HashSet`
     - after: explicit error `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains duplicate route=<route>`
   - added parser guard: `parse_route_allowlist_rejects_duplicate_route`.
2. Startup integration coverage (`crates/executor/src/executor_config_env.rs`):
   - added `executor_config_from_env_rejects_duplicate_route_allowlist_entries`.
3. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new duplicate-route guards.
4. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 337.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q parse_route_allowlist_rejects_duplicate_route` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_duplicate_route_allowlist_entries` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- route allowlist parsing no longer masks duplicate operator config entries, improving fail-closed startup validation and reducing ambiguity in route policy intent.
