# Executor Phase 2B — Slice 178

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- tighten route allowlist parsing to fail closed on empty CSV entries (e.g., trailing commas) instead of silently skipping them.

## Changes

1. Runtime hardening (`crates/executor/src/route_allowlist.rs`):
   - `parse_route_allowlist(...)` now rejects empty normalized entries:
     - before: empty entries were skipped (`continue`)
     - after: explicit error `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains empty route entry`
   - added parser guard:
     - `parse_route_allowlist_rejects_empty_route_entry`
2. Startup integration coverage (`crates/executor/src/executor_config_env.rs`):
   - added:
     - `executor_config_from_env_rejects_empty_route_allowlist_entry`
3. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new empty-entry guards.
4. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 338.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q parse_route_allowlist_rejects_empty_route_entry` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_empty_route_allowlist_entry` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- malformed allowlist CSV no longer degrades silently; startup now fails closed for empty route entries, making route policy intent explicit and auditable.
