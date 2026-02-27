# Executor Phase 2B — Slice 182

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- harden route-allowlist startup diagnostics by adding deterministic typo suggestions for close unknown route tokens.

## Changes

1. Runtime diagnostics hardening (`crates/executor/src/route_allowlist.rs`):
   - added closest-match suggestion logic for known routes:
     - `levenshtein_distance(...)`
     - `known_route_suggestion(...)`
   - unsupported allowlist route now includes optional suggestion:
     - `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains unsupported route=faslane (did you mean route=fastlane?) ...`
   - fail-closed behavior unchanged (still startup reject).
2. Parser guard coverage (`crates/executor/src/route_allowlist.rs`):
   - added:
     - `parse_route_allowlist_rejects_unknown_route_with_suggestion`
   - strengthened:
     - `parse_route_allowlist_rejects_unknown_route` now asserts distant unknown does **not** get suggestion.
3. Startup integration guard (`crates/executor/src/executor_config_env.rs`):
   - added:
     - `executor_config_from_env_rejects_unknown_route_allowlist_entry_with_suggestion`
4. Contract smoke (`tools/executor_contract_smoke_test.sh`):
   - registered:
     - `parse_route_allowlist_rejects_unknown_route_with_suggestion`
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `342`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q parse_route_allowlist_rejects_unknown_route_with_suggestion` — PASS
   - `cargo test -p copybot-executor -q parse_route_allowlist_rejects_unknown_route` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_route_allowlist_entry_with_suggestion` — PASS
3. `cargo test -p copybot-executor -q` — PASS (`621/621`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- route allowlist remains strict fail-closed while now returning actionable typo hints for close operator mistakes, reducing startup triage time.
