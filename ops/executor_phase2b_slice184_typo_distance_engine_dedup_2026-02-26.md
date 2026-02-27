# Executor Phase 2B — Slice 184

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- remove duplicated typo-distance implementations by introducing shared text-distance utility used by both startup env diagnostics and route allowlist suggestions.

## Changes

1. Shared utility added (`crates/executor/src/text_distance.rs`):
   - `levenshtein_distance(...)`
   - `closest_match(...)`
   - module-local tests:
     - `levenshtein_distance_matches_known_cases`
     - `closest_match_returns_lowest_distance_candidate`
2. Runtime wiring (`crates/executor/src/main.rs`):
   - added `mod text_distance;`
3. Env diagnostics refactor (`crates/executor/src/executor_config_env.rs`):
   - removed local Levenshtein implementation and switched suggestion matching to shared `closest_match(...)`.
   - existing suggestion thresholds/behavior unchanged.
4. Route allowlist diagnostics refactor (`crates/executor/src/route_allowlist.rs`):
   - removed local Levenshtein implementation and switched `known_route_suggestion(...)` to shared `closest_match(...)`.
   - route-specific suggestion threshold/behavior unchanged.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `344`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q levenshtein_distance_matches_known_cases` — PASS
   - `cargo test -p copybot-executor -q closest_match_returns_lowest_distance_candidate` — PASS
   - `cargo test -p copybot-executor -q parse_route_allowlist_rejects_unknown_route_with_suggestion` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_non_route_scoped_env_key` — PASS
3. `cargo test -p copybot-executor -q` — PASS (`623/623`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- typo-suggestion behavior is preserved while implementation drift risk is reduced by centralizing distance logic in one module.
