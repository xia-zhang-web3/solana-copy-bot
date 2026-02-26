# Executor Phase 2B — Slice 181

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- improve startup env typo diagnostics while preserving fail-closed behavior for unknown/malformed executor env keys.

## Changes

1. Runtime diagnostics hardening (`crates/executor/src/executor_config_env.rs`):
   - added deterministic typo-suggestion helpers:
     - `levenshtein_distance(...)`
     - `best_match_key(...)`
     - `suggest_route_scoped_key(...)`
     - `suggest_non_route_executor_key(...)`
     - `canonical_uppercase_executor_key_suggestion(...)`
   - unknown route-scoped keys now include suggestion when close match exists:
     - `COPYBOT_EXECUTOR_ROUTE_RPC_SUBMITURL` -> `did you mean COPYBOT_EXECUTOR_ROUTE_RPC_SUBMIT_URL?`
   - unknown non-route keys now include suggestion when close match exists:
     - `COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLD` -> `did you mean COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED?`
   - mixed-case prefix rejects now include canonical uppercase suggestion when derivable.
2. Guard coverage updates (`crates/executor/src/executor_config_env.rs`):
   - strengthened existing tests to assert suggestion presence in:
     - `route_scoped_env_targets_allowlist_rejects_unknown_scoped_key`
     - `executor_config_from_env_rejects_unknown_route_scoped_env_key`
     - `executor_config_from_env_rejects_unknown_non_route_scoped_env_key`
     - `executor_config_from_env_rejects_mixed_case_executor_prefix_keys`
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `341`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q route_scoped_env_targets_allowlist_rejects_unknown_scoped_key` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_route_scoped_env_key` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_non_route_scoped_env_key` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_mixed_case_executor_prefix_keys` — PASS
3. `cargo test -p copybot-executor -q` — PASS (`619/619`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- startup env validation remains fail-closed and now reports actionable typo hints, reducing operator error-recovery time without expanding runtime trust surface.
