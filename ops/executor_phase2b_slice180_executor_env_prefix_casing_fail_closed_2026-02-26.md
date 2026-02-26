# Executor Phase 2B — Slice 180

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- harden startup config parsing to fail-close mixed-case executor env keys (`copybot_executor_*`) with deterministic diagnostics.

## Changes

1. Runtime hardening (`crates/executor/src/executor_config_env.rs`):
   - added shared helper `starts_with_prefix_case_insensitive(...)`.
   - `validate_known_executor_env_keys(...)` now:
     - scans `COPYBOT_EXECUTOR_*` prefix case-insensitively,
     - rejects non-empty mixed-case keys with explicit error:
       - `COPYBOT_EXECUTOR_* env keys must use canonical uppercase prefix COPYBOT_EXECUTOR_: ...`
     - keeps existing behavior for empty values (ignored) and unknown canonical keys.
2. Test environment hygiene (`crates/executor/src/executor_config_env.rs`):
   - `with_clean_executor_env` / `clear_copybot_executor_env` now clear executor-prefixed env keys case-insensitively to avoid test leakage from mixed-case fixtures.
3. Integration guards (`crates/executor/src/executor_config_env.rs`):
   - added reject guard:
     - `executor_config_from_env_rejects_mixed_case_executor_prefix_keys`
   - added empty-ignore guard:
     - `executor_config_from_env_ignores_empty_mixed_case_executor_prefix_key`
4. Contract smoke (`tools/executor_contract_smoke_test.sh`):
   - registered `executor_config_from_env_rejects_mixed_case_executor_prefix_keys`.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `340`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_mixed_case_executor_prefix_keys` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_ignores_empty_mixed_case_executor_prefix_key` — PASS
   - `cargo test -p copybot-executor -q executor_config_from_env_rejects_unknown_non_route_scoped_env_key` — PASS
3. `cargo test -p copybot-executor -q` — PASS (`619/619`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- startup config now fail-closes a common typo class (wrong env key casing) without widening policy surface or changing route/runtime behavior.
