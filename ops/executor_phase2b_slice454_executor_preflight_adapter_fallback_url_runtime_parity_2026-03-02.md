# Executor Phase 2B — Slice 454

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (runtime-parity closure for adapter fallback URL handling).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

Preflight previously validated only adapter primary submit/simulate URLs.  
`COPYBOT_ADAPTER_UPSTREAM_SUBMIT_FALLBACK_URL` and `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_FALLBACK_URL` (plus route overrides) could be malformed or collide with primary endpoints while preflight still returned `PASS`.

## Changes

### 1) Adapter fallback URL parity in preflight

`tools/executor_preflight.sh` now:

1. resolves adapter fallback URLs with runtime-equivalent layering (`route override -> global default`) for each route,
2. validates fallback URL schema via existing strict endpoint identity validator,
3. fail-closes when adapter submit fallback resolves to the same endpoint identity as adapter submit primary,
4. fail-closes when adapter simulate fallback resolves to the same endpoint identity as adapter simulate primary.

### 2) Smoke pin expansion

`run_executor_preflight_case` now includes targeted fail-closed negatives:

1. invalid adapter submit fallback URL scheme (`ftp://...`) -> `preflight_verdict: FAIL`,
2. adapter submit fallback identity collision (`fallback == primary`) -> `preflight_verdict: FAIL`,
3. adapter simulate fallback identity collision (`fallback == primary`) -> `preflight_verdict: FAIL`.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
