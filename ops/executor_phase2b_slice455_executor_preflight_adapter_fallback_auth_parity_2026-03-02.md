# Executor Phase 2B — Slice 455

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (preflight parity with adapter runtime auth resolution on fallback paths).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

When executor bearer auth is required, preflight validated only adapter primary auth token parity.  
Adapter fallback endpoints (submit/simulate) could run with fallback auth tokens that do not match executor bearer requirements, and preflight would pass.

## Changes

### 1) Adapter fallback auth parity in preflight

`tools/executor_preflight.sh` now:

1. reads adapter global fallback auth default (`COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN*`),
2. for each route with configured adapter fallback endpoint (submit or simulate), resolves fallback auth via runtime-equivalent precedence:
   - `COPYBOT_ADAPTER_ROUTE_<ROUTE>_FALLBACK_AUTH_TOKEN*`
   - `COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN*`
   - route primary auth token,
3. fail-closes when fallback auth is missing while bearer auth is required,
4. fail-closes when fallback auth mismatches executor bearer token.

### 2) Smoke pin

`run_executor_preflight_case` adds targeted negative:

1. adapter submit fallback endpoint configured + mismatched adapter fallback auth token -> `preflight_verdict: FAIL`,
2. asserts deterministic diagnostic:
   - `adapter fallback auth token mismatch for route=paper vs executor bearer token`.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
