# Slice 536 — Targeted Smoke Heavy-Case Timeout Guard

Date: 2026-03-05
Owner: execution-dev
Type: DX/runtime hardening (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `tools/ops_scripts_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## Changes

1. Added targeted smoke timeout control:
   1. `OPS_SMOKE_CASE_TIMEOUT_SEC` (default `120`),
   2. strict fail-closed parse and bounds check (`1..120`),
   3. timeout wrapper for heavy targeted cases (`run_target_case_with_timeout`).
2. Heavy targeted cases now run under per-case budget and fail with deterministic marker on timeout:
   1. `targeted smoke case timed out after <N>s: <case>`.
3. Extended smoke dispatcher guard coverage:
   1. invalid timeout token reject (`turbo`),
   2. invalid timeout range reject (`121`),
   3. enforced timeout path (`OPS_SMOKE_CASE_TIMEOUT_SEC=1` + `executor_preflight_fast`).

## Validation

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=auto OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — FAIL-CLOSED PASS (slice 535 guard)
5. `OPS_SMOKE_PROFILE=full OPS_SMOKE_ALLOW_HEAVY_FULL=true OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — PASS
6. `OPS_SMOKE_CASE_TIMEOUT_SEC=turbo OPS_SMOKE_TARGET_CASES=common_timeout_parser bash tools/ops_scripts_smoke_test.sh` — FAIL-CLOSED PASS
7. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_CASE_TIMEOUT_SEC=1 OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — FAIL-CLOSED PASS (timeout marker)
8. `OPS_SMOKE_TARGET_CASES=audit_ops_smoke_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. This keeps targeted smoke runs bounded without requiring full-suite invocation and enforces a hard upper budget for heavy cases unless the case itself is optimized.
