# Slice 535 — Targeted Smoke Full-Heavy Runtime Guard

Date: 2026-03-05
Owner: execution-dev
Type: DX/runtime hardening (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `tools/ops_scripts_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## Changes

1. Added new targeted-dispatch safety switch:
   1. `OPS_SMOKE_ALLOW_HEAVY_FULL` (default: `false`),
   2. strict fail-closed bool parse (`parse_bool_token_strict`),
   3. deterministic block when `OPS_SMOKE_PROFILE=full` and expanded targeted case list includes heavy runtime cases.
2. Added explicit operator guidance on block:
   1. prints heavy case list,
   2. suggests `OPS_SMOKE_PROFILE=fast|auto`,
   3. allows explicit override only with `OPS_SMOKE_ALLOW_HEAVY_FULL=true`.
3. Extended `run_ops_smoke_targeted_dispatch_case` coverage:
   1. invalid `OPS_SMOKE_ALLOW_HEAVY_FULL` token reject,
   2. blocked `full+heavy` path,
   3. explicit override path (`full+heavy+allow=true`) using fast alias fixture.

## Validation

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — FAIL-CLOSED PASS
3. `OPS_SMOKE_PROFILE=full OPS_SMOKE_ALLOW_HEAVY_FULL=true OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=auto OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
5. `OPS_SMOKE_TARGET_CASES=audit_ops_smoke_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. This guard removes accidental heavy `full` targeted runs from day-to-day loops while keeping an explicit escape hatch for intentional deep runs.
