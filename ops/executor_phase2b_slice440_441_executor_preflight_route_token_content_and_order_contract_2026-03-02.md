# Executor Phase 2B — Slices 440-441

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Slice 440 — Route-token content strictness

`executor_preflight.sh` now validates route-array item contract for health route fields:

1. fields:
   1. `enabled_routes`,
   2. `routes`,
   3. `send_rpc_enabled_routes`,
   4. `send_rpc_fallback_routes`,
   5. `send_rpc_routes`.
2. per-item requirements:
   1. item must be `string`,
   2. item must be non-empty after trim,
   3. item must be lowercase,
   4. no duplicate tokens after lowercase normalization.
3. fail-closed diagnostics include field name + index + cause.

Smoke pins added for:

1. non-string entry (`number`) in `enabled_routes`,
2. uppercase entry in `enabled_routes`,
3. duplicate token after normalization in `enabled_routes`,
4. empty token entry in `send_rpc_fallback_routes`.

## Slice 441 — Route ordering determinism

`executor_preflight.sh` now enforces lexicographic sorted order for all five route arrays above.

1. unsorted arrays now fail with explicit `got ... expected ...` diagnostics.
2. fake health defaults updated to sorted order to mirror runtime payload:
   1. `enabled_routes`: `jito,paper,rpc`
   2. `send_rpc_enabled_routes`: `jito,rpc`
3. smoke pins added for unsorted rejects:
   1. unsorted `enabled_routes`
   2. unsorted `send_rpc_enabled_routes`

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted harness:
   1. `run_executor_preflight_case` — PASS
4. Targeted dispatcher:
   1. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full `bash tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight paths above were executed.
