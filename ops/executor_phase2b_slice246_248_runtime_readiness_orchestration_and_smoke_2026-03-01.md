# Executor Phase 2B — Slice 246-248

## Summary

This batch adds a single runtime-readiness orchestration helper for next-code-queue handoff and pins it with smoke coverage.

1. Added `tools/execution_runtime_readiness_report.sh`:
   - Composes two final evidence packages in one command:
     - `tools/adapter_rollout_final_evidence_report.sh`
     - `tools/execution_route_fee_final_evidence_report.sh`
   - Produces normalized aggregate verdict:
     - `runtime_readiness_verdict` (`GO` / `HOLD` / `NO_GO`)
     - `runtime_readiness_reason`
     - `runtime_readiness_reason_code`
   - Captures nested outputs and emits top-level summary/manifest sha fields.
   - Supports optional two-pass package bundle flow with finalized summary/manifest payload.

2. Added smoke guard `run_execution_runtime_readiness_report_case` in `tools/ops_scripts_smoke_test.sh`:
   - GO path assertions (aggregate + nested final verdicts).
   - Bundle-enabled parity assertions via `assert_bundled_summary_manifest_package_status_parity`.
   - Strict fail-closed invalid bool guard for `PACKAGE_BUNDLE_ENABLED`.

3. Updated `ROAD_TO_PRODUCTION.md`:
   - Added items `403` and `404` for runtime readiness orchestration + bundle parity coverage.
4. Updated runbook usage:
   - Added unified helper invocation under `ops/adapter_backend_runbook.md` (item 15).

## Files Changed

- `tools/execution_runtime_readiness_report.sh` (new)
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`
- `ops/adapter_backend_runbook.md`

## Validation

1. `bash -n tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke (sourced helper):
   - `run_execution_runtime_readiness_report_case` — PASS

## Notes

- This helper is aimed at reducing manual handoff churn for `ROAD_TO_PRODUCTION.md` next-code-queue items 1-3 by packaging adapter final + route-fee final readiness in one artifact set.
