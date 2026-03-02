# Executor Phase-2B Slice 292 — Rollout/Runtime Profile Presets

Date: 2026-03-02  
Owner: execution-dev  
Scope: orchestration throughput presets with fail-closed validation

## Change Summary

1. Added server rollout profile presets:
   1. `SERVER_ROLLOUT_PROFILE=full` (default)
   2. `SERVER_ROLLOUT_PROFILE=finals_only` (disables direct go/no-go + direct rehearsal stages)
2. Added runtime readiness profile presets:
   1. `RUNTIME_READINESS_PROFILE=full` (default)
   2. `RUNTIME_READINESS_PROFILE=adapter_only`
   3. `RUNTIME_READINESS_PROFILE=route_fee_only`
3. Kept explicit stage toggles as overrides (`SERVER_ROLLOUT_RUN_*`, `RUNTIME_READINESS_RUN_*`) after profile defaults.
4. Added strict fail-closed validation for unknown profile values.
5. Extended smoke coverage for:
   1. server finals-only profile behavior,
   2. runtime adapter-only and route-fee-only profile behavior,
   3. invalid profile rejection for both orchestrators.

## Files Updated

1. `tools/execution_server_rollout_report.sh`
2. `tools/execution_runtime_readiness_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Validation (targeted)

1. `bash -n tools/execution_server_rollout_report.sh tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES="execution_server_rollout" bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES="execution_runtime_readiness" bash tools/ops_scripts_smoke_test.sh` — PASS

## Result

Operators can switch orchestration breadth via a single validated profile token instead of manually toggling multiple stage flags, reducing rollout command complexity while preserving strict/fail-closed behavior and deterministic summary fields.
