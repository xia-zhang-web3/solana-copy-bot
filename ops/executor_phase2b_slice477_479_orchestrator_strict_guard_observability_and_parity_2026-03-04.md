# Slice 477-479 — Orchestrator strict-guard observability and parity

Date: 2026-03-04  
Owner: execution-dev

## Scope

1. `tools/execution_server_rollout_report.sh`
2. `tools/execution_runtime_readiness_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## What changed

1. Server-rollout helper now extracts nested strict guard diagnostics from `execution_go_nogo_report.sh` output:
   1. `go_nogo_executor_backend_mode_guard_verdict`
   2. `go_nogo_executor_backend_mode_guard_reason_code`
   3. `go_nogo_executor_upstream_endpoint_guard_verdict`
   4. `go_nogo_executor_upstream_endpoint_guard_reason_code`
2. Server-rollout helper fail-closes if nested guard verdict fields are missing/invalid or guard reason-code fields are empty.
3. Runtime-readiness helper now validates nested propagation contract for both final helpers:
   1. `go_nogo_require_executor_upstream` (must be strict bool and match top-level normalized value)
   2. `executor_env_path` (must be non-empty and match top-level `EXECUTOR_ENV_PATH`)
4. Runtime-readiness summary now exposes nested propagation fields:
   1. `adapter_final_nested_go_nogo_require_executor_upstream`
   2. `adapter_final_nested_executor_env_path`
   3. `route_fee_final_nested_go_nogo_require_executor_upstream`
   4. `route_fee_final_nested_executor_env_path`
5. Smoke coverage extended:
   1. server-rollout PASS/SKIP/bundle paths now assert nested strict guard fields,
   2. server-rollout strict placeholder topology -> `NO_GO` pin,
   3. server-rollout strict missing topology -> `NO_GO` pin,
   4. runtime-readiness paths now assert nested strict propagation fields across pass/mock-override/profile/skip branches.

## Validation commands

1. `bash -n tools/execution_server_rollout_report.sh tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh`
2. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh`
3. `OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh`
4. `cargo check -p copybot-executor -q`

## Expected outcome

1. Orchestrator reports surface strict guard reasons/codes directly (no nested log digging for backend/topology gate classification).
2. Propagation drift (`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`, `EXECUTOR_ENV_PATH`) is fail-closed in runtime-readiness.
3. Strict topology regressions are now pinned at orchestrator-level smoke, not only go/no-go unit level.
