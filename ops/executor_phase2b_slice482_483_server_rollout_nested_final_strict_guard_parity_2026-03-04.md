# Executor Phase-2B Slice 482-483 — Server Rollout Nested Final Strict Guard Parity

Date: 2026-03-04  
Owner: execution-dev

## Scope

1. `tools/execution_server_rollout_report.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Why

`execution_server_rollout_report.sh` already enforced nested go/no-go strict guard diagnostics, but did not fail-close on strict propagation drift inside nested final helpers (`executor_final`, `adapter_final`).

This left a remaining observability/parity gap on the top orchestrator path.

## Changes

1. `execution_server_rollout_report.sh`
   1. added fail-closed nested strict validation for `executor_final` output:
      1. `go_nogo_require_executor_upstream` strict bool + parity to `SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM`,
      2. `executor_env_path` non-empty + parity to top-level `EXECUTOR_ENV_PATH`,
      3. `rollout_nested_go_nogo_require_executor_upstream` strict bool + parity,
      4. `rollout_nested_executor_env_path` non-empty + parity.
   2. added fail-closed nested strict validation for `adapter_final` output:
      1. `go_nogo_require_executor_upstream` strict bool + parity,
      2. `executor_env_path` non-empty + parity.
   3. added summary observability fields:
      1. `executor_final_go_nogo_require_executor_upstream`,
      2. `executor_final_executor_env_path`,
      3. `executor_final_rollout_nested_go_nogo_require_executor_upstream`,
      4. `executor_final_rollout_nested_executor_env_path`,
      5. `adapter_final_go_nogo_require_executor_upstream`,
      6. `adapter_final_executor_env_path`.

2. `ops_scripts_smoke_test.sh`
   1. extended `run_execution_server_rollout_report_case` assertions for the new nested strict fields in:
      1. default pass path,
      2. skip-direct path,
      3. `finals_only` profile path,
      4. bundle path,
      5. mock-backend strict override path (`SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false`).
   2. corrected mock-override env-path expectations to assert against the actual nested executor env file used in that scenario.

3. `ROAD_TO_PRODUCTION.md`
   1. added ledger entries `482` and `483`.

## Validation

1. `bash -n tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `5` (executor backend/evidence-chain closure, orchestrator strict parity hardening).
