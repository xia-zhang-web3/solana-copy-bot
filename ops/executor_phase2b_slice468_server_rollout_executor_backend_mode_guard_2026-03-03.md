# Executor Phase 2B — Slice 468

## Title
server-rollout fail-closed guard for executor backend mode (`COPYBOT_EXECUTOR_BACKEND_MODE`)

## Date
2026-03-03

## Scope

1. `tools/execution_server_rollout_report.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`
4. `ops/test_server_rollout_6_1_tracker.md`

## Problem

`copybot-executor` now supports `COPYBOT_EXECUTOR_BACKEND_MODE=mock` for non-live contour.
Without an orchestration-level guard, server rollout could continue with mock mode by mistake.

## Changes

1. Added strict env-file parsing in `tools/execution_server_rollout_report.sh`:
   1. reads `COPYBOT_EXECUTOR_BACKEND_MODE` from `EXECUTOR_ENV_PATH` (defaults to `upstream` if key is absent),
   2. validates token as one of `upstream|mock`.
2. Added new strict rollout gate:
   1. `SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM` (default `true`),
   2. when `true`, rollout fail-closes with `input_error` if executor mode is `mock`.
3. Added observability fields in rollout summary:
   1. `server_rollout_require_executor_upstream`,
   2. `executor_backend_mode`.
4. Added smoke coverage in `run_execution_server_rollout_report_case`:
   1. default path keeps `upstream` and passes baseline expectations,
   2. negative pin: `mock` + default strict guard => exit `3`, `server_rollout_reason_code=input_error`,
   3. controlled override pin: `SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false` + `mock` path allowed for non-live contour and retains expected HOLD result.

## Validation

1. `bash -n tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS (`[ok] execution server rollout report`)
3. `cargo check -p copybot-executor -q` — PASS

## Result

Server rollout orchestration is now fail-closed against accidental executor mock mode by default, while preserving explicit non-live override for controlled contour tests.
