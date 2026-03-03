# Executor Phase 2B — Slice 469

## Title
go/no-go strict executor backend-mode gate (`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`)

## Date
2026-03-03

## Scope

1. `tools/execution_go_nogo_report.sh`
2. `tools/execution_server_rollout_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`
5. `ops/test_server_rollout_6_1_tracker.md`

## Problem

After adding executor `backend_mode={upstream|mock}`, server-rollout had a strict guard, but the standalone go/no-go helper could still classify readiness without explicitly validating executor backend mode.

## Changes

1. Added strict go/no-go gate:
   1. `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (strict boolean, default `false`),
   2. `EXECUTOR_ENV_PATH` (default `/etc/solana-copy-bot/executor.env`).
2. Added env-file parser in `execution_go_nogo_report.sh`:
   1. reads `COPYBOT_EXECUTOR_BACKEND_MODE` from `EXECUTOR_ENV_PATH`,
   2. accepts `upstream|mock`,
   3. defaults to `upstream` when key is absent (runtime-equivalent),
   4. classifies invalid token as `unknown` under strict gate.
3. Added explicit diagnostics to go/no-go summary:
   1. `go_nogo_require_executor_upstream`,
   2. `executor_env_path`,
   3. `executor_backend_mode`,
   4. `executor_backend_mode_guard_verdict/reason/reason_code`.
4. Added fail-closed overall verdict integration:
   1. strict gate + `mock` => `overall_go_nogo_verdict=NO_GO`, `overall_go_nogo_reason_code=executor_backend_mode_not_upstream`,
   2. strict gate + missing env file / invalid token => `overall_go_nogo_verdict=NO_GO`, `overall_go_nogo_reason_code=executor_backend_mode_unknown`.
5. Propagated strict flag through server rollout orchestration:
   1. `execution_server_rollout_report.sh` now passes `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` and `EXECUTOR_ENV_PATH` to nested go-no-go / rehearsal / final evidence helpers.
6. Added smoke coverage (`ops_scripts_smoke_test.sh`):
   1. new targeted case `go_nogo_executor_backend_mode_guard`,
   2. strict reject path (`mock`),
   3. strict pass path (`upstream`),
   4. strict fail-closed for missing env file and invalid mode token,
   5. invalid bool token guard for `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`.

## Validation

1. `bash -n tools/execution_go_nogo_report.sh tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
4. `cargo check -p copybot-executor -q` — PASS

## Result

Go/no-go now has an explicit strict safety gate for executor backend mode, aligned with server-rollout strictness and controllable for non-live contour runs.
