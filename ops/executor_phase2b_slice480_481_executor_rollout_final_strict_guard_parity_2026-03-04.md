# Executor Phase-2B Slice 480-481 — Executor Rollout/Final Strict Guard Parity

Date: 2026-03-04  
Owner: execution-dev

## Scope

1. `tools/executor_rollout_evidence_report.sh`
2. `tools/executor_final_evidence_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Why

`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` + `EXECUTOR_ENV_PATH` were already strict-propagated through server/runtime/readiness chains, but executor-specific evidence chain (`executor_rollout` and `executor_final`) still did not enforce nested propagation parity.

This batch closes that drift path.

## Changes

1. `executor_rollout_evidence_report.sh`
   1. added strict bool input parsing for `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (default `true`),
   2. propagates `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` and `EXECUTOR_ENV_PATH` into nested `execution_devnet_rehearsal.sh`,
   3. validates nested rehearsal outputs fail-closed:
      1. `go_nogo_require_executor_upstream` must be strict bool and equal top-level value,
      2. `executor_env_path` must be non-empty and equal top-level `EXECUTOR_ENV_PATH`,
   4. emits rollout summary observability fields:
      1. `go_nogo_require_executor_upstream`,
      2. `executor_env_path`,
      3. `rehearsal_nested_go_nogo_require_executor_upstream`,
      4. `rehearsal_nested_executor_env_path`.

2. `executor_final_evidence_report.sh`
   1. added strict bool input parsing for `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (default `true`),
   2. propagates it into nested `executor_rollout_evidence_report.sh`,
   3. validates nested rollout outputs fail-closed:
      1. `go_nogo_require_executor_upstream` strict bool + parity,
      2. `executor_env_path` non-empty + parity,
   4. emits final summary observability fields:
      1. `go_nogo_require_executor_upstream`,
      2. `executor_env_path`,
      3. `rollout_nested_go_nogo_require_executor_upstream`,
      4. `rollout_nested_executor_env_path`.

3. `ops_scripts_smoke_test.sh`
   1. extended `run_executor_rollout_evidence_case` positive assertions for new rollout fields,
   2. added rollout negative pin for invalid `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` token,
   3. added profile-path assertions for nested strict fields (`precheck_only` => `n/a`, `rehearsal_only` => parity values),
   4. extended `run_executor_rollout_evidence_case` final-helper assertions for new nested fields,
   5. added final-helper override pin (`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false`) and invalid-token negative pin.

4. `ROAD_TO_PRODUCTION.md`
   1. added ledger entries `480` and `481`.

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=executor_rollout_evidence bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
4. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `5` (executor backend/evidence-chain closure).
