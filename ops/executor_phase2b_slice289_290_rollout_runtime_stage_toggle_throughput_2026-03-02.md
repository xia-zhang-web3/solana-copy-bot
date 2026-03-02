# Executor Phase-2B Slice 289-290 — Rollout/Runtime Stage Toggle Throughput Hardening

Date: 2026-03-02  
Owner: execution-dev  
Scope: server rollout + runtime readiness helper throughput controls

## Change Summary

1. Added strict direct-stage toggles to `execution_server_rollout_report.sh`:
   1. `SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT` (default `true`)
   2. `SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT` (default `true`)
2. Added strict final-stage toggles to `execution_runtime_readiness_report.sh`:
   1. `RUNTIME_READINESS_RUN_ADAPTER_FINAL` (default `true`)
   2. `RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL` (default `true`)
3. Added fail-closed guard in runtime readiness: both stage toggles cannot be `false` simultaneously.
4. Preserved default behavior: when toggles are unset, orchestration flow and verdict precedence remain unchanged.
5. Expanded smoke coverage:
   1. server rollout skip-direct scenario (`go_nogo/rehearsal = SKIP` while finals remain active),
   2. runtime readiness single-stage scenario (`route_fee=SKIP`, adapter active),
   3. runtime readiness misconfiguration reject (both toggles false).

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

Operators can now run focused orchestration stages for faster iteration while keeping strict fail-closed semantics and explicit summary-level stage status (`GO/HOLD/NO_GO/SKIP`) so auditability and gating behavior stay deterministic.
