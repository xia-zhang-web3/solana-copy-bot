# Executor Phase 2B — Slice 299

Date: 2026-03-02
Commit scope: required-stage disabled guard closure for rollout profile/toggle batch

## Goal

Close auditor-reported gaps after profile/toggle rollout batch:
1. enforce fail-closed required-stage semantics in `executor_rollout_evidence_report.sh` when rehearsal stage is disabled;
2. pin adapter required-stage guardrails with explicit negative smoke cases.

## Changes

1. `tools/executor_rollout_evidence_report.sh`
   - Added fail-closed guards:
     - `WINDOWED_SIGNOFF_REQUIRED=true` requires `EXECUTOR_ROLLOUT_RUN_REHEARSAL=true`
     - `ROUTE_FEE_SIGNOFF_REQUIRED=true` requires `EXECUTOR_ROLLOUT_RUN_REHEARSAL=true`

2. `tools/ops_scripts_smoke_test.sh`
   - Extended `run_executor_rollout_evidence_case` with two explicit negative guards:
     - `WINDOWED_SIGNOFF_REQUIRED=true + EXECUTOR_ROLLOUT_RUN_REHEARSAL=false`
     - `ROUTE_FEE_SIGNOFF_REQUIRED=true + EXECUTOR_ROLLOUT_RUN_REHEARSAL=false`
   - Extended `run_adapter_rollout_evidence_case` with three explicit negative guards:
     - `REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED=true + ADAPTER_ROLLOUT_RUN_REHEARSAL=false`
     - `WINDOWED_SIGNOFF_REQUIRED=true + ADAPTER_ROLLOUT_RUN_REHEARSAL=false`
     - `ROUTE_FEE_SIGNOFF_REQUIRED=true + ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF=false`

3. `ROAD_TO_PRODUCTION.md`
   - Added entry `426` documenting this closure.

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh`
2. `OPS_SMOKE_TARGET_CASES="executor_rollout_evidence" bash tools/ops_scripts_smoke_test.sh`
3. `OPS_SMOKE_TARGET_CASES="adapter_rollout_evidence" bash tools/ops_scripts_smoke_test.sh`
4. `cargo check -p copybot-executor -q`

## Ordering residual

- `N=0` unchanged (no executor ordering-chain logic changes).
