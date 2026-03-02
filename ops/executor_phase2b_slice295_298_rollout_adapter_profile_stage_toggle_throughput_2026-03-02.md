# Executor Phase 2B — Slices 295-298

Date: 2026-03-02
Commit scope: rollout evidence throughput controls (executor + adapter) + smoke guard expansion

## Goal

Add strict, fail-closed profile presets and stage toggles to rollout evidence helpers so targeted loops can skip non-required stages without changing default full behavior.

## Changes

1. `tools/executor_rollout_evidence_report.sh`
   - Added `EXECUTOR_ROLLOUT_PROFILE={full|precheck_only|rehearsal_only}`.
   - Added strict overrides:
     - `EXECUTOR_ROLLOUT_RUN_ROTATION`
     - `EXECUTOR_ROLLOUT_RUN_PREFLIGHT`
     - `EXECUTOR_ROLLOUT_RUN_REHEARSAL`
   - Added fail-closed guard: at least one stage must be enabled.
   - Disabled stages now emit deterministic `SKIP` / `stage_disabled` outputs.
   - Verdict chain now evaluates only enabled stages.

2. `tools/adapter_rollout_evidence_report.sh`
   - Added `ADAPTER_ROLLOUT_PROFILE={full|rehearsal_only|route_fee_only}`.
   - Added strict overrides:
     - `ADAPTER_ROLLOUT_RUN_ROTATION`
     - `ADAPTER_ROLLOUT_RUN_REHEARSAL`
     - `ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF`
   - Added fail-closed guards:
     - at least one stage enabled,
     - required-stage constraints (`ROUTE_FEE_SIGNOFF_REQUIRED`, `REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED`, `WINDOWED_SIGNOFF_REQUIRED`) cannot target disabled owning stage.
   - Disabled stages emit deterministic `SKIP` / `stage_disabled` outputs.
   - Verdict chain now evaluates only enabled stages.

3. `tools/ops_scripts_smoke_test.sh`
   - Extended `run_executor_rollout_evidence_case` with:
     - profile assertions for `precheck_only` and `rehearsal_only`,
     - invalid-profile reject guard,
     - all-stages-disabled reject guard.
   - Extended `run_adapter_rollout_evidence_case` with:
     - profile assertions for `rehearsal_only` and `route_fee_only`,
     - invalid-profile reject guard,
     - all-stages-disabled reject guard.

4. `ROAD_TO_PRODUCTION.md`
   - Added entries `424` and `425`.

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh`
2. `OPS_SMOKE_TARGET_CASES="executor_rollout_evidence" bash tools/ops_scripts_smoke_test.sh`
3. `OPS_SMOKE_TARGET_CASES="adapter_rollout_evidence" bash tools/ops_scripts_smoke_test.sh`
4. `cargo check -p copybot-executor -q`

## Ordering residual

- `N=0` remains unchanged (no executor ordering-chain logic changes in this batch).
