# Executor Phase-2B Slice 283-287 — Signoff/Rollout Nested `artifacts_written=true` Contract Completion

Date: 2026-03-02  
Owner: execution-dev  
Scope: signoff/rehearsal/rollout evidence chain

## Change Summary

1. Extended nested artifact-status contract from "strict bool parse" to "`artifacts_written` must be `true`" for signoff/rehearsal/rollout evidence chain when nested output directories are enabled.
2. Preserved non-output mode semantics by gating `must be true` checks on presence of nested output dir paths.
3. Added route-fee signoff per-window field `window_<N>h_go_nogo_artifacts_written`.
4. Expanded smoke assertions to pin new/strengthened fields on direct and bundle paths.

## Files Updated

1. `tools/execution_route_fee_signoff_report.sh`
2. `tools/execution_windowed_signoff_report.sh`
3. `tools/execution_devnet_rehearsal.sh`
4. `tools/executor_rollout_evidence_report.sh`
5. `tools/adapter_rollout_evidence_report.sh`
6. `tools/ops_scripts_smoke_test.sh`
7. `ROAD_TO_PRODUCTION.md`

## Validation (targeted)

1. `bash -n tools/execution_route_fee_signoff_report.sh tools/execution_windowed_signoff_report.sh tools/execution_devnet_rehearsal.sh tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke harness:
   1. `run_execution_route_fee_signoff_case` — PASS
   2. `run_windowed_signoff_report_case` — PASS
   3. `run_devnet_rehearsal_case` — PASS
   4. `run_executor_rollout_evidence_case` — PASS
   5. `run_adapter_rollout_evidence_case` — PASS

## Result

Nested evidence production contract is now consistent across final/orchestrator/signoff/rehearsal/rollout layers: malformed nested booleans fail closed everywhere, and output-enabled nested runs require actual artifact production (`artifacts_written=true`) rather than only boolean shape validity.
