# Slice 534 — Followlist Activity Strict Parity Through Rollout/Readiness/Refactor Chains

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e hardening (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `tools/execution_windowed_signoff_report.sh`
2. `tools/execution_route_fee_signoff_report.sh`
3. `tools/execution_route_fee_final_evidence_report.sh`
4. `tools/execution_devnet_rehearsal.sh`
5. `tools/executor_rollout_evidence_report.sh`
6. `tools/executor_final_evidence_report.sh`
7. `tools/adapter_rollout_evidence_report.sh`
8. `tools/adapter_rollout_final_evidence_report.sh`
9. `tools/execution_server_rollout_report.sh`
10. `tools/execution_runtime_readiness_report.sh`
11. `tools/refactor_phase_gate.sh`
12. `tools/ops_scripts_smoke_test.sh`
13. `ROAD_TO_PRODUCTION.md`

## Changes

1. Propagated `GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY` through signoff/rehearsal/rollout/final/readiness chains.
2. Added fail-closed extraction and parity checks for nested followlist fields:
   1. strict bool parity on `*_go_nogo_require_followlist_activity`,
   2. strict verdict parity (`required=true => non-SKIP`, `required=false => SKIP`),
   3. non-empty reason-code validation.
3. Extended top-level summary outputs so followlist strict observability is available on each orchestration level.
4. Extended smoke assertions for `execution_server_rollout`, `execution_runtime_readiness`, and `refactor_phase_gate`:
   1. pass/skip/profile/bundle/mock parity pins,
   2. strict fail-close followlist inactive path,
   3. strict pass followlist active path,
   4. invalid bool token reject pins for both go/no-go and refactor strict switches.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh tools/execution_server_rollout_report.sh tools/execution_runtime_readiness_report.sh tools/adapter_rollout_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh tools/execution_devnet_rehearsal.sh tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh` — PASS
5. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. This slice keeps the accelerated targeted smoke workflow introduced around `4cc86fc` while closing followlist strict parity drift across nested orchestration layers.
