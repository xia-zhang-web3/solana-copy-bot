# Slice 470 — Executor backend-mode strict guard propagation across runtime helpers

Date: 2026-03-03
Commit scope: tooling hardening (runtime evidence/helper chain)

## Summary

Completed end-to-end propagation of strict executor backend-mode guard settings from top-level runtime evidence helpers down to nested go/no-go invocations.

Added strict parse + passthrough for:
- `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (`true|false|1|0|yes|no|on|off`)
- `EXECUTOR_ENV_PATH`

Across:
- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_devnet_rehearsal.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/adapter_rollout_final_evidence_report.sh`
- `tools/execution_route_fee_final_evidence_report.sh`
- `tools/execution_runtime_readiness_report.sh`

Also added summary observability fields in these helpers:
- `go_nogo_require_executor_upstream`
- `executor_env_path`

## Smoke coverage

Extended `run_execution_runtime_readiness_report_case` with strict propagation pins:

1. strict gate enabled + `COPYBOT_EXECUTOR_BACKEND_MODE=mock` via custom `EXECUTOR_ENV_PATH`:
   - expected `NO_GO` (`exit 3`)
   - asserts strict gate reason is surfaced (`strict executor upstream backend-mode gate not PASS`)

2. explicit override (`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=false`) with same mock env:
   - expected `GO`
   - verifies override path remains usable for controlled non-live contour testing

## Validation runs

1. `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_devnet_rehearsal.sh tools/adapter_rollout_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/execution_route_fee_final_evidence_report.sh tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="execution_runtime_readiness" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES="windowed_signoff" bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES="route_fee_signoff" bash tools/ops_scripts_smoke_test.sh` — PASS
5. `OPS_SMOKE_TARGET_CASES="devnet_rehearsal" bash tools/ops_scripts_smoke_test.sh` — PASS
6. `OPS_SMOKE_TARGET_CASES="adapter_rollout_evidence" bash tools/ops_scripts_smoke_test.sh` — PASS
7. `cargo check -p copybot-executor -q` — PASS

## Notes

Defaults for the new strict gate in these intermediate helpers remain `false` for backward compatibility; fail-closed behavior is activated explicitly by setting `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true`.
