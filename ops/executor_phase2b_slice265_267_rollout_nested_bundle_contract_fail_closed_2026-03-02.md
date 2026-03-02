# Executor Phase 2B — Slices 265-267

## Summary

This batch extends fail-closed nested bundle contract enforcement to rollout evidence helpers.

1. `tools/executor_rollout_evidence_report.sh`
   - Added strict validation for nested rehearsal output field:
     - `package_bundle_enabled` must be bool and equal `false`.
   - On malformed/non-false nested value, appends `input_errors` and fails closed.
   - Added summary field:
     - `rehearsal_nested_package_bundle_enabled`

2. `tools/adapter_rollout_evidence_report.sh`
   - Added strict validation for nested outputs:
     - rehearsal `package_bundle_enabled`
     - route/fee signoff `package_bundle_enabled`
   - On malformed/non-false nested values, appends `input_errors` and fails closed.
   - Added summary fields:
     - `rehearsal_nested_package_bundle_enabled`
     - `route_fee_signoff_nested_package_bundle_enabled`

3. `tools/ops_scripts_smoke_test.sh`
   - Extended assertions for:
     - `run_executor_rollout_evidence_case`
     - `run_adapter_rollout_evidence_case`
   - Both direct and bundle branches now assert nested contract fields are `false`.

4. `ROAD_TO_PRODUCTION.md`
   - Added item `410` for rollout evidence nested contract closure.

## Files Changed

- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke via sourced harness:
   - `run_executor_rollout_evidence_case` — PASS
   - `run_adapter_rollout_evidence_case` — PASS

## Notes

- Scope is ops helper/orchestration hardening only; no Rust runtime changes.
