# Executor Phase 2B — Slices 271-274

## Summary

This batch closes residual fail-open parsing for nested `artifacts_written` fields across rollout/runtime/final wrapper chains.

1. `tools/executor_rollout_evidence_report.sh`
   - Replaced permissive `normalize_bool_token(extract_field("artifacts_written"))` with strict parsing:
     - `extract_bool_field_strict("artifacts_written", ...)`
   - Hardened fields:
     - `rotation_artifacts_written`
     - `preflight_artifacts_written`
     - `rehearsal_artifacts_written`
   - Malformed/missing nested value now appends `input_errors` and fails closed (`executor_rollout_verdict=NO_GO`, `reason_code=input_error`).

2. `tools/adapter_rollout_evidence_report.sh`
   - Strict parsing added for nested artifact-status fields:
     - `rotation_artifacts_written`
     - `go_nogo_artifacts_written`
     - `rehearsal_artifacts_written`
     - `windowed_signoff_artifacts_written`
     - `rehearsal_route_fee_signoff_artifacts_written`
     - `route_fee_signoff_artifacts_written`
   - Any malformed/missing boolean token now produces explicit `input_error` and fail-closed verdict.

3. `tools/execution_runtime_readiness_report.sh`
   - Strict parsing added for final-helper artifact status:
     - `adapter_artifacts_written`
     - `route_fee_artifacts_written`
   - Malformed/missing nested value now contributes to `input_errors` and yields `runtime_readiness_verdict=NO_GO`.

4. Final wrappers strict nested artifact-status parsing:
   - `tools/executor_final_evidence_report.sh` (`rollout_artifacts_written`)
   - `tools/adapter_rollout_final_evidence_report.sh` (`rollout_artifacts_written`)
   - `tools/execution_route_fee_final_evidence_report.sh` (`signoff_artifacts_written`)
   - Malformed/missing nested value now enters `input_errors` and forces wrapper-level fail-closed verdict.

5. `tools/ops_scripts_smoke_test.sh`
   - Tightened affected checks from substring matching to strict field equality (`assert_field_equals`) for:
     - `signoff_artifacts_written`
     - `rollout_artifacts_written`
     - `rotation_artifacts_written`
     - `route_fee_signoff_artifacts_written`
     - `go_nogo_artifacts_written`
     - `windowed_signoff_artifacts_written`
     - `rehearsal_route_fee_signoff_artifacts_written`
     - `rehearsal_artifacts_written`

6. `ROAD_TO_PRODUCTION.md`
   - Added item `412` documenting nested `artifacts_written` fail-closed closure.

## Files Changed

- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/execution_runtime_readiness_report.sh`
- `tools/executor_final_evidence_report.sh`
- `tools/adapter_rollout_final_evidence_report.sh`
- `tools/execution_route_fee_final_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n` on all touched scripts — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke via sourced harness:
   - `run_execution_route_fee_signoff_case` — PASS
   - `run_executor_rollout_evidence_case` — PASS
   - `run_adapter_rollout_evidence_case` — PASS
   - `run_execution_runtime_readiness_report_case` — PASS

## Notes

- Scope is ops/orchestration hardening only; Rust runtime paths are unchanged.
- Behavior change is intentionally fail-closed for malformed nested artifact-status booleans.
