# Executor Phase 2B — Slices 253-257

## Summary

This batch closes nested package bundle fan-out for rollout/signoff/rehearsal orchestrators.

1. `tools/execution_windowed_signoff_report.sh`
   - Nested `execution_go_nogo_report.sh` call now sets `PACKAGE_BUNDLE_ENABLED="false"`.

2. `tools/execution_route_fee_signoff_report.sh`
   - Nested `execution_go_nogo_report.sh` call now sets `PACKAGE_BUNDLE_ENABLED="false"`.

3. `tools/execution_devnet_rehearsal.sh`
   - Nested calls now set `PACKAGE_BUNDLE_ENABLED="false"`:
     - `execution_go_nogo_report.sh`
     - `execution_windowed_signoff_report.sh`
     - `execution_route_fee_signoff_report.sh`

4. `tools/executor_rollout_evidence_report.sh`
   - Nested `execution_devnet_rehearsal.sh` call now sets `PACKAGE_BUNDLE_ENABLED="false"`.

5. `tools/adapter_rollout_evidence_report.sh`
   - Nested calls now set `PACKAGE_BUNDLE_ENABLED="false"`:
     - `execution_devnet_rehearsal.sh`
     - `execution_route_fee_signoff_report.sh`

6. `tools/ops_scripts_smoke_test.sh`
   - Extended bundle branches to assert nested captures include `package_bundle_enabled: false` for:
     - windowed signoff (`window_24h_capture_path`)
     - route/fee signoff (`window_24h_go_nogo_capture_path`)
     - devnet rehearsal (`artifact_go_nogo_nested_capture`, `artifact_windowed_signoff_nested_capture`, `artifact_route_fee_signoff_nested_capture`)
     - executor rollout (`artifact_rehearsal_capture`)
     - adapter rollout (`artifact_rehearsal_capture`, `artifact_route_fee_signoff_capture`)

7. `ROAD_TO_PRODUCTION.md`
   - Added item `407` for this isolation closure.

## Files Changed

- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_devnet_rehearsal.sh`
- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n` on all touched scripts — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke via sourced harness:
   - `run_windowed_signoff_report_case` — PASS
   - `run_execution_route_fee_signoff_case` — PASS
   - `run_devnet_rehearsal_case` — PASS
   - `run_executor_rollout_evidence_case` — PASS
   - `run_adapter_rollout_evidence_case` — PASS

## Notes

- Scope is orchestration/package behavior only; no Rust runtime changes.
