# Executor Phase 2B — Slice 240-245

## Summary

This batch closes bundle finalization/order drift for six non-final evidence helpers by applying the same two-pass pattern used in final wrappers.

1. `tools/execution_go_nogo_report.sh`
2. `tools/execution_devnet_rehearsal.sh`
3. `tools/execution_windowed_signoff_report.sh`
4. `tools/execution_route_fee_signoff_report.sh`
5. `tools/executor_rollout_evidence_report.sh`
6. `tools/adapter_rollout_evidence_report.sh`

Changes applied across all six:

- Introduced `run_package_bundle_once()` helper in artifact block.
- Switched to two-pass flow:
  - pass 1: resolve package status fields,
  - finalize summary + manifest,
  - pass 2: package finalized payload.
- Summary artifacts now persist only non-recursive package status fields:
  - `package_bundle_artifacts_written`
  - `package_bundle_exit_code`
  - `package_bundle_error`
- Removed recursive bundle-artifact hash fields from manifests.

Smoke coverage expansion:

- `tools/ops_scripts_smoke_test.sh` bundle-enabled branches for all six helpers now call
  `assert_bundled_summary_manifest_package_status_parity`.
- Parity helper now validates bundled manifest by checking presence of stdout `summary_sha256` value (works for both key-value and list-style manifests).

ROAD sync:

- Added items `401` and `402` in `ROAD_TO_PRODUCTION.md`.

## Files Changed

- `tools/execution_go_nogo_report.sh`
- `tools/execution_devnet_rehearsal.sh`
- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_go_nogo_report.sh tools/execution_devnet_rehearsal.sh tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke (sourced helpers):
   - `run_go_nogo_artifact_export_case` — PASS
   - `run_windowed_signoff_report_case` — PASS
   - `run_execution_route_fee_signoff_case` — PASS
   - `run_devnet_rehearsal_case` — PASS
   - `run_executor_rollout_evidence_case` — PASS
   - `run_adapter_rollout_evidence_case` — PASS

## Notes

- No Rust runtime behavior changed in this batch; scope is ops helper artifact consistency + smoke coverage.
