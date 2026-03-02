# Executor Phase 2B — Slices 261-264

## Summary

This batch upgrades nested bundle isolation from "env forcing only" to explicit fail-closed contract checks across signoff/rehearsal helpers.

1. `tools/execution_windowed_signoff_report.sh`
   - For each window, now strictly validates nested go/no-go output field:
     - `package_bundle_enabled` must be a valid bool token and equal `false`.
   - Adds per-window summary field:
     - `window_<N>h_go_nogo_nested_package_bundle_enabled`.
   - On malformed/non-false nested value, appends input error and fails closed.

2. `tools/execution_route_fee_signoff_report.sh`
   - For each window, now strictly validates nested go/no-go output field:
     - `package_bundle_enabled` must be a valid bool token and equal `false`.
   - Adds per-window summary field:
     - `window_<N>h_go_nogo_nested_package_bundle_enabled`.
   - On malformed/non-false nested value, appends input error and fails closed.

3. `tools/execution_devnet_rehearsal.sh`
   - Strictly validates nested helper `package_bundle_enabled` from:
     - `execution_go_nogo_report.sh`
     - `execution_windowed_signoff_report.sh`
     - `execution_route_fee_signoff_report.sh`
   - Adds stage-level summary fields:
     - `go_nogo_nested_package_bundle_enabled`
     - `windowed_signoff_nested_package_bundle_enabled`
     - `route_fee_signoff_nested_package_bundle_enabled`
   - On malformed/non-false nested values, records config error and fails closed.

4. `tools/ops_scripts_smoke_test.sh`
   - Extended targeted cases to assert new explicit summary fields:
     - `run_windowed_signoff_report_case`
     - `run_execution_route_fee_signoff_case`
     - `run_devnet_rehearsal_case`
   - Assertions added for both direct outputs and bundle-enabled outputs where applicable.

5. `ROAD_TO_PRODUCTION.md`
   - Added item `409` for this fail-closed nested contract batch.

## Files Changed

- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_devnet_rehearsal.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_devnet_rehearsal.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke via sourced harness:
   - `run_windowed_signoff_report_case` — PASS
   - `run_execution_route_fee_signoff_case` — PASS
   - `run_devnet_rehearsal_case` — PASS

## Notes

- Scope is orchestration/ops helper hardening only; no Rust runtime code changes.
