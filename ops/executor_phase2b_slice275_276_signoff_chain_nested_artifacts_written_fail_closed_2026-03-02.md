# Executor Phase 2B — Slices 275-276

## Summary

This batch closes the remaining fail-open nested `artifacts_written` parsing in the signoff chain.

1. `tools/execution_devnet_rehearsal.sh`
   - Replaced permissive parsing for nested artifact status with strict parsing:
     - go/no-go nested `artifacts_written`
     - windowed signoff nested `artifacts_written`
     - route/fee signoff nested `artifacts_written`
   - Added fail-closed behavior:
     - malformed/missing bool token appends `config_errors`
     - value is set to `unknown`
     - rehearsal verdict resolves to `NO_GO` via existing config-error gate

2. `tools/execution_windowed_signoff_report.sh`
   - Replaced permissive per-window nested go/no-go `artifacts_written` parsing with strict parsing.
   - Added fail-closed behavior:
     - malformed/missing bool token appends `input_errors`
     - value is set to `unknown`
     - signoff verdict resolves via existing input-error gate (`NO_GO`)

3. `tools/ops_scripts_smoke_test.sh`
   - Tightened one remaining weak artifacts-written assertion:
     - `window_24h_go_nogo_artifacts_written` moved from substring check to `assert_field_equals ... true`

4. `ROAD_TO_PRODUCTION.md`
   - Added item `413` documenting signoff-chain nested `artifacts_written` closure.

## Files Changed

- `tools/execution_devnet_rehearsal.sh`
- `tools/execution_windowed_signoff_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_devnet_rehearsal.sh tools/execution_windowed_signoff_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke via sourced harness:
   - `run_windowed_signoff_report_case` — PASS
   - `run_devnet_rehearsal_case` — PASS

## Notes

- Scope is ops/orchestration hardening only; no Rust runtime changes.
- Existing package-bundle parser paths remain intentionally unchanged in this slice.
