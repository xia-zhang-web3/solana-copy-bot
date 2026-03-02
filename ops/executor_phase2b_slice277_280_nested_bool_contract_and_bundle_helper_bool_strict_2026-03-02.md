# Executor Phase 2B Slice 277-280 — Nested Bool Contract + Bundle Helper Bool Strict (2026-03-02)

## Scope

Close remaining fail-open bool parsing on nested machine-readable fields in rollout/signoff/rehearsal chains and harden bundle helper output contract parsing.

## Changes

1. Nested policy/stability bool fail-closed parsing
   - `tools/execution_windowed_signoff_report.sh`
     - `dynamic_cu_policy_config_enabled`
     - `dynamic_tip_policy_config_enabled`
   - `tools/execution_devnet_rehearsal.sh`
     - `go_nogo_require_jito_rpc_policy`
     - `go_nogo_require_fastlane_disabled`
     - `submit_fastlane_enabled`
     - `windowed_signoff_require_dynamic_hint_source_pass`
     - `windowed_signoff_require_dynamic_tip_policy_pass`
     - `primary_route_stable`
     - `fallback_route_stable`
   - `tools/adapter_rollout_evidence_report.sh`
     - `go_nogo_require_jito_rpc_policy`
     - `go_nogo_require_fastlane_disabled`
     - `submit_fastlane_enabled`
     - `route_fee_signoff_required` (rehearsal nested)
     - `route_fee_primary_route_stable` / `route_fee_fallback_route_stable` (rehearsal nested)
     - `primary_route_stable` / `fallback_route_stable` (route-fee nested)

   Pattern:
   - read raw with `extract_field` + `trim_string`
   - parse with `extract_bool_field_strict`
   - on failure append explicit `input_error`/`config_error` and set field to `unknown`

2. Bundle helper output bool contract hardening
   - Updated all bundle-capable helpers:
     - `tools/execution_go_nogo_report.sh`
     - `tools/execution_devnet_rehearsal.sh`
     - `tools/execution_windowed_signoff_report.sh`
     - `tools/execution_route_fee_signoff_report.sh`
     - `tools/executor_rollout_evidence_report.sh`
     - `tools/adapter_rollout_evidence_report.sh`
     - `tools/executor_final_evidence_report.sh`
     - `tools/adapter_rollout_final_evidence_report.sh`
     - `tools/execution_route_fee_final_evidence_report.sh`
     - `tools/execution_server_rollout_report.sh`
     - `tools/execution_runtime_readiness_report.sh`

   `run_package_bundle_once()` now strictly parses nested `artifacts_written`:
   - success + valid bool -> preserve normal path
   - success + invalid/missing bool -> fail-closed contract error:
     - `package_bundle_exit_code=1`
     - `package_bundle_artifacts_written=false`
     - `package_bundle_error="bundle helper returned invalid artifacts_written token: ..."`
     - bundle path/hash fields reset to `n/a`

3. Smoke assertion hardening
   - `tools/ops_scripts_smoke_test.sh`
     - migrated affected boolean checks from `assert_contains` to `assert_field_equals` in:
       - `run_devnet_rehearsal_case`
       - `run_adapter_rollout_evidence_case`
   - Added shared helper source at top:
     - `source "$ROOT_DIR/tools/lib/common.sh"`
     - removes implicit dependency on caller shell state for `sha256_file_value`

4. ROAD sync
   - Added entries `414` and `415` in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `bash -n` changed scripts — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. Targeted smoke harness (sourced):
   - `run_windowed_signoff_report_case` — PASS
   - `run_devnet_rehearsal_case` — PASS

## Notes

- Full `tools/ops_scripts_smoke_test.sh` was not run in this slice (targeted harness only).
