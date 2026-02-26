# Executor Phase 2B Slice 129 — Route/Fee Signoff Strict Bool + Final Wrapper Short-Circuit (2026-02-26)

## Scope

- harden route/fee signoff helper against fail-open boolean env normalization
- short-circuit final route/fee evidence wrapper on invalid top-level bool input before nested signoff helper invocation

## Changes

1. Updated `tools/execution_route_fee_signoff_report.sh`:
   - added strict boolean parser (`true/false/1/0/yes/no/on/off`) for:
     - `GO_NOGO_REQUIRE_JITO_RPC_POLICY`
     - `GO_NOGO_REQUIRE_FASTLANE_DISABLED`
     - `GO_NOGO_TEST_MODE`
   - invalid tokens now produce explicit `input_error` entries and fail-closed `signoff_verdict=NO_GO`
   - helper loop remains skipped when `input_errors>0`, so no nested heavy checks run for invalid input
2. Updated `tools/execution_route_fee_final_evidence_report.sh`:
   - added strict boolean parser for:
     - `GO_NOGO_REQUIRE_JITO_RPC_POLICY`
     - `GO_NOGO_REQUIRE_FASTLANE_DISABLED`
     - `GO_NOGO_TEST_MODE`
   - added top-level input short-circuit: on invalid bool token, nested `execution_route_fee_signoff_report.sh` is not invoked
   - final package classifies as fail-closed `input_error` with `signoff_artifacts_written=false`
3. Updated `tools/ops_scripts_smoke_test.sh`:
   - added invalid-token guard for signoff helper (`GO_NOGO_REQUIRE_JITO_RPC_POLICY=maybe`)
   - added invalid-token guard for final wrapper (`GO_NOGO_REQUIRE_FASTLANE_DISABLED=sometimes`)
   - assertions include `input_error` classification and `signoff_artifacts_written=false` for final wrapper
4. Updated ROAD ledger item `289`.

## Files

- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_route_fee_final_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `GO_NOGO_REQUIRE_JITO_RPC_POLICY=maybe CONFIG_PATH=configs/paper.toml SERVICE=copybot-smoke-service bash tools/execution_route_fee_signoff_report.sh 24 60` — PASS (`exit 3`, `signoff_verdict=NO_GO`, explicit bool-token input error)
3. `GO_NOGO_REQUIRE_FASTLANE_DISABLED=sometimes CONFIG_PATH=configs/paper.toml SERVICE=copybot-smoke-service bash tools/execution_route_fee_final_evidence_report.sh 24 60` — PASS (`exit 3`, `signoff_reason_code=input_error`, `final_route_fee_package_reason_code=input_error`, `signoff_artifacts_written=false`)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` intentionally not executed in this slice to avoid long-running smoke loops during active iteration; only targeted invalid-token branches were validated directly.
