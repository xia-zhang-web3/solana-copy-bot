# Executor Phase 2B — Slice 201

Date: 2026-02-28

## Scope

Completed optional evidence bundle parity for remaining route-fee/windowed helper chain:

- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_route_fee_final_evidence_report.sh`

## Changes

1. Added optional auto-bundle controls and output fields:
   - `PACKAGE_BUNDLE_ENABLED`
   - `PACKAGE_BUNDLE_LABEL`
   - `PACKAGE_BUNDLE_OUTPUT_DIR`
   - emitted `package_bundle_*` machine-readable fields
2. Added fail-closed behavior when bundling is requested but not written.
3. Added strict input guard for scripts with optional artifacts root:
   - `PACKAGE_BUNDLE_ENABLED=true` now requires `OUTPUT_DIR` for:
     - `execution_windowed_signoff_report.sh`
     - `execution_route_fee_signoff_report.sh`
4. Added manifest parity fields for bundle artifacts:
   - `package_bundle_path_sha256`
   - `package_bundle_sha256_path_sha256`
   - `package_bundle_contents_manifest_sha256`
5. Smoke coverage expanded in `tools/ops_scripts_smoke_test.sh`:
   - bundle-enabled positive guards for all three helpers
   - missing-`OUTPUT_DIR` negative guards for windowed + route-fee signoff
   - summary/manifest SHA parity checks for new bundle branches

## Verification

- `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
- `cargo check -p copybot-executor -q` — PASS
- Targeted windowed signoff bundle + missing output-dir guards — PASS
- Targeted route-fee signoff bundle + missing output-dir guards — PASS
- Targeted route-fee final bundle guard — PASS

Note: full `tools/ops_scripts_smoke_test.sh` run in this environment still exits early on pre-existing macOS path alias expectation (`/var` vs `/private/var`) in an unrelated evidence-bundle assertion block.
