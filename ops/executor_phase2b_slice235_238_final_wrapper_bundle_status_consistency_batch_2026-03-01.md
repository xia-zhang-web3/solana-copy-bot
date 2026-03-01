# Executor Phase 2B — Slice 235-238

## Summary

This batch closes bundle-status drift class across all final evidence wrappers.

1. Updated final wrappers:
   - `tools/execution_route_fee_final_evidence_report.sh`
   - `tools/executor_final_evidence_report.sh`
   - `tools/adapter_rollout_final_evidence_report.sh`
2. Implemented two-pass package flow in each wrapper:
   - pass 1: resolve package status from `evidence_bundle_pack.sh`;
   - write summary + manifest using resolved status;
   - pass 2: package finalized summary/manifest payload.
3. Summary artifacts now keep only non-recursive package status fields:
   - `package_bundle_artifacts_written`
   - `package_bundle_exit_code`
   - `package_bundle_error`
4. Removed recursive bundle-hash fields from wrapper manifests (`package_bundle_*_sha256` over bundle artifacts), eliminating self-referential/unstable manifest payload.
5. Extended smoke coverage in `tools/ops_scripts_smoke_test.sh`:
   - added shared bundle-content parity helper,
   - route-fee final / executor final / adapter final bundle branches now assert bundled summary package-status parity with stdout.

## Files Changed

- `tools/execution_route_fee_final_evidence_report.sh`
- `tools/executor_final_evidence_report.sh`
- `tools/adapter_rollout_final_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_route_fee_final_evidence_report.sh tools/executor_final_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted smoke functions:
   - `run_execution_route_fee_signoff_case`
   - `run_executor_rollout_evidence_case`
   - `run_adapter_rollout_evidence_case`
   (invoked via sourced smoke helpers) — PASS
3. `cargo check -p copybot-executor -q` — PASS

## Notes

- Full long `tools/ops_scripts_smoke_test.sh` not run in this slice.
- No Rust runtime behavior changed.
