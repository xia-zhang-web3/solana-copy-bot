# Executor Phase 2B — Slice 233

## Summary

This slice closes server-rollout bundle ordering drift and strengthens bundle-content regression guards.

1. `tools/execution_server_rollout_report.sh`
   - Moved `evidence_bundle_pack.sh` invocation to run **after** top-level summary and manifest are fully materialized.
   - Result: generated bundle now captures `execution_server_rollout_manifest_*` and summary snapshot with package-bundle block already present.
2. `tools/ops_scripts_smoke_test.sh`
   - Extended `run_execution_server_rollout_report_case` bundle branch:
     - verify tar contains summary and manifest entries,
     - extract bundled summary/manifest text and assert expected fields (`package_bundle_*`, `summary_sha256`, `preflight_capture_sha256`).
3. `ROAD_TO_PRODUCTION.md`
   - Added entries 393–394 documenting order fix and smoke coverage hardening.

## Files Changed

- `tools/execution_server_rollout_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted smoke function run (`run_execution_server_rollout_report_case`) with local fixture sourcing — PASS
3. `cargo check -p copybot-executor -q` — PASS

## Notes

- Full long `tools/ops_scripts_smoke_test.sh` intentionally not executed in this slice.
- This is an ops/runtime packaging hardening slice; no Rust runtime logic changed.
