# Executor Phase 2B — Slice 229-232

## Summary

This batch adds a single orchestration helper for server rollout evidence flow and pins its fail-closed behavior.

1. Added `tools/execution_server_rollout_report.sh`:
   - Runs full chain in order:
     - `tools/execution_adapter_preflight.sh`
     - `tools/execution_fee_calibration_report.sh`
     - `tools/execution_go_nogo_report.sh`
     - `tools/execution_devnet_rehearsal.sh`
     - `tools/executor_final_evidence_report.sh`
     - `tools/adapter_rollout_final_evidence_report.sh`
   - Captures each stage output and exit code.
   - Emits machine-readable stage verdict/reason fields and consolidated `server_rollout_verdict`.
2. Added deterministic artifact model:
   - stage captures under `OUTPUT_ROOT/steps/*_capture_<ts>.txt`
   - top-level summary and manifest with checksum fields (`*_capture_sha256`, `summary_sha256`, `manifest_sha256`)
3. Added fail-closed startup validation and strict bool parsing for orchestration gates.
4. Added optional top-level package bundling support via `tools/evidence_bundle_pack.sh`.
5. Added smoke coverage in `tools/ops_scripts_smoke_test.sh`:
   - deterministic HOLD path on smoke fixtures (calibration WARN with downstream stage GO)
   - bundle-enabled path
   - invalid-bool fail-closed path

## Files Changed

- `tools/execution_server_rollout_report.sh` (new)
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `GO_NOGO_REQUIRE_JITO_RPC_POLICY=maybe EXECUTOR_ENV_PATH=/tmp/missing-executor.env ADAPTER_ENV_PATH=/tmp/missing-adapter.env CONFIG_PATH=/tmp/missing.toml bash tools/execution_server_rollout_report.sh 24 60` — PASS (`exit 3`, `server_rollout_reason_code=input_error`)
3. `cargo check -p copybot-executor -q` — PASS

## Notes

- Full long `tools/ops_scripts_smoke_test.sh` was intentionally not executed in this cycle.
- This slice targets runtime rollout operability and evidence-chain consolidation, not Rust runtime logic.
