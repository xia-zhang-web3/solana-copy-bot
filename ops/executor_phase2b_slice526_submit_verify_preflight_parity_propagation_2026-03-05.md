# Slice 526 — submit-verify preflight parity propagation through rollout/final/orchestrator

## Scope

1. `tools/executor_rollout_evidence_report.sh`
   - Extract + strict-parse preflight submit-verify fields:
     - `executor_submit_verify_strict`
     - `executor_submit_verify_configured`
     - `executor_submit_verify_fallback_configured`
   - Fail-closed invariants:
     - `strict=true => configured=true`
     - `fallback_configured=true => configured=true`
   - Summary export:
     - `preflight_executor_submit_verify_*`

2. `tools/executor_final_evidence_report.sh`
   - Extract nested rollout preflight submit-verify fields.
   - Fail-closed parsing + invariants.
   - Summary export:
     - `rollout_nested_preflight_executor_submit_verify_*`

3. `tools/execution_server_rollout_report.sh`
   - Extract nested executor-final rollout preflight submit-verify fields.
   - Fail-closed parsing + invariants.
   - Summary export:
     - `executor_final_rollout_nested_preflight_executor_submit_verify_*`

4. `tools/ops_scripts_smoke_test.sh`
   - Added assertions for new fields in:
     - `executor_rollout_evidence` (pass/bundle/precheck/rehearsal_only/final/final-bundle)
     - `execution_server_rollout` (pass/skip_direct/profile_skip/bundle/mock_backend_allowed)

5. `ROAD_TO_PRODUCTION.md`
   - Added item `526`.

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="executor_rollout_evidence_fast,execution_server_rollout_fast" bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

- This slice is fail-closed parity hardening + observability propagation only.
- No runtime Rust behavior changed.
