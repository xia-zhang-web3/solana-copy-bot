# Executor Phase 2B — Slice 239

## Summary

This slice fixes a targeted smoke flake in `run_adapter_rollout_evidence_case`.

1. `tools/ops_scripts_smoke_test.sh`
   - `run_adapter_rollout_evidence_case` now calls `write_fake_journalctl` internally.
   - Restored canonical expectations:
     - `dynamic_cu_hint_api_total: 1`
     - `dynamic_cu_hint_rpc_total: 1`
2. `ROAD_TO_PRODUCTION.md`
   - Added item 400 for deterministic adapter-rollout smoke behavior.

## Files Changed

- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted run `run_adapter_rollout_evidence_case` (sourced smoke helpers) — PASS

## Notes

- No production runtime behavior changed; this is smoke-test determinism hardening.
