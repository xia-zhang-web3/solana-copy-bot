# Refactor Evidence: Phase 7 Slice 2 (`tools.lib.verdict_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move duplicated verdict normalization and fallback-selection helpers into shared shell library.

Added to `tools/lib/common.sh`:
1. `first_non_empty`
2. `normalize_gate_verdict`
3. `normalize_preflight_verdict`
4. `normalize_go_nogo_verdict`
5. `normalize_rotation_verdict`
6. `normalize_rehearsal_verdict`

Rewired scripts:
1. `tools/execution_go_nogo_report.sh` (removed local `first_non_empty`, `normalize_gate_verdict`, `normalize_preflight_verdict`)
2. `tools/execution_devnet_rehearsal.sh` (removed local `normalize_go_nogo_verdict`)
3. `tools/adapter_rollout_evidence_report.sh` (removed local `normalize_rotation_verdict`, `normalize_rehearsal_verdict`)

## Files Changed
1. `tools/lib/common.sh`
2. `tools/execution_go_nogo_report.sh`
3. `tools/execution_devnet_rehearsal.sh`
4. `tools/adapter_rollout_evidence_report.sh`
5. `ops/refactor_phase7_slice2_verdict_normalization_helpers_evidence_2026-02-21.md`

## Validation Commands
1. `bash -n tools/adapter_secret_rotation_report.sh`
2. `bash -n tools/execution_devnet_rehearsal.sh`
3. `bash -n tools/adapter_rollout_evidence_report.sh`
4. `bash -n tools/execution_go_nogo_report.sh`
5. `bash -n tools/refactor_loc_report.sh`
6. `bash -n tools/refactor_baseline_prepare.sh`
7. `bash -n tools/refactor_normalize_output.sh`
8. `bash -n tools/refactor_perf_report.sh`
9. `bash -n tools/refactor_phase_gate.sh`
10. `tools/ops_scripts_smoke_test.sh`
11. `cargo test --workspace -q`

## Results
- All commands above: PASS.

## Notes
- `shellcheck` remains unavailable on this machine; contract/syntax parity is validated through `bash -n` and smoke regression.
- This slice is move-only/helper consolidation, with no output-schema changes.
