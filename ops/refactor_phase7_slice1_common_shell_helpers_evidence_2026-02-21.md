# Refactor Evidence: Phase 7 Slice 1 (`tools.lib.common_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Extract duplicated shell helper functions into shared library `tools/lib/common.sh` and wire targeted ops scripts to source it.

Extracted helpers:
1. `trim_string`
2. `normalize_bool_token`
3. `extract_field`

Scripts rewired to use shared helpers:
1. `tools/execution_go_nogo_report.sh`
2. `tools/execution_devnet_rehearsal.sh`
3. `tools/adapter_rollout_evidence_report.sh`
4. `tools/adapter_secret_rotation_report.sh`

## Files Changed
1. `tools/lib/common.sh` (new)
2. `tools/execution_go_nogo_report.sh`
3. `tools/execution_devnet_rehearsal.sh`
4. `tools/adapter_rollout_evidence_report.sh`
5. `tools/adapter_secret_rotation_report.sh`
6. `ops/refactor_phase7_slice1_common_shell_helpers_evidence_2026-02-21.md`

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
- `shellcheck` is not available on this local machine (`shellcheck:no`); syntax and behavior parity were validated via `bash -n` and smoke regression pack.
- Script interfaces and output contracts were preserved (move-only helper extraction).
