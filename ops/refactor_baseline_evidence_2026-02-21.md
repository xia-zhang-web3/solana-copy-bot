# Refactor Baseline Evidence (2026-02-21)

Commit SHA:
- `6138e5062323287159e60e2e13e3fa2cab18fd6b`

Short SHA:
- `6138e50`

Branch:
- `refactor/phase-1-adapter`

## Scope
Phase `0` baseline freeze for refactor program (`ops/refactor_master_plan_2026-02-21.md`, rev-5).

## Mandatory Baseline Commands Executed
1. `tools/refactor_baseline_prepare.sh tmp/refactor-baseline-baseline0`
2. `cargo test --workspace -q`
3. `tools/ops_scripts_smoke_test.sh`
4. `bash -n tools/refactor_loc_report.sh`
5. `bash -n tools/refactor_baseline_prepare.sh`
6. `bash -n tools/refactor_normalize_output.sh`
7. `bash -n tools/refactor_perf_report.sh`
8. `bash -n tools/refactor_phase_gate.sh`
9. `bash -n tools/adapter_secret_rotation_report.sh`
10. `bash -n tools/execution_devnet_rehearsal.sh`
11. `bash -n tools/adapter_rollout_evidence_report.sh`
12. `bash -n tools/execution_go_nogo_report.sh`
13. `tools/refactor_phase_gate.sh baseline --output-dir tmp/refactor-baseline-baseline0/run1`
14. `tools/refactor_phase_gate.sh baseline --output-dir tmp/refactor-baseline-baseline0/run2`
15. `cmp -s tmp/refactor-baseline-baseline0/run1/orchestrators.normalized.hashes tmp/refactor-baseline-baseline0/run2/orchestrators.normalized.hashes`
16. `tools/refactor_perf_report.sh --runs 3 --output-dir tmp/refactor-baseline-baseline0/perf -- tools/ops_scripts_smoke_test.sh`

## Baseline Command Results
- `cargo test --workspace -q`: PASS.
- `tools/ops_scripts_smoke_test.sh`: PASS.
- All listed `bash -n` syntax checks: PASS.
- Phase-gate run1/run2 normalized checksum identity: PASS.
- Perf report generated (3 runs, median): PASS.

## Two-Run Normalized Identity
`run1` and `run2` `orchestrators.normalized.hashes` are identical:
- `6133973837c5e91e35cddc24d994f406dae34e1c1e34e3dff97763553cf06272`
- `c5d18ce05c6709bc4ea7bb9a3be9d6616170fcfb23e205f19a521bc88f79dacb`
- `bea42cd827cb60f0d5832d0f18386a96f189749bb43101c2658dd527d3d6890a`

## Performance Baseline
Artifact: `ops/evidence/refactor/baseline/6138e50/perf_report.json`
- command: `tools/ops_scripts_smoke_test.sh`
- run_1_ms: `20950.986959040165`
- run_2_ms: `19905.269833048806`
- run_3_ms: `19578.59899988398`
- median_ms: `19905.269833048806`

## Committed Baseline Artifacts
Directory:
- `ops/evidence/refactor/baseline/6138e50/`

Included files:
1. `ops/evidence/refactor/baseline/6138e50/orchestrators.normalized.hashes`
2. `ops/evidence/refactor/baseline/6138e50/orchestrators.normalized.run2.hashes`
3. `ops/evidence/refactor/baseline/6138e50/orchestrators.normalized.sha256`
4. `ops/evidence/refactor/baseline/6138e50/orchestrators.raw.sha256`
5. `ops/evidence/refactor/baseline/6138e50/go_nogo_normalized.txt`
6. `ops/evidence/refactor/baseline/6138e50/rehearsal_normalized.txt`
7. `ops/evidence/refactor/baseline/6138e50/rollout_normalized.txt`
8. `ops/evidence/refactor/baseline/6138e50/perf_report.json`

## Raw Artifact Storage
Raw captures are available in local evidence workspace:
- `tmp/refactor-baseline-baseline0/run1/raw/`
- `tmp/refactor-baseline-baseline0/run2/raw/`

## Notes
- Local `shellcheck` is unavailable.
- Local Docker daemon is unavailable for shellcheck container fallback.
- This baseline evidence therefore relies on deterministic phase-gate outputs and checksum identity.
