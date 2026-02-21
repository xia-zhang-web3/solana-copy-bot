# Refactor Phase -1 Evidence (2026-02-21)

Commit SHA:
- `6138e5062323287159e60e2e13e3fa2cab18fd6b`

Branch:
- `refactor/phase-1-adapter`

## Objective
Validate mandatory pre-phase tooling and deterministic phase-gate behavior before baseline freeze.

## Executed Commands
1. `bash -n tools/refactor_loc_report.sh`
2. `bash -n tools/refactor_baseline_prepare.sh`
3. `bash -n tools/refactor_normalize_output.sh`
4. `bash -n tools/refactor_perf_report.sh`
5. `bash -n tools/refactor_phase_gate.sh`
6. `tools/refactor_phase_gate.sh baseline --output-dir tmp/refactor-baseline-baseline0/run1`
7. `tools/refactor_phase_gate.sh baseline --output-dir tmp/refactor-baseline-baseline0/run2`
8. `cmp -s tmp/refactor-baseline-baseline0/run1/orchestrators.normalized.hashes tmp/refactor-baseline-baseline0/run2/orchestrators.normalized.hashes`
9. `tools/refactor_perf_report.sh --runs 3 --output-dir tmp/refactor-baseline-baseline0/perf -- tools/ops_scripts_smoke_test.sh`

## Results
- Syntax checks: PASS.
- Phase-gate run1/run2 normalized identity: PASS.
- Perf report produced: `tmp/refactor-baseline-baseline0/perf/perf_report.json`.

## Deterministic Hashes (run1 == run2)
- `6133973837c5e91e35cddc24d994f406dae34e1c1e34e3dff97763553cf06272`
- `c5d18ce05c6709bc4ea7bb9a3be9d6616170fcfb23e205f19a521bc88f79dacb`
- `bea42cd827cb60f0d5832d0f18386a96f189749bb43101c2658dd527d3d6890a`

## Notes
- Local `shellcheck` is unavailable.
- Local Docker daemon is unavailable for shellcheck container fallback.
- Script-gate determinism is still validated via `refactor_phase_gate` and normalized checksum identity.
