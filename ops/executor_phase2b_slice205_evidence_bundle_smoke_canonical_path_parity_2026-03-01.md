# Executor Phase 2B — Slice 205

Date: 2026-03-01

## Scope

Fix flaky path equality in evidence bundle smoke assertions caused by `/var` vs `/private/var` symlink normalization on macOS.

## Changes

1. Updated `tools/ops_scripts_smoke_test.sh` (`run_evidence_bundle_pack_case`) to canonicalize expected directories with `pwd -P`:
   - `evidence_dir_canonical`
   - `output_dir_canonical`
2. Kept strict equality assertions against helper machine-readable output fields (`evidence_dir`, `output_dir`) using canonicalized expected values.
3. Updated `ROAD_TO_PRODUCTION.md` entry `365`.

## Verification

- `bash -n tools/ops_scripts_smoke_test.sh` — PASS
- Targeted evidence bundle parity check:
  - Run `tools/evidence_bundle_pack.sh` with temp dirs under `TMP_DIR`.
  - Assert `evidence_dir` and `output_dir` fields equal canonical (`pwd -P`) expected paths.
  - Assert `file_count=3` and archive exists.
- `cargo check -p copybot-executor -q` — PASS
