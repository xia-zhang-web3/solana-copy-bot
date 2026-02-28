# Executor Phase 2B — Slice 191

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close medium residuals in `tools/evidence_bundle_pack.sh` for repeated-run recursion and same-second name collision.

## Changes

1. `tools/evidence_bundle_pack.sh` hardening:
   - added optional `BUNDLE_TIMESTAMP_UTC` override for reproducible runs/tests.
   - added strict timestamp format validation (`YYYYMMDDTHHMMSSZ`).
   - added collision-safe naming: if `<label>_<timestamp>.*` exists, helper now appends numeric suffix (`_1`, `_2`, ...).
   - excluded prior helper outputs from packaging list by label pattern:
     - `<label>_*.tar.gz`
     - `<label>_*.sha256`
     - `<label>_*.contents.sha256`
   - result: repeated runs with default `OUTPUT_DIR=$EVIDENCE_DIR` no longer recursively package prior bundle artifacts.
2. `tools/ops_scripts_smoke_test.sh` coverage:
   - extended `run_evidence_bundle_pack_case` with self-output idempotence/collision guard:
     - two runs with same `BUNDLE_LABEL` and fixed `BUNDLE_TIMESTAMP_UTC`
     - asserts `file_count=1` on both runs
     - asserts second run chooses different bundle path (collision suffix)
     - asserts archive inventory excludes `*.tar.gz` / `*.sha256` artifacts.
3. `ROAD_TO_PRODUCTION.md`:
   - added item `351`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted helper check (separate output dir) — PASS
3. Targeted self-output repeated-run check (same label + same timestamp) — PASS
   - second run file_count remained `1`
   - second bundle path changed with suffix
   - tar inventory excluded bundle/checksum artifacts
4. `cargo check -p copybot-executor -q` — PASS
5. `cargo test -p copybot-executor -q` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- evidence pack helper now behaves deterministically/safely across repeated invocations and concurrent same-second runs, matching fail-closed hardening goals for ops evidence workflows.
