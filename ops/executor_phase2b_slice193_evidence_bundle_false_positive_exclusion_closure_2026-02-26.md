# Executor Phase 2B — Slice 193

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual low where broad artifact-name matcher could exclude legitimate timestamped evidence files (false positive).

## Changes

1. `tools/evidence_bundle_pack.sh`:
   - removed broad filename-regex suppression logic.
   - introduced precise helper-output index tracking:
     - `.copybot_evidence_bundle_outputs.txt` in `OUTPUT_DIR`
     - stores only relative paths of helper-generated artifacts from successful runs
   - pack-time exclusion now uses exact-match list from index + index file itself.
   - recursive protection retained for same-label/cross-label repeated runs, now without pattern-based false positives.
2. `tools/ops_scripts_smoke_test.sh`:
   - evidence pack case now adds `incident_20260226T000030Z.tar.gz` as legitimate input file.
   - smoke asserts this file is present in resulting bundle (guards against false-positive exclusion).
   - existing same-label/cross-label recursion guards remain in place.
3. `ROAD_TO_PRODUCTION.md`:
   - added item `353`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted false-positive check — PASS:
   - input includes `incident_20260226T000030Z.tar.gz`
   - bundle includes this file.
3. Targeted same-label/cross-label recursion checks — PASS:
   - repeated runs keep stable `file_count`
   - no recursive inclusion of prior helper artifacts.
4. `cargo check -p copybot-executor -q` — PASS
5. `cargo test -p copybot-executor -q` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- helper now excludes only known self-generated artifacts, preserving legitimate evidence files with timestamped archive-like names while keeping recursion hardening intact.
