# Executor Phase 2B — Slice 192

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual low in evidence bundle helper where cross-label repeated runs with `OUTPUT_DIR=$EVIDENCE_DIR` could recursively include prior bundle artifacts.

## Changes

1. `tools/evidence_bundle_pack.sh`:
   - replaced label-specific artifact exclusion with generalized helper-artifact signature matcher:
     - `<label>_<YYYYMMDDTHHMMSSZ>[ _N ].tar.gz`
     - `<label>_<YYYYMMDDTHHMMSSZ>[ _N ].sha256`
     - `<label>_<YYYYMMDDTHHMMSSZ>[ _N ].contents.sha256`
   - exclusion now applies regardless of current `BUNDLE_LABEL`, eliminating cross-label recursion.
2. `tools/ops_scripts_smoke_test.sh`:
   - extended `run_evidence_bundle_pack_case` with cross-label repeated-run guard:
     - run label A then label B on same evidence dir
     - assert `file_count=1` for label B
     - assert tar inventory excludes prior bundle/checksum artifacts.
3. `ROAD_TO_PRODUCTION.md`:
   - added item `352`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted helper checks — PASS:
   - same-label repeated run (fixed timestamp): stable `file_count=1`, collision-safe bundle path
   - cross-label repeated run (same dir): stable `file_count=1`, no recursive bundle/checksum inclusion
3. `cargo check -p copybot-executor -q` — PASS
4. `cargo test -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- evidence bundle packaging is now non-recursive for both same-label and cross-label repeated runs under self-output mode.
