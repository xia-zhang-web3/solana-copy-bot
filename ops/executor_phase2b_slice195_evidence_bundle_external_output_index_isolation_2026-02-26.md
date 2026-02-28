# Executor Phase 2B — Slice 195

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual low where poisoned index file in external `OUTPUT_DIR` could suppress evidence triplet files from bundle input.

## Changes

1. `tools/evidence_bundle_pack.sh`:
   - index ingestion is now gated by `bundle_index_relative` presence:
     - helper reads/parses `.copybot_evidence_bundle_outputs.txt` only when that index is inside `EVIDENCE_DIR`.
   - when `OUTPUT_DIR` is outside `EVIDENCE_DIR`, index is ignored for exclusions.
2. `tools/ops_scripts_smoke_test.sh`:
   - added explicit outside-output poisoning guard:
     - create evidence dir with `outside_triplet_*.{tar.gz,sha256,contents.sha256}` + keep file
     - place poisoned index in external output dir listing triplet files
     - assert helper still bundles all 4 files (`file_count=4`) and tar inventory contains all entries.
3. `ROAD_TO_PRODUCTION.md`:
   - added item `355`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted outside-output poisoned-index repro — PASS:
   - triplet files are preserved in bundle, no suppression from external index.
3. `cargo check -p copybot-executor -q` — PASS
4. `cargo test -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- external `OUTPUT_DIR` index poisoning path is removed; index-based exclusion is now scoped to in-tree helper outputs only.
