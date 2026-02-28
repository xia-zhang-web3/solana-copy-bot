# Executor Phase 2B — Slice 194

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual low where poisoned `.copybot_evidence_bundle_outputs.txt` could suppress legitimate evidence files.

## Changes

1. `tools/evidence_bundle_pack.sh` index trust hardening:
   - added candidate validation for index entries:
     - non-empty safe relative path (no absolute / traversal forms)
     - scoped to `OUTPUT_DIR` subtree (when output is inside evidence tree)
     - helper artifact extension only (`.tar.gz`, `.sha256`, `.contents.sha256`)
   - added exact exclusion promotion only for complete, existing helper triplets:
     - `<stem>.tar.gz`
     - `<stem>.sha256`
     - `<stem>.contents.sha256`
   - invalid or partial index entries are ignored (not excluded).
   - index rewrite now preserves only validated existing helper entries + current run outputs (self-healing from poisoned/stale lines).
2. `tools/ops_scripts_smoke_test.sh`:
   - extended evidence pack guard with explicit poisoning scenario:
     - create `keep.txt`
     - inject poisoned index line `keep.txt`
     - assert helper still bundles `keep.txt` (`file_count=1` and tar inventory contains file).
3. `ROAD_TO_PRODUCTION.md`:
   - added item `354`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted poisoning repro — PASS:
   - poisoned index with `keep.txt` no longer drops evidence file
3. Targeted same-label/cross-label recursion checks — PASS
4. `cargo check -p copybot-executor -q` — PASS
5. `cargo test -p copybot-executor -q` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- bundle helper now excludes only validated helper outputs and is resilient to index poisoning / stale index entries.
