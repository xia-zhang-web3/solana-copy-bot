# Executor Phase 2B — Slice 190

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- add a reusable evidence packaging helper for rollout/rehearsal artifact directories.

## Changes

1. New helper (`tools/evidence_bundle_pack.sh`):
   - packages an evidence directory into `*.tar.gz`.
   - emits file-level contents manifest (`*.contents.sha256`) for all bundled files.
   - emits bundle-level checksum file (`*.sha256`).
   - fail-closed validation for missing directory / empty evidence set / invalid bundle label.
2. Ops smoke coverage (`tools/ops_scripts_smoke_test.sh`):
   - added `run_evidence_bundle_pack_case`.
   - validates:
     - `artifacts_written: true`
     - expected file count
     - output checksum matches checksum file content
     - tar inventory contains expected files.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `350`.

## Validation

1. `bash -n tools/evidence_bundle_pack.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted helper run:
   - `bash tools/evidence_bundle_pack.sh <tmp-evidence-dir>` — PASS (bundle + manifests created)
3. `cargo check -p copybot-executor -q` — PASS
4. `cargo test -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- local/server rollout evidence trees can now be archived with deterministic checksums for handoff/audit workflows without manual tar/hash steps.
