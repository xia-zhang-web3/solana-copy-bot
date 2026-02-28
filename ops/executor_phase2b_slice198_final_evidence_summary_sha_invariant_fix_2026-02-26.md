# Executor Phase 2B — Slice 198

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- fix stale `summary_sha256` publication in final evidence helpers after auto-bundle fields are appended to summary artifacts.

## Changes

1. `tools/adapter_rollout_final_evidence_report.sh`:
   - reordered write flow:
     - write base summary file
     - resolve/append `package_bundle_*` fields
     - compute `summary_sha256` from finalized summary file
     - generate manifest using finalized summary hash
   - removed pre-append summary hash publication path.
2. `tools/executor_final_evidence_report.sh`:
   - applied the same finalized-summary hash ordering.
3. `tools/ops_scripts_smoke_test.sh`:
   - added helper `assert_sha256_field_matches_file`.
   - added checksum invariants in both executor and adapter final package cases:
     - bundle disabled path
     - bundle enabled path
   - assertions now verify:
     - `summary_sha256 == sha256(artifact_summary)`
     - `manifest_sha256 == sha256(artifact_manifest)`.
4. `ROAD_TO_PRODUCTION.md`:
   - added item `358`.

## Validation

1. `bash -n tools/adapter_rollout_final_evidence_report.sh tools/executor_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted invariant repro (4 cases) — PASS:
   - `ADAPTER_DISABLED: MATCH=yes`
   - `ADAPTER_ENABLED: MATCH=yes`
   - `EXECUTOR_DISABLED: MATCH=yes`
   - `EXECUTOR_ENABLED: MATCH=yes`
3. `cargo check -p copybot-executor -q` — PASS

## Result

- checksum publication is now consistent with on-disk artifacts for both final evidence helpers across bundle on/off modes.
