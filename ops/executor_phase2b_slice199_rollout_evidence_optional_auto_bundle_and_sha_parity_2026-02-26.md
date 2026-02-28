# Executor Phase 2B — Slice 199

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- extend optional one-command evidence archive packaging from final wrappers to direct rollout helpers.
- keep checksum publication deterministic and file-consistent in rollout summary/manifest artifacts.

## Changes

1. `tools/executor_rollout_evidence_report.sh`:
   - added optional bundle controls:
     - `PACKAGE_BUNDLE_ENABLED` (strict bool, default `false`)
     - `PACKAGE_BUNDLE_LABEL` (default `executor_rollout_evidence`)
     - `PACKAGE_BUNDLE_OUTPUT_DIR` (default `OUTPUT_DIR`)
   - fail-closed input guard:
     - `PACKAGE_BUNDLE_ENABLED=true` requires non-empty `OUTPUT_DIR`.
   - when `OUTPUT_DIR` is present:
     - generates captures as before
     - optionally runs `tools/evidence_bundle_pack.sh "$OUTPUT_DIR"`
     - emits `package_bundle_*` machine-readable fields
     - appends package fields into summary artifact
     - computes `summary_sha256` from finalized summary
     - writes manifest including bundle checksum metadata
     - emits `manifest_sha256`.
   - if bundling is requested but not written, exits `3`.
2. `tools/adapter_rollout_evidence_report.sh`:
   - same optional bundle controls and fail-closed behavior:
     - `PACKAGE_BUNDLE_ENABLED`, `PACKAGE_BUNDLE_LABEL`, `PACKAGE_BUNDLE_OUTPUT_DIR`
     - strict bool parsing + `OUTPUT_DIR` requirement
     - `package_bundle_*` fields emitted
     - finalized summary hash and manifest bundle checksums.
3. `tools/ops_scripts_smoke_test.sh`:
   - rollout pass-path assertions now include:
     - `manifest_sha256`
     - hash/file invariant checks for summary and manifest.
   - added bundle-enabled rollout guard cases for both helpers:
     - verify `package_bundle_enabled=true`
     - verify bundle write success (`package_bundle_artifacts_written=true`, `package_bundle_exit_code=0`)
     - verify bundle sha field + bundle artifact file existence.
4. `ROAD_TO_PRODUCTION.md`:
   - added item `359`.

## Validation

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted checksum invariant repro (4 cases) — PASS:
   - `EXEC_ROLLOUT_DISABLED: MATCH=yes`
   - `EXEC_ROLLOUT_ENABLED: MATCH=yes`
   - `ADAPTER_ROLLOUT_DISABLED: MATCH=yes`
   - `ADAPTER_ROLLOUT_ENABLED: MATCH=yes`
3. `cargo check -p copybot-executor -q` — PASS

## Result

- rollout helpers now support direct archive packaging without requiring final wrappers, and checksum fields are pinned to actual on-disk artifacts in bundle-on/off modes.
