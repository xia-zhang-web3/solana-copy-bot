# Executor Phase 2B Slice 436 — executor preflight signer-source default parity

Date: 2026-03-02  
Owner: execution-dev

## Scope

Close Medium regression from slice 435:

1. `tools/executor_preflight.sh` now defaults `COPYBOT_EXECUTOR_SIGNER_SOURCE` to `file` when missing/empty, matching runtime behavior (`resolve_signer_source_config`).
2. Kept strict validation for configured values: only `file|kms` are accepted.
3. Kept `/healthz` signer-source parity check, but mismatch is enforced only when expected signer source is valid.
4. Added smoke pin in `run_executor_preflight_case` for missing signer-source env:
   1. remove `COPYBOT_EXECUTOR_SIGNER_SOURCE` from executor env fixture,
   2. force fake health `signer_source=file`,
   3. assert `preflight_verdict=PASS`,
   4. assert `executor_signer_source_expected=file`.

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Validation

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh`
2. `cargo check -p copybot-executor -q`
3. Targeted harness run:
   1. source smoke harness (without final line),
   2. `create_legacy_db`,
   3. `run_executor_preflight_case`.

## Notes

1. Full `tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight case was executed.
