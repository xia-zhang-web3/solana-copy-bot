# Executor Phase 2B Slice 435 — executor preflight health identity + routes alias contract hardening

Date: 2026-03-02  
Owner: execution-dev

## Scope

Runtime/e2e hardening for `tools/executor_preflight.sh` health contract checks:

1. enforce `signer_source` parity (`executor env` vs `/healthz`);
2. enforce `signer_pubkey` parity (`executor env` vs `/healthz`);
3. enforce `submit_fastlane_enabled` parity (`executor env` vs `/healthz`);
4. enforce bidirectional `enabled_routes` parity (missing + unexpected);
5. enforce alias consistency `routes` ↔ `enabled_routes` when both fields are present.

Smoke fixture was expanded to emit configurable health signer/source/fastlane/alias fields and to pin new negative rejects in `run_executor_preflight_case`.

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5` (executor backend hardening/evidence chain).

## Validation

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh`
2. `cargo check -p copybot-executor -q`
3. Targeted harness run:
   1. source smoke harness (without last line),
   2. `create_legacy_db`,
   3. `run_executor_preflight_case`.

## Notes

1. Full `tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight case was executed.
