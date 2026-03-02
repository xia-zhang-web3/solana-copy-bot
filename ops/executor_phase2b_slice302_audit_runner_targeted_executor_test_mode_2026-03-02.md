# Executor Phase 2B — Slice 302

Date: 2026-03-02  
Commit: pending

## Scope

Added targeted executor-test mode controls to audit wrappers so `audit_quick.sh` baseline can run selected executor tests instead of full `copybot-executor` suite, with strict fail-closed mode/targets validation and propagation through `audit_standard.sh` / `audit_full.sh`.

## Changes

1. `tools/audit_quick.sh`
   1. Added strict parsing:
      1. `AUDIT_EXECUTOR_TEST_MODE={full|targeted}`
      2. `AUDIT_EXECUTOR_TEST_TARGETS` (required non-empty in targeted mode)
   2. `run_executor_tests()` now supports targeted loop:
      1. CSV split + trim
      2. one `cargo test -p copybot-executor -q <target>` per entry
      3. fail-closed on effectively empty target list
   3. Executor baseline log now includes test mode.
2. `tools/audit_standard.sh`
   1. Added strict mode/targets parse for executor baseline settings.
   2. Propagates `AUDIT_EXECUTOR_TEST_MODE` + `AUDIT_EXECUTOR_TEST_TARGETS` into `tools/audit_quick.sh`.
3. `tools/audit_full.sh`
   1. Added same strict parse + propagation as `audit_standard.sh`.
4. `tools/ops_scripts_smoke_test.sh`
   1. Added `run_audit_executor_test_mode_guard_case`:
      1. invalid mode reject
      2. targeted+empty targets reject
      3. targeted baseline pass via `audit_quick.sh`
      4. propagation pass via `audit_standard.sh`
      5. propagation pass via `audit_full.sh`
   2. Added targeted dispatcher alias: `audit_executor_test_mode_guard`.
   3. Added guard case call in full smoke `main()`.
5. `ROAD_TO_PRODUCTION.md`
   1. Added item `429` documenting targeted executor-test throughput controls.

## Validation

1. `bash -n tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="audit_executor_test_mode_guard" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS

