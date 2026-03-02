# Executor Phase 2B — Slice 304

Date: 2026-03-02  
Commit: pending

## Scope

Closed residual risk for targeted executor-test mode where broad substring filters could match multiple tests and where duplicate targets could re-run the same test silently.

## Changes

1. `tools/audit_quick.sh`
   1. Tightened targeted target resolution:
      1. enumerate full executor test list via `cargo test -p copybot-executor -q -- --list`
      2. resolve each target token to exactly one concrete test name:
         1. exact match preferred
         2. single substring match accepted
         3. unknown -> fail-closed
         4. ambiguous (>1 substring match) -> fail-closed with matched test list
      3. reject duplicate resolved targets
      4. execute resolved exact test names only
2. `tools/ops_scripts_smoke_test.sh`
   1. Extended `run_audit_executor_test_mode_guard_case`:
      1. ambiguous target reject pin (`AUDIT_EXECUTOR_TEST_TARGETS=route`)
      2. duplicate target reject pin (`constant_time_eq_checks_content,constant_time_eq_checks_content`)
3. `ROAD_TO_PRODUCTION.md`
   1. Added item `431` documenting ambiguity/duplicate fail-closed closure.

## Validation

1. `bash -n tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="audit_executor_test_mode_guard" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_EXECUTOR_TESTS=false AUDIT_EXECUTOR_TEST_MODE=targeted AUDIT_EXECUTOR_TEST_TARGETS=route bash tools/audit_quick.sh` — FAIL (expected, rc=1, ambiguous target)
4. `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_EXECUTOR_TESTS=false AUDIT_EXECUTOR_TEST_MODE=targeted AUDIT_EXECUTOR_TEST_TARGETS=constant_time_eq_checks_content,constant_time_eq_checks_content bash tools/audit_quick.sh` — FAIL (expected, rc=1, duplicate target)
5. `cargo check -p copybot-executor -q` — PASS

