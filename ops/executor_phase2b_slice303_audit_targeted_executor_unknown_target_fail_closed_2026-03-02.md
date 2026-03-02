# Executor Phase 2B — Slice 303

Date: 2026-03-02  
Commit: pending

## Scope

Closed fail-open behavior in targeted executor-test mode where unknown targets could pass as `running 0 tests` with `PASS`.

## Changes

1. `tools/audit_quick.sh`
   1. In `run_executor_tests()` targeted path:
      1. collect normalized target list
      2. run `cargo test -p copybot-executor -q -- --list` once (timeout-guarded)
      3. fail-closed if list extraction fails or has no `: test` entries
      4. reject unknown targets with explicit error:
         1. `unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: <target>`
      5. run per-target `cargo test` only after validation
2. `tools/ops_scripts_smoke_test.sh`
   1. Extended `run_audit_executor_test_mode_guard_case`:
      1. negative unknown-target pin for `audit_quick.sh`
      2. propagation negative unknown-target pins for `audit_standard.sh` and `audit_full.sh`
3. `ROAD_TO_PRODUCTION.md`
   1. Added item `430` documenting unknown-target fail-closed closure.

## Validation

1. `bash -n tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="audit_executor_test_mode_guard" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_EXECUTOR_TESTS=false AUDIT_EXECUTOR_TEST_MODE=targeted AUDIT_EXECUTOR_TEST_TARGETS=definitely_nonexistent_test_name_12345 bash tools/audit_quick.sh` — FAIL (expected, rc=1)
4. `cargo check -p copybot-executor -q` — PASS

