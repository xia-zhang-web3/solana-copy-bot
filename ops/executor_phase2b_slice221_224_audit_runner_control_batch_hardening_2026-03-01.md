# Executor Phase 2B — Batch Slice 221-224

Date: 2026-03-01

## Scope

Apply a larger audit-runner hardening batch instead of micro-slices:

1. quick-runner executor-test skip gate
2. standard-runner baseline control propagation + package skip gate
3. full-runner baseline/workspace control propagation + workspace skip gate
4. expanded smoke guard coverage for new controls

## Changes

1. `tools/audit_quick.sh`
   - Added strict `AUDIT_SKIP_EXECUTOR_TESTS` gate (`true|false|1|0|yes|no|on|off`, default `false`).
   - Executor baseline test step is now conditional:
     - `false` => run executor tests with timeout gate
     - `true` => explicit skip log
2. `tools/audit_standard.sh`
   - Added strict gates:
     - `AUDIT_SKIP_EXECUTOR_TESTS` (default `false`)
     - `AUDIT_SKIP_PACKAGE_TESTS` (default `false`)
     - `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` (`>=1`, default parsed package timeout)
   - Propagates canonical controls into quick baseline:
     - `AUDIT_SKIP_CONTRACT_SMOKE`
     - `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC`
     - `AUDIT_SKIP_EXECUTOR_TESTS`
     - `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC`
   - Changed-package test phase now supports explicit skip via `AUDIT_SKIP_PACKAGE_TESTS=true`.
3. `tools/audit_full.sh`
   - Added strict gates:
     - `AUDIT_SKIP_EXECUTOR_TESTS` (default `false`)
     - `AUDIT_SKIP_WORKSPACE_TESTS` (default `false`)
     - `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` (`>=1`, default parsed workspace timeout)
   - Propagates canonical controls into quick baseline:
     - `AUDIT_SKIP_CONTRACT_SMOKE`
     - `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC`
     - `AUDIT_SKIP_EXECUTOR_TESTS`
     - `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC`
   - Workspace test phase now supports explicit skip via `AUDIT_SKIP_WORKSPACE_TESTS=true`.
4. `tools/ops_scripts_smoke_test.sh`
   - Added new guards:
     - `run_audit_skip_gate_strict_bool_batch_case`
     - `run_audit_standard_executor_test_timeout_guard_case`
     - `run_audit_full_executor_test_timeout_guard_case`
   - Registered these in smoke `main()` audit guard block.
5. `ROAD_TO_PRODUCTION.md`
   - Added entries `381..384`.

## Verification

- `bash -n tools/lib/common.sh tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guards:
  - `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_EXECUTOR_TESTS=maybe bash tools/audit_quick.sh` => `rc=1`
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_PACKAGE_TESTS=maybe bash tools/audit_standard.sh` => `rc=1`
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_SKIP_WORKSPACE_TESTS=maybe bash tools/audit_full.sh` => `rc=1`
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_EXECUTOR_TEST_TIMEOUT_SEC=0 bash tools/audit_standard.sh` => `rc=1`
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_EXECUTOR_TEST_TIMEOUT_SEC=abc bash tools/audit_full.sh` => `rc=1`
- `cargo check -p copybot-executor -q` — PASS
