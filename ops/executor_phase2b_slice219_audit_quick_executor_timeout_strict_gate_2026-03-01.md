# Executor Phase 2B — Slice 219

Date: 2026-03-01

## Scope

Bound and harden `audit_quick.sh` executor test step with strict timeout gate and fail-fast validation.

## Changes

1. `tools/audit_quick.sh`
   - Added strict `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` gate (`parse_u64_token_strict`, `>=1`, default `600`).
   - Added `run_executor_tests()` wrapper:
     - `timeout "$executor_test_timeout_sec" cargo test -p copybot-executor -q` when `timeout` is available.
     - fallback to direct cargo execution otherwise.
   - Baseline log now includes timeout observability:
     - `[audit:quick] cargo test -p copybot-executor -q (timeout=<N>s)`
   - Invalid/zero timeout now fail-closed before baseline starts.
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_executor_test_timeout_guard_case`.
   - Guard asserts:
     - invalid `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC=abc` => `exit 1` with expected error text.
     - failure occurs before quick baseline test line.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `379`.

## Verification

- `bash -n tools/lib/common.sh tools/audit_quick.sh tools/ops_scripts_smoke_test.sh tools/audit_standard.sh tools/audit_full.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_EXECUTOR_TEST_TIMEOUT_SEC=abc bash tools/audit_quick.sh`
  - result: `rc=1`, explicit timeout error, no quick baseline start line.
- `cargo check -p copybot-executor -q` — PASS
