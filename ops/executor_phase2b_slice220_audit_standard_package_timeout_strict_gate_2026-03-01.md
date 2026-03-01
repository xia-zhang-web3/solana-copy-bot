# Executor Phase 2B — Slice 220

Date: 2026-03-01

## Scope

Bound and harden changed-package test step in `audit_standard.sh` with strict timeout gate and fail-fast validation.

## Changes

1. `tools/audit_standard.sh`
   - Added strict `AUDIT_PACKAGE_TEST_TIMEOUT_SEC` gate (`parse_u64_token_strict`, `>=1`, default `600`).
   - Added `run_package_tests()` wrapper:
     - `timeout "$package_test_timeout_sec" cargo test -p "$package" -q` when `timeout` is available.
     - fallback to direct cargo execution otherwise.
   - Changed-package test log now includes timeout observability:
     - `[audit:standard] cargo test -p <package> -q (timeout=<N>s)`
   - Invalid/zero timeout now fail-closed before quick baseline starts.
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_standard_package_test_timeout_guard_case`.
   - Guard asserts:
     - invalid `AUDIT_PACKAGE_TEST_TIMEOUT_SEC=abc` => `exit 1` with expected error.
     - failure occurs before quick baseline starts.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `380`.

## Verification

- `bash -n tools/audit_standard.sh tools/ops_scripts_smoke_test.sh tools/lib/common.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_PACKAGE_TEST_TIMEOUT_SEC=abc bash tools/audit_standard.sh`
  - result: `rc=1`, explicit timeout error, no quick baseline start line.
- `cargo check -p copybot-executor -q` — PASS
