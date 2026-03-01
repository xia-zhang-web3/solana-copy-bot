# Executor Phase 2B — Slice 218

Date: 2026-03-01

## Scope

Bound and harden `audit_full.sh` workspace test step with strict timeout gate and fail-fast validation.

## Changes

1. `tools/audit_full.sh`
   - Added strict `AUDIT_WORKSPACE_TEST_TIMEOUT_SEC` gate (`parse_u64_token_strict`, `>=1`, default `900`).
   - Added `run_workspace_tests()` wrapper:
     - `timeout "$workspace_test_timeout_sec" cargo test --workspace -q` when `timeout` is available.
     - fallback to direct cargo execution otherwise.
   - Workspace test log now includes timeout observability:
     - `[audit:full] cargo test --workspace -q (timeout=<N>s)`
   - Invalid/zero workspace timeout now fail-closed before baseline.
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_workspace_test_timeout_guard_case`.
   - Guard asserts:
     - invalid `AUDIT_WORKSPACE_TEST_TIMEOUT_SEC=abc` => `exit 1` with expected error text.
     - failure occurs before baseline starts.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `378`.

## Verification

- `bash -n tools/audit_full.sh tools/ops_scripts_smoke_test.sh tools/lib/common.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_WORKSPACE_TEST_TIMEOUT_SEC=abc bash tools/audit_full.sh`
  - result: `rc=1`, explicit timeout error, no baseline start line.
- `cargo check -p copybot-executor -q` — PASS
