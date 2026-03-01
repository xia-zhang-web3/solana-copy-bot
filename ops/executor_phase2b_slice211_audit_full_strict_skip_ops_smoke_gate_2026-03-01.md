# Executor Phase 2B — Slice 211

Date: 2026-03-01

## Scope

Harden `tools/audit_full.sh` smoke-skip gate to strict/fail-closed behavior and pin it with ops smoke guard coverage.

## Changes

1. `tools/audit_full.sh`
   - Added shared helper import:
     - `source "$SCRIPT_DIR/lib/common.sh"`
   - Added strict parsing for `AUDIT_SKIP_OPS_SMOKE`:
     - invalid token => explicit error + `exit 1`
   - Added conditional execution:
     - `AUDIT_SKIP_OPS_SMOKE=false` => run `tools/ops_scripts_smoke_test.sh`
     - `AUDIT_SKIP_OPS_SMOKE=true` => skip with explicit log line
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_full_strict_bool_guard_case`.
   - New guard asserts `AUDIT_SKIP_OPS_SMOKE=maybe` causes `audit_full.sh` to fail with `exit 1` and expected strict-bool error.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `371`.

## Verification

- `bash -n tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_OPS_SMOKE=maybe bash tools/audit_full.sh` — `rc=1`, strict-bool error emitted
- `cargo check -p copybot-executor -q` — PASS
