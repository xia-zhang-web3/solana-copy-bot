# Executor Phase 2B — Slice 212

Date: 2026-03-01

## Scope

Harden `tools/audit_quick.sh` contract-smoke skip gate to strict/fail-closed behavior and pin with ops smoke guard.

## Changes

1. `tools/audit_quick.sh`
   - Added shared helper import:
     - `source "$SCRIPT_DIR/lib/common.sh"`
   - Added strict parsing for `AUDIT_SKIP_CONTRACT_SMOKE`:
     - invalid token => explicit error + `exit 1`
   - Added conditional contract-smoke execution:
     - `AUDIT_SKIP_CONTRACT_SMOKE=false` => run `tools/executor_contract_smoke_test.sh`
     - `AUDIT_SKIP_CONTRACT_SMOKE=true` => skip with explicit log line
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_quick_strict_bool_guard_case`.
   - Guard asserts `AUDIT_SKIP_CONTRACT_SMOKE=maybe` causes `audit_quick.sh` to fail with `exit 1` and expected strict-bool error.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `372`.

## Verification

- `bash -n tools/audit_quick.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_CONTRACT_SMOKE=maybe bash tools/audit_quick.sh` — `rc=1`, strict-bool error emitted
- `cargo check -p copybot-executor -q` — PASS
