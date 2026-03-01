# Executor Phase 2B — Slice 215

Date: 2026-03-01

## Scope

Harden `audit_standard.sh` contract-smoke skip behavior by validating and propagating `AUDIT_SKIP_CONTRACT_SMOKE` at standard-runner boundary.

## Changes

1. `tools/audit_standard.sh`
   - Added strict/fail-closed parse for `AUDIT_SKIP_CONTRACT_SMOKE`.
   - Default source when unset:
     - `AUDIT_SKIP_CONTRACT_SMOKE := AUDIT_SKIP_OPS_SMOKE` (canonical parsed value).
   - Propagates parsed contract-smoke gate into quick baseline:
     - `AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" bash tools/audit_quick.sh`
   - Invalid token now exits `1` before quick baseline starts.
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_standard_contract_smoke_strict_bool_guard_case`.
   - Guard asserts:
     - `AUDIT_SKIP_CONTRACT_SMOKE=maybe` => `exit 1` with expected strict-bool error.
     - `audit_standard` fails before quick baseline line.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `375`.

## Verification

- `bash -n tools/audit_standard.sh tools/ops_scripts_smoke_test.sh tools/audit_quick.sh` — PASS
- Targeted guards:
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=maybe bash tools/audit_standard.sh` — `rc=1`, strict-bool error
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_DIFF_RANGE=not-a-valid-diff-range...HEAD bash tools/audit_standard.sh` — `rc=1`, invalid diff-range error, no quick-baseline line
- `cargo check -p copybot-executor -q` — PASS
