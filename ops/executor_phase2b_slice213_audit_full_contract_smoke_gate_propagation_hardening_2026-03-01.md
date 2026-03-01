# Executor Phase 2B — Slice 213

Date: 2026-03-01

## Scope

Harden `audit_full.sh` contract-smoke skip behavior by validating and propagating `AUDIT_SKIP_CONTRACT_SMOKE` at the full-runner boundary.

## Changes

1. `tools/audit_full.sh`
   - Added strict/fail-closed parse for `AUDIT_SKIP_CONTRACT_SMOKE`.
   - Default source when unset:
     - `AUDIT_SKIP_CONTRACT_SMOKE := AUDIT_SKIP_OPS_SMOKE` (canonical parsed value).
   - Added explicit propagation into quick runner:
     - `AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" bash tools/audit_quick.sh`
   - Invalid token now exits `1` before baseline execution.
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_full_contract_smoke_strict_bool_guard_case`.
   - Guard asserts `AUDIT_SKIP_CONTRACT_SMOKE=maybe` triggers `exit 1` and expected strict-bool error in `audit_full.sh`.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `373`.

## Verification

- `bash -n tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guards:
  - `AUDIT_SKIP_OPS_SMOKE=maybe bash tools/audit_full.sh` — `rc=1`, strict-bool error
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=maybe bash tools/audit_full.sh` — `rc=1`, strict-bool error
- `cargo check -p copybot-executor -q` — PASS
