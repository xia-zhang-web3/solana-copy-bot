# Executor Phase 2B — Slice 216

Date: 2026-03-01

## Scope

Harden audit runner timeout control for ops smoke by replacing fixed timeout with strict configurable gate.

## Changes

1. `tools/lib/common.sh`
   - Added `parse_u64_token_strict()` for shared strict integer parsing.
2. `tools/audit_standard.sh`
   - Added `AUDIT_OPS_SMOKE_TIMEOUT_SEC` parsing via `parse_u64_token_strict`.
   - Enforced `>= 1`; invalid/zero now fail-closed with explicit `exit 1`.
   - `run_ops_smoke` now uses `timeout "$ops_smoke_timeout_sec"` instead of fixed `300`.
3. `tools/audit_full.sh`
   - Added same strict `AUDIT_OPS_SMOKE_TIMEOUT_SEC` gate (`>=1`).
   - `run_ops_smoke` now uses parsed timeout value instead of fixed `300`.
4. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_ops_smoke_timeout_guard_case`.
   - Guard asserts:
     - `audit_standard.sh` rejects non-integer timeout (`abc`) with `exit 1`.
     - `audit_full.sh` rejects zero timeout (`0`) with `exit 1`.
5. `ROAD_TO_PRODUCTION.md`
   - Added entry `376`.

## Verification

- `bash -n tools/lib/common.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guards:
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_OPS_SMOKE_TIMEOUT_SEC=abc bash tools/audit_standard.sh` — `rc=1`, explicit timeout error
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_OPS_SMOKE_TIMEOUT_SEC=0 bash tools/audit_full.sh` — `rc=1`, explicit timeout error
- `cargo check -p copybot-executor -q` — PASS
