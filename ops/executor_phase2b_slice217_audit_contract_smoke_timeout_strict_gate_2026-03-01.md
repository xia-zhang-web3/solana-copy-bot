# Executor Phase 2B — Slice 217

Date: 2026-03-01

## Scope

Harden contract-smoke timeout control across audit runners with strict parsing and explicit propagation.

## Changes

1. `tools/audit_quick.sh`
   - Added strict `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC` gate (`parse_u64_token_strict`, `>=1`).
   - Added bounded contract-smoke runner:
     - `timeout "$contract_smoke_timeout_sec" bash tools/executor_contract_smoke_test.sh` when timeout exists.
   - Invalid/zero timeout now fail-closed with explicit `exit 1`.
2. `tools/audit_standard.sh`
   - Added strict `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC` parsing (`>=1`), defaulting to parsed `AUDIT_OPS_SMOKE_TIMEOUT_SEC` when unset.
   - Propagates canonical contract timeout into quick baseline invocation.
3. `tools/audit_full.sh`
   - Added same strict contract timeout parsing (`>=1`), defaulting to parsed ops timeout when unset.
   - Propagates canonical contract timeout into quick baseline invocation.
4. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_contract_smoke_timeout_guard_case`.
   - Guard asserts:
     - `audit_quick.sh` rejects non-integer contract timeout (`abc`) before baseline tests.
     - `audit_standard.sh` rejects zero contract timeout (`0`) with `exit 1`.
5. `ROAD_TO_PRODUCTION.md`
   - Added entry `377`.

## Verification

- `bash -n tools/lib/common.sh tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guards:
  - `AUDIT_SKIP_CONTRACT_SMOKE=false AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC=abc bash tools/audit_quick.sh` — `rc=1`, explicit timeout error, no quick baseline start line
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC=0 bash tools/audit_standard.sh` — `rc=1`, explicit timeout error
- `cargo check -p copybot-executor -q` — PASS
