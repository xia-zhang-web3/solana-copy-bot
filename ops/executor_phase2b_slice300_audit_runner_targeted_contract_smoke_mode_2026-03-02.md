# Executor Phase 2B — Slice 300

Date: 2026-03-02
Commit scope: audit runner targeted contract-smoke mode propagation + strict guards

## Goal

Reduce heavy baseline runtime during iteration by allowing audit runners to use targeted contract smoke subsets, while preserving strict fail-closed input validation.

## Changes

1. `tools/audit_quick.sh`
   - Added strict mode gate:
     - `AUDIT_CONTRACT_SMOKE_MODE={full|targeted}`
   - Added strict targeted-list guard:
     - `AUDIT_CONTRACT_SMOKE_TARGET_TESTS` must be non-empty when mode is `targeted`.
   - Contract smoke invocation now propagates mode/targets to `tools/executor_contract_smoke_test.sh` via:
     - `EXECUTOR_CONTRACT_SMOKE_MODE`
     - `EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS`

2. `tools/audit_standard.sh`
   - Added strict parsing for `AUDIT_CONTRACT_SMOKE_MODE` and `AUDIT_CONTRACT_SMOKE_TARGET_TESTS`.
   - Propagates mode/targets into delegated `tools/audit_quick.sh` call.

3. `tools/audit_full.sh`
   - Added strict parsing for `AUDIT_CONTRACT_SMOKE_MODE` and `AUDIT_CONTRACT_SMOKE_TARGET_TESTS`.
   - Propagates mode/targets into delegated `tools/audit_quick.sh` call.

4. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_contract_smoke_mode_guard_case`:
     - invalid mode rejects (`exit 1`)
     - targeted mode with empty targets rejects (`exit 1`)
     - targeted mode propagation validated through `audit_standard.sh` and `audit_full.sh` with fast baseline settings.
   - Added targeted dispatch alias: `audit_contract_smoke_mode_guard`.

5. `ROAD_TO_PRODUCTION.md`
   - Added entry `427`.

## Validation

1. `bash -n tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh`
2. `OPS_SMOKE_TARGET_CASES="audit_contract_smoke_mode_guard" bash tools/ops_scripts_smoke_test.sh`
3. `cargo check -p copybot-executor -q`

## Ordering residual

- `N=0` unchanged (no executor ordering-chain logic changes).
