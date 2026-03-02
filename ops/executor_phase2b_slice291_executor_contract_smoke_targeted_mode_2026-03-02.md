# Executor Phase-2B Slice 291 — `executor_contract_smoke` Targeted Mode

Date: 2026-03-02  
Owner: execution-dev  
Scope: throughput hardening for contract smoke loop

## Change Summary

1. Added explicit mode switch in `tools/executor_contract_smoke_test.sh`:
   1. `EXECUTOR_CONTRACT_SMOKE_MODE=full` (default; existing behavior preserved)
   2. `EXECUTOR_CONTRACT_SMOKE_MODE=targeted` (new)
2. Added `EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS=<csv>` for targeted mode.
3. Added strict fail-closed validations:
   1. unknown mode -> fail,
   2. unknown target test -> fail,
   3. effectively empty target list -> fail.
4. Added targeted registration guard:
   1. targeted tests must exist in canonical `contract_guard_tests`,
   2. targeted tests must be present in `cargo test -- --list` output.
5. Preserved full-mode baseline semantics:
   1. still runs `cargo test -p copybot-executor -q`,
   2. still honors `EXECUTOR_CONTRACT_SMOKE_RUN_EACH_GUARD` policy for full canonical guard set.

## Files Updated

1. `tools/executor_contract_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## Validation (targeted)

1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `EXECUTOR_CONTRACT_SMOKE_MODE=targeted EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS="constant_time_eq_checks_content,parse_bool_token_accepts_true_forms" bash tools/executor_contract_smoke_test.sh` — PASS
4. `EXECUTOR_CONTRACT_SMOKE_MODE=targeted EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS="unknown_test" bash tools/executor_contract_smoke_test.sh` — expected fail (unknown target)
5. `EXECUTOR_CONTRACT_SMOKE_MODE=targeted EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS="," bash tools/executor_contract_smoke_test.sh` — expected fail (empty target list)

## Result

Contract smoke loop now supports deterministic narrow runs for iterative hardening without forcing full `copybot-executor` suite on every tiny slice, while preserving canonical full-mode baseline for release/audit closures.
