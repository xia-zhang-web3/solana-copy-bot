# Executor Phase 3 Slice 2 Evidence — Idempotency Reopen Persistence

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added explicit restart-persistence test for idempotency store:
   1. write submit response in first store instance,
   2. drop/reopen store on the same SQLite file path,
   3. verify response remains retrievable.
2. Added this test to contract smoke guard set for ongoing regression coverage.

## Files

1. `crates/executor/src/idempotency.rs`
2. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. This specifically validates restart-safety for completed submit idempotency rows, which is required for durable duplicate defense.
