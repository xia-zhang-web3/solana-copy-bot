# Executor Phase 3 Slice 9 Evidence — Dedicated Submit Deadline Exhaustion Coverage

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added dedicated submit-path deadline exhaustion regression test:
   1. `handle_submit_rejects_when_submit_deadline_budget_exhausted`.
   2. Scenario: small submit budget + delayed submit-verify endpoint -> expected retryable reject with code `executor_submit_timeout_budget_exceeded`.
2. Added this test to contract smoke guard pack.

## Files

1. `crates/executor/src/main.rs`
2. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q handle_submit_rejects_when_submit_deadline_budget_exhausted` — PASS
2. `cargo test -p copybot-executor -q` — PASS (75)
3. `bash tools/executor_contract_smoke_test.sh` — PASS
4. `cargo test --workspace -q` — PASS

