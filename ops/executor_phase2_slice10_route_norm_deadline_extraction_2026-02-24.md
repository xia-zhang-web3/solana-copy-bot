# Executor Phase 2 Slice 10 Evidence (2026-02-24)

## Scope

Closed the residual route-normalization duplication note and continued modular extraction with `SubmitDeadline` moved out of `main.rs`.

## Implemented

1. Route normalization de-dup:
   1. Added shared helper module: `crates/executor/src/route_normalization.rs`.
   2. Removed duplicate `normalize_route` implementations from:
      1. `crates/executor/src/main.rs`
      2. `crates/executor/src/submit_response.rs`
      3. `crates/executor/src/simulate_response.rs`
   3. Rewired all route-normalization callsites to shared helper.
   4. Added unit coverage:
      1. `normalize_route_trims_and_lowercases`
2. New extraction slice (deadline module):
   1. Added `crates/executor/src/submit_deadline.rs` with extracted:
      1. `SubmitDeadline::new`
      2. `SubmitDeadline::remaining_timeout`
   2. Updated callsites to use shared deadline type:
      1. `crates/executor/src/main.rs`
      2. `crates/executor/src/send_rpc.rs`
      3. `crates/executor/src/submit_verify.rs`
   3. Preserved existing fail-closed timeout reject behavior:
      1. code: `executor_submit_timeout_budget_exceeded`
   4. Added unit coverage:
      1. `submit_deadline_remaining_timeout_returns_positive_duration`
      2. `submit_deadline_remaining_timeout_rejects_when_budget_exhausted`
3. Contract guard pack updates:
   1. `tools/executor_contract_smoke_test.sh` now includes:
      1. `normalize_route_trims_and_lowercases`
      2. `submit_deadline_remaining_timeout_rejects_when_budget_exhausted`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q normalize_route_trims_and_lowercases` — PASS
2. `cargo test -p copybot-executor -q submit_deadline_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_submit_deadline_budget_exhausted` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
