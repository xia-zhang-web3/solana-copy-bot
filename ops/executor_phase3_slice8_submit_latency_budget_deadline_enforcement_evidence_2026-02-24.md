# Executor Phase 3 Slice 8 Evidence — Submit Latency Budget Deadline Enforcement

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added submit-path total deadline budget enforcement:
   1. New config: `COPYBOT_EXECUTOR_SUBMIT_TOTAL_BUDGET_MS`.
   2. Default budget derived from request timeout via `default_submit_total_budget_ms(...)`.
   3. Startup fail-closed if budget is below effective request timeout floor.
2. Added `SubmitDeadline` helper and propagated deadline through submit network path:
   1. upstream submit forwarding,
   2. send-rpc broadcasting,
   3. submit-signature verify polling (including retry sleep phase).
3. Per-hop request timeout now uses remaining budget (`RequestBuilder::timeout(...)`) for submit path.
4. Added unit guard for runtime timeout floor interaction:
   1. `min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor` is preserved in contract guard pack.
5. Logging now includes `submit_total_budget_ms` at startup.

## Files

1. `crates/executor/src/main.rs`
2. `crates/executor/src/send_rpc.rs`
3. `crates/executor/src/submit_verify.rs`
4. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (74)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS
4. `bash -n tools/executor_contract_smoke_test.sh crates/executor/src/main.rs crates/executor/src/send_rpc.rs crates/executor/src/submit_verify.rs` — PASS

