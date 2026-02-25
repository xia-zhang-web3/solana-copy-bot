# Executor Phase 2B Slice 22 Evidence (2026-02-25)

## Scope

1. Moved `submit_budget` guard tests from `crates/executor/src/main.rs` into `crates/executor/src/submit_budget.rs`.
2. Preserved existing test names (`min_claim_ttl_sec_for_submit_path_*`) so contract smoke coverage remains stable.
3. Reduced `main.rs` test-module surface without changing runtime logic.

## Files

1. `crates/executor/src/submit_budget.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_accounts_for_verify_and_fallback_hops`
3. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor`
4. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_respects_submit_total_budget_floor`
5. `bash -n tools/executor_contract_smoke_test.sh`
6. `bash tools/executor_contract_smoke_test.sh`
