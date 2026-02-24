# Executor Phase 2 Slice 2 Evidence (2026-02-24)

## Scope

Extended `tx_build` primitives with compute-budget validation and wired them into submit-path contract checks.

## Implemented

1. `crates/executor/src/tx_build.rs`:
   1. Added `ComputeBudgetBounds` + `ComputeBudgetValidationError`.
   2. Added `validate_submit_compute_budget()` for deterministic range checks.
   3. Added `tx_build_*` unit tests for accepted/rejected compute-budget values.
2. `crates/executor/src/main.rs`:
   1. Replaced inline compute-budget range checks in `handle_submit()` with `tx_build::validate_submit_compute_budget()`.
   2. Added error mapper preserving existing reject contract:
      1. code: `invalid_compute_budget`
      2. detail: unchanged bound messages.
   3. Added submit-path tests:
      1. `handle_submit_rejects_compute_budget_limit_out_of_range`
      2. `handle_submit_rejects_compute_budget_price_out_of_range`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added both new guard tests to contract smoke list.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q tx_build_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_compute_budget_limit_out_of_range` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_compute_budget_price_out_of_range` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

