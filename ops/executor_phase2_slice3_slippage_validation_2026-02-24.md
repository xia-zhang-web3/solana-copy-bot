# Executor Phase 2 Slice 3 Evidence (2026-02-24)

## Scope

Extended `tx_build` primitives with submit slippage policy validation and wired this path into `handle_submit` while preserving reject-code contract.

## Implemented

1. `crates/executor/src/tx_build.rs`:
   1. Added `SlippageValidationError`.
   2. Added `validate_submit_slippage_policy(slippage_bps, route_slippage_cap_bps, epsilon)`.
   3. Added `tx_build_*` slippage tests for valid and reject branches.
2. `crates/executor/src/main.rs`:
   1. Replaced inline slippage checks in `handle_submit` with shared validator.
   2. Added mapper preserving existing terminal reject contract:
      1. `invalid_slippage_bps`
      2. `invalid_route_slippage_cap_bps`
      3. `slippage_exceeds_route_cap`
   3. Added submit-path regression tests:
      1. `handle_submit_rejects_slippage_bps_out_of_range`
      2. `handle_submit_rejects_slippage_exceeding_route_cap`
3. `tools/executor_contract_smoke_test.sh`:
   1. Included both new submit guard tests in contract smoke pack.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q tx_build_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_slippage_bps_out_of_range` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_slippage_exceeding_route_cap` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

