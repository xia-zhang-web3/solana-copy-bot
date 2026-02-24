# Executor Phase 2 Slice 45 Evidence (2026-02-24)

## Scope

Extract `/simulate` business handler from `main.rs` into a dedicated module.

## Implemented

1. Added `crates/executor/src/simulate_handler.rs` with `handle_simulate`.
2. Moved unchanged simulate business flow into the new module:
   1. common contract validation,
   2. simulate basic request validation,
   3. route normalization and upstream forwarding,
   4. upstream outcome parsing and reject mapping,
   5. route/contract echo validation,
   6. simulate detail normalization and success payload assembly.
3. Updated `crates/executor/src/main.rs`:
   1. added `mod simulate_handler;`
   2. switched call-sites/imports to `crate::simulate_handler::handle_simulate`;
   3. removed now-unused simulate-specific imports.

## Effect

1. `main.rs` is smaller and focused on endpoint wiring.
2. Simulate business path is isolated for future phase-2 slices.
3. No change in `/simulate` contract behavior or reject semantics.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q handle_simulate_rejects_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_route_mismatch` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
