# Executor Phase 2 Slice 1 Evidence (2026-02-24)

## Scope

Started Phase 2 transaction-build core extraction with deterministic, testable primitives.

### Implemented

1. Added `crates/executor/src/tx_build.rs`:
   1. `resolve_submit_tip_lamports()` with route policy application + hard guards (`tip max`, `allow_nonzero_tip`).
   2. `build_submit_forward_payload()` with deterministic tip rewrite and strict JSON object contract.
   3. Dedicated unit tests (`tx_build_*`).
2. Integrated submit path in `crates/executor/src/main.rs`:
   1. Replaced inline tip policy block with `tx_build::resolve_submit_tip_lamports()`.
   2. Replaced inline forward payload mutation with `tx_build::build_submit_forward_payload()`.
   3. Added explicit error mappers preserving reject-code contract (`invalid_tip_lamports`, `tip_not_supported`, `invalid_request_body`).

## Files

1. `crates/executor/src/tx_build.rs`
2. `crates/executor/src/main.rs`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q tx_build_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
3. `cargo test -p copybot-executor -q handle_submit_allows_rpc_tip_when_nonzero_tip_disabled` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_nonzero_tip_for_jito_when_disabled` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS

