# Executor Phase 2 Slice 46 Evidence (2026-02-24)

## Scope

Extract `/submit` business handler from `main.rs` into dedicated module.

## Implemented

1. Added `crates/executor/src/submit_handler.rs` containing `handle_submit`.
2. Moved unchanged submit business flow into the new module:
   1. common contract + identity/slippage/compute-budget validation,
   2. tip policy resolution and forward payload rewrite,
   3. idempotency claim/load + in-flight reject handling,
   4. upstream submit forwarding and business reject mapping,
   5. submit transport extraction (`tx_signature` / `signed_tx_base64` -> send-rpc),
   6. submit signature visibility verification,
   7. response echo validation + submitted_at resolution,
   8. fee-hint parsing/resolution and success payload assembly,
   9. first-write-wins idempotency persistence and canonical conflict response.
3. Updated `crates/executor/src/main.rs`:
   1. added `mod submit_handler;`
   2. switched endpoint to `crate::submit_handler::handle_submit`;
   3. removed now-unused submit-specific imports.

## Effect

1. `main.rs` now focuses on startup + endpoint ingress wiring.
2. Submit business path is isolated for subsequent phase-2 slices.
3. No change in `/submit` contract semantics or reject taxonomy.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
2. `cargo test -p copybot-executor -q handle_submit_returns_cached_response_for_duplicate_client_order_id` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
