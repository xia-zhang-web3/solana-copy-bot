# Executor Phase 2 Slice 7 Evidence — Send RPC Backend Chain Unification

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Unified send-RPC endpoint/auth selection with `RouteBackend` as single source-of-truth:
   1. added `RouteBackend::send_rpc_endpoint_chain()`,
   2. updated `send_signed_transaction_via_rpc` to consume this chain.
2. Removed duplicated chain construction logic from `send_rpc.rs`.
3. Added module-level test:
   1. `send_rpc_endpoint_chain_preserves_primary_fallback_and_auth`.

## Files

1. `crates/executor/src/route_backend.rs`
2. `crates/executor/src/send_rpc.rs`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. Behavior remains unchanged; this reduces drift risk between submit/simulate and send_rpc route backend selection paths.
