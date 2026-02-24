# Executor Phase 2 Slice 41 Evidence (2026-02-24)

## Scope

Micro cleanup for send-RPC fail-closed topology messaging consistency.

## Implemented

1. `crates/executor/src/send_rpc.rs`:
   1. Added local helper `reject_send_rpc_fallback_without_primary(route)`.
   2. Replaced duplicate `Reject::terminal("adapter_send_rpc_not_configured", ...)` construction in:
      1. runtime defense-in-depth invariant check,
      2. `send_rpc_endpoint_chain_checked` error mapping.
2. Behavior unchanged:
   1. same reject code,
   2. same detail string,
   3. same terminal semantics.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_fallback_without_primary_url` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
