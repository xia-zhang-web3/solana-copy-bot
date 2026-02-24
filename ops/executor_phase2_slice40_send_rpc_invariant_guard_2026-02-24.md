# Executor Phase 2 Slice 40 Evidence (2026-02-24)

## Scope

Targeted defense-in-depth hardening for send-RPC topology invariant.

## Implemented

1. `crates/executor/src/send_rpc.rs`:
   1. Added explicit local runtime guard:
      1. if `send_rpc_fallback_url` is set while `send_rpc_url` is missing,
      2. return terminal reject `adapter_send_rpc_not_configured`.
   2. Existing config-level and chain-level checks remain unchanged.

## Contract Impact

1. No reject-code change.
2. No behavior change for valid configs.
3. Invalid topology remains fail-closed with existing detail format.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_fallback_without_primary_url` — PASS
2. `cargo test -p copybot-executor -q route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
