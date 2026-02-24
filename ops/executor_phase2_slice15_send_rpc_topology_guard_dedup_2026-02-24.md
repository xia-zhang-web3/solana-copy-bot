# Executor Phase 2 Slice 15 Evidence (2026-02-24)

## Scope

Continued Phase 2 hardening by centralizing send-RPC endpoint topology validation in `route_backend` so the fail-closed primary/fallback invariant is enforced through a shared checked-chain helper.

## Implemented

1. `crates/executor/src/route_backend.rs`:
   1. Added `SendRpcEndpointChainError` with explicit variant:
      1. `FallbackWithoutPrimary`
   2. Added shared checked helper:
      1. `send_rpc_endpoint_chain_checked()`
   3. Added module guard test:
      1. `route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary`
2. `crates/executor/src/send_rpc.rs`:
   1. Replaced inline topology check with shared checked helper call.
   2. Preserved existing reject-code semantics:
      1. `adapter_send_rpc_not_configured` for fallback-without-primary misconfiguration.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added direct module guard:
      1. `route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary` — PASS
2. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_fallback_without_primary_url` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
