# Executor Phase 2B — Slice 162

Date: 2026-02-26  
Owner: execution-dev

## Scope

- preserve availability on declared-oversized success JSON by allowing fallback endpoint traversal.

## Changes

1. Runtime behavior (`crates/executor/src/upstream_forward.rs`):
   - oversized declared success `Content-Length` now returns retryable `upstream_response_too_large`.
   - when fallback endpoint exists, primary declared-oversized response now advances to fallback instead of terminal stop.
2. Runtime behavior (`crates/executor/src/send_rpc.rs`):
   - oversized declared success `Content-Length` now returns retryable `send_rpc_response_too_large`.
   - fallback traversal is now available for this branch.
3. Integration coverage (`crates/executor/src/main.rs`):
   - updated existing declared-oversized reject tests to assert `retryable=true`.
   - added fallback guards:
     - `forward_to_upstream_uses_fallback_after_primary_declared_oversized_content_length`
     - `send_signed_transaction_via_rpc_uses_fallback_after_primary_declared_oversized_content_length`
4. Contract smoke registry (`tools/executor_contract_smoke_test.sh`):
   - registered both new fallback guards.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 322.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests (`declared_oversized_content_length`) — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- declared-oversized success responses remain fail-closed (`*_response_too_large`) but no longer force immediate terminal stop when fallback endpoint is configured.
- primary oversize -> fallback success now works for both upstream forward and send-rpc paths.
