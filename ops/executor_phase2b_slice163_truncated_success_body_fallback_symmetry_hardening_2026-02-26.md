# Executor Phase 2B — Slice 163

Date: 2026-02-26  
Owner: execution-dev

## Scope

- close asymmetry between declared-oversized and truncated (was_truncated) success JSON branches by enabling fallback traversal for both.

## Changes

1. Runtime (`crates/executor/src/upstream_forward.rs`):
   - success-JSON parse error on `body_read.was_truncated=true` now maps to retryable `upstream_response_too_large`.
   - when fallback endpoint exists, this retryable branch now continues to fallback.
2. Runtime (`crates/executor/src/send_rpc.rs`):
   - success-JSON parse error on `body_read.was_truncated=true` now maps to retryable `send_rpc_response_too_large`.
   - fallback traversal now applies to this branch too.
3. Integration tests (`crates/executor/src/main.rs`):
   - `forward_to_upstream_uses_fallback_after_primary_truncated_success_body`
   - `send_signed_transaction_via_rpc_uses_fallback_after_primary_truncated_success_body`
   - added chunked-response helper `spawn_one_shot_upstream_chunked_raw` to produce oversized success body without declared `Content-Length`.
4. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both truncated-success fallback guards.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 323.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests (`truncated_success_body`) — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- success oversized handling is now symmetric:
  - declared oversize (`Content-Length`) -> retryable too-large with fallback.
  - non-declared oversize (`was_truncated`) -> retryable too-large with fallback.
- fail-closed classification remains `*_response_too_large` while availability improves via endpoint-chain retry.
