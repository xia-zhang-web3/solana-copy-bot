# Executor Phase 2B Slice 143 — Request Body Limit + send-rpc Blockhash Classifier Tightening (2026-02-26)

## Scope

- close `L-7`: explicit request body size limit at ingress
- close `L-4`: overly broad send-rpc blockhash-expired substring classification (`"recent blockhash"`)

## Changes

1. Router request body cap (`main.rs`):
   - added `DEFAULT_MAX_REQUEST_BODY_BYTES = 256 * 1024`
   - introduced `build_router(state: Arc<AppState>) -> Router`
   - applied `DefaultBodyLimit::max(DEFAULT_MAX_REQUEST_BODY_BYTES)` for all executor routes
   - `main()` now uses `build_router(...)`
2. send-rpc error classifier tightening (`send_rpc.rs`):
   - removed generic `payload_lower.contains("recent blockhash")` from `BlockhashExpired` path
   - retained specific expiry phrases:
     - `"blockhash not found"`
     - `"block height exceeded"`
     - `"transaction is too old"`
3. Guard tests:
   - unit:
     - `classify_send_rpc_error_payload_treats_generic_recent_blockhash_text_as_terminal`
     - `classify_send_rpc_error_payload_keeps_blockhash_not_found_as_expired`
   - integration:
     - `router_rejects_oversized_request_body_before_handler`
     - `send_signed_transaction_via_rpc_treats_generic_recent_blockhash_text_as_terminal`
4. Smoke registry:
   - added all four guards to `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/main.rs`
- `crates/executor/src/send_rpc.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q router_rejects_oversized_request_body_before_handler` — PASS
3. `cargo test -p copybot-executor -q classify_send_rpc_error_payload_treats_generic_recent_blockhash_text_as_terminal` — PASS
4. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_treats_generic_recent_blockhash_text_as_terminal` — PASS
5. `cargo test -p copybot-executor -q` — PASS (528/528)
6. `bash tools/executor_contract_smoke_test.sh` — PASS
