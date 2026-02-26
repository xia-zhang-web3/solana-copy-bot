# Executor Phase 2B Slice 118 — Error Body Read Byte-Cap Hardening (2026-02-26)

## Scope

- prevent unbounded reads of oversized upstream/send-rpc HTTP error bodies
- preserve existing fail-closed codes/retryability and detail truncation contract

## Changes

1. Added bounded HTTP error body reader in `http_utils`:
   - `MAX_HTTP_ERROR_BODY_READ_BYTES = 4096`
   - `read_response_body_limited(response, max_bytes)`
   - behavior:
     - reads body incrementally via `response.chunk()`
     - stops after byte cap and appends `...[truncated]`
     - returns deterministic fallback detail on read error
2. Switched non-2xx paths to bounded reader:
   - `upstream_forward.rs` (upstream HTTP errors)
   - `send_rpc.rs` (send-rpc HTTP errors)
3. Kept existing char-level truncation (`MAX_HTTP_ERROR_BODY_DETAIL_CHARS`) after bounded read.
4. Added unit test:
   - `read_response_body_limited_truncates_large_http_body`
5. Updated integration guards to force payload sizes above byte-cap:
   - `forward_to_upstream_truncates_large_http_error_body_detail`
   - `send_signed_transaction_via_rpc_truncates_large_http_error_body_detail`
   - `send_signed_transaction_via_rpc_truncates_large_error_payload_detail`
6. Added helper guard into contract smoke list.

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q read_response_body_limited_truncates_large_http_body` — PASS
3. `cargo test -p copybot-executor -q forward_to_upstream_truncates_large_http_error_body_detail` — PASS
4. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_truncates_large_http_error_body_detail` — PASS
5. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_truncates_large_error_payload_detail` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
7. `cargo test -p copybot-executor -q` — PASS
