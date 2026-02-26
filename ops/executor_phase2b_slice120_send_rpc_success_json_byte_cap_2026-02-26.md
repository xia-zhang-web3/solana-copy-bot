# Executor Phase 2B Slice 120 — Send-RPC Success JSON Byte-Cap Hardening (2026-02-26)

## Scope

- close residual memory-hardening gap on send-rpc `200 OK` JSON path
- avoid unbounded `response.json()` reads in send-rpc success branch

## Changes

1. `send_rpc` success path now uses bounded body read + explicit parse:
   - before: `response.json().await`
   - after: `read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES)` + `serde_json::from_str(...)`
2. Added new byte-cap constant:
   - `MAX_HTTP_JSON_BODY_READ_BYTES = 64 * 1024`
3. Added integration guard:
   - `send_signed_transaction_via_rpc_rejects_oversized_json_response_body`
   - verifies oversized 200-JSON response fails closed with `send_rpc_invalid_json`
   - verifies tail marker does not leak in reject detail
4. Registered new test in `tools/executor_contract_smoke_test.sh`.
5. Updated ROAD ledger item `279`.

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_oversized_json_response_body` — PASS
3. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
