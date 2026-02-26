# Executor Phase 2B Slice 121 — Send-RPC Oversized JSON Diagnostics Hardening (2026-02-26)

## Scope

- improve reject classification for oversized bounded JSON in send-rpc success path
- keep fail-closed behavior unchanged

## Changes

1. Added truncation-marker helper in `http_utils`:
   - `body_text_was_truncated(body_text: &str) -> bool`
2. Updated send-rpc success parse mapping:
   - when bounded body parse fails and body has truncation marker:
     - code: `send_rpc_response_too_large`
   - otherwise keep existing:
     - code: `send_rpc_invalid_json`
3. Updated oversized JSON integration guard expectation:
   - `send_signed_transaction_via_rpc_rejects_oversized_json_response_body`
   - now expects `send_rpc_response_too_large` + `exceeded max bytes`
4. Added helper unit test:
   - `body_text_was_truncated_detects_truncation_marker`
5. Added helper test into smoke list.
6. Updated ROAD ledger item `280`.

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_oversized_json_response_body` — PASS
3. `cargo test -p copybot-executor -q body_text_was_truncated_detects_truncation_marker` — PASS
4. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
