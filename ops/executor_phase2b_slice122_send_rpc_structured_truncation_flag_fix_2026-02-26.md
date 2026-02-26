# Executor Phase 2B Slice 122 — Send-RPC Structured Truncation Flag Fix (2026-02-26)

## Scope

- fix false-positive classification risk in send-rpc oversized JSON diagnostics
- harden upstream-forward `200 OK` JSON path with bounded read + explicit oversized classification
- keep fail-closed behavior and memory caps intact

## Changes

1. `http_utils::read_response_body_limited` now returns structured metadata:
   - `ReadResponseBody { text, was_truncated }`
   - removes dependence on text-suffix heuristics for truncation detection
2. Updated call sites:
   - `upstream_forward.rs` reads `body.text` for detail truncation
   - `upstream_forward.rs` success JSON path now uses bounded read + `serde_json::from_str`:
     - `was_truncated=true` parse failure => `upstream_response_too_large`
     - `was_truncated=false` parse failure => `upstream_invalid_json`
   - `send_rpc.rs`:
     - non-2xx path reads `body.text`
     - success JSON parse uses `body_read.was_truncated` to classify
       - `true` => `send_rpc_response_too_large`
       - `false` => `send_rpc_invalid_json`
3. Added regression integration guard:
   - `send_signed_transaction_via_rpc_keeps_invalid_json_classification_with_marker_suffix`
   - proves malformed small JSON ending with `...[truncated]` remains `send_rpc_invalid_json`.
4. Added upstream integration guards:
   - `forward_to_upstream_rejects_oversized_json_response_body`
   - `forward_to_upstream_keeps_invalid_json_classification_with_marker_suffix`
5. Updated smoke registry to include new regression guards.
6. Updated ROAD ledger items `281`, `282`.

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_oversized_json_response_body` — PASS
3. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_keeps_invalid_json_classification_with_marker_suffix` — PASS
4. `cargo test -p copybot-executor -q forward_to_upstream_rejects_oversized_json_response_body` — PASS
5. `cargo test -p copybot-executor -q forward_to_upstream_keeps_invalid_json_classification_with_marker_suffix` — PASS
6. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_` — PASS
7. `bash tools/executor_contract_smoke_test.sh` — PASS
8. `cargo test -p copybot-executor -q` — PASS
