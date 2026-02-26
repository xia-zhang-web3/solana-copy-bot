# Executor Phase 2B Slice 115 — Upstream Error Detail Truncation Hardening (2026-02-26)

## Scope

- harden runtime error-detail handling for large upstream/send-rpc HTTP bodies
- keep error-code/retryability semantics unchanged (fail-closed classification preserved)

## Changes

1. Added shared helper and limit in `http_utils`:
   - `MAX_HTTP_ERROR_BODY_DETAIL_CHARS = 1024`
   - `truncate_detail_chars(value, max_chars)`
2. Applied truncation in upstream forward HTTP-status rejects:
   - `upstream_http_unavailable`
   - `upstream_http_rejected`
3. Applied truncation in send-rpc rejects:
   - HTTP status rejects (`send_rpc_http_unavailable` / `send_rpc_http_rejected`)
   - JSON-RPC `error` payload detail mapping (`retryable` / `terminal` / `blockhash_expired` branches)
4. Added tests:
   - unit tests for truncation helper (`short`, `long`, UTF-8 boundary)
   - integration guard `forward_to_upstream_truncates_large_http_error_body_detail`
5. Added smoke registration for the new integration guard.

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q truncate_detail_chars_` — PASS
3. `cargo test -p copybot-executor -q forward_to_upstream_truncates_large_http_error_body_detail` — PASS
4. `cargo test -p copybot-executor -q send_rpc_deadline_context_` — PASS
5. `cargo test -p copybot-executor -q upstream_forward_deadline_context_` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
7. `cargo test -p copybot-executor -q` — PASS
