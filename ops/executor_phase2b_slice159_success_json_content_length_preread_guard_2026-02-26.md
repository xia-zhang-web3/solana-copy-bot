# Executor Phase 2B — Slice 159

Date: 2026-02-26  
Owner: execution-dev

## Scope

- add pre-read fail-closed guard for oversized declared `Content-Length` on success JSON paths.

## Changes

1. Runtime changes:
   - `crates/executor/src/upstream_forward.rs`
     - before `read_response_body_limited(...)`, reject terminal `upstream_response_too_large` if `response.content_length() > MAX_HTTP_JSON_BODY_READ_BYTES`.
   - `crates/executor/src/send_rpc.rs`
     - before `read_response_body_limited(...)`, reject terminal `send_rpc_response_too_large` on oversized declared `Content-Length`.
   - `crates/executor/src/submit_verify.rs`
     - before body read, classify oversized declared `Content-Length` as `rpc response_too_large` reason and continue retry loop (strict mode later fails with `upstream_submit_signature_unseen`).
2. Added integration guards in `crates/executor/src/main.rs`:
   - `forward_to_upstream_rejects_oversized_declared_content_length_before_json_read`
   - `send_signed_transaction_via_rpc_rejects_oversized_declared_content_length_before_json_read`
   - `verify_submit_signature_rejects_oversized_declared_content_length_before_json_read`
3. Updated two existing oversized tests to assert on stable `max_bytes=65536` marker (compatible with both pre-read and truncation-driven too-large paths).
4. Registered all 3 new guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap item 319 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted new tests (3) — PASS
3. `cargo test -p copybot-executor -q` — PASS (`575/575`)
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- Success JSON paths now short-circuit declared-oversized responses before read/parse.
- Classification remains fail-closed and deterministic (`*_response_too_large` / strict unseen path).
