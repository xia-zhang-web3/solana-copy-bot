# Executor Phase 2B Slice 123 — Submit-Verify Success JSON Byte-Cap Hardening (2026-02-26)

## Scope

- close residual memory-hardening gap in submit-verify `200 OK` JSON path
- avoid unbounded `response.json()` reads in signature-visibility polling
- keep fail-closed strict-mode behavior and diagnostics determinism

## Changes

1. `submit_verify` success path now uses bounded body read + explicit parse:
   - before: `response.json().await`
   - after: `read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES)` + `serde_json::from_str(...)`
2. Structured oversized classification in submit-verify reason path:
   - `was_truncated=true` + parse failure => `rpc response_too_large endpoint=<...> max_bytes=<...> err=<...>`
   - `was_truncated=false` + parse failure => `rpc invalid_json endpoint=<...> err=<...>`
3. Strict submit-verify behavior remains fail-closed:
   - unresolved visibility still returns retryable `upstream_submit_signature_unseen`
   - reason now preserves bounded oversized-vs-invalid diagnostics.
4. Added integration guards:
   - `verify_submit_signature_rejects_oversized_json_response_body`
   - `verify_submit_signature_keeps_invalid_json_classification_with_marker_suffix`
5. Registered new guards in `tools/executor_contract_smoke_test.sh`.
6. Updated ROAD ledger item `283`.

## Files

- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q verify_submit_signature_rejects_oversized_json_response_body` — PASS
3. `cargo test -p copybot-executor -q verify_submit_signature_keeps_invalid_json_classification_with_marker_suffix` — PASS
4. `cargo test -p copybot-executor -q verify_submit_signature_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
