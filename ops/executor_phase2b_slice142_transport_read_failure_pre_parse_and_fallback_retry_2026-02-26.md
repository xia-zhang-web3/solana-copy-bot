# Executor Phase 2B Slice 142 — Transport Read-Failure Pre-Parse + Fallback Retry (2026-02-26)

## Scope

- close medium residual: avoid accepting transport-incomplete responses when partial bytes happen to form valid JSON
- close low residual: make retryable response-read failures participate in fallback traversal for `upstream_forward` and `send_rpc`

## Changes

1. `upstream_forward.rs`:
   - moved `read_error_class` handling before `serde_json::from_slice(...)`
   - response-read failure now short-circuits as retryable `upstream_unavailable`
   - if fallback endpoint exists, path now retries fallback instead of returning immediately
2. `send_rpc.rs`:
   - moved `read_error_class` handling before `serde_json::from_slice(...)`
   - response-read failure now short-circuits as retryable `send_rpc_unavailable`
   - if fallback endpoint exists, path now retries fallback instead of returning immediately
3. `submit_verify.rs`:
   - moved `read_error_class` precedence before parse branch, recorded as `response_read_failed` reason
   - keeps strict-mode fail-closed via `upstream_submit_signature_unseen` after retry loop
4. Integration guards (`main.rs`):
   - `forward_to_upstream_rejects_partial_valid_json_body_as_response_read_failed`
   - `forward_to_upstream_uses_fallback_after_primary_response_read_failure`
   - `send_signed_transaction_via_rpc_rejects_partial_valid_json_body_as_response_read_failed`
   - `send_signed_transaction_via_rpc_uses_fallback_after_primary_response_read_failure`
   - `verify_submit_signature_treats_partial_valid_json_body_as_response_read_failed`
5. Smoke registry:
   - added all five guards to `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q forward_to_upstream_rejects_partial_valid_json_body_as_response_read_failed` — PASS
3. `cargo test -p copybot-executor -q forward_to_upstream_uses_fallback_after_primary_response_read_failure` — PASS
4. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_partial_valid_json_body_as_response_read_failed` — PASS
5. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_uses_fallback_after_primary_response_read_failure` — PASS
6. `cargo test -p copybot-executor -q verify_submit_signature_treats_partial_valid_json_body_as_response_read_failed` — PASS
7. `cargo test -p copybot-executor -q` — PASS (524/524)
8. `bash tools/executor_contract_smoke_test.sh` — PASS
