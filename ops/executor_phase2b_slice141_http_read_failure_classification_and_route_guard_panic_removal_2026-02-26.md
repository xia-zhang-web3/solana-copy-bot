# Executor Phase 2B Slice 141 — HTTP Read-Failure Classification + Route Guard Panic Removal (2026-02-26)

## Scope

- close low residual around misclassification of partial/incomplete HTTP bodies as `invalid_json`
- improve submit-verify diagnostics for non-success HTTP responses
- remove brittle `.expect("checked above")` panic path in submit action-context guard

## Changes

1. `http_utils.rs`:
   - `ReadResponseBody` extended with structured `read_error_class: Option<&'static str>`
   - `read_response_body_limited(...)` now marks read-failure class on chunk/read errors (both empty and partial body branches)
2. `upstream_forward.rs`:
   - success JSON parse path now checks `body_read.read_error_class`
   - incomplete/corrupted transport body is classified as retryable `upstream_unavailable` with explicit `response read failed` detail
3. `send_rpc.rs`:
   - success JSON parse path now checks `body_read.read_error_class`
   - incomplete/corrupted transport body is classified as retryable `send_rpc_unavailable` with explicit `response read failed` detail
4. `submit_verify.rs`:
   - non-success HTTP path now reads bounded response body and includes truncated body detail in `last_reason`
   - success JSON parse path now checks `body_read.read_error_class` and tracks `response_read_failed` reason instead of `invalid_json`
5. `route_executor.rs`:
   - replaced submit-path `.expect("checked above")` extractions with explicit `ok_or_else(...)?` fail-closed checks, removing panic-prone branch
6. Integration guards added (`main.rs`) and registered in smoke:
   - `forward_to_upstream_classifies_incomplete_json_body_as_response_read_failed`
   - `send_signed_transaction_via_rpc_classifies_incomplete_json_body_as_response_read_failed`
   - `verify_submit_signature_includes_non_success_http_body_in_reason`
   - new helper for deterministic malformed transport responses:
     - `spawn_one_shot_upstream_incomplete_body(...)`

## Files

- `crates/executor/src/http_utils.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q forward_to_upstream_classifies_incomplete_json_body_as_response_read_failed` — PASS
3. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_classifies_incomplete_json_body_as_response_read_failed` — PASS
4. `cargo test -p copybot-executor -q verify_submit_signature_includes_non_success_http_body_in_reason` — PASS
5. `cargo test -p copybot-executor -q route_executor_action_context_` — PASS
6. `cargo test -p copybot-executor -q` — PASS (519/519)
7. `bash tools/executor_contract_smoke_test.sh` — PASS
