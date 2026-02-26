# Executor Phase 2B — Slice 164

Date: 2026-02-26  
Owner: execution-dev

## Scope

- eliminate residual gap where truncated response body could be accepted when truncated prefix is still valid JSON.

## Changes

1. Runtime (`crates/executor/src/upstream_forward.rs`):
   - added pre-parse `body_read.was_truncated` guard after read-error check.
   - truncated success body now classifies as retryable `upstream_response_too_large` and participates in fallback chain before any JSON parse.
2. Runtime (`crates/executor/src/send_rpc.rs`):
   - added pre-parse `body_read.was_truncated` guard.
   - truncated success body now classifies as retryable `send_rpc_response_too_large` with fallback traversal before parse.
3. Runtime (`crates/executor/src/submit_verify.rs`):
   - added pre-parse `body_read.was_truncated` guard to set `last_reason=response_too_large` and continue retry loop.
4. Integration tests (`crates/executor/src/main.rs`):
   - strengthened truncated-success fallback tests to use *valid JSON prefix + oversized whitespace tail* (would parse successfully without this fix):
     - `forward_to_upstream_uses_fallback_after_primary_truncated_success_body`
     - `send_signed_transaction_via_rpc_uses_fallback_after_primary_truncated_success_body`
   - added verify-path guard:
     - `verify_submit_signature_classifies_truncated_valid_json_prefix_as_response_too_large`
   - added helper `build_truncated_valid_json_prefix_body`.
5. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered verify guard test above.
6. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 324.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests (`truncated_success_body`, `truncated_valid_json_prefix`) — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- truncated success-body handling is now fail-closed *before* JSON parse across forward/send-rpc/verify paths.
- truncated-but-valid JSON prefixes can no longer pass as success outcomes.
