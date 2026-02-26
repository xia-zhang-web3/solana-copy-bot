# Executor Phase 2B — Slice 170

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering chain changes).

## Scope

- improve `send_rpc` error-payload classifier parity for providers that encode retryable signal in string-valued `error.data`.

## Changes

1. Runtime (`crates/executor/src/send_rpc.rs`):
   - `extract_send_rpc_error_text` now also includes `error.data` when it is a non-empty string.
   - existing structured extraction remains:
     - `message`
     - `data.message`
     - `data.details`
     - `data.err`
2. Unit coverage (`crates/executor/src/send_rpc.rs`):
   - added:
     - `classify_send_rpc_error_payload_uses_string_data_for_retryable`
     - `extract_send_rpc_error_text_reads_string_data_field`
3. Integration coverage (`crates/executor/src/main.rs`):
   - added:
     - `send_signed_transaction_via_rpc_uses_string_data_for_retryable_classification`
   - verifies fallback success when retryable signal comes from `error.data: "temporarily unavailable"`.
4. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered all new guards.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 330.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q classify_send_rpc_error_payload_uses_string_data_for_retryable` — PASS
   - `cargo test -p copybot-executor -q extract_send_rpc_error_text_reads_string_data_field` — PASS
   - `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_uses_string_data_for_retryable_classification` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- send-rpc retryability classification now remains available for providers using string-only `error.data`, without re-opening broad full-payload substring matching.
