# Executor Phase 2B — Slice 166

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (unchanged by this slice).

## Scope

- harden `send_rpc` error-payload classifier to avoid retryable false-positives from unstructured JSON fields.

## Changes

1. Runtime (`crates/executor/src/send_rpc.rs`):
   - added `extract_send_rpc_error_text(error_payload)`:
     - reads only structured textual fields used for classifier semantics:
       - `message`
       - `data.message`
       - `data.details`
       - `data.err`
     - ignores opaque/unstructured fields (for example `data.raw_payload` blobs).
   - `classify_send_rpc_error_payload` now classifies from the structured extracted text instead of full `error_payload.to_string()`.
2. Unit coverage (`crates/executor/src/send_rpc.rs`):
   - added:
     - `classify_send_rpc_error_payload_ignores_timeout_marker_in_unstructured_data`
     - `classify_send_rpc_error_payload_uses_structured_data_message_for_retryable`
     - `extract_send_rpc_error_text_reads_message_and_structured_data_fields`
3. Integration coverage (`crates/executor/src/main.rs`):
   - added:
     - `send_signed_transaction_via_rpc_ignores_timeout_marker_in_unstructured_error_data`
     - `send_signed_transaction_via_rpc_uses_data_message_for_retryable_classification`
4. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered all 5 new guards.
5. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 326.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests (`classify_send_rpc_error_payload_*`, `extract_send_rpc_error_text_*`, `send_signed_transaction_via_rpc_*unstructured*`, `send_signed_transaction_via_rpc_*data_message*`) — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- `send_rpc` retryability classification is now tied to explicit structured diagnostics, reducing false retry/fallback on payload-noise substrings while preserving intended retry behavior for structured provider messages.
