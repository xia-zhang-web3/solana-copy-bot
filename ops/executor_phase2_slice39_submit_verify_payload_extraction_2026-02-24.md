# Executor Phase 2 Slice 39 Evidence (2026-02-24)

## Scope

Continued submit-pipeline decomposition by moving submit verify JSON serialization into a dedicated payload module.

## Implemented

1. Added `crates/executor/src/submit_verify_payload.rs`:
   1. `submit_signature_verification_to_json(...)`
   2. direct tests for `Seen` and `Unseen` payload shapes.
2. Updated `crates/executor/src/submit_verify.rs`:
   1. removed serializer function (kept verification logic only).
3. Updated `crates/executor/src/main.rs`:
   1. imports serializer from new payload module.
4. Updated `tools/executor_contract_smoke_test.sh`:
   1. includes `submit_verify_payload_serializes_seen_shape`.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q submit_verify_payload_` — PASS
2. `cargo test -p copybot-executor -q verify_submit_signature_seen_when_rpc_reports_confirmation` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
