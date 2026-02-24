# Executor Phase 2 Slice 11 Evidence (2026-02-24)

## Scope

Continued modular extraction by moving key-shape validation primitives out of `main.rs` into a shared module and rewiring all consumers.

## Implemented

1. `crates/executor/src/key_validation.rs`:
   1. Added shared validators:
      1. `validate_pubkey_like(...)`
      2. `validate_signature_like(...)`
   2. Added module tests:
      1. `key_validation_accepts_pubkey_and_signature_shapes`
      2. `key_validation_rejects_wrong_lengths`
2. `crates/executor/src/main.rs`:
   1. Removed inline implementations for pubkey/signature shape checks.
   2. Imported shared validators from `key_validation`.
3. `crates/executor/src/send_rpc.rs`:
   1. Switched signature validation import to shared module.
4. `crates/executor/src/submit_transport.rs`:
   1. Switched upstream signature validation to shared module.
5. `tools/executor_contract_smoke_test.sh`:
   1. Added guard test:
      1. `key_validation_accepts_pubkey_and_signature_shapes`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q key_validation_` — PASS
2. `cargo test -p copybot-executor -q validate_signature_like_requires_64_bytes` — PASS
3. `cargo test -p copybot-executor -q submit_transport_extract_rejects_invalid_signature` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
