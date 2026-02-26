# Executor Phase 2B — Slice 158

Date: 2026-02-26  
Owner: execution-dev

## Scope

- extend integration coverage for full submit chain when upstream returns `signed_tx_base64` and executor performs `send_rpc` plus signature visibility verification.

## Changes

1. Added integration test in `crates/executor/src/main.rs`:
   - `handle_submit_uses_send_rpc_and_records_seen_signature_verification_when_enabled`
   - Flow: upstream success (`signed_tx_base64`) -> send-rpc success (`result` signature) -> submit-verify success row (`confirmationStatus=confirmed`) -> final submit success payload.
   - Assertions include:
     - `submit_transport=adapter_send_rpc`
     - `submit_signature_verify.enabled=true`
     - `submit_signature_verify.seen=true`
     - `submit_signature_verify.confirmation_status=confirmed`
2. Added integration test in `crates/executor/src/main.rs`:
   - `handle_submit_rejects_after_send_rpc_when_signature_verify_strict_unseen`
   - Flow: upstream success (`signed_tx_base64`) -> send-rpc success -> submit-verify returns pending row (`null`) under strict mode -> retryable reject.
   - Assertions include:
     - `code=upstream_submit_signature_unseen`
     - detail includes `reason=signature status pending`
3. Registered both tests in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap item 318 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q handle_submit_uses_send_rpc_and_records_seen_signature_verification_when_enabled` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_after_send_rpc_when_signature_verify_strict_unseen` — PASS
4. `cargo test -p copybot-executor -q` — PASS (`572/572`)
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- Full-chain submit behavior (`signed_tx_base64 -> send_rpc -> submit_verify`) now has explicit integration guards for both success and strict-unseen reject outcomes.
