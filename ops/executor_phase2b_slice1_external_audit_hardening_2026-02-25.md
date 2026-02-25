# Executor Phase 2B Slice 1 Evidence (2026-02-25)

## Scope

Address new external-auditor findings on claim TTL safety, idempotency key normalization, HMAC replay/signature handling, and submit identity ordering.

## Implemented

1. Claim TTL budget hardening:
   1. `min_claim_ttl_sec_for_submit_path` now takes `submit_total_budget_ms`.
   2. Derived TTL now uses `max(hop_budget_estimate, submit_total_budget_ms + safety_padding)`.
   3. Startup guard in `executor_config_env.rs` updated to pass `submit_total_budget_ms`.
2. Idempotency key normalization consistency:
   1. `load_submit_response` now trims and validates non-empty `client_order_id`.
   2. `store_submit_response` now trims/validates `client_order_id` and `request_id` before write.
   3. Added test `idempotency_normalizes_client_order_id_for_store_and_lookup`.
3. HMAC verification hardening:
   1. Signature payload now uses exact raw bytes (`build_hmac_payload_bytes`) instead of `String::from_utf8_lossy`.
   2. Nonce is now inserted into replay cache only after successful signature verification.
   3. Nonce expiry now aligns with accepted timestamp skew window (`timestamp + max_skew`).
   4. Added tests:
      1. `auth_verifier_hmac_accepts_valid_signature_and_detects_replay`
      2. `auth_verifier_hmac_invalid_signature_does_not_burn_nonce`
      3. `auth_verifier_hmac_keeps_nonce_through_forward_skew_window`
4. Post-submit identity gap hardening:
   1. `validate_submit_response_request_identity` moved before send-rpc/submit-verify stages.
   2. Added test `handle_submit_rejects_request_id_mismatch_before_send_rpc`.
5. Smoke guard pack updated with all new targeted tests in `tools/executor_contract_smoke_test.sh`.

## Effect

1. Claim lock window cannot be shorter than configured submit deadline budget.
2. `client_order_id` idempotency behavior is consistent for whitespace-variant inputs.
3. Invalid HMAC signatures no longer poison nonce cache; replay window covers forward-skew edge.
4. Upstream identity mismatch is fail-closed before any additional transaction send/verify activity.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q auth_verifier_hmac_accepts_valid_signature_and_detects_replay` — PASS
3. `cargo test -p copybot-executor -q auth_verifier_hmac_invalid_signature_does_not_burn_nonce` — PASS
4. `cargo test -p copybot-executor -q auth_verifier_hmac_keeps_nonce_through_forward_skew_window` — PASS
5. `cargo test -p copybot-executor -q idempotency_normalizes_client_order_id_for_store_and_lookup` — PASS
6. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_respects_submit_total_budget_floor` — PASS
7. `cargo test -p copybot-executor -q handle_submit_rejects_request_id_mismatch_before_send_rpc` — PASS
8. `bash tools/executor_contract_smoke_test.sh` — PASS
