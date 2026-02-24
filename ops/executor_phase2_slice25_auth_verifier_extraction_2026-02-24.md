# Executor Phase 2 Slice 25 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving ingress auth verification logic out of `main.rs` into a dedicated module.

## Implemented

1. `crates/executor/src/auth_verifier.rs`:
   1. Added shared `AuthVerifier` with constructor:
      1. `AuthVerifier::new(bearer_token, hmac_key_id, hmac_secret, hmac_ttl_sec)`
   2. Moved ingress verify path:
      1. Bearer token check,
      2. optional HMAC header envelope validation,
      3. timestamp TTL window check,
      4. nonce replay guard with in-memory TTL map,
      5. deterministic reject-code mapping (`auth_*`, `hmac_*`).
   3. Kept internal header accessor as module-private helper.
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod auth_verifier`
   2. Rewired runtime/test setup to use `AuthVerifier::new(...)`.
   3. Removed duplicated inline `AuthVerifier`/`HmacConfig`/header helper implementations.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added direct auth-verifier guards:
      1. `auth_verifier_rejects_wrong_bearer_token`
      2. `auth_verifier_accepts_correct_bearer_token`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q require_authenticated_mode_fails_closed_by_default` — PASS
2. `cargo test -p copybot-executor -q auth_verifier_rejects_wrong_bearer_token` — PASS
3. `cargo test -p copybot-executor -q auth_verifier_accepts_correct_bearer_token` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
