# Executor Phase 2B Slice 135 — Auth Key-ID Constant-Time + Bool Guard Matrix Polish (2026-02-26)

## Scope

- close auth timing-hardening gap on HMAC key-id mismatch check
- close residual strict-bool test gaps for runtime env parsing coverage

## Changes

1. Auth verifier hardening (`crates/executor/src/auth_verifier.rs`)
   - replaced variable-time key-id comparison (`key_id != hmac.key_id`) with `constant_time_eq(key_id.as_bytes(), hmac.key_id.as_bytes())`
   - added guard test `auth_verifier_hmac_rejects_key_id_mismatch`
2. Bool strict-parser coverage polish (`crates/executor/src/executor_config_env.rs`)
   - added `executor_config_from_env_rejects_invalid_allow_unauthenticated_token`
   - added `executor_config_from_env_rejects_invalid_submit_verify_strict_token`
3. Contract smoke registry update (`tools/executor_contract_smoke_test.sh`)
   - registered new auth + bool guard tests
4. Ledger update (`ROAD_TO_PRODUCTION.md`)
   - added item `295`.

## Files

- `crates/executor/src/auth_verifier.rs`
- `crates/executor/src/executor_config_env.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q auth_verifier_hmac_rejects_key_id_mismatch` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_invalid_allow_unauthenticated_token` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_invalid_submit_verify_strict_token` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
