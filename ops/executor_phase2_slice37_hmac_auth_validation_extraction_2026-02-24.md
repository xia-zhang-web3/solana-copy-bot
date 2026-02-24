# Executor Phase 2 Slice 37 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by centralizing HMAC auth guard rules in `auth_mode` module.

## Implemented

1. `crates/executor/src/auth_mode.rs`:
   1. added `validate_hmac_auth_config(hmac_key_id, hmac_secret, hmac_ttl_sec)`,
   2. moved fail-closed rules into shared module:
      1. key-id/secret pair must be configured together,
      2. when enabled, TTL must be in `5..=300`.
   3. added direct unit tests for:
      1. partial pair rejection,
      2. TTL out-of-range rejection,
      3. disabled-HMAC bypass behavior.
2. `crates/executor/src/main.rs`:
   1. replaced inline HMAC validation with shared helper call.
3. `tools/executor_contract_smoke_test.sh`:
   1. added `validate_hmac_auth_config_rejects_partial_pair` guard test.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q validate_hmac_auth_config_` — PASS
2. `cargo test -p copybot-executor -q require_authenticated_mode_fails_without_bearer_when_auth_required` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
