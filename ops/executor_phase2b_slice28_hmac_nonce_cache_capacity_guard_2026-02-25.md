# Executor Phase 2B Slice 28 Evidence (2026-02-25)

## Scope

1. Added configurable HMAC nonce replay-cache capacity guard in `crates/executor/src/auth_verifier.rs`.
2. Added startup/env plumbing for `COPYBOT_EXECUTOR_HMAC_NONCE_CACHE_MAX_ENTRIES` (default `100000`, fail-closed if `0`).
3. Preserved existing HMAC replay semantics and added explicit overflow reject code `hmac_replay_cache_overflow` (`retryable=true`).
4. Updated contract smoke guard pack and runbook docs.

## Files

1. `crates/executor/src/auth_verifier.rs`
2. `crates/executor/src/executor_config_env.rs`
3. `crates/executor/src/main.rs`
4. `crates/executor/src/request_ingress.rs`
5. `tools/executor_contract_smoke_test.sh`
6. `ops/executor_backend_runbook.md`
7. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q auth_verifier_`
3. `cargo test -p copybot-executor -q auth_verifier_hmac_rejects_when_nonce_cache_capacity_reached`
4. `bash tools/executor_contract_smoke_test.sh`
