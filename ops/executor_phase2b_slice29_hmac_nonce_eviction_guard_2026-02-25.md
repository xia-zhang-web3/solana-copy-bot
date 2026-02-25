# Executor Phase 2B Slice 29 Evidence (2026-02-25)

## Scope

1. Added explicit HMAC replay-cache TTL-eviction guard test in `crates/executor/src/auth_verifier.rs`.
2. Ensured contract smoke tracks this branch to prevent regressions around nonce-cache saturation and stale entry cleanup.

## Files

1. `crates/executor/src/auth_verifier.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q auth_verifier_hmac_evicts_expired_nonce_before_capacity_check`
3. `cargo test -p copybot-executor -q auth_verifier_`
4. `bash tools/executor_contract_smoke_test.sh`
