# Executor Phase 2B Slice 31 Evidence (2026-02-25)

## Scope

1. Optimized HMAC auth guard tests to reduce runtime while preserving assertions.
2. Reduced skew-window replay test duration by lowering TTL and wait interval.
3. Removed sleep from nonce-eviction capacity test by pre-seeding an expired nonce entry directly in cache.

## Files

1. `crates/executor/src/auth_verifier.rs`
2. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q auth_verifier_hmac_keeps_nonce_through_forward_skew_window`
3. `cargo test -p copybot-executor -q auth_verifier_hmac_evicts_expired_nonce_before_capacity_check`
4. `cargo test -p copybot-executor -q auth_verifier_`
5. `bash tools/executor_contract_smoke_test.sh`
