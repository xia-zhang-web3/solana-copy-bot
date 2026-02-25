# Executor Phase 2B Slice 21 Evidence (2026-02-25)

## Scope

1. Reduced hot-path idempotency churn by throttling global stale-claim cleanup cadence.
2. Added same-key stale-claim reclaim retry path to preserve correctness when global cleanup is throttled.
3. Added unit coverage for cleanup interval policy and per-key stale reclaim behavior.
4. Updated contract smoke guard pack and roadmap ledger.

## Files

1. `crates/executor/src/idempotency.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q claim_cleanup_interval_sec_clamps_bounds`
3. `cargo test -p copybot-executor -q should_run_claim_cleanup_respects_interval`
4. `cargo test -p copybot-executor -q stale_claim_is_reclaimed_for_same_key_when_global_cleanup_throttled`
5. `bash -n tools/executor_contract_smoke_test.sh`
6. `bash tools/executor_contract_smoke_test.sh`
