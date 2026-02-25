# Executor Phase 2B Slice 25 Evidence (2026-02-25)

## Scope

1. Added shared SQLite cleanup marker for claim cleanup cadence across multiple executor processes using the same idempotency DB.
2. Global cleanup (`DELETE stale claims`) now runs only when both local and DB-level cadence windows require it.
3. Existing same-key stale-claim reclaim fallback remains intact to avoid stale conflicts even when global cleanup is throttled.
4. Added cross-store test coverage for marker behavior.

## Files

1. `crates/executor/src/idempotency.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q claim_cleanup_interval_sec_clamps_bounds`
3. `cargo test -p copybot-executor -q should_run_claim_cleanup_respects_interval`
4. `cargo test -p copybot-executor -q stale_claim_is_reclaimed_for_same_key_when_global_cleanup_throttled`
5. `cargo test -p copybot-executor -q global_cleanup_marker_prevents_redundant_cleanup_across_store_instances`
6. `bash -n tools/executor_contract_smoke_test.sh`
7. `bash tools/executor_contract_smoke_test.sh`
