# Refactor Evidence: Phase 3 Slice 5 (`storage.discovery`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of discovery/followlist methods from `crates/storage/src/lib.rs` into `crates/storage/src/discovery.rs`.

Extracted API:
1. `SqliteStore::upsert_wallet`
2. `SqliteStore::insert_wallet_metric`
3. `SqliteStore::persist_discovery_cycle`
4. `SqliteStore::list_active_follow_wallets`
5. `SqliteStore::was_wallet_followed_at`
6. `SqliteStore::deactivate_follow_wallet`
7. `SqliteStore::activate_follow_wallet`
8. `SqliteStore::reconcile_followlist`

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/discovery.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-storage -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`
5. `cargo test -p copybot-storage -q live_unrealized`
6. `cargo test -p copybot-storage -q live_unrealized_pnl_sol_ignores_micro_swap_outlier_price`
7. `cargo test -p copybot-storage -q live_unrealized_pnl_sol_counts_missing_when_only_micro_quotes_exist`
8. `cargo test -p copybot-storage -q finalize_execution_confirmed_order_is_atomic_and_idempotent`
9. `cargo test -p copybot-storage -q finalize_execution_confirmed_order_accounts_for_fee_in_cost_and_pnl`
10. `cargo test -p copybot-storage -q persist_discovery_cycle_keeps_only_latest_wallet_metric_windows`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/storage/src/lib.rs`: raw `3052`, runtime (excl `#[cfg(test)]`) `2191`, cfg-test `861`
- `crates/storage/src/discovery.rs`: raw `341`, runtime `341`, cfg-test `0`

## Notes
- `persist_discovery_cycle` transaction boundaries and retention logic are unchanged.
- Followlist reconciliation semantics are unchanged (structural extraction only).
