# Refactor Evidence: Phase 3 Slice 6 (`storage.execution_orders`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of copy-signals and execution-order lifecycle methods from `crates/storage/src/lib.rs` into `crates/storage/src/execution_orders.rs`.

Extracted API:
1. `SqliteStore::insert_copy_signal`
2. `SqliteStore::list_copy_signals_by_status`
3. `SqliteStore::list_copy_signals_by_status_with_side_priority`
4. `SqliteStore::update_copy_signal_status`
5. `SqliteStore::execution_order_by_client_order_id`
6. `SqliteStore::insert_execution_order_pending`
7. `SqliteStore::mark_order_simulated`
8. `SqliteStore::mark_order_submitted`
9. `SqliteStore::set_order_attempt`
10. `SqliteStore::mark_order_confirmed`
11. `SqliteStore::mark_order_dropped`
12. `SqliteStore::mark_order_failed`

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/execution_orders.rs`

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
- `crates/storage/src/lib.rs`: raw `2570`, runtime (excl `#[cfg(test)]`) `1709`, cfg-test `861`
- `crates/storage/src/execution_orders.rs`: raw `492`, runtime `492`, cfg-test `0`

## Notes
- Cross-domain transactional methods (`finalize_execution_confirmed_order`, positions/fills flow) intentionally remain in `lib.rs` for the next transactions/fills slice.
- Helper calls (`u64_to_sql_i64`, `parse_non_negative_i64`) are preserved via parent-module access (`super::*`).
