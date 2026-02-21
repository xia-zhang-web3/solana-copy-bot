# Refactor Evidence: Phase 3 Slice 8 (`storage.shadow`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shadow-lot and shadow-close lifecycle methods from `crates/storage/src/lib.rs` into `crates/storage/src/shadow.rs`.

Extracted API:
1. `SqliteStore::insert_shadow_lot`
2. `SqliteStore::list_shadow_lots`
3. `SqliteStore::list_open_shadow_lots_older_than`
4. `SqliteStore::has_shadow_lots`
5. `SqliteStore::list_shadow_open_pairs`
6. `SqliteStore::close_shadow_lots_fifo_atomic`
7. `SqliteStore::update_shadow_lot`
8. `SqliteStore::delete_shadow_lot`
9. `SqliteStore::insert_shadow_closed_trade`

Private helper moved with identical logic:
- `SqliteStore::close_shadow_lots_fifo_atomic_once`

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/shadow.rs`
3. `ops/refactor_phase3_slice8_shadow_evidence_2026-02-21.md`

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
- `crates/storage/src/lib.rs`: raw `1939`, runtime (excl `#[cfg(test)]`) `1078`, cfg-test `861`
- `crates/storage/src/shadow.rs`: raw `371`, runtime `371`, cfg-test `0`

## Notes
- Retry/backoff and SQLite contention accounting in shadow FIFO close flow remain unchanged.
- SQL text, transaction boundaries (`BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`), and error/context messages are preserved.
