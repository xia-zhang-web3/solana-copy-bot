# Refactor Evidence: Phase 3 Slice 1 (`storage.migrations`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of storage migration logic from `crates/storage/src/lib.rs` into `crates/storage/src/migrations.rs`.

Extracted API:
1. `SqliteStore::run_migrations`
2. `SqliteStore::read_migration_files` (private helper)

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/migrations.rs`

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
- `crates/storage/src/lib.rs`: raw `3643`, runtime (excl `#[cfg(test)]`) `2782`, cfg-test `861`
- `crates/storage/src/migrations.rs`: raw `78`, runtime `78`, cfg-test `0`

## Notes
- Migration SQL flow and error messages are preserved.
- This slice is structural only (method move to module + wiring).
