# Refactor Evidence: Phase 3 Slice 3 (`storage.sqlite_retry`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of SQLite retry helpers from `crates/storage/src/lib.rs` into `crates/storage/src/sqlite_retry.rs`.

Extracted API:
1. `SqliteStore::execute_with_retry` (`pub(crate)`)
2. `is_retryable_sqlite_message` (private)
3. `is_retryable_sqlite_error` (`pub(crate)`)
4. `is_retryable_sqlite_anyhow_error` (public re-export from `copybot-storage`)

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/sqlite_retry.rs`

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
- `crates/storage/src/lib.rs`: raw `3545`, runtime (excl `#[cfg(test)]`) `2684`, cfg-test `861`
- `crates/storage/src/sqlite_retry.rs`: raw `65`, runtime `65`, cfg-test `0`

## Notes
- External API compatibility preserved via `pub use sqlite_retry::is_retryable_sqlite_anyhow_error`.
- Retry backoff constants/counters are unchanged and still sourced from `lib.rs`.
