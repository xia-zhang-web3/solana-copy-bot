# Refactor Evidence: Phase 3 Slice 7 (`storage.risk_metrics`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of realized/unrealized PnL, drawdown, and rug-rate methods from `crates/storage/src/lib.rs` into `crates/storage/src/risk_metrics.rs`.

Extracted API:
1. `SqliteStore::shadow_open_lots_count`
2. `SqliteStore::shadow_open_notional_sol`
3. `SqliteStore::shadow_realized_pnl_since`
4. `SqliteStore::live_realized_pnl_since`
5. `SqliteStore::live_max_drawdown_since`
6. `SqliteStore::live_unrealized_pnl_sol`
7. `SqliteStore::live_max_drawdown_with_unrealized_since`
8. `SqliteStore::shadow_rug_loss_count_since`
9. `SqliteStore::shadow_rug_loss_rate_recent`

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/risk_metrics.rs`

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
- `crates/storage/src/lib.rs`: raw `2297`, runtime (excl `#[cfg(test)]`) `1436`, cfg-test `861`
- `crates/storage/src/risk_metrics.rs`: raw `280`, runtime `280`, cfg-test `0`

## Notes
- Live unrealized path continues to use reliable pricing via `reliable_token_sol_price_for_live_unrealized`.
- SQL, thresholds, and fail-closed checks in drawdown/rug-rate logic are unchanged.
