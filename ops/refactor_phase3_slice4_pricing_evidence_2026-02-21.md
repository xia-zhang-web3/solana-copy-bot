# Refactor Evidence: Phase 3 Slice 4 (`storage.pricing`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of pricing methods from `crates/storage/src/lib.rs` into `crates/storage/src/pricing.rs`.

Extracted API:
1. `SqliteStore::latest_token_sol_price`
2. `SqliteStore::reliable_token_sol_price_for_stale_close`
3. `SqliteStore::reliable_token_sol_price_for_live_unrealized` (`pub(crate)`)
4. `SqliteStore::reliable_token_sol_price` (private helper)

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/pricing.rs`

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
- `crates/storage/src/lib.rs`: raw `3382`, runtime (excl `#[cfg(test)]`) `2521`, cfg-test `861`
- `crates/storage/src/pricing.rs`: raw `176`, runtime `176`, cfg-test `0`

## Notes
- Stale-close and live-unrealized reliable pricing behavior is preserved.
- SQL, sample gates, and median aggregation logic are unchanged (structural extraction only).
