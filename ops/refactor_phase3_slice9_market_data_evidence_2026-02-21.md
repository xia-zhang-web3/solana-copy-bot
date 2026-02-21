# Refactor Evidence: Phase 3 Slice 9 (`storage.market_data`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of observed-swaps and token-quality market-data methods from `crates/storage/src/lib.rs` into `crates/storage/src/market_data.rs`.

Extracted API:
1. `SqliteStore::insert_observed_swap`
2. `SqliteStore::load_observed_swaps_since`
3. `SqliteStore::for_each_observed_swap_since`
4. `SqliteStore::list_unique_sol_buy_mints_since`
5. `SqliteStore::token_market_stats`
6. `SqliteStore::get_token_quality_cache`
7. `SqliteStore::upsert_token_quality_cache`
8. `SqliteStore::fetch_token_quality_from_helius`

Private helpers moved with identical behavior:
1. `SqliteStore::row_to_swap_event`
2. `rpc_result`
3. `post_helius_json`
4. `fetch_token_holders`
5. `fetch_token_age_seconds`

## Files Changed
1. `crates/storage/src/lib.rs`
2. `crates/storage/src/market_data.rs`
3. `ops/refactor_phase3_slice9_market_data_evidence_2026-02-21.md`

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
- `crates/storage/src/lib.rs`: raw `1483`, runtime (excl `#[cfg(test)]`) `620`, cfg-test `863`
- `crates/storage/src/market_data.rs`: raw `453`, runtime `453`, cfg-test `0`

## Notes
- `execution_orders` shared helpers (`u64_to_sql_i64`, `parse_non_negative_i64`) remain in `lib.rs` unchanged.
- SQL queries, JSON-RPC request/response handling, and token-age/holders parsing paths were moved without behavioral edits.
