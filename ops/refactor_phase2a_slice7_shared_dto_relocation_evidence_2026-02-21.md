# Refactor Evidence: Phase 2a Slice 7 (`shared DTO relocation`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move shared DTOs/outcomes ownership from `copybot-storage` to `copybot-core-types` while preserving `copybot-storage` compatibility re-exports.

Moved DTOs/outcomes:
1. `WalletMetricRow`
2. `WalletUpsertRow`
3. `CopySignalRow`
4. `ExecutionOrderRow`
5. `InsertExecutionOrderPendingOutcome`
6. `FinalizeExecutionConfirmOutcome`
7. `TokenQualityCacheRow`
8. `TokenQualityRpcRow`

Compatibility:
- `copybot-storage` now re-exports all moved types via `pub use copybot_core_types::{...}`.
- Existing external imports from `copybot-storage` remain valid (no behavior change).

## Files Changed
1. `crates/core-types/src/lib.rs`
2. `crates/storage/src/lib.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test --workspace -q`
3. `tools/ops_scripts_smoke_test.sh`
4. `cargo test -p copybot-app -q risk_guard_infra_blocks_when_parser_stall_detected`
5. `cargo test -p copybot-app -q risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold`
6. `cargo test -p copybot-app -q risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary`
7. `cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price`
8. `cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing`
9. `cargo test -p copybot-app -q risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades`
10. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_route_price_above_pretrade_cap`
11. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_duplicate_submit_route_order_after_normalization`
12. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_duplicate_primary_and_fallback_adapter_endpoint`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/core-types/src/lib.rs`: raw `136`, runtime `136`, cfg-test `0`
- `crates/storage/src/lib.rs`: raw `3712`, runtime (excl `#[cfg(test)]`) `2851`, cfg-test `861`

## Notes
- This slice is ownership isolation only; runtime semantics unchanged.
- Re-export layer in `copybot-storage` preserves API stability for `discovery`, `shadow`, and `execution` call sites.
