# Refactor Evidence: Phase 2a Slice 1 (`config_contract`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of execution runtime contract validation from `crates/app/src/main.rs` into `crates/app/src/config_contract.rs`, with explicit sub-validator split required by the plan.

Extracted API:
1. `contains_placeholder_value`
2. `validate_execution_runtime_contract`

Internal split in new module:
1. `validate_execution_mode_contract`
2. `validate_signer_contract`
3. `validate_adapter_contract`
4. `validate_routes_contract`

Moved helper logic:
1. `is_production_env_profile`
2. `is_valid_contract_version_token`
3. `validate_unique_normalized_route_list`
4. `validate_unique_normalized_route_map_keys`
5. `validate_adapter_endpoint_url`
6. `adapter_endpoint_identity`
7. `is_loopback_host`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/config_contract.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`
5. Phase 2a targeted pack:
   1. `cargo test -p copybot-app -q risk_guard_infra_blocks_when_parser_stall_detected`
   2. `cargo test -p copybot-app -q risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold`
   3. `cargo test -p copybot-app -q risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary`
   4. `cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price`
   5. `cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing`
   6. `cargo test -p copybot-app -q risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades`
   7. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_route_price_above_pretrade_cap`
   8. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_duplicate_submit_route_order_after_normalization`
   9. `cargo test -p copybot-app -q validate_execution_runtime_contract_rejects_duplicate_primary_and_fallback_adapter_endpoint`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `5153`, runtime (excl `#[cfg(test)]`) `3014`, cfg-test `2139`
- `crates/app/src/config_contract.rs`: raw `628`, runtime `628`, cfg-test `0`

## Notes
- This slice keeps contract behavior intact while isolating execution config contract validation.
- `main.rs` now wires to `config_contract` via:
  - `use crate::config_contract::{contains_placeholder_value, validate_execution_runtime_contract};`
