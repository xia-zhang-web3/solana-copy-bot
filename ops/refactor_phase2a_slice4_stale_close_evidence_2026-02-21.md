# Refactor Evidence: Phase 2a Slice 4 (`stale_close`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of stale lot cleanup routine from `crates/app/src/main.rs` into `crates/app/src/stale_close.rs`.

Extracted API:
1. `close_stale_shadow_lots`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/stale_close.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price`
3. `cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing`
4. `cargo test -p copybot-app -q`
5. `cargo test --workspace -q`
6. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4969`, runtime (excl `#[cfg(test)]`) `2830`, cfg-test `2139`
- `crates/app/src/stale_close.rs`: raw `106`, runtime `106`, cfg-test `0`

## Notes
- Stale-close risk-event details and fail-safe semantics are preserved.
- `stale_close` module uses existing constants and `sanitize_json_value` via parent-module wiring.
