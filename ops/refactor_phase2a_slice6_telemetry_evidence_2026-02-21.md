# Refactor Evidence: Phase 2a Slice 6 (`telemetry`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of telemetry-oriented helpers from `crates/app/src/main.rs` into `crates/app/src/telemetry.rs`.

Extracted API:
1. `reason_to_key`
2. `reason_to_stage`
3. `format_error_chain`
4. `record_shadow_queue_full_buy_drop`
5. `record_shadow_queue_full_sell_outcome`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/telemetry.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4828`, runtime (excl `#[cfg(test)]`) `2689`, cfg-test `2139`
- `crates/app/src/telemetry.rs`: raw `103`, runtime `103`, cfg-test `0`

## Notes
- Telemetry keys/stages and queue-full outcome counters are preserved byte-for-byte.
- Structured logging fields for queue-full drop/keep paths are unchanged.
