# Refactor Evidence: Phase 5A Slice 8 (`ingestion.helius_parser`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of Helius parsing helpers from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/helius_parser.rs`.

Extracted runtime functions:
1. `parse_logs_notification`
2. `extract_account_keys`
3. `extract_program_ids`
4. `extract_program_id_from_log`
5. `infer_swap_from_json_balances`
6. `signer_sol_delta`
7. `dominant_non_sol_leg`
8. `parse_ui_amount_json`
9. `value_to_string_vec`
10. `detect_dex_hint`

Wiring updates:
1. `source.rs` declares `mod helius_parser;`
2. Call sites continue to use `HeliusWsSource::{...}` unchanged
3. No API widening (`pub(super)` helpers only)

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/helius_parser.rs`
3. `ops/refactor_phase5a_slice8_helius_parser_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-ingestion -q reorder_releases_oldest_slot_signature`
3. `cargo test -p copybot-ingestion -q reorder_uses_arrival_sequence_within_same_slot`
4. `cargo test -p copybot-ingestion -q notification_queue_drop_oldest_keeps_freshest_items`
5. `cargo test -p copybot-app -q risk_guard_infra_no_progress_does_not_block_when_grpc_transaction_updates_advance`
6. `cargo test -p copybot-app -q risk_guard_infra_no_progress_still_blocks_when_only_grpc_ping_total_advances`
7. `cargo test -p copybot-ingestion -q`
8. `cargo test --workspace -q`
9. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/ingestion/src/source.rs`: raw `1706`, runtime (excl `#[cfg(test)]`) `1212`, cfg-test `494`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`
- `crates/ingestion/src/source/rate_limit.rs`: raw `68`, runtime `68`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/telemetry.rs`: raw `272`, runtime `272`, cfg-test `0`
- `crates/ingestion/src/source/yellowstone.rs`: raw `359`, runtime `359`, cfg-test `0`
- `crates/ingestion/src/source/helius_pipeline.rs`: raw `582`, runtime `582`, cfg-test `0`
- `crates/ingestion/src/source/helius_parser.rs`: raw `326`, runtime `326`, cfg-test `0`

## Notes
- Helius parse/reject behavior and DEX-hint inference paths are preserved.
- No public API changes in `copybot-ingestion`.
