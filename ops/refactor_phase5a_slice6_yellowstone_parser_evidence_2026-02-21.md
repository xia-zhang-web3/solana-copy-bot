# Refactor Evidence: Phase 5A Slice 6 (`ingestion.yellowstone_parser`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of Yellowstone subscribe/parse helpers from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/yellowstone.rs`.

Extracted helpers:
1. `build_yellowstone_subscribe_request`
2. `parse_yellowstone_update`
3. proto transaction parsing helpers and balance inference helpers:
   - `parse_yellowstone_transaction_update`
   - `decode_signature_from_proto`
   - `proto_account_keys`
   - `extract_program_ids_from_proto`
   - `decode_program_id_from_compiled_instruction`
   - `decode_program_id_from_inner_instruction`
   - `infer_swap_from_proto_balances`
   - `parse_proto_ui_amount`
   - `signer_sol_delta_from_proto`

Wiring updates:
1. `source.rs` imports runtime helpers from `source/yellowstone.rs`
2. test-only import for `infer_swap_from_proto_balances` retained via `#[cfg(test)]`
3. Yellowstone stream loop call sites remain unchanged semantically

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/yellowstone.rs`
3. `ops/refactor_phase5a_slice6_yellowstone_parser_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `2579`, runtime (excl `#[cfg(test)]`) `2085`, cfg-test `494`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`
- `crates/ingestion/src/source/rate_limit.rs`: raw `68`, runtime `68`, cfg-test `0`
- `crates/ingestion/src/source/telemetry.rs`: raw `272`, runtime `272`, cfg-test `0`
- `crates/ingestion/src/source/yellowstone.rs`: raw `359`, runtime `359`, cfg-test `0`

## Notes
- Subscribe request payload and parser fail-closed behavior are preserved.
- No public API changes in `copybot-ingestion`.
