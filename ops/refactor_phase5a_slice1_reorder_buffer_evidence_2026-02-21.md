# Refactor Evidence: Phase 5A Slice 1 (`ingestion.reorder_buffer`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shared reorder-buffer logic from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/reorder.rs`.

Extracted shared logic:
1. `ReorderBuffer` state holder (`hold_ms`, `max_buffer`, ordered buffer map)
2. `ReorderBuffer::push`
3. `ReorderBuffer::pop_ready`
4. `ReorderBuffer::pop_earliest`
5. `ReorderBuffer::wait_duration`
6. `ReorderRelease` carrier used by both Helius and Yellowstone sources

Wiring updates:
1. `HeliusWsSource` now stores `reorder: ReorderBuffer`
2. `YellowstoneGrpcSource` now stores `reorder: ReorderBuffer`
3. Both sources delegate release/wait behavior to shared `ReorderBuffer` and keep telemetry side effects unchanged

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/reorder.rs`
3. `ops/refactor_phase5a_slice1_reorder_buffer_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `3525`, runtime (excl `#[cfg(test)]`) `3037`, cfg-test `488`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`

## Notes
- Reorder release ordering remains `(slot, arrival_seq, signature)`; behavior for same-slot arrival sequence is unchanged.
- Telemetry update points (`note_reorder_buffer_size`, `push_reorder_hold`, `push_ingestion_lag`) remain in source-specific paths.
