# Refactor Evidence: Phase 5A Slice 3 (`ingestion.queue_primitives`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of queue primitives from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/queue.rs`.

Extracted primitives:
1. `QueueOverflowPolicy` (+ `parse`/`as_str`)
2. `QueuePushResult`
3. `OverflowQueue<T>` and internal queue state
4. queue operations: `new`, `push`, `pop`, `close`

Wiring updates:
1. `source.rs` imports queue primitives from `source/queue.rs`
2. Existing queue aliases remain in `source.rs`:
   - `NotificationQueue = OverflowQueue<LogsNotification>`
   - `RawObservationQueue = OverflowQueue<FetchedObservation>`

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/queue.rs`
3. `ops/refactor_phase5a_slice3_queue_primitives_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `3245`, runtime (excl `#[cfg(test)]`) `2757`, cfg-test `488`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`

## Notes
- Queue semantics are unchanged (`block` vs `drop_oldest` policy behavior preserved).
- No public API changes in `copybot-ingestion`.
