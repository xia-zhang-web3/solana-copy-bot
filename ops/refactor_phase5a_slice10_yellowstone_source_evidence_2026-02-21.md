# Refactor Evidence: Phase 5A Slice 10 (`ingestion.yellowstone_source`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of `YellowstoneGrpcSource` lifecycle/reorder wiring from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/yellowstone_source.rs`.

Extracted runtime methods:
1. `new`
2. `next_observation`
3. `ensure_pipeline_running`
4. `spawn_pipeline`
5. `recv_from_pipeline`
6. `push_reorder_entry`
7. `pop_ready_observation`
8. `pop_earliest_observation`
9. `reorder_wait_duration`
10. `apply_reorder_release`
11. `maybe_report_pipeline_metrics`
12. `runtime_snapshot`

Wiring updates:
1. `source.rs` declares `mod yellowstone_source;`
2. Existing call sites (`IngestionSource::next_observation`, runtime snapshot paths) remain unchanged
3. `yellowstone_source` reuses parent private types/enums (`YellowstonePipeline`, `YellowstoneRecvOutcome`) without API widening

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/yellowstone_source.rs`
3. `ops/refactor_phase5a_slice10_yellowstone_source_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `1153`, runtime (excl `#[cfg(test)]`) `654`, cfg-test `499`
- `crates/ingestion/src/source/yellowstone_source.rs`: raw `258`, runtime `258`, cfg-test `0`
- `crates/ingestion/src/source/yellowstone_pipeline.rs`: raw `331`, runtime `331`, cfg-test `0`
- `crates/ingestion/src/source/helius_pipeline.rs`: raw `584`, runtime `584`, cfg-test `0`
- `crates/ingestion/src/source/helius_parser.rs`: raw `326`, runtime `326`, cfg-test `0`
- `crates/ingestion/src/source/yellowstone.rs`: raw `359`, runtime `359`, cfg-test `0`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`
- `crates/ingestion/src/source/rate_limit.rs`: raw `68`, runtime `68`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/telemetry.rs`: raw `272`, runtime `272`, cfg-test `0`

## Notes
- Yellowstone preflight config guards, reconnect/backoff behavior, and queue overflow semantics are preserved.
- `source.rs` runtime LOC is now below soft target (`654` < `800`).
