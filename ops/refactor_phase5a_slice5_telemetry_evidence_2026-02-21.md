# Refactor Evidence: Phase 5A Slice 5 (`ingestion.telemetry`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of telemetry state and reporting logic from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/telemetry.rs`.

Extracted telemetry components:
1. `IngestionTelemetry` state container
2. `Default` initialization for all telemetry counters/samples
3. telemetry methods:
   - `push_fetch_latency`
   - `push_ingestion_lag`
   - `push_reorder_hold`
   - `note_reorder_buffer_size`
   - `note_parse_rejected`
   - `note_parse_fallback`
   - `maybe_report`
   - `snapshot`
4. parse reject classifier: `classify_parse_reject_reason`

Wiring updates:
1. `source.rs` imports `IngestionTelemetry` from `source/telemetry.rs`
2. test-only import for `classify_parse_reject_reason` retained via `#[cfg(test)]`
3. telemetry counters used directly in runtime loops remain available via `pub(super)` fields (no behavior change)

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/telemetry.rs`
3. `ops/refactor_phase5a_slice5_telemetry_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `2923`, runtime (excl `#[cfg(test)]`) `2433`, cfg-test `490`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`
- `crates/ingestion/src/source/rate_limit.rs`: raw `68`, runtime `68`, cfg-test `0`
- `crates/ingestion/src/source/telemetry.rs`: raw `272`, runtime `272`, cfg-test `0`

## Notes
- Metrics keys and report payload fields are preserved.
- No public API changes in `copybot-ingestion`.
