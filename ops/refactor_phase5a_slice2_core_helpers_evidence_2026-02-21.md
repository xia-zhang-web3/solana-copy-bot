# Refactor Evidence: Phase 5A Slice 2 (`ingestion.core_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shared ingestion helper functions from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/core.rs`.

Extracted helpers:
1. seen-signature helpers (`is_seen_signature`, `mark_seen_signature`, `prune_seen_signatures`)
2. fallback/normalization helper (`normalize_program_ids_or_fallback`)
3. retry/backoff helpers (`parse_retry_after`, `compute_retry_delay`, `sleep_with_backoff`)
4. throughput helper (`effective_per_endpoint_rps_limit`)
5. telemetry/stat helpers (`push_sample`, `percentile`, `increment_atomic_usize`, `decrement_atomic_usize`)

Wiring updates:
1. `source.rs` imports helpers from `source/core.rs`
2. call sites remain unchanged semantically (same names, same arguments, same control flow)

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/core.rs`
3. `ops/refactor_phase5a_slice2_core_helpers_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `3361`, runtime (excl `#[cfg(test)]`) `2873`, cfg-test `488`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`

## Notes
- This slice is move-only: helper code moved as-is to `core.rs` and re-imported.
- No public API changes in `copybot-ingestion`.
