# Refactor Evidence: Phase 5A Slice 4 (`ingestion.rate_limit`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of rate-limit primitives from `crates/ingestion/src/source.rs` into `crates/ingestion/src/source/rate_limit.rs`.

Extracted primitives:
1. `TokenBucketLimiter` (+ internal `TokenBucketState`)
2. `HeliusEndpoint`

Wiring updates:
1. `source.rs` imports `HeliusEndpoint` / `TokenBucketLimiter` from `source/rate_limit.rs`
2. Existing runtime behavior and call sites remain unchanged (`new`, `acquire`, endpoint limiter usage)

## Files Changed
1. `crates/ingestion/src/source.rs`
2. `crates/ingestion/src/source/rate_limit.rs`
3. `ops/refactor_phase5a_slice4_rate_limit_evidence_2026-02-21.md`

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
- `crates/ingestion/src/source.rs`: raw `3183`, runtime (excl `#[cfg(test)]`) `2695`, cfg-test `488`
- `crates/ingestion/src/source/core.rs`: raw `183`, runtime `183`, cfg-test `0`
- `crates/ingestion/src/source/reorder.rs`: raw `105`, runtime `101`, cfg-test `4`
- `crates/ingestion/src/source/queue.rs`: raw `124`, runtime `124`, cfg-test `0`
- `crates/ingestion/src/source/rate_limit.rs`: raw `68`, runtime `68`, cfg-test `0`

## Notes
- No public API changes in `copybot-ingestion`.
- Limiter/backoff semantics remain unchanged.
