# Refactor Evidence: Phase 8 Slice 8 (`discovery.followlist`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of followlist ranking/selection helpers from `crates/discovery/src/lib.rs` into `crates/discovery/src/followlist.rs`.

Moved helpers:
1. `rank_follow_candidates`
2. `desired_wallets`
3. `top_wallet_labels`
4. internal comparator `cmp_score_then_trades`

Wiring:
1. Added `mod followlist;` and helper imports in `lib.rs`.
2. Replaced inline ranking/top-wallet logic in `run_cycle` with calls to moved helpers.

Behavior scope:
- No scoring threshold changes (`min_score` and `follow_top_n` unchanged).
- Same sort precedence (score desc -> trades desc -> wallet_id asc).
- Same top-wallet output format.

## Files Changed
1. `crates/discovery/src/lib.rs`
2. `crates/discovery/src/followlist.rs`
3. `ops/refactor_phase8_slice8_discovery_followlist_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-discovery -q`
3. `cargo test -p copybot-shadow -q`
4. `cargo test --workspace -q`
5. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/discovery/src/lib.rs`: raw `718`, runtime (excl `#[cfg(test)]`) `606`, cfg-test `112`
- `crates/discovery/src/followlist.rs`: raw `43`, runtime `43`, cfg-test `0`

## Notes
- This slice closes planned Phase 8 discovery target module `followlist.rs`.
- Followlist persistence path in `run_cycle` remains unchanged (`store.persist_discovery_cycle` contract preserved).
