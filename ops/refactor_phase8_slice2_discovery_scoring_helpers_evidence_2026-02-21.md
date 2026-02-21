# Refactor Evidence: Phase 8 Slice 2 (`discovery.scoring_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of pure scoring helper functions from `crates/discovery/src/lib.rs` into `crates/discovery/src/scoring.rs`.

Moved functions (logic preserved 1:1):
1. `tanh01`
2. `hold_time_quality_score`
3. `median_i64`

Wiring changes:
1. Added `mod scoring;`
2. Added `use self::scoring::{hold_time_quality_score, median_i64, tanh01};`
3. Removed duplicated local definitions from `lib.rs`.

## Files Changed
1. `crates/discovery/src/lib.rs`
2. `crates/discovery/src/scoring.rs`
3. `ops/refactor_phase8_slice2_discovery_scoring_helpers_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-discovery -q`
3. `cargo test -p copybot-shadow -q`
4. `cargo test --workspace -q`
5. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## Notes
- During extraction, scoring semantics were preserved exactly (piecewise `hold_time_quality_score` retained) to avoid behavior drift.
- This slice is pure helper relocation with unchanged call graph at runtime.
