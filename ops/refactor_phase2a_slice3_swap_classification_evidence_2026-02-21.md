# Refactor Evidence: Phase 2a Slice 3 (`swap_classification`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of swap side/key classification helpers from `crates/app/src/main.rs` into `crates/app/src/swap_classification.rs`.

Extracted API:
1. `classify_swap_side`
2. `shadow_task_key_for_swap`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/swap_classification.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `5065`, runtime (excl `#[cfg(test)]`) `2926`, cfg-test `2139`
- `crates/app/src/swap_classification.rs`: raw `27`, runtime `27`, cfg-test `0`

## Notes
- `ShadowSwapSide` and `ShadowTaskKey` remain in `main.rs`; module functions operate on these types via `super` wiring only.
- No behavior changes intended; this is a pure extraction slice.
