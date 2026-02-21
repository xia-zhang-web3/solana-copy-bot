# Refactor Evidence: Phase 2a Slice 5 (`task_spawns`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of task spawn helpers from `crates/app/src/main.rs` into `crates/app/src/task_spawns.rs`.

Extracted API:
1. `spawn_discovery_task`
2. `spawn_shadow_snapshot_task`
3. `spawn_execution_task`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/task_spawns.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4924`, runtime (excl `#[cfg(test)]`) `2785`, cfg-test `2139`
- `crates/app/src/task_spawns.rs`: raw `55`, runtime `55`, cfg-test `0`

## Notes
- Task spawn behavior is preserved exactly (same SQLite open paths, same discovery/shadow/execution calls, same error strings).
- Extraction keeps visibility at `pub(crate)` and retains existing call sites in `run_app_loop`.
