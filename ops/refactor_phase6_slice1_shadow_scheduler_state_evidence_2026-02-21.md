# Refactor Evidence: Phase 6 Slice 1 (`app.shadow_scheduler_state`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move-only introduction of a `ShadowScheduler` state holder in `crates/app/src/main.rs`.

Changes:
1. Added `ShadowScheduler` struct with grouped shadow loop state fields:
   - `shadow_workers`
   - `shadow_snapshot_handle`
   - `pending_shadow_tasks`
   - `pending_shadow_task_count`
   - `ready_shadow_keys`
   - `ready_shadow_key_set`
   - `inflight_shadow_keys`
   - `shadow_queue_backpressure_active`
   - `shadow_scheduler_needs_reset`
   - `held_shadow_sells`
   - `shadow_holdback_counts`
2. Added `ShadowScheduler::new()` constructor with previous default initializations.
3. In `run_app_loop`, replaced local shadow-state variable block with `let mut shadow_scheduler = ShadowScheduler::new();`.
4. Rewired loop references from standalone locals to `shadow_scheduler.<field>`.

Non-goals in this slice (left for Phase 6 slice 2/3):
1. No helper signature changes to `&mut ShadowScheduler` yet.
2. No queue helper relocation into `impl ShadowScheduler` yet.

## Files Changed
1. `crates/app/src/main.rs`
2. `ops/refactor_phase6_slice1_shadow_scheduler_state_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4850`, runtime (excl `#[cfg(test)]`) `2711`, cfg-test `2139`

## Notes
- This slice is state-packaging/wiring only; queue/risk semantics are preserved.
- Designed to reduce mutable-local sprawl ahead of signature consolidation in next sub-slice.
