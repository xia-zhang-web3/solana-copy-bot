# Refactor Evidence: Phase 6 Slice 3 (`app.shadow_scheduler_impl`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Queue/scheduler helper consolidation into `impl ShadowScheduler` in `crates/app/src/main.rs`.

Moved into `impl ShadowScheduler`:
1. `key_has_pending_or_inflight`
2. `should_hold_sell_for_causality`
3. `hold_sell_for_causality`
4. `release_held_shadow_sells`
5. `should_process_shadow_inline`
6. `spawn_shadow_tasks_up_to_limit`
7. `enqueue_shadow_task`
8. `handle_shadow_enqueue_overflow`
9. `evict_one_pending_buy_task`
10. `dequeue_next_shadow_task`
11. `rebuild_ready_queue`
12. `mark_task_complete`

Runtime wiring updates:
1. `run_app_loop` migrated from free helper calls to method calls on `shadow_scheduler`.
2. Scheduler-focused tests updated to call `ShadowScheduler` methods directly.

Non-goals:
1. No orchestration redesign and no behavioral policy changes.
2. No extraction into separate files/modules in this slice (in-file consolidation only).

## Files Changed
1. `crates/app/src/main.rs`
2. `ops/refactor_phase6_slice3_shadow_scheduler_impl_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4613`, runtime (excl `#[cfg(test)]`) `2588`, cfg-test `2025`

## Notes
- This completes Phase 6 (2b) planned sub-slice #3: queue helper consolidation into `ShadowScheduler` while preserving behavior.
- Remaining Phase 6 evidence is now split by sub-slices (state holder, signature rewiring, impl consolidation).
