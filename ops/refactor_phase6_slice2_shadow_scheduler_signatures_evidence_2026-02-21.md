# Refactor Evidence: Phase 6 Slice 2 (`app.shadow_scheduler_signatures`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Mechanical signature rewiring in `crates/app/src/main.rs` so shadow queue/scheduler helpers consume `ShadowScheduler` directly instead of passing multiple queue maps/sets/counters.

Updated helpers:
1. `key_has_pending_or_inflight`
2. `should_hold_sell_for_causality`
3. `hold_sell_for_causality`
4. `release_held_shadow_sells`
5. `should_process_shadow_inline`
6. `spawn_shadow_tasks_up_to_limit`
7. `enqueue_shadow_task`
8. `handle_shadow_enqueue_overflow`
9. `dequeue_next_shadow_task`
10. `rebuild_shadow_ready_queue`
11. `mark_shadow_task_complete`

Rewiring completed in:
1. `run_app_loop` call sites for all helpers above.
2. Existing queue/scheduler tests to use `ShadowScheduler` in helper invocation paths.

Non-goals in this slice (left for Phase 6 slice 3):
1. No relocation of queue helpers into `impl ShadowScheduler` yet.
2. No queue/risk policy logic changes.

## Files Changed
1. `crates/app/src/main.rs`
2. `ops/refactor_phase6_slice2_shadow_scheduler_signatures_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4660`, runtime (excl `#[cfg(test)]`) `2616`, cfg-test `2044`

## Notes
- This is wiring-only consolidation to reduce parameter sprawl and local mutable coupling around shadow scheduling.
- Behavioral parity is validated through unchanged app/workspace test suites plus ops smoke.
