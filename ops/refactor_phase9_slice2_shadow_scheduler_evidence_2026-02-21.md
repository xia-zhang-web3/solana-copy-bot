# Refactor Evidence: Phase 9 Slice 2 (`app.shadow_scheduler`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shadow scheduler state/types/methods from `crates/app/src/main.rs` into `crates/app/src/shadow_scheduler.rs`.

Extracted items:
1. `ShadowScheduler`
2. `ShadowTaskOutput`
3. `ShadowTaskInput`
4. `ShadowTaskKey`
5. `ShadowSwapSide`
6. Scheduler methods (queueing/overflow/rebuild/dispatch)

Wiring updates:
1. `main.rs` adds `mod shadow_scheduler;`
2. `main.rs` imports scheduler types from module
3. `shadow_runtime_helpers.rs` now imports scheduler types from `shadow_scheduler`
4. Runtime call sites and tests remain on the same method names/logic

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/shadow_scheduler.rs`
3. `crates/app/src/shadow_runtime_helpers.rs`
4. `ops/refactor_phase9_slice2_shadow_scheduler_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4038`, runtime (excl `#[cfg(test)]`) `2013`, cfg-test `2025`
- `crates/app/src/shadow_scheduler.rs`: raw `407`, runtime `407`, cfg-test `0`
- `crates/app/src/shadow_runtime_helpers.rs`: raw `198`, runtime `198`, cfg-test `0`

## Notes
1. This is move-only extraction; no scheduler behavior change intended.
2. `main.rs` runtime LOC is now near Phase 9 threshold target (`~2000`).
