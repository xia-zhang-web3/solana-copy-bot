# Refactor Evidence: Phase 9 Slice 1 (`app.shadow_runtime_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shadow runtime helper functions from `crates/app/src/main.rs` into `crates/app/src/shadow_runtime_helpers.rs`.

Extracted helpers:
1. `insert_observed_swap_with_retry`
2. `apply_follow_snapshot_update`
3. `spawn_shadow_worker_task`
4. `find_last_pending_buy_index`
5. `handle_shadow_task_output`
6. internal helpers:
   1. `panic_payload_to_string`
   2. `shadow_task`

Wiring updates:
1. `main.rs` adds `mod shadow_runtime_helpers;`
2. `main.rs` imports extracted helpers from the module
3. Existing call sites preserved (no behavior rewrite)

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/shadow_runtime_helpers.rs`
3. `ops/refactor_phase9_rfc_2026-02-21.md`
4. `ops/refactor_phase9_slice1_shadow_runtime_helpers_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `4442`, runtime (excl `#[cfg(test)]`) `2417`, cfg-test `2025`
- `crates/app/src/shadow_runtime_helpers.rs`: raw `194`, runtime `194`, cfg-test `0`

## Notes
1. This slice is move-only; scheduler semantics and risk behavior are unchanged.
2. `tracing` macros are imported inside the new module to preserve logging behavior at existing call paths.
