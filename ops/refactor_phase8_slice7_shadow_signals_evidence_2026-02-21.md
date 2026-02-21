# Refactor Evidence: Phase 8 Slice 7 (`shadow.signals`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of gate-drop signal logging helper from `crates/shadow/src/lib.rs` to `crates/shadow/src/signals.rs`.

Moved:
1. `log_gate_drop` helper function.

Wiring:
1. Added `mod signals;` in `lib.rs`.
2. Added `use self::signals::log_gate_drop;`.
3. Replaced call sites from `Self::log_gate_drop(...)` to `log_gate_drop(...)`.

Behavior scope:
- No logging field/content changes.
- Gate decision flow and drop taxonomy unchanged.

## Files Changed
1. `crates/shadow/src/lib.rs`
2. `crates/shadow/src/signals.rs`
3. `ops/refactor_phase8_slice7_shadow_signals_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-shadow -q`
3. `cargo test -p copybot-discovery -q`
4. `cargo test --workspace -q`
5. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/shadow/src/lib.rs`: raw `688`, runtime (excl `#[cfg(test)]`) `363`, cfg-test `325`
- `crates/shadow/src/signals.rs`: raw `33`, runtime `33`, cfg-test `0`

## Notes
- This slice aligns with planned Phase 8 shadow target module `signals.rs`.
- Logging remains skipped when both runtime+temporal follow are false and no unfollowed-sell-exit (same as before).
