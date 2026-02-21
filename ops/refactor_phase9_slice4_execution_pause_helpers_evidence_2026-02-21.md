# Refactor Evidence: Phase 9 Slice 4 (`app.execution_pause_helpers`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of execution BUY pause/resume gating logic from `crates/app/src/main.rs` into `crates/app/src/execution_pause_helpers.rs`.

Extracted helper:
1. `resolve_buy_submit_pause_reason`

Wiring updates:
1. `main.rs` adds `mod execution_pause_helpers;`
2. `main.rs` imports `resolve_buy_submit_pause_reason`
3. `execution_interval` branch now delegates pause-reason resolution to helper
4. Pause precedence and log messages remain unchanged:
   1. operator emergency stop
   2. risk hard stop
   3. infra outage gate (when enabled)

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/execution_pause_helpers.rs`
3. `ops/refactor_phase9_slice4_execution_pause_helpers_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `3939`, runtime (excl `#[cfg(test)]`) `1914`, cfg-test `2025`
- `crates/app/src/execution_pause_helpers.rs`: raw `69`, runtime `69`, cfg-test `0`

## Notes
1. Move-only extraction; no execution trigger behavior changes.
2. `main.rs` runtime LOC reduced further below 2000.
