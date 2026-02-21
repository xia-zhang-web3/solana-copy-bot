# Refactor Evidence: Phase 8 Slice 1 (`discovery.windows`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction in discovery crate:
1. `DiscoveryWindowState` moved from `crates/discovery/src/lib.rs` to `crates/discovery/src/windows.rs`.
2. `cmp_swap_order` moved from `crates/discovery/src/lib.rs` to `crates/discovery/src/windows.rs`.
3. `lib.rs` wired with `mod windows;` and `use self::windows::{cmp_swap_order, DiscoveryWindowState};`.

Behavioral scope:
- No algorithm changes.
- No public API changes.
- In-memory window eviction/sort behavior remains unchanged.

## Files Changed
1. `crates/discovery/src/lib.rs`
2. `crates/discovery/src/windows.rs`
3. `ops/refactor_phase8_slice1_discovery_windows_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-discovery -q`
3. `cargo test -p copybot-shadow -q`
4. `cargo test --workspace -q`
5. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/discovery/src/lib.rs`: raw `1118`, runtime (excl `#[cfg(test)]`) `1006`, cfg-test `112`
- `crates/discovery/src/windows.rs`: raw `30`, runtime `30`, cfg-test `0`

## Notes
- This is the first Phase 8 discovery split slice aligned with target module `windows.rs`.
- Comparator and window-state logic remain in the same crate with `pub(super)` visibility for move-only parity.
