# Refactor Evidence: Phase 8 Slice 4 (`shadow.candidate`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction in shadow crate:
1. `ShadowCandidate` moved from `crates/shadow/src/lib.rs` to `crates/shadow/src/candidate.rs`.
2. `to_shadow_candidate` moved from `impl ShadowService` to module-level helper in `candidate.rs`.
3. `lib.rs` wired with `mod candidate;` and `use self::candidate::{to_shadow_candidate, ShadowCandidate};`.

Behavioral scope:
- No candidate derivation logic changes.
- Buy/sell SOL-leg classification and sizing semantics preserved.

## Files Changed
1. `crates/shadow/src/lib.rs`
2. `crates/shadow/src/candidate.rs`
3. `ops/refactor_phase8_slice4_shadow_candidate_evidence_2026-02-21.md`

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
- `crates/shadow/src/lib.rs`: raw `864`, runtime (excl `#[cfg(test)]`) `540`, cfg-test `324`
- `crates/shadow/src/candidate.rs`: raw `36`, runtime `36`, cfg-test `0`

## Notes
- This slice aligns with planned Phase 8 shadow split target `candidate.rs`.
- `SOL_MINT` and `EPS` remain authoritative constants in `lib.rs`; `candidate.rs` consumes them via `super` imports.
