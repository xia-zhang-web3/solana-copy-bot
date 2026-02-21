# Refactor Evidence: Phase 5B Slice 2 (`config.schema`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of config schema/default structs from `crates/config/src/lib.rs` into `crates/config/src/schema.rs`.

Extracted types + default impls:
1. `AppConfig`
2. `SystemConfig`
3. `ExecutionConfig`
4. `SqliteConfig`
5. `IngestionConfig`
6. `DiscoveryConfig`
7. `RiskConfig`
8. `ShadowConfig`

Wiring updates:
1. `lib.rs` declares `mod schema;`
2. `lib.rs` re-exports schema types via `pub use self::schema::{...}`
3. `load_from_path` / `load_from_env_or_default` logic remains in `lib.rs` unchanged

## Files Changed
1. `crates/config/src/lib.rs`
2. `crates/config/src/schema.rs`
3. `ops/refactor_phase5b_slice2_schema_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-config -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/config/src/lib.rs`: raw `989`, runtime (excl `#[cfg(test)]`) `729`, cfg-test `260`
- `crates/config/src/schema.rs`: raw `379`, runtime `379`, cfg-test `0`
- `crates/config/src/env_parsing.rs`: raw `176`, runtime `176`, cfg-test `0`

## Notes
- Config type names and serde defaults are preserved via `pub use` re-exports.
- No env precedence or parse semantics changes in this slice.
