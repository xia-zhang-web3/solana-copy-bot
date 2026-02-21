# Refactor Evidence: Phase 5B Slice 1 (`config.env_parsing`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of env/route parsing helpers from `crates/config/src/lib.rs` into `crates/config/src/env_parsing.rs`.

Extracted helpers:
1. `parse_env_bool`
2. `validate_adapter_route_policy_completeness`
3. `parse_execution_route_map_env`
4. `parse_execution_route_list_env`
5. internal helper: `map_contains_route`

Wiring updates:
1. `lib.rs` declares `mod env_parsing;`
2. `lib.rs` imports helpers from `env_parsing` and keeps call sites unchanged
3. No config contract/precedence changes

## Files Changed
1. `crates/config/src/lib.rs`
2. `crates/config/src/env_parsing.rs`
3. `ops/refactor_phase5b_slice1_env_parsing_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt`
2. `cargo test -p copybot-config -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/config/src/lib.rs`: raw `1362`, runtime (excl `#[cfg(test)]`) `1102`, cfg-test `260`
- `crates/config/src/env_parsing.rs`: raw `176`, runtime `176`, cfg-test `0`

## Notes
- Route map/list normalization and duplicate-route fail-closed behavior preserved.
- Adapter route policy completeness checks stay fail-closed for adapter mode.
