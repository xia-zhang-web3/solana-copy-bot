# Refactor Evidence: Phase 8 Slice 5 (`shadow.quality_gates`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of shadow quality-gate and token-quality refresh logic from `crates/shadow/src/lib.rs` into `crates/shadow/src/quality_gates.rs`.

Moved to `quality_gates.rs`:
1. `ShadowService::drop_reason_for_buy_quality_gate`
2. `ShadowService::resolve_token_quality`
3. `ShadowService::fetch_token_quality_from_helius_guarded`

Wiring:
1. Added `mod quality_gates;` in `lib.rs`.
2. Runtime call sites unchanged (still invoked from `process_swap`).

Behavior scope:
- No changes to quality gate thresholds/ordering.
- Same cache freshness, RPC refresh and fallback behavior.

## Files Changed
1. `crates/shadow/src/lib.rs`
2. `crates/shadow/src/quality_gates.rs`
3. `ops/refactor_phase8_slice5_shadow_quality_gates_evidence_2026-02-21.md`

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
- `crates/shadow/src/lib.rs`: raw `724`, runtime (excl `#[cfg(test)]`) `400`, cfg-test `324`
- `crates/shadow/src/quality_gates.rs`: raw `151`, runtime `151`, cfg-test `0`

## Notes
- This slice aligns with planned Phase 8 shadow target module `quality_gates.rs`.
- Existing quality gate semantics remain fail-closed and unchanged.
