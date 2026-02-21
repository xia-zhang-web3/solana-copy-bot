# RFC: Phase 9 App Loop Redesign (2c) — Incremental Start (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Context
`crates/app/src/main.rs` remains significantly above target size after Phases 2a/6.
Phase 9 is optional/high-risk and must proceed only through small move-only slices with strict parity gates.

## Phase 9 goals
1. Reduce `app/main.rs` size and local complexity in the event-loop area.
2. Keep runtime behavior unchanged (fail-closed/risk-gate semantics preserved).
3. Avoid orchestration redesign in early slices.

## Guardrails
1. Move-only first: extraction/wiring only, no logic rewrites.
2. Preserve public/runtime contract and telemetry keys.
3. Run mandatory pack per slice:
   1. `cargo test -p copybot-app -q`
   2. `cargo test --workspace -q`
   3. `tools/ops_scripts_smoke_test.sh`
4. Evidence file per slice with LOC snapshot and command outputs.

## Planned slice order
1. Slice 1: extract shadow runtime helper functions (`shadow_runtime_helpers.rs`).
2. Slice 2+: continue helper-level extractions from app loop hotspots.
3. Final (optional): evaluate if deeper loop orchestration redesign is still needed.

## Non-goals for early slices
1. No scheduler algorithm changes.
2. No risk-gate decision changes.
3. No cross-crate interface changes.

## Approval note
Proceeding under explicit operator direction to continue the refactor program after Phase 8 approvals.
