# Executor Phase 2B Slice 53 — TX Build Submit Planner Consolidation (2026-02-25)

## Scope

- continue Phase 2B tx-build core consolidation
- reduce submit-handler drift by centralizing build-step orchestration in `tx_build`
- add explicit instruction-composition primitives for downstream route executors
- keep reject contract behavior unchanged

## Changes

1. Added `tx_build::build_submit_plan(...)` with typed inputs/results:
   - validates slippage policy
   - resolves tip policy
   - validates compute-budget bounds
   - builds forward payload with tip rewrite
2. Added `SubmitInstructionPlan` primitive (compute budget + optional tip instruction) and attached it to `SubmitBuildPlan`.
3. Added planner result/error models:
   - `SubmitBuildPlanInputs`
   - `SubmitBuildPlan`
   - `SubmitBuildPlanError`
4. Switched `submit_handler` to consume `build_submit_plan(...)`, map planner errors back to existing reject mappers, and emit normalized instruction-plan debug signal.
5. Added planner/instruction unit coverage:
   - `tx_build_instruction_plan_omits_tip_when_zero`
   - `tx_build_instruction_plan_includes_tip_when_positive`
   - `tx_build_plan_builds_payload_and_tip_policy`
   - `tx_build_plan_rejects_slippage_before_other_checks`
   - `tx_build_plan_rejects_invalid_payload_for_tip_rewrite`
6. Added planner guard test to contract smoke list.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/tx_build.rs`
- `crates/executor/src/submit_handler.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q tx_build_instruction_plan_` — PASS
3. `cargo test -p copybot-executor -q tx_build_plan_` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_slippage_exceeding_route_cap` — PASS
5. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
