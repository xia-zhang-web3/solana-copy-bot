# Executor Phase 2B Slice 54 — Planner Tip Observability + Positive-Tip Coverage (2026-02-25)

## Scope

- close low audit notes from instruction-plan slice
- remove ambiguity in submit instruction-plan debug signal
- extend integrated planner coverage for positive-tip route behavior

## Changes

1. Updated submit-handler debug signal to log `tip_instruction_lamports` as `Option` (`?`) and added explicit `tip_instruction_present` boolean.
2. Added integrated tx-build planner test for positive tip on `jito` route:
   - `tx_build_plan_preserves_positive_tip_for_jito_route`
   - verifies payload tip is preserved (`12000`)
   - verifies instruction-plan tip is `Some(12000)`
   - verifies no forced policy code for non-RPC route
3. Added new planner-positive-tip test to contract smoke guard list.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/submit_handler.rs`
- `crates/executor/src/tx_build.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q tx_build_plan_preserves_positive_tip_for_jito_route` — PASS
3. `cargo test -p copybot-executor -q tx_build_plan_` — PASS
4. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
