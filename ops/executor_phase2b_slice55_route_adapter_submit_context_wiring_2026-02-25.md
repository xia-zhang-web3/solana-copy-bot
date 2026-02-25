# Executor Phase 2B Slice 55 — Route Adapter Submit Context Wiring (2026-02-25)

## Scope

- continue Phase 2B route-adapter foundation for tx-build core handoff
- wire `SubmitInstructionPlan` through route-execution boundary as typed context
- preserve existing submit/simulate forwarding semantics

## Changes

1. Added `RouteSubmitExecutionContext` in `route_executor` with optional `SubmitInstructionPlan`.
2. Extended `execute_route_action(...)` to accept submit context and pass it into adapter execution.
3. Updated submit path wiring (`submit_handler`) to pass built instruction plan into route execution context.
4. Updated simulate path wiring (`simulate_handler`) to pass default empty submit context.
5. Updated `route_adapters` submit execution to accept context and emit adapter-boundary debug signal when instruction plan is present.
6. Added unit guard `route_executor_submit_execution_context_defaults_none` and registered it in contract smoke.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/submit_handler.rs`
- `crates/executor/src/simulate_handler.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_route_payload_mismatch_before_forward` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
