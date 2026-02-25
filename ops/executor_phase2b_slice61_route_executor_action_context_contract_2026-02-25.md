# Executor Phase 2B Slice 61 — Route Executor Action-Context Contract (2026-02-25)

## Scope

- harden route-layer context invariants before adapter dispatch
- prevent cross-action misuse of submit context

## Changes

1. Added route-layer action/context validator:
   - `submit` requires `RouteSubmitExecutionContext.instruction_plan=Some(...)`
   - `simulate` rejects when submit instruction plan is present
2. Wired validator into `execute_route_action(...)` before adapter selection/forwarding.
3. Added route-executor unit guards:
   - `route_executor_action_context_rejects_submit_without_plan`
   - `route_executor_action_context_rejects_simulate_with_plan`
4. Added integration guard:
   - `execute_route_action_rejects_simulate_with_submit_instruction_plan_before_forward`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_action_context_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_with_submit_instruction_plan_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_without_instruction_plan_before_forward` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
