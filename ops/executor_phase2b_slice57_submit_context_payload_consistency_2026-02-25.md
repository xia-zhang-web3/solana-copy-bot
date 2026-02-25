# Executor Phase 2B Slice 57 — Submit Context Payload Consistency Guard (2026-02-25)

## Scope

- continue route-adapter hardening after typed context wiring
- make `SubmitInstructionPlan` handoff enforceable at adapter boundary
- fail-close on planner/payload drift before upstream forwarding

## Changes

1. Added submit adapter consistency guard that validates:
   - `tip_lamports` matches `instruction_plan.tip_instruction_lamports.unwrap_or(0)`
   - `compute_budget.cu_limit` matches planned CU limit
   - `compute_budget.cu_price_micro_lamports` matches planned CU price
2. Reused validated submit payload map for both RPC/non-RPC adapter branches and applied consistency guard before forward.
3. Added unit guards in `route_adapters`:
   - `validate_submit_instruction_plan_payload_consistency_accepts_matching_payload`
   - `validate_submit_instruction_plan_payload_consistency_rejects_tip_mismatch`
   - `validate_submit_instruction_plan_payload_consistency_rejects_cu_limit_mismatch`
4. Added integration guard in `main.rs`:
   - `handle_submit_rejects_instruction_plan_payload_mismatch_before_forward`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_submit_instruction_plan_payload_consistency_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_instruction_plan_payload_mismatch_before_forward` — PASS
4. `cargo test -p copybot-executor -q handle_submit_wires_instruction_plan_presence_into_route_adapter_context` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
