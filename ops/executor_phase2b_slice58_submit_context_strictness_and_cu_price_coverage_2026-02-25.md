# Executor Phase 2B Slice 58 — Submit Context Strictness + CU Price Coverage (2026-02-25)

## Scope

- close remaining low findings from audit on submit-context consistency guard
- enforce mandatory `instruction_plan` in submit adapter path
- expand explicit branch coverage for `cu_price_micro_lamports` mismatch

## Changes

1. `route_adapters::execute_submit` now fail-closes when `submit_context.instruction_plan` is `None`:
   - reject code: `invalid_request_body`
   - detail: missing instruction plan at route-adapter boundary
2. Existing context-consistency guard is now always executed in submit path (no optional skip).
3. Added unit branch coverage:
   - `validate_submit_instruction_plan_payload_consistency_rejects_cu_price_mismatch`
4. Added integration coverage:
   - `execute_route_action_rejects_submit_without_instruction_plan_before_forward`
   - `handle_submit_rejects_instruction_plan_cu_price_payload_mismatch_before_forward`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_submit_instruction_plan_payload_consistency_rejects_cu_price_mismatch` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_without_instruction_plan_before_forward` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_instruction_plan_cu_price_payload_mismatch_before_forward` — PASS
5. `cargo test -p copybot-executor -q handle_submit_rejects_instruction_plan_payload_mismatch_before_forward` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
