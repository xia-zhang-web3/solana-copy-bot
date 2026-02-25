# Executor Phase 2B Slice 73 — Route Deadline Context Guard (2026-02-25)

## Scope

- harden route-layer submit budget boundary
- fail-close direct route-layer calls that omit/abuse submit deadline context

## Changes

1. Added `validate_route_executor_deadline_context(...)` in `route_executor`:
   - reject `submit` when:
     - `instruction_plan` is present
     - `submit_deadline` is missing
   - reject `simulate` when:
     - `submit_deadline` is present
2. Wired deadline-context guard into `execute_route_action(...)` before adapter dispatch.
3. Added unit coverage:
   - `route_executor_deadline_context_rejects_submit_with_plan_without_deadline`
   - `route_executor_deadline_context_rejects_simulate_with_deadline`
   - `route_executor_deadline_context_accepts_submit_with_plan_and_deadline`
   - `route_executor_deadline_context_accepts_simulate_without_deadline`
4. Added integration pre-forward coverage:
   - `execute_route_action_rejects_submit_with_plan_without_deadline_before_forward`
   - `execute_route_action_rejects_simulate_with_deadline_before_forward`
5. Updated submit integration tests that assert later guard priority (`fastlane feature gate`) to pass explicit submit deadline, preserving intended reject-code assertions.
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_deadline_context_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_with_plan_without_deadline_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_with_deadline_before_forward` — PASS
5. `cargo test -p copybot-executor -q execute_route_action_rejects_fastlane_submit_when_feature_disabled_before_forward` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
