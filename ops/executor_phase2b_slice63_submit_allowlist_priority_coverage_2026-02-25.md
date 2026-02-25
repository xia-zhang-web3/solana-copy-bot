# Executor Phase 2B Slice 63 — Submit Allowlist Priority Coverage (2026-02-25)

## Scope

- close audit low-gap for allowlist integration coverage on submit action
- lock in route-layer priority: `route_not_allowed` before submit action/context checks

## Changes

1. Added integration test:
   - `execute_route_action_rejects_submit_route_not_in_allowlist_before_forward`
2. Scenario details:
   - route `jito` is known and backend-configured
   - route absent from `route_allowlist`
   - action = `submit`
   - submit context intentionally default (`instruction_plan=None`)
3. Asserted result:
   - reject code is `route_not_allowed` (proves allowlist check happens before submit-context check)
   - reject occurs pre-forward
4. Registered guard in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_route_not_in_allowlist_before_forward` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_not_in_allowlist_before_forward` — PASS
4. `cargo test -p copybot-executor -q route_executor_allowlist_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
