# Executor Phase 2B Slice 108 — Deadline vs Action-Context Ordering Matrix (2026-02-26)

## Scope

- close ordering coverage for cases where both `deadline_context` and `action_context` are invalid in one request
- pin deterministic priority: `deadline_context` must reject first

## Changes

1. Added submit integration priority guard:
   - `execute_route_action_rejects_deadline_context_before_action_context_on_submit`
   - setup passes malformed submit context (`missing slippage expectation`) plus missing deadline
   - assert reject is `invalid_request_body` with `missing deadline`
2. Added simulate integration priority guard:
   - `execute_route_action_rejects_deadline_context_before_action_context_on_simulate`
   - setup passes malformed simulate context (`instruction_plan: Some`) plus present submit deadline
   - assert reject is `invalid_request_body` with `must not include submit deadline`
3. Registered both guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_deadline_context_before_action_context_on_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_deadline_context_before_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
