# Executor Phase 2B Slice 107 — Simulate Action-Context Ordering Matrix (2026-02-26)

## Scope

- complete guard-ordering matrix for simulate path
- prove `action_context` reject priority over policy gates (`allowlist`, `backend`, `feature_gate`)

## Changes

1. Added simulate integration priority guards in `main.rs`:
   - `execute_route_action_rejects_simulate_action_context_before_allowlist_check`
   - `execute_route_action_rejects_simulate_action_context_before_backend_check`
   - `execute_route_action_rejects_simulate_action_context_before_fastlane_feature_gate`
2. All three tests pass malformed simulate context (`instruction_plan: Some(...)`) and assert deterministic pre-policy reject:
   - `invalid_request_body`
   - detail contains `must not include submit instruction plan`
3. Registered new guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_action_context_before_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_action_context_before_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
