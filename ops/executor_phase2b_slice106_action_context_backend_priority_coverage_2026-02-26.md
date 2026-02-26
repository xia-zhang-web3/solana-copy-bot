# Executor Phase 2B Slice 106 — Action-Context vs Backend Priority Coverage (2026-02-26)

## Scope

- close remaining guard-ordering coverage gap for action-context precedence against backend checks

## Changes

1. Added integration priority guard in `main.rs`:
   - `execute_route_action_rejects_action_context_before_backend_check`
   - setup removes `rpc` backend and passes malformed submit context (`RouteSubmitExecutionContext::default()`)
   - expected reject stays `invalid_request_body` (`missing instruction plan`) to prove action-context guard runs before backend topology guard
2. Registered new guard in `tools/executor_contract_smoke_test.sh`.
3. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_action_context_before_backend_check` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_action_context_before_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
