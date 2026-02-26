# Executor Phase 2B Slice 112 — Route-Hint Allowlist/Backend Submit Symmetry (2026-02-26)

## Scope

- close remaining low-gap in route-hint ordering matrix
- add submit-path parity for route-hint priority over allowlist/backend policy gates

## Changes

1. Added integration guards in `main.rs`:
   - `execute_route_action_rejects_submit_route_hint_mismatch_before_allowlist_check`
   - `execute_route_action_rejects_submit_route_hint_missing_before_backend_check`
2. Both tests pass valid submit context + deadline so only route-hint ordering is under test:
   - assert `invalid_request_body`
   - assert detail contains route-hint boundary message
3. Registered new guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_hint_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_route_hint_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
