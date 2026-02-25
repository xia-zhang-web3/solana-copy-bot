# Executor Phase 2B Slice 64 — Route-Layer Backend Presence Guard (2026-02-25)

## Scope

- harden route-layer topology enforcement for allowlisted routes
- reject invalid backend topology before adapter/context logic

## Changes

1. Added route-layer backend presence validator in `route_executor`:
   - allowlisted route without backend now fails with:
     - code: `route_not_allowed`
     - detail: `route=<normalized_route> not configured`
2. Wired backend presence check into `execute_route_action(...)` before:
   - fastlane feature gate
   - action/context invariants
   - adapter dispatch
3. Added unit coverage:
   - `route_executor_backend_configured_rejects_missing_backend`
   - `route_executor_backend_configured_accepts_present_backend`
4. Added integration pre-forward coverage:
   - `execute_route_action_rejects_allowlisted_route_without_backend_before_forward` (simulate)
   - `execute_route_action_rejects_submit_allowlisted_route_without_backend_before_context_check` (submit + invalid context to prove priority)
5. Registered guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_backend_configured_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_allowlisted_route_without_backend_before_forward` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_allowlisted_route_without_backend_before_context_check` — PASS
5. `cargo test -p copybot-executor -q route_executor_allowlist_` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
