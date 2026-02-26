# Executor Phase 2B Slice 105 — Route-Executor Action-Context Guard Ordering (2026-02-26)

## Scope

- harden route-layer guard ordering so malformed action context is rejected before policy gates
- prevent allowlist/feature-gate errors from masking `invalid_request_body` context failures

## Changes

1. Reordered guards in `execute_route_action` (`route_executor.rs`):
   - `validate_route_executor_action_context` now runs before:
     - `validate_route_executor_allowlist`
     - `validate_route_executor_backend_configured`
     - `validate_route_executor_feature_gate`
2. Added integration priority coverage in `main.rs`:
   - `execute_route_action_rejects_action_context_before_allowlist_check`
   - `execute_route_action_rejects_action_context_before_fastlane_feature_gate`
3. Registered both guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_action_context_before_` — PASS
3. `cargo test -p copybot-executor -q route_executor_action_context_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
