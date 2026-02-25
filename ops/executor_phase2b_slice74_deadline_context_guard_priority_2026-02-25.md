# Executor Phase 2B Slice 74 — Deadline-Context Guard Priority (2026-02-25)

## Scope

- harden route-layer guard ordering for deadline-context invariants
- prevent policy-gate rejects from masking malformed internal submit context

## Changes

1. Reordered `execute_route_action(...)` guard chain:
   - before:
     - `route_hint` -> `payload_shape` -> `allowlist` -> `backend` -> `feature_gate` -> `deadline_context` -> `action_context`
   - now:
     - `route_hint` -> `payload_shape` -> `deadline_context` -> `allowlist` -> `backend` -> `feature_gate` -> `action_context`
2. Added integration pre-forward guard:
   - `execute_route_action_rejects_deadline_context_before_allowlist_check`
   - scenario: submit with valid plan + missing deadline on non-allowlisted route
   - expected: `invalid_request_body` (`missing deadline`) instead of `route_not_allowed`
3. Added integration pre-forward guard:
   - `execute_route_action_rejects_deadline_context_before_fastlane_feature_gate`
   - scenario: submit with valid plan + missing deadline on `fastlane` route (allowlisted/configured, feature disabled)
   - expected: `invalid_request_body` (`missing deadline`) instead of `fastlane_not_enabled`
4. Registered both guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_deadline_context_before_allowlist_check` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_deadline_context_before_fastlane_feature_gate` — PASS
4. `cargo test -p copybot-executor -q route_executor_deadline_context_` — PASS
5. `cargo test -p copybot-executor -q execute_route_action_rejects_fastlane_submit_when_feature_disabled_before_forward` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
