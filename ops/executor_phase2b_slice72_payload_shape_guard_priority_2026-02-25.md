# Executor Phase 2B Slice 72 — Payload-Shape Guard Priority (2026-02-25)

## Scope

- enforce route-layer guard order so malformed payload expectations fail before policy gates
- lock this order with integration pre-forward coverage

## Changes

1. Reordered `execute_route_action(...)` guard chain:
   - before:
     - `route_hint` -> `allowlist` -> `backend` -> `feature_gate` -> `payload_shape` -> `action_context`
   - now:
     - `route_hint` -> `payload_shape` -> `allowlist` -> `backend` -> `feature_gate` -> `action_context`
2. Added integration guard:
   - `execute_route_action_rejects_payload_shape_before_allowlist_check`
   - scenario: route `jito` not allowlisted + empty token expectation
   - expected: `invalid_request_body` (`empty token expectation`) instead of `route_not_allowed`
3. Added integration guard:
   - `execute_route_action_rejects_payload_shape_before_fastlane_feature_gate`
   - scenario: route `fastlane` allowlisted/configured but disabled + empty token expectation
   - expected: `invalid_request_body` (`empty token expectation`) instead of `fastlane_not_enabled`
4. Registered both guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_payload_shape_before_allowlist_check` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_payload_shape_before_fastlane_feature_gate` — PASS
4. `cargo test -p copybot-executor -q route_executor_payload_expectations_shape_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
