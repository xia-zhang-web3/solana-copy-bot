# Executor Phase 2B Slice 68 — Route-Hint Guard Priority Coverage (2026-02-25)

## Scope

- lock in route-executor guard ordering for payload-route validation
- prove malformed `route_hint` rejects happen before allowlist/backend checks

## Changes

1. Added integration guard:
   - `execute_route_action_rejects_route_hint_mismatch_before_allowlist_check`
   - setup: route `jito` (not allowlisted by default), payload route hint `rpc`
   - assert: terminal `invalid_request_body` (`route payload mismatch`), not `route_not_allowed`
2. Added integration guard:
   - `execute_route_action_rejects_route_hint_missing_before_backend_check`
   - setup: route `rpc` allowlisted but backend removed, payload route hint missing
   - assert: terminal `invalid_request_body` (`route payload hint missing...`), not backend reject
3. Registered both tests in contract smoke.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_route_hint_mismatch_before_allowlist_check` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_route_hint_missing_before_backend_check` — PASS
4. `cargo test -p copybot-executor -q execute_route_action_rejects_(submit|simulate)_route_payload_hint_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
