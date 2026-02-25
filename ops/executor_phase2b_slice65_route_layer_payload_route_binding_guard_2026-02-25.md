# Executor Phase 2B Slice 65 — Route-Layer Payload-Route Binding Guard (2026-02-25)

## Scope

- harden route-layer boundary against route argument vs payload expectation drift
- fail-close malformed direct `execute_route_action(...)` calls before policy/backend checks

## Changes

1. Added route-layer payload-route validator in `route_executor`:
   - new `RouteActionPayloadExpectations.route_hint: Option<&str>`
   - new guard `validate_route_executor_payload_route_expectation(...)`
2. Guard behavior:
   - missing `route_hint` -> terminal reject:
     - code: `invalid_request_body`
     - detail: `route payload hint missing at route-executor boundary`
   - mismatch after normalization -> terminal reject:
     - code: `invalid_request_body`
     - detail includes `route payload mismatch ...`
3. Wired guard into `execute_route_action(...)` immediately after route resolution and before:
   - allowlist checks
   - backend presence checks
   - feature gate checks
   - action/context checks
   - adapter dispatch
4. Updated route-layer call sites to pass route hint from typed request route:
   - `submit_handler`
   - `simulate_handler`
5. Added coverage:
   - unit: `route_executor_payload_route_expectation_rejects_missing_hint`
   - unit: `route_executor_payload_route_expectation_rejects_mismatch`
   - unit: `route_executor_payload_route_expectation_accepts_match_case_insensitive`
   - integration: `execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward`
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/submit_handler.rs`
- `crates/executor/src/simulate_handler.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_payload_route_expectation_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward` — PASS
4. `cargo test -p copybot-executor -q route_executor_backend_configured_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
