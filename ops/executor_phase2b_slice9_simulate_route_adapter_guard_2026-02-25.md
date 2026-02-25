# Executor Phase 2B Slice 9 Evidence (2026-02-25)

## Scope

Close remaining conceptual adapter-boundary gap by applying the same route-echo guard strategy to simulate payloads.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added `validate_simulate_payload_for_route` with fail-closed checks for:
1. JSON-object payload shape,
1. required `route` field,
1. string `route` type,
1. normalized route equality against expected adapter route.
1. Wired simulate payload validation into all route adapters (`paper`, `rpc`, `jito`, `fastlane`) before upstream forwarding.
1. Added unit tests:
1. `validate_simulate_payload_for_route_rejects_mismatched_route`,
1. `validate_simulate_payload_for_route_rejects_missing_route`,
1. `validate_simulate_payload_for_route_rejects_non_string_route`.
1. `crates/executor/src/main.rs`:
1. Added integration guard test `handle_simulate_rejects_route_payload_mismatch_before_forward`.
1. `tools/executor_contract_smoke_test.sh`:
1. Added the new simulate guard tests to contract guard coverage.

## Effect

1. Simulate and submit paths now both enforce route-echo consistency at route-adapter boundary.
1. Mismatched request-vs-raw-body route is rejected with fail-closed `invalid_request_body` before any upstream I/O.
1. Guard coverage now explicitly locks the simulate route-mismatch/missing/non-string branches.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_mismatched_route` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_route` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_non_string_route` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_route_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
