# Executor Phase 2B Slice 7 Evidence (2026-02-25)

## Scope

Close the low-severity test-gap for RPC tip guard parsing branches and continue route-adapter hardening with explicit submit payload route checks at adapter boundary.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
   1. Added shared submit payload parser:
      1. `parse_submit_payload_object(raw_body)`
   2. Added adapter-boundary submit route validation:
      1. `validate_submit_payload_for_route(raw_body, expected_route)`
      2. Enforces:
         1. payload is JSON object,
         2. `route` field exists and is string,
         3. normalized payload route matches adapter route.
   3. `submit` hooks for `paper`, `jito`, `fastlane` now call route validator before forwarding.
   4. `RpcRouteExecutor::submit` now composes:
      1. route validator + tip validator.
2. RPC tip guard coverage expansion:
   1. `validate_rpc_submit_tip_payload_rejects_invalid_json`
   2. `validate_rpc_submit_tip_payload_rejects_non_object_payload`
3. Added adapter route mismatch test:
   1. `validate_submit_payload_for_route_rejects_mismatched_route`
4. Guard-pack update:
   1. Added the new tests in `tools/executor_contract_smoke_test.sh`.

## Effect

1. Adapter layer now fail-closes on submit payload route drift, independent of earlier handler-level validation.
2. RPC tip guard parsing branches are explicitly covered.
3. Existing normal-path forwarding semantics stay unchanged.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_adapter_from_kind_maps_expected_label` — PASS
3. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_nonzero_tip` — PASS
4. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_invalid_json` — PASS
5. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_non_object_payload` — PASS
6. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_mismatched_route` — PASS
7. `cargo test -p copybot-executor -q route_executor_` — PASS
8. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
9. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
10. `bash -n tools/executor_contract_smoke_test.sh` — PASS
