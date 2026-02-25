# Executor Phase 2B Slice 15 Evidence (2026-02-25)

## Scope

Strengthen route-adapter boundary validation for raw payload identity fields, while keeping runtime compatibility for existing payloads where these fields may be absent in synthetic/unit contexts.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added `validate_optional_payload_non_empty_string_field(...)`.
1. Applied to submit validator for:
1. `request_id`
1. `signal_id`
1. `client_order_id`
1. Applied to simulate validator for:
1. `request_id`
1. `signal_id`
1. Added unit tests:
1. `validate_submit_payload_for_route_rejects_empty_client_order_id_when_present`
1. `validate_submit_payload_for_route_rejects_non_string_request_id_when_present`
1. `validate_simulate_payload_for_route_rejects_empty_signal_id_when_present`
1. `validate_simulate_payload_for_route_rejects_non_string_request_id_when_present`
1. `crates/executor/src/main.rs`:
1. Added integration tests:
1. `handle_simulate_rejects_non_string_request_id_payload_before_forward`
1. `handle_submit_rejects_empty_client_order_id_payload_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added new unit+integration identity-guard tests to contract guard pack.

## Effect

1. Route-adapter boundary now catches malformed identity fields in raw payload before any upstream forwarding.
1. Reject semantics remain fail-closed with `invalid_request_body`.
1. Defense-in-depth against request-vs-raw drift is expanded beyond route/action/contract_version to identity fields.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_empty_client_order_id_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_non_string_request_id_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_empty_signal_id_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_non_string_request_id_when_present` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_non_string_request_id_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_empty_client_order_id_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
