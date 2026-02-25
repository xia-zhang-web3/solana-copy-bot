# Executor Phase 2B Slice 16 Evidence (2026-02-25)

## Scope

Close residual low-severity coverage gap for `validate_optional_payload_non_empty_string_field` by covering the missing field combinations and extending integration pre-forward guards.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added unit test:
1. `validate_submit_payload_for_route_rejects_empty_signal_id_when_present`
1. Added unit test:
1. `validate_simulate_payload_for_route_rejects_empty_request_id_when_present`
1. `crates/executor/src/main.rs`:
1. Added integration test:
1. `handle_submit_rejects_empty_signal_id_payload_before_forward`
1. Added integration test:
1. `handle_simulate_rejects_empty_request_id_payload_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new tests to contract guard pack.

## Effect

1. Optional identity helper coverage is now complete for all active field applications:
1. submit: `request_id`, `signal_id`, `client_order_id`
1. simulate: `request_id`, `signal_id`
1. Malformed empty identity values are now proven (unit + integration) to fail-closed before any upstream I/O.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_empty_signal_id_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_empty_request_id_when_present` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
