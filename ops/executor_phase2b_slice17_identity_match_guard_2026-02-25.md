# Executor Phase 2B Slice 17 Evidence (2026-02-25)

## Scope

Extend adapter-boundary identity validation from shape-only checks to shape + expected-value consistency checks for raw payload fields when present.

## Implemented

1. `crates/executor/src/route_executor.rs`:
1. Added `RouteActionPayloadExpectations` and propagated it through `execute_route_action(...)`.
1. `crates/executor/src/simulate_handler.rs`:
1. `execute_route_action(...)` now passes expected `request_id` and `signal_id`.
1. `crates/executor/src/submit_handler.rs`:
1. `execute_route_action(...)` now passes expected `request_id`, `signal_id`, and `client_order_id`.
1. `crates/executor/src/route_adapters.rs`:
1. Upgraded optional identity helper to enforce equality with expected values when provided.
1. Added unit tests:
1. `validate_submit_payload_for_route_rejects_request_id_mismatch_when_expected`
1. `validate_simulate_payload_for_route_rejects_signal_id_mismatch_when_expected`
1. `crates/executor/src/main.rs`:
1. Added integration tests:
1. `handle_submit_rejects_request_id_payload_mismatch_before_forward`
1. `handle_simulate_rejects_signal_id_payload_mismatch_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new guard tests to contract guard pack.

## Effect

1. Raw payload identity drift is now rejected at route-adapter boundary before any upstream call.
1. Submit/simulate paths now have symmetric defense-in-depth across route/action/contract_version/identity consistency.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_request_id_mismatch_when_expected` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_signal_id_mismatch_when_expected` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_request_id_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_signal_id_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
