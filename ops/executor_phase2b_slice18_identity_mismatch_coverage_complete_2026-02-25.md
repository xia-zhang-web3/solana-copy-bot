# Executor Phase 2B Slice 18 Evidence (2026-02-25)

## Scope

Close residual low-severity coverage gaps for adapter-boundary expected identity matching by covering all missing mismatch branches.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added unit tests for missing mismatch branches:
1. `validate_submit_payload_for_route_rejects_signal_id_mismatch_when_expected`
1. `validate_submit_payload_for_route_rejects_client_order_id_mismatch_when_expected`
1. `validate_simulate_payload_for_route_rejects_request_id_mismatch_when_expected`
1. `crates/executor/src/main.rs`:
1. Added integration pre-forward tests:
1. `handle_submit_rejects_signal_id_payload_mismatch_before_forward`
1. `handle_submit_rejects_client_order_id_payload_mismatch_before_forward`
1. `handle_simulate_rejects_request_id_payload_mismatch_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new mismatch tests to contract guard list.

## Effect

1. Expected identity match coverage is now complete for all applied fields:
1. submit: `request_id`, `signal_id`, `client_order_id`
1. simulate: `request_id`, `signal_id`
1. Both unit and integration paths now prove fail-closed mismatch rejection before forwarding.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_signal_id_mismatch_when_expected` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_client_order_id_mismatch_when_expected` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_request_id_mismatch_when_expected` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_signal_id_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_client_order_id_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_request_id_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
