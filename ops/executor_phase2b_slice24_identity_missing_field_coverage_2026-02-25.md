# Executor Phase 2B Slice 24 Evidence (2026-02-25)

## Scope

1. Added strict missing-field coverage for adapter-boundary identity checks when expected values are provided.
2. Covered unit branches for:
   1. submit: missing `request_id`, `signal_id`, `client_order_id`
   2. simulate: missing `request_id`, `signal_id`
3. Added integration pre-forward guards for missing identity fields:
   1. simulate: missing `request_id`, missing `signal_id`
   2. submit: missing `request_id`, missing `signal_id`, missing `client_order_id`
4. Updated contract smoke guard list and roadmap ledger.

## Files

1. `crates/executor/src/route_adapters.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_request_id_when_expected`
3. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_signal_id_when_expected`
4. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_client_order_id_when_expected`
5. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_request_id_when_expected`
6. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_signal_id_when_expected`
7. `cargo test -p copybot-executor -q handle_simulate_rejects_missing_request_id_payload_before_forward`
8. `cargo test -p copybot-executor -q handle_simulate_rejects_missing_signal_id_payload_before_forward`
9. `cargo test -p copybot-executor -q handle_submit_rejects_missing_request_id_payload_before_forward`
10. `cargo test -p copybot-executor -q handle_submit_rejects_missing_signal_id_payload_before_forward`
11. `cargo test -p copybot-executor -q handle_submit_rejects_missing_client_order_id_payload_before_forward`
12. `bash -n tools/executor_contract_smoke_test.sh`
13. `bash tools/executor_contract_smoke_test.sh`
