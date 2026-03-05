# Executor Phase 2B Slice 552 — Simulate Response Extended Identity Parity

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/simulate_response.rs`
2. `crates/executor/src/simulate_handler.rs`
3. `crates/executor/src/reject_mapping.rs`
4. `crates/executor/src/main.rs`
5. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Added optional upstream simulate identity echo validation for:
   1. `request_id`
   2. `signal_id`
   3. `side`
   4. `token`
2. Validation behavior:
   1. enforced only when field is present in upstream response,
   2. `side` comparison is case-insensitive,
   3. request/token/identity expected values are compared with trim parity.
3. Runtime wiring:
   1. `handle_simulate` now enforces `validate_simulate_response_identity(...)`.
4. Added reject mappings:
   1. `simulation_request_id_mismatch`
   2. `simulation_signal_id_mismatch`
   3. `simulation_side_mismatch`
   4. `simulation_token_mismatch`

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q simulate_response_validation_identity_rejects_signal_id_mismatch`
3. `timeout 120 cargo test -p copybot-executor -q simulate_response_validation_identity_accepts_side_case_insensitive`
4. `timeout 120 cargo test -p copybot-executor -q simulate_response_validation_identity_rejects_token_mismatch`
5. `timeout 120 cargo test -p copybot-executor -q handle_simulate_rejects_upstream_signal_id_mismatch`
6. `timeout 120 cargo test -p copybot-executor -q handle_simulate_accepts_upstream_side_echo_with_case_difference`

## Contract Notes

1. Upstream simulate responses that echo identity now fail-closed on drift.
2. Compatibility is preserved for upstream simulate responses that omit optional identity fields.
