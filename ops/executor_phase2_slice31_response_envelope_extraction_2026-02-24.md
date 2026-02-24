# Executor Phase 2 Slice 31 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by removing duplicate endpoint response-wrapping logic from `main.rs`.

## Implemented

1. Added `crates/executor/src/response_envelope.rs`:
   1. `success_or_reject_to_http(result, client_order_id, contract_version)`.
   2. Centralized mapping:
      1. success -> `StatusCode::OK` + success JSON,
      2. reject -> `StatusCode::OK` + `reject_to_json(...)`.
2. Updated `crates/executor/src/main.rs`:
   1. `/simulate` now returns shared envelope helper output.
   2. `/submit` now returns shared envelope helper output with `client_order_id` propagation.
3. Added module tests:
   1. success payload envelope,
   2. reject payload with `client_order_id`,
   3. reject payload without `client_order_id`.
4. Updated `tools/executor_contract_smoke_test.sh` with direct module guard coverage.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q response_envelope_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
