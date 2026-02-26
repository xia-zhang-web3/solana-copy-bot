# Executor Phase 2B Slice 98 — Unknown Status Precedence Over Retryable Type (2026-02-26)

## Scope

- extend unknown-status precedence matrix to reject-envelope type validation
- prove malformed `retryable` cannot mask unknown non-empty upstream `status`

## Changes

1. Added unit test in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_unknown_status_before_invalid_retryable_type`
2. Added simulate integration guard in `main.rs`:
   - `handle_simulate_rejects_unknown_upstream_status_before_retryable_type_validation`
3. Added submit integration guard in `main.rs`:
   - `handle_submit_rejects_unknown_upstream_status_before_retryable_type_validation`
4. All tests use payload pattern:
   - `{"status":"pending","ok":false,"accepted":false,"retryable":"true"}`
5. Asserted terminal reject contract:
   - `code = "upstream_invalid_status"`
   - detail contains `unknown upstream status=pending`
6. Registered guards in `tools/executor_contract_smoke_test.sh`.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_unknown_status_before_invalid_retryable_type` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_unknown_upstream_status_before_retryable_type_validation` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_unknown_upstream_status_before_retryable_type_validation` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
