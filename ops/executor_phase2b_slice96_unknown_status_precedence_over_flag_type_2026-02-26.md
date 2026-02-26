# Executor Phase 2B Slice 96 — Unknown Status Precedence Over Flag Type (2026-02-26)

## Scope

- close coverage gap for unknown upstream `status` precedence when `ok/accepted` are malformed
- prove status-domain reject (`upstream_invalid_status`) wins over flag-type rejects (`upstream_invalid_response`)

## Changes

1. Kept `parse_upstream_outcome(...)` ordering with unknown-status guard before `ok/accepted` parsing.
2. Added unit tests in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_unknown_status_before_invalid_ok_type`
   - `upstream_outcome_rejects_unknown_status_before_invalid_accepted_type`
3. Added simulate-path integration tests in `main.rs`:
   - `handle_simulate_rejects_unknown_upstream_status_even_with_invalid_ok_type`
   - `handle_simulate_rejects_unknown_upstream_status_even_with_invalid_accepted_type`
4. Registered new guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_unknown_status_before_invalid_ok_type` — PASS
3. `cargo test -p copybot-executor -q upstream_outcome_rejects_unknown_status_before_invalid_accepted_type` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_unknown_upstream_status_even_with_invalid_ok_type` — PASS
5. `cargo test -p copybot-executor -q handle_simulate_rejects_unknown_upstream_status_even_with_invalid_accepted_type` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
7. `cargo test -p copybot-executor -q` — PASS
