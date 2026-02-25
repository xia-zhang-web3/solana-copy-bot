# Executor Phase 2B Slice 87 — Upstream Reject Retryable Null Coverage (2026-02-25)

## Scope

- close explicit coverage gap for `retryable: null` in upstream reject parsing
- prove fail-closed behavior for null branch at unit and integration levels

## Changes

1. Added unit test in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_null_retryable_when_present`
   - asserts terminal `upstream_invalid_response` and strict type detail
2. Added integration test in `main.rs`:
   - `handle_simulate_rejects_upstream_retryable_null`
   - verifies malformed upstream reject (`retryable: null`) is rejected before downstream success path
3. Registered both tests in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_null_retryable_when_present` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_retryable_null` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
