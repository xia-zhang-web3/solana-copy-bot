# Executor Phase 2B Slice 90 — Upstream Accepted-Type Integration Coverage (2026-02-26)

## Scope

- close remaining integration coverage gap for malformed upstream `accepted`
- keep parity with already-covered malformed `ok` branch

## Changes

1. Added integration test in `main.rs`:
   - `handle_simulate_rejects_upstream_accepted_type_invalid`
   - scenario: upstream responds with `accepted: "true"` and otherwise success-like envelope
   - expectation: terminal `upstream_invalid_response` with detail `upstream accepted must be boolean when present`
2. Registered new guard in `tools/executor_contract_smoke_test.sh`.
3. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_accepted_type_invalid` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
