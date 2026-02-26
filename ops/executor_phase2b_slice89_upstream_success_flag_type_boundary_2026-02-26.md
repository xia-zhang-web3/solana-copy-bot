# Executor Phase 2B Slice 89 — Upstream Success-Flag Type Boundary Hardening (2026-02-26)

## Scope

- harden `parse_upstream_outcome` around `ok` / `accepted` type boundary
- fail-close malformed success flags before outcome classification

## Changes

1. Updated `parse_upstream_outcome(...)`:
   - `ok` and `accepted` now parsed as strict optional booleans
   - malformed-present (`non-bool|null`) now returns terminal:
     - `code = "upstream_invalid_response"`
     - `retryable = false`
2. Preserved behavior for absent fields:
   - missing `ok`/`accepted` still resolved by existing status-based fallback path
3. Added unit tests in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_non_bool_ok_when_present`
   - `upstream_outcome_rejects_null_accepted_when_present`
4. Added simulate integration test in `main.rs`:
   - `handle_simulate_rejects_upstream_ok_type_invalid`
5. Registered new tests in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_non_bool_ok_when_present` — PASS
3. `cargo test -p copybot-executor -q upstream_outcome_rejects_null_accepted_when_present` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_ok_type_invalid` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
