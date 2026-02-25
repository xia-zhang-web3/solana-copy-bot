# Executor Phase 2B Slice 88 — Upstream Status Type Boundary Hardening (2026-02-25)

## Scope

- harden `upstream_outcome` status parsing at boundary
- close fail-open path where malformed `status` could be ignored when `ok/accepted=true`

## Changes

1. Updated `parse_upstream_outcome(...)`:
   - new strict status parser: `present status => non-empty string`
   - malformed-present (`non-string|empty`) now fail-closed:
     - `code = "upstream_invalid_response"`
     - `retryable = false`
2. Preserved existing behavior for missing status:
   - no `status` key still allowed and resolved via `ok/accepted` fallback logic
3. Added unit coverage in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_non_string_status_when_present`
   - `upstream_outcome_rejects_empty_status_when_present`
4. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_upstream_status_type_invalid`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_non_string_status_when_present` — PASS
3. `cargo test -p copybot-executor -q upstream_outcome_rejects_empty_status_when_present` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_status_type_invalid` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
