# Executor Phase 2B Slice 86 — Upstream Reject Retryable Type Boundary Hardening (2026-02-25)

## Scope

- harden `upstream_outcome` reject parsing for `retryable`
- remove fail-open behavior where malformed `retryable` silently degraded to `false`

## Changes

1. Updated `parse_upstream_outcome(...)` reject branch:
   - parse `retryable` via strict optional-boolean helper
   - present non-boolean (`non-bool|null`) now fail-closed to terminal:
     - `code = "upstream_invalid_response"`
     - detail explains invalid `retryable` type
2. Kept backward-compatible behavior for missing key:
   - missing `retryable` still defaults to `false`
3. Added unit coverage in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_non_bool_retryable_when_present`
4. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_upstream_retryable_type_invalid`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_non_bool_retryable_when_present` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_retryable_type_invalid` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
