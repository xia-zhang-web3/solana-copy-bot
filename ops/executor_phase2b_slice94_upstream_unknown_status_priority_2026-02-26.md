# Executor Phase 2B Slice 94 — Upstream Unknown-Status Priority Hardening (2026-02-26)

## Scope

- harden `upstream_outcome` classification ordering for unknown statuses
- ensure malformed non-empty `status` cannot be masked by reject flags

## Changes

1. Updated `parse_upstream_outcome(...)` in `upstream_outcome.rs`:
   - `status` domain validation now runs before reject/success envelope interpretation
   - unknown non-empty status now always returns terminal:
     - `code = "upstream_invalid_status"`
     - detail `unknown upstream status=<value>`
2. Preserved existing behavior for known statuses and for missing status.
3. Added unit coverage:
   - `upstream_outcome_rejects_unknown_status_even_with_reject_flags`
4. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_unknown_upstream_status_even_with_reject_flags`
5. Registered guards in `tools/executor_contract_smoke_test.sh`.
6. Updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_unknown_status_even_with_reject_flags` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_unknown_upstream_status_even_with_reject_flags` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
