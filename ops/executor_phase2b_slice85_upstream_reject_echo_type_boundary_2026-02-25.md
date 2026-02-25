# Executor Phase 2B Slice 85 — Upstream Reject Echo Type Boundary Hardening (2026-02-25)

## Scope

- harden `upstream_outcome` reject parsing at type boundary
- prevent silent fallback when upstream reject payload contains malformed `code`/`detail`

## Changes

1. Updated `parse_upstream_outcome(...)` reject branch:
   - added strict parser for optional reject fields:
     - present `code` must be non-empty string
     - present `detail` must be non-empty string
   - malformed-present (`non-string|empty|null`) now fail-closed to:
     - `code = "upstream_invalid_response"`
     - terminal semantics (`retryable = false`)
2. Kept existing fallback behavior for truly absent fields:
   - missing `code` -> `default_reject_code`
   - missing `detail` -> `"upstream rejected request"`
3. Added unit tests in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_non_string_reject_code_when_present`
   - `upstream_outcome_rejects_null_reject_detail_when_present`
4. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_upstream_reject_code_type_invalid`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_reject_code_type_invalid` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
