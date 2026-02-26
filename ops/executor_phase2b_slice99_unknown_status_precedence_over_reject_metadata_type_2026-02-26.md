# Executor Phase 2B Slice 99 — Unknown Status Precedence Over Reject Metadata Type (2026-02-26)

## Scope

- extend unknown-status precedence matrix to reject metadata fields
- prove malformed reject `code/detail` cannot override unknown-status classification

## Changes

1. Added unit tests in `upstream_outcome.rs`:
   - `upstream_outcome_rejects_unknown_status_before_invalid_reject_code_type`
   - `upstream_outcome_rejects_unknown_status_before_invalid_reject_detail_type`
2. Added simulate integration guards in `main.rs`:
   - `handle_simulate_rejects_unknown_upstream_status_before_reject_code_type_validation`
   - `handle_simulate_rejects_unknown_upstream_status_before_reject_detail_type_validation`
3. Added submit integration guards in `main.rs`:
   - `handle_submit_rejects_unknown_upstream_status_before_reject_code_type_validation`
   - `handle_submit_rejects_unknown_upstream_status_before_reject_detail_type_validation`
4. All tests assert:
   - terminal reject
   - `code = "upstream_invalid_status"`
   - detail contains `unknown upstream status=pending`
5. Registered guards in `tools/executor_contract_smoke_test.sh`.
6. Updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_unknown_status_before_invalid_reject_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_unknown_upstream_status_before_reject_` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_unknown_upstream_status_before_reject_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
