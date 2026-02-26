# Executor Phase 2B Slice 97 — Submit Unknown-Status Precedence Coverage (2026-02-26)

## Scope

- extend unknown-status precedence coverage from simulate to submit path
- prove submit path keeps `upstream_invalid_status` priority over malformed `ok/accepted` types

## Changes

1. Added submit integration tests in `main.rs`:
   - `handle_submit_rejects_unknown_upstream_status_even_with_invalid_ok_type`
   - `handle_submit_rejects_unknown_upstream_status_even_with_invalid_accepted_type`
2. Both tests exercise upstream payloads:
   - `{"status":"pending","ok":"true","accepted":true}`
   - `{"status":"pending","ok":true,"accepted":"true"}`
3. Asserted terminal reject contract:
   - `code = "upstream_invalid_status"`
   - detail contains `unknown upstream status=pending`
4. Registered guards in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_unknown_upstream_status_even_with_invalid_ok_type` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_unknown_upstream_status_even_with_invalid_accepted_type` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
