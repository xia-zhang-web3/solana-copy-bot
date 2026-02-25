# Executor Phase 2B Slice 82 — Submit-Response Null Boundary Follow-up (2026-02-25)

## Scope

- close null-handling gap in submit-response echo type boundary
- enforce: if echo field key is present, value must be non-empty string (null no longer treated as absent)

## Changes

1. Updated `parse_optional_non_empty_string_field(...)` in `submit_response`:
   - removed `null -> Ok(None)` behavior
   - `null` now maps to `FieldMustBeNonEmptyStringWhenPresent`
2. Added unit coverage for null branches:
   - `submit_response_validate_route_and_contract_rejects_null_route`
   - `submit_response_validate_route_and_contract_rejects_null_contract_version`
   - `submit_response_validate_request_identity_rejects_null_request_id`
   - `submit_response_validate_request_identity_rejects_null_client_order_id`
3. Added integration coverage:
   - `handle_submit_rejects_when_upstream_request_id_is_null`
4. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/submit_response.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_response_validate_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_request_id_is_null` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_request_id_type_is_invalid` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
