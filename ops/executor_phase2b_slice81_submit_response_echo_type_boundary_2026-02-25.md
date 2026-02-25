# Executor Phase 2B Slice 81 — Submit-Response Echo Type Boundary (2026-02-25)

## Scope

- harden submit response optional echo fields against malformed types/empty values
- prevent silent acceptance of invalid upstream echo payloads

## Changes

1. Updated submit response validators:
   - `validate_submit_response_route_and_contract(...)`
   - `validate_submit_response_request_identity(...)`
2. Added strict parser for optional echo fields:
   - `route`
   - `contract_version`
   - `client_order_id`
   - `request_id`
3. New validation error:
   - `SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }`
4. Updated reject mapping:
   - maps to terminal `submit_adapter_invalid_response`
   - detail: `upstream <field> must be non-empty string when present`
5. Added unit coverage in `submit_response.rs`:
   - `submit_response_validate_route_and_contract_rejects_non_string_contract_version`
   - `submit_response_validate_request_identity_rejects_non_string_request_id`
   - `submit_response_validate_request_identity_rejects_empty_client_order_id`
6. Added integration coverage in `main.rs`:
   - `handle_submit_rejects_when_upstream_request_id_type_is_invalid`
   - `handle_submit_rejects_when_upstream_client_order_id_is_empty`
7. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/submit_response.rs`
- `crates/executor/src/reject_mapping.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_response_validate_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_request_id_type_is_invalid` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_client_order_id_is_empty` — PASS
5. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_returns_conflicting_transport_artifacts` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
