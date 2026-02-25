# Executor Phase 2B Slice 83 — Simulate-Response Echo Type/Null Boundary Hardening (2026-02-25)

## Scope

- harden optional simulate-response echo fields at parser boundary
- enforce: if `route` / `contract_version` key is present, value must be non-empty string
- close null fail-open gap by treating explicit `null` as invalid-present

## Changes

1. Added strict optional string parser in `simulate_response`:
   - `parse_optional_non_empty_string_field(...)`
   - present non-string/empty/null now rejects with `FieldMustBeNonEmptyStringWhenPresent`
2. Switched `validate_simulate_response_route_and_contract(...)` to strict parser for:
   - `route`
   - `contract_version`
3. Mapped new validation error to terminal reject in `reject_mapping`:
   - code: `simulation_invalid_response`
   - detail: `upstream <field> must be non-empty string when present`
4. Added unit coverage:
   - `simulate_response_validation_rejects_non_string_route`
   - `simulate_response_validation_rejects_null_route`
   - `simulate_response_validation_rejects_non_string_contract_version`
   - `simulate_response_validation_rejects_empty_contract_version`
5. Added integration coverage:
   - `handle_simulate_rejects_upstream_contract_version_type_invalid`
   - `handle_simulate_rejects_upstream_route_null`
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/simulate_response.rs`
- `crates/executor/src/reject_mapping.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q simulate_response_validation_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_contract_version_type_invalid` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_route_null` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
