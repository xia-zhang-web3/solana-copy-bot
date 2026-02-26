# Executor Phase 2B Slice 93 — Fee-Hint Null Boundary Hardening (2026-02-26)

## Scope

- harden submit fee-hint parser boundary for malformed present-null fields
- remove fail-open behavior where `null` fee hints were silently treated as absent

## Changes

1. Updated `parse_optional_non_negative_u64_field(...)` in `fee_hints.rs`:
   - removed `null => Ok(None)` special-case
   - now `present null` rejects as `FieldMustBeNonNegativeIntegerWhenPresent`
2. Added unit tests:
   - `parse_response_fee_hint_fields_rejects_null_network_fee`
   - `parse_response_fee_hint_fields_rejects_null_ata_create_rent`
3. Added integration test in `main.rs`:
   - `handle_submit_rejects_null_fee_hint_field_from_upstream_response`
   - verifies terminal `submit_adapter_invalid_response`
4. Registered new tests in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/fee_hints.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q parse_response_fee_hint_fields_rejects_null_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_null_fee_hint_field_from_upstream_response` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
