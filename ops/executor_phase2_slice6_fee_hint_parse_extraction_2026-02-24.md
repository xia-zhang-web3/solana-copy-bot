# Executor Phase 2 Slice 6 Evidence (2026-02-24)

## Scope

Continued submit-path extraction by moving upstream fee-hint field parsing into the `fee_hints` module and preserving reject-code behavior.

## Implemented

1. `crates/executor/src/fee_hints.rs`:
   1. Added parsed response model:
      1. `ParsedResponseFeeHints`
   2. Added typed parse error:
      1. `FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent`
   3. Added parser:
      1. `parse_response_fee_hint_fields(&Value)`
   4. Added module tests:
      1. `parse_response_fee_hint_fields_accepts_missing_values`
      2. `parse_response_fee_hint_fields_rejects_invalid_field_type`
2. `crates/executor/src/main.rs`:
   1. Replaced inline fee-hint field parsing with `parse_response_fee_hint_fields`.
   2. Added mapper preserving existing fail-closed reject behavior:
      1. code: `submit_adapter_invalid_response`
      2. detail: `<field> must be non-negative integer when present`
   3. Added integration guard test:
      1. `handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard tests:
      1. `parse_response_fee_hint_fields_rejects_invalid_field_type`
      2. `handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q parse_response_fee_hint_fields_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
