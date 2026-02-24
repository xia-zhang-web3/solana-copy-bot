# Executor Phase 2 Slice 12 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving request contract validation logic from `main.rs` into a dedicated shared module with typed errors and stable reject mapping.

## Implemented

1. `crates/executor/src/common_contract.rs`:
   1. Added input model:
      1. `CommonContractInputs`
   2. Added typed error model:
      1. `CommonContractValidationError`
   3. Added validator:
      1. `validate_common_contract_inputs(...)`
   4. Added unit tests:
      1. `common_contract_validation_accepts_valid_inputs`
      2. `common_contract_validation_rejects_invalid_side`
      3. `common_contract_validation_rejects_notional_too_high`
2. `crates/executor/src/main.rs`:
   1. `validate_common_contract(...)` now delegates to `validate_common_contract_inputs(...)`.
   2. Added mapper:
      1. `map_common_contract_validation_error_to_reject(...)`
   3. Preserved reject-code contract:
      1. `contract_version_missing`
      2. `contract_version_mismatch`
      3. `route_not_allowed`
      4. `fastlane_not_enabled`
      5. `invalid_side`
      6. `invalid_token`
      7. `invalid_notional_sol`
      8. `notional_too_high`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard coverage:
      1. `common_contract_validation_rejects_invalid_side`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q common_contract_` — PASS
2. `cargo test -p copybot-executor -q validate_common_contract_rejects_fastlane_when_feature_disabled` — PASS
3. `cargo test -p copybot-executor -q key_validation_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
