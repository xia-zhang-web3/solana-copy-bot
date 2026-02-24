# Executor Phase 2 Slice 29 Evidence (2026-02-24)

## Scope

Continued Phase 2 cleanup by removing an extra indirection layer in `main.rs` for common contract validation.

## Implemented

1. `crates/executor/src/main.rs`:
   1. Removed local wrapper `validate_common_contract(...)`.
   2. Updated `/simulate` and `/submit` handlers to call:
      1. `validate_common_contract_inputs(CommonContractInputs { ... })`
      2. `.map_err(map_common_contract_validation_error_to_reject)`
   3. Updated fastlane gate test to use the same direct call path.

## Behavior/Contract

1. No contract change.
2. Reject codes/details remain unchanged because the same mapper is used:
   1. `contract_version_missing`
   2. `contract_version_mismatch`
   3. `route_not_allowed`
   4. `fastlane_not_enabled`
   5. `invalid_side`
   6. `invalid_token`
   7. `invalid_notional_sol`
   8. `notional_too_high`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q validate_common_contract_rejects_fastlane_when_feature_disabled` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
