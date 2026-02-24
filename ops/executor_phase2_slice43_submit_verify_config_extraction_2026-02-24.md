# Executor Phase 2 Slice 43 Evidence (2026-02-24)

## Scope

Extract submit-signature verification config parsing/building from `submit_verify.rs` into a dedicated config module.

## Implemented

1. Added new module `crates/executor/src/submit_verify_config.rs`:
   1. `SubmitSignatureVerifyConfig`
   2. `parse_submit_signature_verify_config`
   3. `build_submit_signature_verify_config`
2. Reduced `crates/executor/src/submit_verify.rs` to runtime verification logic only.
3. Updated wiring in `crates/executor/src/main.rs`:
   1. added `mod submit_verify_config;`
   2. switched imports to use the new module for config type/parsing/building.
4. Updated `crates/executor/src/submit_budget.rs` to reference `submit_verify_config::SubmitSignatureVerifyConfig`.

## Effect

1. Clear separation between config construction and runtime verify execution.
2. No contract behavior change for submit verify config/env validation.
3. Existing tests continue to validate fallback/attempts/interval fail-closed rules.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q build_submit_signature_verify_config_` — PASS
2. `cargo test -p copybot-executor -q verify_submit_signature_` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
