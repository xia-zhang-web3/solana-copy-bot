# Executor Phase 2 Slice 36 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving contract-version parsing/validation out of `main.rs`.

## Implemented

1. `crates/executor/src/contract_version.rs`:
   1. added `parse_contract_version(value) -> Result<String>`,
   2. retained existing fail-closed constraints:
      1. non-empty,
      2. max length 64,
      3. token charset `[A-Za-z0-9._-]`.
   3. added tests:
      1. accepts trimmed valid value,
      2. rejects invalid token format.
2. `crates/executor/src/main.rs`:
   1. `ExecutorConfig::from_env` now delegates contract-version parsing to shared helper.
3. `tools/executor_contract_smoke_test.sh`:
   1. added `parse_contract_version_rejects_invalid_value` guard test.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q contract_version_` — PASS
2. `cargo test -p copybot-executor -q parse_contract_version_` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
