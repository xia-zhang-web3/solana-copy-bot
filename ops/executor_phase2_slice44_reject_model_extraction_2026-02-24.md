# Executor Phase 2 Slice 44 Evidence (2026-02-24)

## Scope

Extract shared reject envelope model from `main.rs` into dedicated module.

## Implemented

1. Added new module `crates/executor/src/reject.rs`:
   1. `Reject` struct (`retryable`, `code`, `detail`).
   2. Builders `Reject::terminal` and `Reject::retryable`.
2. Removed inline `Reject` model from `crates/executor/src/main.rs`.
3. Wired root export via `pub(crate) use crate::reject::Reject;` so existing module call-sites remain unchanged.
4. Added direct guard test in `reject.rs`:
   1. `reject_builders_set_retryable_flag`.
5. Added the new guard test to `tools/executor_contract_smoke_test.sh` contract guard pack.

## Effect

1. Core reject model is now isolated from handler/orchestration code.
2. No behavior change in reject mapping or HTTP envelope semantics.
3. Existing modules keep using the same `crate::Reject` surface.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q reject_builders_set_retryable_flag` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
