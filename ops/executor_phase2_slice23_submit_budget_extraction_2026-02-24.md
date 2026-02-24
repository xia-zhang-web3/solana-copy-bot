# Executor Phase 2 Slice 23 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving submit budget/claim TTL math out of `main.rs` into a dedicated module.

## Implemented

1. `crates/executor/src/submit_budget.rs`:
   1. Added shared budget helpers:
      1. `default_submit_total_budget_ms(request_timeout_ms)`
      2. `min_claim_ttl_sec_for_submit_path(request_timeout_ms, route_backends, submit_signature_verify)`
   2. Added module constants:
      1. `DEFAULT_SUBMIT_TOTAL_BUDGET_MS`
      2. internal `CLAIM_TTL_SAFETY_PADDING_MS`
   3. Preserved semantics:
      1. request-timeout floor at 500ms,
      2. topology-aware hops (submit + send_rpc + verify),
      3. saturating arithmetic + ceil ms->sec conversion.
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod submit_budget`
   2. Rewired executor config and tests to shared budget helper imports.
   3. Removed duplicated inline budget helper implementations and local constants.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_accounts_for_verify_and_fallback_hops` — PASS
2. `cargo test -p copybot-executor -q min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_submit_deadline_budget_exhausted` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
