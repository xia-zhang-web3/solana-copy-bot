# Executor Phase 2B — Slice 165

Date: 2026-02-26  
Owner: execution-dev

## Scope

- improve submit-verify fallback observability and close fallback coverage for oversized success-body branches.

## Changes

1. Runtime (`crates/executor/src/submit_verify.rs`):
   - switched endpoint loop to indexed iteration.
   - added centralized `set_reason_and_continue` closure that:
     - sets `last_reason` deterministically,
     - emits fallback-transition warning when another endpoint exists in current attempt.
   - warning includes route, endpoint, attempt index, endpoint index, and reason.
2. Integration coverage (`crates/executor/src/main.rs`):
   - added verify fallback guards:
     - `verify_submit_signature_uses_fallback_after_primary_declared_oversized_content_length`
     - `verify_submit_signature_uses_fallback_after_primary_truncated_success_body`
   - both tests prove fallback endpoint succeeds and returns `SubmitSignatureVerification::Seen` with expected confirmation status.
3. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new verify fallback guards.
4. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 325.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests (`verify_submit_signature_uses_fallback_after_primary_...`) — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- submit-verify fallback path is now better observable in runtime logs.
- oversized success-body branches in verify path now have explicit fallback-to-success integration coverage.
