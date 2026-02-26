# Executor Phase 2B — Slice 168

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering/runtime-guard reordering changes).

## Scope

- extend full submit pipeline integration coverage for verify-endpoint fallback on oversized success-body classes.

## Changes

1. Integration coverage (`crates/executor/src/main.rs`):
   - added:
     - `handle_submit_uses_verify_fallback_after_primary_declared_oversized_content_length`
     - `handle_submit_uses_verify_fallback_after_primary_truncated_success_body`
   - both tests execute full `handle_submit` chain:
     - upstream `/submit` success with `signed_tx_base64`,
     - send-rpc success signature result,
     - verify primary failure on oversized success-body class,
     - verify fallback success (`confirmationStatus`),
     - final response confirms `submit_signature_verify.enabled=true` and `seen=true`.
2. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new full-chain verify fallback guards.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 328.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q handle_submit_uses_verify_fallback_after_primary_declared_oversized_content_length` — PASS
   - `cargo test -p copybot-executor -q handle_submit_uses_verify_fallback_after_primary_truncated_success_body` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- end-to-end submit handler path now has explicit regression-proof coverage for verify fallback across both oversized success-body failure classes.
