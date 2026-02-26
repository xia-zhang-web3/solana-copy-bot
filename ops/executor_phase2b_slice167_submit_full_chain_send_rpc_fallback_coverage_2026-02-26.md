# Executor Phase 2B — Slice 167

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- extend full submit pipeline integration coverage for send-rpc fallback on oversized success-body failure classes.

## Changes

1. Integration coverage (`crates/executor/src/main.rs`):
   - added:
     - `handle_submit_uses_send_rpc_fallback_after_primary_declared_oversized_content_length`
     - `handle_submit_uses_send_rpc_fallback_after_primary_truncated_success_body`
   - both tests exercise full `handle_submit` chain:
     - upstream `/submit` success with `signed_tx_base64`,
     - primary send-rpc failure on oversized success response class,
     - fallback send-rpc success result signature,
     - final response contains expected `tx_signature` and `submit_transport=adapter_send_rpc`.
2. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new full-chain fallback guards.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 327.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q handle_submit_uses_send_rpc_fallback_after_primary_declared_oversized_content_length` — PASS
   - `cargo test -p copybot-executor -q handle_submit_uses_send_rpc_fallback_after_primary_truncated_success_body` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- full submit pipeline now has explicit regression-proof coverage for send-rpc fallback across both oversized success-body boundary classes.
