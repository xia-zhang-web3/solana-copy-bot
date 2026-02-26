# Executor Phase 2B — Slice 169

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual low-gap in full-chain verify-fallback tests by pinning explicit oversized failure cause at handler level.

## Changes

1. Integration coverage (`crates/executor/src/main.rs`):
   - added strict-mode full-chain negative guards (no verify fallback endpoint):
     - `handle_submit_rejects_when_verify_primary_declared_oversized_without_fallback`
     - `handle_submit_rejects_when_verify_primary_truncated_without_fallback`
   - both tests run full `handle_submit` chain:
     - upstream `/submit` success with `signed_tx_base64`,
     - send-rpc success,
     - verify primary oversized class failure,
     - strict-mode reject `upstream_submit_signature_unseen`.
   - assertions now explicitly pin cause text in `reject.detail`:
     - declared case: contains `response_too_large` + `declared_content_length`
     - truncated case: contains `response_too_large` + `max_bytes=65536`
2. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new strict-mode cause guards.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 329.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_verify_primary_declared_oversized_without_fallback` — PASS
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_verify_primary_truncated_without_fallback` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- full-chain verify coverage now pins both success-via-fallback behavior and explicit oversized-cause classification, eliminating false-green space where primary failure reason could drift undetected.
