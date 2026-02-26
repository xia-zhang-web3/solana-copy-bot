# Executor Phase 2B — Slice 172

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- extend full submit pipeline coverage for upstream oversized success-body classes so fallback and cause classification are pinned at `handle_submit` boundary.

## Changes

1. Integration coverage (`crates/executor/src/main.rs`):
   - added fallback-success guards:
     - `handle_submit_uses_upstream_fallback_after_primary_declared_oversized_content_length`
     - `handle_submit_uses_upstream_fallback_after_primary_truncated_success_body`
   - added no-fallback cause guards:
     - `handle_submit_rejects_when_upstream_primary_declared_oversized_without_fallback`
     - `handle_submit_rejects_when_upstream_primary_truncated_without_fallback`
   - tests run full `handle_submit` chain:
     - upstream `/submit` primary oversized branch,
     - optional upstream fallback,
     - downstream send-rpc submission when upstream fallback succeeds.
   - no-fallback assertions pin explicit cause:
     - `code=upstream_response_too_large`
     - declared case contains `declared content-length` and `max_bytes=65536`
     - truncated case contains `max_bytes=65536`
2. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered all four new full-chain guards.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 332.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q handle_submit_uses_upstream_fallback_after_primary_declared_oversized_content_length` — PASS
   - `cargo test -p copybot-executor -q handle_submit_uses_upstream_fallback_after_primary_truncated_success_body` — PASS
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_primary_declared_oversized_without_fallback` — PASS
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_primary_truncated_without_fallback` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- upstream oversized fallback semantics are now pinned at helper + full submit-chain levels, and no-fallback branches include explicit cause assertions, reducing regression surface for upstream retry/cause classification drift.
