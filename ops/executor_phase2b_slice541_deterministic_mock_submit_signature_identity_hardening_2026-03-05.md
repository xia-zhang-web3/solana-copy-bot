# Slice 541 — Deterministic Mock Submit Signature Identity Hardening

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e progress (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `ROAD_TO_PRODUCTION.md`

## Changes

1. Replaced constant mock submit signature with identity-derived deterministic signature:
   1. mock submit `tx_signature` now hashes (`request_id`, `client_order_id`) under mock namespace,
   2. same request identity yields stable signature, different identities diverge.
2. Refactored deterministic signature generation into shared helper used by both mock and paper namespaces:
   1. `deterministic_submit_signature("executor-mock-submit-signature", ...)`,
   2. `deterministic_submit_signature("executor-paper-submit-signature", ...)`.
3. Added regression coverage:
   1. `build_mock_submit_backend_response_uses_deterministic_signature_per_identity`.

## Validation

1. `timeout 120 cargo check -p copybot-executor -q` — PASS
2. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_uses_deterministic_signature_per_identity` — PASS
3. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_includes_signature_and_identity` — PASS
4. `timeout 120 cargo test -p copybot-executor -q internal_paper_backend` — PASS

## Notes

1. This improves non-live observability/idempotency traces by avoiding one static mock signature across unrelated submits while preserving deterministic replay behavior.
