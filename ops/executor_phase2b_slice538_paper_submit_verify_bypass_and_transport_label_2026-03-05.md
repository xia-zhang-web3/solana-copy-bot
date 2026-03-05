# Slice 538 — Paper Submit Verify Bypass and Transport Label

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e progress (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/submit_handler.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Changes

1. `submit_handler` now classifies normalized route kind and applies paper-specific submit behavior:
   1. for route `paper`, submit signature verification is skipped regardless of strict verify config,
   2. submit response transport label is emitted as `executor_paper_internal` for upstream-signature artifacts on paper route.
2. Non-paper routes preserve previous behavior:
   1. strict submit-verify still performs network verification and fails closed on unseen signatures,
   2. transport labels remain unchanged (`upstream_signature`, `adapter_send_rpc`).
3. Added integration test coverage for strict-mode paper path:
   1. `handle_submit_skips_signature_verify_for_internal_paper_route_in_strict_mode`.

## Validation

1. `timeout 120 cargo check -p copybot-executor -q` — PASS
2. `timeout 120 cargo test -p copybot-executor -q handle_submit_skips_signature_verify_for_internal_paper_route_in_strict_mode` — PASS
3. `timeout 120 cargo test -p copybot-executor -q handle_submit_rejects_after_send_rpc_when_signature_verify_strict_unseen` — PASS
4. `timeout 120 cargo test -p copybot-executor -q internal_paper_backend` — PASS

## Notes

1. This slice keeps strict submit-verify safety guarantees for real on-chain routes while removing false strict failures for internal paper execution in non-live mode.
