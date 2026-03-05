# Slice 539 — Mock Backend Submit Verify Bypass

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e progress (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/submit_handler.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Changes

1. Extended submit verify bypass condition in submit handler:
   1. bypass now applies for `route=paper` and for `backend_mode=mock`,
   2. strict submit verify network probing is not executed in these non-live execution contours.
2. Added explicit mock transport observability label:
   1. `submit_transport=executor_mock_internal` for mock-mode upstream-signature artifacts.
3. Preserved strict behavior on real upstream path:
   1. non-mock, non-paper routes still call submit verify and fail closed under strict mode when signature remains unseen.
4. Added integration guard test:
   1. `handle_submit_skips_signature_verify_for_mock_backend_in_strict_mode`.

## Validation

1. `timeout 120 cargo check -p copybot-executor -q` — PASS
2. `timeout 120 cargo test -p copybot-executor -q handle_submit_skips_signature_verify_for_mock_backend_in_strict_mode` — PASS
3. `timeout 120 cargo test -p copybot-executor -q handle_submit_skips_signature_verify_for_internal_paper_route_in_strict_mode` — PASS
4. `timeout 120 cargo test -p copybot-executor -q handle_submit_rejects_after_send_rpc_when_signature_verify_strict_unseen` — PASS

## Notes

1. This closes non-live strict-verify drift for embedded mock execution and keeps strict-onchain verification guarantees for real upstream contours unchanged.
