# Slice 540 — Mock+Paper Transport Label Priority Alignment

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e progress (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/submit_handler.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Changes

1. Aligned `submit_transport` labeling priority with route-adapter execution priority:
   1. for upstream-signature submit artifacts, `paper` label now has precedence over mock-mode label,
   2. this matches route-adapter behavior where `paper` path is selected before mock path.
2. Added edge-case integration coverage for mixed mode:
   1. `handle_submit_uses_paper_transport_label_when_route_is_paper_in_mock_mode`.
3. Existing strict bypass behavior remains unchanged:
   1. mock/paper still return `submit_signature_verify.enabled=false`,
   2. strict non-mock/non-paper verify path remains fail-closed.

## Validation

1. `timeout 120 cargo check -p copybot-executor -q` — PASS
2. `timeout 120 cargo test -p copybot-executor -q handle_submit_uses_paper_transport_label_when_route_is_paper_in_mock_mode` — PASS
3. `timeout 120 cargo test -p copybot-executor -q handle_submit_skips_signature_verify_for_mock_backend_in_strict_mode` — PASS
4. `timeout 120 cargo test -p copybot-executor -q handle_submit_skips_signature_verify_for_internal_paper_route_in_strict_mode` — PASS

## Notes

1. This closes the low-severity observability mismatch reported in audit (mock+paper edge case), without changing runtime execution semantics.
