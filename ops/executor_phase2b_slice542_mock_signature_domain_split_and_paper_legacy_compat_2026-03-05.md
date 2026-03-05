# Executor Phase 2B Slice 542 — Mock Signature Domain Split + Paper Legacy Compatibility

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Signature hashing helper is now field-domain based (`deterministic_submit_signature_from_fields`).
2. Mock submit signature domain widened to include full request identity:
   1. `request_id`
   2. `client_order_id`
   3. `signal_id`
   4. `route_hint`
   5. `side`
   6. `token`
3. Paper submit signature domain remains legacy-only (`request_id` + `client_order_id`) to preserve artifact compatibility.
4. Added test coverage for:
   1. mock signature divergence when extended identity fields change,
   2. paper signature stability when only non-legacy fields change,
   3. namespace separation between mock and paper signatures.

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_uses_deterministic_signature_per_identity`
3. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_signature_changes_for_extended_identity_fields`
4. `timeout 120 cargo test -p copybot-executor -q build_paper_submit_backend_response_keeps_legacy_signature_inputs_only`
5. `timeout 120 cargo test -p copybot-executor -q mock_and_paper_signatures_use_distinct_namespaces`
6. `timeout 120 cargo test -p copybot-executor -q internal_paper_backend`

## Contract Notes

1. Non-live mock telemetry now has lower collision risk when request/client IDs repeat but other identity fields differ.
2. Paper signature remains stable for existing non-live artifacts keyed by request/client order identity.
