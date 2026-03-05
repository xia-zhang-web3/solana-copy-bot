# Executor Phase 2B Slice 546 — Mock Signature Delimiter Collision Hardening

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Split deterministic signature encoders by compatibility intent:
   1. `deterministic_submit_signature_from_tagged_fields(...)` for mock path
   2. `deterministic_submit_signature_from_legacy_fields(...)` for paper path
2. New tagged-field hasher for mock:
   1. each field is hashed as `<u64_len_be><raw_bytes>`
   2. removes ambiguity from delimiter-based concatenation.
3. Paper path remains legacy (colon-joined) for artifact compatibility.

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q mock_submit_signature_avoids_delimiter_collision_with_tagged_fields`
3. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_uses_deterministic_signature_per_identity`
4. `timeout 120 cargo test -p copybot-executor -q build_paper_submit_backend_response_keeps_legacy_signature_inputs_only`
5. `timeout 120 cargo test -p copybot-executor -q mock_and_paper_signatures_use_distinct_namespaces`

## Contract Notes

1. Mock signatures remain deterministic but now have stronger collision resistance for complex identity strings.
2. Paper signatures preserve previous encoding semantics by design.
