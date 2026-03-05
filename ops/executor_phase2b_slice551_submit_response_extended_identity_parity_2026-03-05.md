# Executor Phase 2B Slice 551 — Submit Response Extended Identity Parity

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/submit_response.rs`
2. `crates/executor/src/submit_handler.rs`
3. `crates/executor/src/reject_mapping.rs`
4. `crates/executor/src/main.rs`
5. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Added optional upstream submit identity echo validation for:
   1. `signal_id`
   2. `side`
   3. `token`
2. Validation behavior:
   1. fields are only enforced when present in upstream response,
   2. `side` comparison is case-insensitive,
   3. expected identity tokens are normalized with `trim()`.
3. Wired validator into submit runtime path:
   1. `handle_submit` now enforces `validate_submit_response_extended_identity(...)`.
4. Added explicit reject classifications:
   1. `submit_adapter_signal_id_mismatch`
   2. `submit_adapter_side_mismatch`
   3. `submit_adapter_token_mismatch`

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q submit_response_validate_extended_identity_rejects_signal_id_mismatch`
3. `timeout 120 cargo test -p copybot-executor -q submit_response_validate_extended_identity_accepts_side_case_insensitive`
4. `timeout 120 cargo test -p copybot-executor -q submit_response_validate_extended_identity_rejects_token_mismatch`
5. `timeout 120 cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_signal_id_mismatches_requested_identity`
6. `timeout 120 cargo test -p copybot-executor -q handle_submit_accepts_upstream_side_echo_with_case_difference`

## Contract Notes

1. Upstream responses that echo identity fields are now fail-closed on drift for `signal_id/side/token`.
2. Existing compatibility is preserved for upstreams that omit these optional fields.
