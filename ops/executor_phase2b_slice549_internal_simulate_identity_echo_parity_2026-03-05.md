# Executor Phase 2B Slice 549 — Internal Simulate Identity Echo Parity

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Internal simulate responses (`mock`, `paper`) now include optional identity echo fields:
   1. `request_id`
   2. `signal_id`
   3. `side` (canonical lower-case, trimmed)
   4. `token` (trimmed)
2. Shared helper now serves both simulate and submit internal response builders:
   1. `append_optional_identity_echo_fields(...)`
3. This keeps non-live simulate observability aligned with non-live submit observability.

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q build_mock_simulate_backend_response_includes_contract_fields`
3. `timeout 120 cargo test -p copybot-executor -q build_paper_simulate_backend_response_includes_contract_fields`
4. `timeout 120 cargo test -p copybot-executor -q execute_route_action_uses_embedded_mock_backend_for_simulate`
5. `timeout 120 cargo test -p copybot-executor -q execute_route_action_uses_internal_paper_backend_for_simulate_in_upstream_mode`

## Contract Notes

1. Required simulate contract fields are unchanged; new fields are additive observability echoes.
2. Internal simulate responses now carry request identity context needed for correlation/debug in non-live runs.
