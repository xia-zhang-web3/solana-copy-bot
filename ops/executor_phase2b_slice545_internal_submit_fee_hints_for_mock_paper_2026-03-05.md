# Executor Phase 2B Slice 545 — Internal Submit Fee Hints for Mock/Paper

Date: 2026-03-05  
Owner: execution-dev  
Status: PASS

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `crates/executor/src/main.rs`
3. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Internal submit branches now append fee hints from `SubmitInstructionPlan`:
   1. `base_fee_lamports` (`5000`)
   2. `priority_fee_lamports` (`cu_limit * cu_price_micro_lamports / 1_000_000`)
   3. `network_fee_lamports` (`base + priority`)
2. Applied to both internal non-live branches:
   1. `route=paper`
   2. `backend_mode=mock`
3. Existing required submit-contract fields unchanged; fee-hint fields are additive.

## Validation

Commands executed (all PASS):

1. `timeout 120 cargo check -p copybot-executor -q`
2. `timeout 120 cargo test -p copybot-executor -q execute_route_action_uses_embedded_mock_backend_for_submit`
3. `timeout 120 cargo test -p copybot-executor -q execute_route_action_uses_internal_paper_backend_for_submit_in_upstream_mode`
4. `timeout 120 cargo test -p copybot-executor -q build_mock_submit_backend_response_includes_signature_and_identity`

## Contract Notes

1. Non-live submit responses now carry fee hints compatible with downstream fee decomposition and telemetry checks.
2. Fee hints are deterministic for repeated identical instruction plans.
