# Executor Phase 2B Slice 56 — Submit Context Wiring Guard (2026-02-25)

## Scope

- close audit low-gap on typed submit-context handoff coverage
- keep runtime forwarding semantics unchanged
- add regression-proof integration guard for `submit -> route_adapter` context wiring

## Changes

1. Added test-only submit-context capture helpers in `route_adapters` keyed by `client_order_id`.
2. Recorded `instruction_plan.is_some()` during submit adapter execution under `#[cfg(test)]`.
3. Added integration guard `handle_submit_wires_instruction_plan_presence_into_route_adapter_context` in `main.rs`.
4. Registered the new guard in `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap ledger.

## Files

- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q handle_submit_wires_instruction_plan_presence_into_route_adapter_context` — PASS
3. `cargo test -p copybot-executor -q route_executor_` — PASS
4. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
