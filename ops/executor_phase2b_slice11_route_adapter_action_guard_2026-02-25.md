# Executor Phase 2B Slice 11 Evidence (2026-02-25)

## Scope

Continue route-adapter boundary hardening by enforcing action-level payload consistency in addition to route consistency.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added shared action validators:
1. `validate_required_payload_action_field(...)` for simulate requests.
1. `validate_optional_payload_action_field(...)` for submit requests.
1. Updated adapter-boundary validators:
1. `validate_simulate_payload_for_route(...)` now also requires payload `action=simulate`.
1. `validate_submit_payload_for_route(...)` now enforces payload `action=submit` if `action` is present.
1. Added unit tests:
1. `validate_submit_payload_for_route_accepts_missing_action`
1. `validate_submit_payload_for_route_rejects_mismatched_action_when_present`
1. `validate_simulate_payload_for_route_rejects_missing_action`
1. `validate_simulate_payload_for_route_rejects_non_string_action`
1. `validate_simulate_payload_for_route_rejects_mismatched_action`
1. `crates/executor/src/main.rs`:
1. Added integration guard tests:
1. `handle_simulate_rejects_action_payload_mismatch_before_forward`
1. `handle_submit_rejects_action_payload_mismatch_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added new action-guard tests to contract smoke guard pack.

## Effect

1. Adapter-boundary payload contract now rejects route/action drift before any upstream I/O.
1. Simulate path is strict (`action` mandatory and must match).
1. Submit path remains backward-compatible for payloads without `action`, while still fail-closing mismatched action if provided.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_mismatched_action_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_mismatched_action` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_action_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_action_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
