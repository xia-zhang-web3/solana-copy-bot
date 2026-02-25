# Executor Phase 2B Slice 10 Evidence (2026-02-25)

## Scope

Address audit informational gap by removing duplicated submit/simulate route payload guard logic inside route adapters, then extend guard coverage with simulate happy-path case-insensitive route matching.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added shared parser helper: `parse_payload_object_for_action(raw_body, action_label)`.
1. Added shared route validator: `validate_payload_route_for_action(raw_body, expected_route, action_label)`.
1. Rewired both:
1. `validate_submit_payload_for_route(...)` -> shared validator with `action_label="submit"`,
1. `validate_simulate_payload_for_route(...)` -> shared validator with `action_label="simulate"`.
1. Preserved fail-closed reject code (`invalid_request_body`) and action-specific error detail text.
1. Added test: `validate_simulate_payload_for_route_accepts_matching_route_case_insensitive`.
1. `tools/executor_contract_smoke_test.sh`:
1. Added new simulate happy-path guard test to `contract_guard_tests`.

## Effect

1. Submit and simulate adapter-boundary route guards now share one implementation, reducing drift risk.
1. Any future update to route-echo contract semantics lands in one helper path.
1. Simulate guard behavior explicitly confirms normalized/case-insensitive route acceptance.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_mismatched_route` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_mismatched_route` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_accepts_matching_route_case_insensitive` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_route_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
