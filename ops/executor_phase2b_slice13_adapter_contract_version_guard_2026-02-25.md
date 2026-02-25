# Executor Phase 2B Slice 13 Evidence (2026-02-25)

## Scope

Harden adapter-boundary payload contract by enforcing `contract_version` consistency in both simulate and submit guard paths before upstream forwarding.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added `validate_required_payload_contract_version_field(...)`.
1. Updated:
1. `validate_submit_payload_for_route(...)` to require payload `contract_version` and exact match to expected executor contract.
1. `validate_simulate_payload_for_route(...)` to require payload `contract_version` and exact match to expected executor contract.
1. Updated adapter call sites to pass `state.config.contract_version`.
1. Updated `validate_rpc_submit_tip_payload(...)` signature to propagate expected contract version.
1. Added unit tests for missing/mismatched contract version in both submit and simulate validators.
1. `crates/executor/src/main.rs`:
1. Added integration tests:
1. `handle_simulate_rejects_contract_version_payload_mismatch_before_forward`
1. `handle_submit_rejects_contract_version_payload_mismatch_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new guard tests to contract guard pack.

## Effect

1. Payload tampering/drift where request object is valid but raw payload contract version differs is now fail-closed at adapter boundary.
1. Both `/simulate` and `/submit` reject pre-forward with `invalid_request_body` on contract-version mismatch.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_contract_version` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_contract_version_mismatch` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_contract_version` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_contract_version_mismatch` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_contract_version_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_contract_version_payload_mismatch_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
