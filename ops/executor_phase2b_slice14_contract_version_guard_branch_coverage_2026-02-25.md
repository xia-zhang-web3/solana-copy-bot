# Executor Phase 2B Slice 14 Evidence (2026-02-25)

## Scope

Close remaining low-severity branch gaps for adapter-boundary `contract_version` validation and extend pre-forward integration guard coverage.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added submit-path unit tests:
1. `validate_submit_payload_for_route_rejects_non_string_contract_version`
1. `validate_submit_payload_for_route_rejects_empty_contract_version`
1. Added simulate-path unit tests:
1. `validate_simulate_payload_for_route_rejects_non_string_contract_version`
1. `validate_simulate_payload_for_route_rejects_empty_contract_version`
1. `crates/executor/src/main.rs`:
1. Added integration guard tests:
1. `handle_simulate_rejects_non_string_contract_version_payload_before_forward`
1. `handle_submit_rejects_empty_contract_version_payload_before_forward`
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new unit/integration guard tests to `contract_guard_tests`.

## Effect

1. Adapter-boundary contract-version validator now has explicit coverage for all reject branches:
1. missing,
1. non-string,
1. non-empty,
1. mismatch.
1. Integration tests prove these malformed payloads fail-closed before upstream forwarding.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_non_string_contract_version` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_empty_contract_version` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_non_string_contract_version` — PASS
1. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_empty_contract_version` — PASS
1. `cargo test -p copybot-executor -q handle_simulate_rejects_non_string_contract_version_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_empty_contract_version_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
