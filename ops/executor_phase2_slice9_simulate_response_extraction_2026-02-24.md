# Executor Phase 2 Slice 9 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction on simulate-path by moving upstream response validation/normalization and success payload building into a dedicated module.

## Implemented

1. `crates/executor/src/simulate_response.rs`:
   1. Added typed validation errors:
      1. `RouteMismatch`
      2. `ContractVersionMismatch`
   2. Added validators/builders:
      1. `validate_simulate_response_route_and_contract(...)`
      2. `resolve_simulate_response_detail(...)`
      3. `build_simulate_success_payload(...)`
   3. Added module tests:
      1. `simulate_response_validation_rejects_route_mismatch`
      2. `simulate_response_detail_defaults_when_missing`
      3. `simulate_response_payload_contains_expected_fields`
2. `crates/executor/src/main.rs`:
   1. Replaced inline simulate response checks and payload construction with module calls.
   2. Added mapper preserving existing reject code/detail contract:
      1. `simulation_route_mismatch`
      2. `simulation_contract_version_mismatch`
   3. Added integration guard test:
      1. `handle_simulate_rejects_upstream_route_mismatch`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard tests:
      1. `handle_simulate_rejects_upstream_route_mismatch`
      2. `simulate_response_validation_rejects_route_mismatch`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q simulate_response_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_route_mismatch` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
