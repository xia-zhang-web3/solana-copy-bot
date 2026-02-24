# Executor Phase 2 Slice 5 Evidence (2026-02-24)

## Scope

Continued submit-path extraction by moving upstream submit-response echo/metadata validation into a dedicated module with typed errors and stable reject-code mapping.

## Implemented

1. `crates/executor/src/submit_response.rs`:
   1. Added route + contract echo validation:
      1. `validate_submit_response_route_and_contract(...)`
   2. Added request identity echo validation:
      1. `validate_submit_response_request_identity(...)`
   3. Added submitted-at normalization:
      1. `resolve_submit_response_submitted_at(...)`
   4. Added typed error surface:
      1. `RouteMismatch`
      2. `ContractVersionMismatch`
      3. `ClientOrderIdMismatch`
      4. `RequestIdMismatch`
      5. `SubmittedAtMustBeNonEmptyRfc3339`
      6. `SubmittedAtInvalidRfc3339`
   5. Added module unit tests (`submit_response_*`).
2. `crates/executor/src/main.rs`:
   1. Replaced inline upstream submit-response validation blocks with module calls.
   2. Added reject mapper preserving existing contract codes/details:
      1. `submit_adapter_route_mismatch`
      2. `submit_adapter_contract_version_mismatch`
      3. `submit_adapter_client_order_id_mismatch`
      4. `submit_adapter_request_id_mismatch`
      5. `submit_adapter_invalid_response` (submitted_at invalid)
   3. Added integration guard test:
      1. `handle_submit_rejects_invalid_submitted_at_in_upstream_response`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard tests:
      1. `handle_submit_rejects_invalid_submitted_at_in_upstream_response`
      2. `submit_response_resolve_submitted_at_rejects_invalid_rfc3339`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q submit_response_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_invalid_submitted_at_in_upstream_response` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
