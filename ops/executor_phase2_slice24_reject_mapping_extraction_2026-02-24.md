# Executor Phase 2 Slice 24 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving reject-mapping helpers out of `main.rs` into a dedicated shared module.

## Implemented

1. `crates/executor/src/reject_mapping.rs`:
   1. Added shared reject mapping helpers for:
      1. common contract validation errors,
      2. simulate response validation errors,
      3. fee-hint parsing/consistency errors,
      4. submit transport/response validation errors,
      5. compute-budget/slippage/tip policy errors,
      6. parsed upstream reject normalization.
   2. Added shared reject payload helpers:
      1. `reject_to_json`
      2. `simulate_http_status_for_reject`
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod reject_mapping`
   2. Rewired submit/simulate paths to shared mapping imports.
   3. Removed duplicated inline `map_*_to_reject` and reject payload helper implementations.
   4. Exposed `Reject` constructors/fields as `pub(crate)` to allow module-scoped mapping reuse.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q handle_submit_rejects_compute_budget_limit_out_of_range` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_route_mismatch` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
