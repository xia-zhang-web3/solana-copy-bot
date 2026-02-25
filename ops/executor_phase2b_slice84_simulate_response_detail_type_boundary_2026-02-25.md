# Executor Phase 2B Slice 84 — Simulate-Response Detail Type Boundary Hardening (2026-02-25)

## Scope

- harden `simulate` success-detail boundary
- keep default-detail fallback only for missing key
- enforce: when `detail` key is present, value must be non-empty string

## Changes

1. Updated `resolve_simulate_response_detail(...)`:
   - now returns `Result<String, SimulateResponseValidationError>`
   - uses strict parser shared with other optional echo fields
   - present `detail` with `non-string|empty|null` now rejects as invalid-present
2. Wired `handle_simulate(...)` to map detail parse failures through `map_simulate_response_validation_error_to_reject` before building downstream success payload.
3. Added unit coverage in `simulate_response.rs`:
   - `simulate_response_detail_rejects_non_string_when_present`
   - `simulate_response_detail_rejects_null_when_present`
   - `simulate_response_detail_rejects_empty_when_present`
4. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_upstream_detail_type_invalid`
5. Registered new guards in `tools/executor_contract_smoke_test.sh` and updated roadmap ledger.

## Files

- `crates/executor/src/simulate_response.rs`
- `crates/executor/src/simulate_handler.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q simulate_response_detail_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_detail_type_invalid` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
