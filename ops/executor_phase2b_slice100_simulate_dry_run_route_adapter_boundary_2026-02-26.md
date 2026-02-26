# Executor Phase 2B Slice 100 — Simulate dry_run Route-Adapter Boundary (2026-02-26)

## Scope

- harden simulate payload boundary at route-adapter layer
- enforce `dry_run=true` contract on raw payload before forwarding

## Changes

1. Added boolean boundary helper in `route_adapters.rs`:
   - `validate_optional_payload_bool_field(...)`
2. Updated `validate_simulate_payload_for_route(...)`:
   - now requires `dry_run` at route-adapter boundary with expected value `true`
   - rejects:
     - missing `dry_run`
     - non-bool `dry_run`
     - `dry_run=false`
3. Added unit tests in `route_adapters.rs`:
   - `validate_simulate_payload_for_route_rejects_missing_dry_run`
   - `validate_simulate_payload_for_route_rejects_non_bool_dry_run`
   - `validate_simulate_payload_for_route_rejects_dry_run_false`
4. Added integration pre-forward guard in `main.rs`:
   - `handle_simulate_rejects_dry_run_payload_mismatch_before_forward`
5. Registered all new guards in `tools/executor_contract_smoke_test.sh`.
6. Updated roadmap ledger.

## Files

- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_dry_run` — PASS
3. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_non_bool_dry_run` — PASS
4. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_dry_run_false` — PASS
5. `cargo test -p copybot-executor -q handle_simulate_rejects_dry_run_payload_mismatch_before_forward` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
7. `cargo test -p copybot-executor -q` — PASS
