# Executor Phase 2B Slice 101 — Submit Slippage Payload Consistency Guard (2026-02-26)

## Scope

- prevent raw submit payload drift for risk fields already validated in typed request
- enforce route-adapter boundary consistency for `slippage_bps` and `route_slippage_cap_bps`

## Changes

1. Extended `RouteSubmitExecutionContext` with typed expectations:
   - `expected_slippage_bps: Option<f64>`
   - `expected_route_slippage_cap_bps: Option<f64>`
2. Wired expectations from `submit_handler.rs` into `execute_route_action(...)`.
3. Added submit route-adapter boundary guard in `route_adapters.rs`:
   - rejects submit when slippage expectations are missing in context
   - validates payload fields as finite numbers
   - rejects mismatch against typed expectations
4. Added unit tests:
   - `validate_submit_slippage_payload_consistency_accepts_matching_values`
   - `validate_submit_slippage_payload_consistency_rejects_missing_slippage_bps`
   - `validate_submit_slippage_payload_consistency_rejects_non_numeric_slippage_bps`
   - `validate_submit_slippage_payload_consistency_rejects_slippage_mismatch`
   - `validate_submit_slippage_payload_consistency_rejects_route_slippage_cap_mismatch`
5. Added integration pre-forward guard:
   - `handle_submit_rejects_slippage_payload_mismatch_before_forward`
6. Registered all new tests in `tools/executor_contract_smoke_test.sh`.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/submit_handler.rs`
- `crates/executor/src/route_adapters.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_submit_slippage_payload_consistency_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_slippage_payload_mismatch_before_forward` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
5. `cargo test -p copybot-executor -q` — PASS
