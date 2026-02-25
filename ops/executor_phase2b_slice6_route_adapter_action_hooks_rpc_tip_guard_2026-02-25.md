# Executor Phase 2B Slice 6 Evidence (2026-02-25)

## Scope

Continue route-adapter implementation by introducing explicit action hooks per adapter and adding route-specific defense-in-depth for RPC submit tip policy.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
   1. Adapter execution now uses explicit action hooks (`simulate` + `submit`) for each adapter type.
   2. `RouteAdapter::execute(...)` now delegates into per-adapter action-specific methods.
2. RPC route-specific hardening:
   1. Added `validate_rpc_submit_tip_payload(raw_body)` guard for `RpcRouteExecutor::submit`.
   2. Guard behavior:
      1. fail-closed on non-object/invalid payload shape (`invalid_request_body`)
      2. fail-closed on non-integer tip field (`invalid_request_body`)
      3. terminal reject on non-zero tip (`tip_not_supported`)
3. Added tests:
   1. `validate_rpc_submit_tip_payload_accepts_zero_tip`
   2. `validate_rpc_submit_tip_payload_rejects_nonzero_tip`
4. Contract guard list update:
   1. `tools/executor_contract_smoke_test.sh` now includes `validate_rpc_submit_tip_payload_rejects_nonzero_tip`.

## Effect

1. Route adapters now have explicit per-action extension points for upcoming route-specific tx-build/send logic.
2. RPC route has an additional safety guard at adapter boundary, independent from submit-handler normalization.
3. Existing forward-to-upstream behavior remains unchanged for valid payloads.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_adapter_from_kind_maps_expected_label` — PASS
3. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_nonzero_tip` — PASS
4. `cargo test -p copybot-executor -q route_executor_` — PASS
5. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
6. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
7. `bash -n tools/executor_contract_smoke_test.sh` — PASS
