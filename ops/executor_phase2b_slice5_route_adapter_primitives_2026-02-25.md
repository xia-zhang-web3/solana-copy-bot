# Executor Phase 2B Slice 5 Evidence (2026-02-25)

## Scope

Continue Phase 2B route-adapter implementation by introducing explicit route-adapter primitives and switching route dispatch to this typed adapter layer.

## Implemented

1. Added `crates/executor/src/route_adapters.rs`:
   1. Adapter types:
      1. `PaperRouteExecutor`
      2. `RpcRouteExecutor`
      3. `JitoRouteExecutor`
      4. `FastlaneRouteExecutor`
   2. Typed selector:
      1. `RouteAdapter::from_kind(RouteExecutorKind)`
      2. `RouteAdapter::as_str()`
   3. Unified execution boundary:
      1. `RouteAdapter::execute(...)` delegates to selected adapter.
2. Updated `crates/executor/src/route_executor.rs`:
   1. Replaced inline per-route `execute_*_route_action` functions with adapter dispatch through `RouteAdapter`.
   2. Preserved existing forwarding semantics (`forward_to_upstream`) for all adapters.
3. Registered module:
   1. `crates/executor/src/main.rs` adds `mod route_adapters;`
4. Extended contract smoke guard list:
   1. `route_adapter_from_kind_maps_expected_label`.

## Effect

1. Dispatch path is now:
   1. normalize route
   2. resolve route kind
   3. select typed adapter
   4. execute through adapter
2. Endpoint handlers remain untouched while route-specific behavior can now be evolved inside dedicated adapter implementations.
3. Current runtime behavior is intentionally unchanged (forwarding-equivalent).

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_` — PASS
3. `cargo test -p copybot-executor -q route_adapter_from_kind_maps_expected_label` — PASS
4. `cargo test -p copybot-executor -q classify_normalized_route_maps_known_and_unknown_values` — PASS
5. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
6. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
7. `bash -n tools/executor_contract_smoke_test.sh` — PASS
