# Executor Phase 2B Slice 4 Evidence (2026-02-25)

## Scope

Close residual route-executor normalization noise and continue Phase 2B route-adapter foundation work by introducing a shared normalized route classifier.

## Implemented

1. `crates/executor/src/route_policy.rs`:
   1. Added `classify_normalized_route(route: &str) -> RouteKind`.
   2. `classify_route` now delegates to the normalized classifier.
   3. Added test `classify_normalized_route_maps_known_and_unknown_values`.
2. `crates/executor/src/route_executor.rs`:
   1. `execute_route_action` now normalizes route once and resolves executor kind from the normalized route directly.
   2. Removed redundant second normalization path in dispatch (`normalize -> resolve(normalized)` only).
   3. Route kind mapping now uses shared `RouteKind` classification source.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard test `classify_normalized_route_maps_known_and_unknown_values`.

## Effect

1. `route_executor` is fully self-contained and avoids redundant normalize cycles.
2. Route-token classification logic now has an explicit normalized source-of-truth for future route-adapter expansion.
3. Drift risk between normalization/classification call-sites is reduced.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_` — PASS
3. `cargo test -p copybot-executor -q classify_normalized_route_maps_known_and_unknown_values` — PASS
4. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
5. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
6. `bash -n tools/executor_contract_smoke_test.sh` — PASS
