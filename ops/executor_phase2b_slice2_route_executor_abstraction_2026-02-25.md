# Executor Phase 2B Slice 2 Evidence (2026-02-25)

## Scope

Start Phase 2B route-adapter abstraction without changing current upstream-forwarding behavior.

## Implemented

1. Added new module `crates/executor/src/route_executor.rs`:
   1. `RouteExecutorKind` enum for deterministic route executor selection (`paper|rpc|jito|fastlane`).
   2. `resolve_route_executor_kind(route)` helper using shared route classification.
   3. `execute_route_action(...)` as a single route execution boundary for upstream actions.
   4. Explicit per-route execution functions (`paper/rpc/jito/fastlane`) currently delegating to existing `forward_to_upstream`.
2. Switched handler call-sites to the new route execution boundary:
   1. `crates/executor/src/simulate_handler.rs`
   2. `crates/executor/src/submit_handler.rs`
3. Registered module in runtime:
   1. `crates/executor/src/main.rs` (`mod route_executor;`)
4. Added module coverage:
   1. `route_executor_resolve_maps_known_routes_case_insensitive`
   2. `route_executor_resolve_rejects_unknown_route`
   3. `route_executor_kind_as_str_matches_contract_route_tokens`
5. Extended guard list in `tools/executor_contract_smoke_test.sh` with route-executor tests.

## Effect

1. Route execution dispatch for `/simulate` and `/submit` is now centralized in one module.
2. Next Phase 2B slices can plug route-specific tx-build/send logic into per-route executors without reworking endpoint handlers again.
3. Existing behavior and reject-code semantics for upstream forwarding remain unchanged.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
