# Executor Phase 2 Slice 6 Evidence — Route Backend Module Extraction

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Extracted route backend primitives from `main.rs` into dedicated module:
   1. `RouteBackend` struct
   2. `UpstreamAction` enum
   3. endpoint-chain selector
   4. auth-token selection by attempt index
2. Updated upstream forwarding path to use module API:
   1. `endpoint_chain(action)`
   2. `auth_token_for_attempt(action, attempt_idx)`
3. Added module-level tests for endpoint ordering and auth token selection.

## Files

1. `crates/executor/src/route_backend.rs`
2. `crates/executor/src/main.rs`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. Behavior remains unchanged; this is a structural extraction to reduce `main.rs` coupling and prepare deeper route-adapter work.
