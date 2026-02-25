# Executor Phase 2B Slice 30 Evidence (2026-02-25)

## Scope

1. Added runtime graceful shutdown handling for `copybot-executor` server startup path.
2. Hooked `axum::serve` into `with_graceful_shutdown(shutdown_signal())`.
3. Added explicit signal handling for:
   1. `SIGINT` (`tokio::signal::ctrl_c`) on all platforms,
   2. `SIGTERM` on unix targets.

## Files

1. `crates/executor/src/main.rs`
2. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q request_ingress_`
3. `bash tools/executor_contract_smoke_test.sh`
