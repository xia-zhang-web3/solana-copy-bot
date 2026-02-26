# Executor Phase 2B Slice 140 — Runtime State `Arc` + Secret Clone Dedup (2026-02-26)

## Scope

- close residual memory-hardening concern: per-request deep clone of `AppState` via axum `State<AppState>`
- reduce secret duplication from fallback/default clone chains by making `SecretValue` clone share backing storage

## Changes

1. Shared router/request state:
   - `main.rs`: executor state is now `Arc<AppState>` at startup
   - router wiring remains `with_state(state.clone())` but clones are now cheap `Arc` bumps
   - request handlers now extract `State<Arc<AppState>>`:
     - `request_endpoints.rs::{simulate,submit}`
     - `healthz_endpoint.rs::healthz`
   - background worker now accepts shared state:
     - `idempotency_cleanup_worker.rs::spawn_response_cleanup_worker(state: Arc<AppState>)`
2. Shared secret clone semantics:
   - `secret_value.rs`: `SecretValue` switched from `Zeroizing<String>` to `Arc<Zeroizing<String>>`
   - existing `SecretValue` clones (including route fallback/default chains) no longer create independent plaintext allocations
   - added guard unit test:
     - `secret_value_clone_shares_backing_allocation`
3. Smoke guard registry:
   - `tools/executor_contract_smoke_test.sh` now includes `secret_value_clone_shares_backing_allocation`

## Files

- `crates/executor/src/main.rs`
- `crates/executor/src/request_endpoints.rs`
- `crates/executor/src/healthz_endpoint.rs`
- `crates/executor/src/idempotency_cleanup_worker.rs`
- `crates/executor/src/secret_value.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q` — PASS (516/516)
3. `cargo test -p copybot-executor -q secret_value_clone_shares_backing_allocation` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
