# Executor Phase 2 Slice 47 Evidence (2026-02-24)

## Scope

Extract HTTP ingress handlers for `/simulate` and `/submit` from `main.rs` into dedicated module.

## Implemented

1. Added `crates/executor/src/request_endpoints.rs`:
   1. `simulate` endpoint handler,
   2. `submit` endpoint handler.
2. Moved unchanged ingress flow into module:
   1. auth verification via `verify_auth_or_reject`,
   2. request JSON parsing via `parse_json_or_reject`,
   3. business-handler dispatch (`handle_simulate` / `handle_submit`),
   4. HTTP response envelope mapping via `success_or_reject_to_http`.
3. Updated `crates/executor/src/main.rs`:
   1. added `mod request_endpoints;`
   2. router now uses imported handlers from `request_endpoints` module;
   3. removed duplicate ingress code and now-unused imports.

## Effect

1. `main.rs` now focuses further on startup/router wiring.
2. Ingress HTTP behavior is isolated from business handler modules.
3. No change in endpoint contract semantics for auth/json/reject envelopes.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q handle_simulate_rejects_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
