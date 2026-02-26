# Executor Phase 2B Slice 144 — Route Backend Code Split + Startup Consistency Guard (2026-02-26)

## Scope

- close `L-5`: distinguish allowlist rejection from backend-missing rejection
- close `L-8`: add explicit allowlist/backend cross-validation at startup
- tighten signer keypair validation memory hygiene (follow-up for `L-10` partial state)

## Changes

1. Dedicated backend-missing reject code:
   - `route_executor.rs`: `validate_route_executor_backend_configured` now returns
     - `code=route_backend_not_configured`
     - detail `route=<name> not configured`
   - `upstream_forward.rs`: missing route backend now returns `route_backend_not_configured`
   - `send_rpc.rs`: missing route backend now returns `route_backend_not_configured`
   - allowlist rejection remains unchanged: `route_not_allowed`
2. Startup allowlist/backend consistency validation:
   - `executor_config_env.rs` adds `validate_route_backend_allowlist_consistency(...)`
   - called in `ExecutorConfig::from_env()` after backend map assembly
   - rejects:
     - missing allowlisted routes in backend map
     - extra backend routes not present in allowlist
3. Signer keypair validation hardening:
   - `signer_source.rs` now decodes expected signer pubkey into `Zeroizing<Vec<u8>>`
   - compares bytes directly with keypair public half (`keypair_bytes[32..64]`)
   - avoids intermediate base58 string derived from keypair bytes

## Tests Added/Updated

1. `executor_config_env.rs`:
   - `route_backend_allowlist_consistency_rejects_missing_allowlisted_route`
   - `route_backend_allowlist_consistency_rejects_route_outside_allowlist`
   - `route_backend_allowlist_consistency_accepts_exact_match`
2. `main.rs` integration:
   - `forward_to_upstream_rejects_missing_route_backend_with_specific_code`
   - `send_signed_transaction_via_rpc_rejects_missing_route_backend_with_specific_code`
   - updated backend-missing route-executor integration assertions to `route_backend_not_configured`
3. `route_executor.rs` unit:
   - updated `route_executor_backend_configured_rejects_missing_backend` assertion to new code
4. Smoke registry:
   - added guards for new startup consistency and backend code-split tests

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/signer_source.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_backend_configured_rejects_missing_backend` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_allowlisted_route_without_backend_before_forward` — PASS
4. `cargo test -p copybot-executor -q forward_to_upstream_rejects_missing_route_backend_with_specific_code` — PASS
5. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_missing_route_backend_with_specific_code` — PASS
6. `cargo test -p copybot-executor -q route_backend_allowlist_consistency_` — PASS
7. `cargo test -p copybot-executor -q` — PASS (533/533)
8. `bash tools/executor_contract_smoke_test.sh` — PASS
