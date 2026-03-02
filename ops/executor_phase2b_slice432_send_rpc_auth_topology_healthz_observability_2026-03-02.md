# Executor Phase 2B Slice 432 — send-rpc auth topology fail-closed + healthz observability

Date: 2026-03-02  
Owner: execution-dev  
Type: runtime/e2e hardening (not coverage-only)  
Ordering policy status: `N=0` (from `ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/executor_config_env.rs`
2. `crates/executor/src/healthz_payload.rs`
3. `crates/executor/src/healthz_endpoint.rs`
4. `crates/executor/src/main.rs`
5. `tools/executor_contract_smoke_test.sh`
6. `ops/executor_contract_v1.md`
7. `ops/executor_backend_master_plan_2026-02-24.md`
8. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. Added fail-closed config invariants for send-rpc primary auth:
   1. route-scoped `COPYBOT_EXECUTOR_ROUTE_<ROUTE>_SEND_RPC_AUTH_TOKEN*` now requires effective primary send-rpc endpoint for that route.
   2. global `COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN*` now requires at least one configured send-rpc primary endpoint across allowlist routes.
2. Expanded `/healthz` payload with deterministic send-rpc topology:
   1. `send_rpc_enabled_routes`
   2. `send_rpc_fallback_routes`
   3. backward-compat alias `send_rpc_routes` (same as enabled list)
3. Startup log now emits sorted `send_rpc_routes` and `send_rpc_fallback_routes`.
4. Contract smoke guard registry updated with new config and healthz tests.
5. Canonical docs updated to include optional healthz send-rpc topology fields.

## Targeted Verification

1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_route_specific_send_rpc_auth_without_send_rpc_endpoint` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_route_specific_send_rpc_auth_file_without_send_rpc_endpoint` — PASS
5. `cargo test -p copybot-executor -q executor_config_from_env_rejects_global_send_rpc_auth_without_any_send_rpc_endpoint` — PASS
6. `cargo test -p copybot-executor -q executor_config_from_env_rejects_global_send_rpc_auth_file_without_any_send_rpc_endpoint` — PASS
7. `cargo test -p copybot-executor -q healthz_payload_includes_send_rpc_route_topology` — PASS
8. `cargo test -p copybot-executor -q executor_config_from_env_does_not_inherit_send_rpc_fallback_auth_without_fallback_endpoint` — PASS
9. `EXECUTOR_CONTRACT_SMOKE_MODE=targeted EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS="executor_config_from_env_rejects_global_send_rpc_auth_without_any_send_rpc_endpoint,healthz_payload_includes_send_rpc_route_topology" bash tools/executor_contract_smoke_test.sh` — PASS

## Risk / Residual

1. Full `cargo test -p copybot-executor -q` and full `bash tools/executor_contract_smoke_test.sh` were not re-run in this slice; only targeted guard pack executed.
