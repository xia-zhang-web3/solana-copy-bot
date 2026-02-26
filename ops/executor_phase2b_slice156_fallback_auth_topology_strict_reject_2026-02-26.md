# Executor Phase 2B — Slice 156

Date: 2026-02-26  
Owner: execution-dev

## Scope

- enforce strict topology consistency: fallback auth credentials must not be configured without matching fallback endpoints.

## Changes

1. `crates/executor/src/executor_config_env.rs`
   - Added strict reject for route-scoped upstream fallback auth token/file when route has no submit/simulate fallback endpoint.
   - Added strict reject for route-scoped send-rpc fallback auth token/file when route has no send-rpc fallback endpoint.
   - Added strict reject for global upstream fallback auth token/file when no route has any upstream fallback endpoint.
   - Added strict reject for global send-rpc fallback auth token/file when no route has any send-rpc fallback endpoint.
   - Preserved valid precedence when topology exists:
     - route-specific fallback auth
     - global fallback auth
     - primary-auth inheritance (last-resort)
2. Added 4 integration guards:
   - `executor_config_from_env_rejects_route_specific_upstream_fallback_auth_without_fallback_endpoint`
   - `executor_config_from_env_rejects_route_specific_send_rpc_fallback_auth_without_fallback_endpoint`
   - `executor_config_from_env_rejects_global_upstream_fallback_auth_without_any_fallback_endpoint`
   - `executor_config_from_env_rejects_global_send_rpc_fallback_auth_without_any_fallback_endpoint`
3. Registered all 4 guards in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap item 316 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted runs for all 4 new reject guards — PASS
3. `cargo test -p copybot-executor -q` — PASS (`566/566`)

## Result

- Config now fails closed on fallback-credential/topology mismatch.
- Unused fallback credentials are no longer silently accepted when retry path is impossible.
