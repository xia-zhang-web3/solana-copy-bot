# Executor Phase 2B — Slice 155

Date: 2026-02-26  
Owner: execution-dev

## Scope

- reduce unnecessary secret materialization in backend config by tightening fallback auth-token inheritance rules.

## Changes

1. Updated fallback auth inheritance in `crates/executor/src/executor_config_env.rs`:
   - upstream fallback auth (`fallback_auth_token`) now inherits from primary only if `submit_fallback_url` or `simulate_fallback_url` is configured.
   - send-rpc fallback auth (`send_rpc_fallback_auth_token`) now inherits from primary only if `send_rpc_fallback_url` is configured.
2. Added integration tests:
   - `executor_config_from_env_does_not_inherit_upstream_fallback_auth_without_fallback_endpoint`
   - `executor_config_from_env_inherits_upstream_fallback_auth_when_fallback_endpoint_present`
   - `executor_config_from_env_does_not_inherit_send_rpc_fallback_auth_without_fallback_endpoint`
   - `executor_config_from_env_inherits_send_rpc_fallback_auth_when_fallback_endpoint_present`
3. Added smoke registrations for all four tests in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap entry 315 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted test runs for all 4 new fallback-auth tests — PASS
3. `cargo test -p copybot-executor -q` — PASS (`562/562`)

## Result

- Config semantics are now route-topology aware: fallback secrets are inherited only when fallback execution path exists.
- Runtime behavior for actual fallback attempts remains unchanged.
