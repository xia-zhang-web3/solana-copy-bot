# Slice 523 Evidence — Executor Runtime Placeholder Endpoint Guard

Date (UTC): 2026-03-05
Owner: execution-dev
Scope: `copybot-executor` runtime config hardening (upstream endpoint placeholder fail-close)

## Summary

This slice adds runtime fail-closed validation in `ExecutorConfig::from_env` so `COPYBOT_EXECUTOR_BACKEND_MODE=upstream` cannot start with placeholder endpoint hosts:

1. `example.com` / `*.example.com`
2. `executor.mock.local` / `*.executor.mock.local`

Guard now applies to:

1. Route upstream endpoints: submit/simulate + fallback variants
2. Route send-rpc endpoints: primary + fallback
3. Submit-verify endpoints: `COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL` (+ fallback)

`backend_mode=mock` intentionally remains compatible with synthetic `executor.mock.local` URLs.

## Files

1. `crates/executor/src/http_utils.rs`
2. `crates/executor/src/executor_config_env.rs`
3. `ROAD_TO_PRODUCTION.md`

## Validation

1. `cargo fmt --all` — PASS
2. `cargo test -p copybot-executor -q endpoint_placeholder_host_detects_known_placeholder_hosts` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_placeholder_route_endpoint_in_upstream_mode` — PASS
4. `cargo test -p copybot-executor -q executor_config_from_env_rejects_placeholder_submit_verify_endpoint_in_upstream_mode` — PASS
5. `cargo test -p copybot-executor -q executor_config_from_env_accepts_placeholder_route_endpoint_in_mock_mode` — PASS
6. `cargo check -p copybot-executor -q` — PASS

## Notes

1. Existing `executor_config_env` test fixtures were moved from `*.example.com` to `*.integration.test` where success-path coverage is expected in upstream mode.
2. Placeholder host detection is centralized in `http_utils::endpoint_placeholder_host` for reuse.
