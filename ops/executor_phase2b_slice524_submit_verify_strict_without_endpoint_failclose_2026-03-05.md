# Slice 524 Evidence — Submit Verify Strict-Mode Fail-Close

Date (UTC): 2026-03-05
Owner: execution-dev
Scope: `copybot-executor` submit-verify strict-mode config contract

## Summary

This slice closes a fail-open misconfiguration path in executor startup config:

1. Before: `COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT=true` with no `COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL` resolved to `submit_signature_verify=None` (verify silently disabled).
2. After: strict mode without primary verify endpoint is rejected fail-closed at config parse.

## Files

1. `crates/executor/src/submit_verify_config.rs`
2. `crates/executor/src/executor_config_env.rs`
3. `ROAD_TO_PRODUCTION.md`

## Behavior Change

1. New fail-closed error:
   `COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL`
2. Strict mode remains supported when primary verify endpoint is configured.

## Validation

1. `cargo test -p copybot-executor -q build_submit_signature_verify_config_rejects_strict_without_primary_endpoint` — PASS
2. `cargo test -p copybot-executor -q build_submit_signature_verify_config_accepts_strict_with_primary_endpoint` — PASS
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_submit_verify_strict_without_verify_rpc_url` — PASS
4. `cargo check -p copybot-executor -q` — PASS
