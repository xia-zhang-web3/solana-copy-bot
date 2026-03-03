# Executor Phase 2B — Slice 457

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (auth parity closure for adapter send-rpc paths).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

`executor_preflight` already validated adapter send-rpc URL/fallback topology (slice 456), but did not enforce bearer-auth token parity for send-rpc primary/fallback paths when executor bearer auth is required.

## Changes

### 1) Adapter send-rpc auth parity in preflight

`tools/executor_preflight.sh` now:

1. reads adapter global send-rpc auth defaults:
   - `COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN*`
   - `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN*`,
2. resolves route send-rpc auth token with runtime-equivalent precedence:
   - `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_AUTH_TOKEN*`
   - `COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN*`,
3. resolves route send-rpc fallback auth token with runtime-equivalent precedence:
   - `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_FALLBACK_AUTH_TOKEN*`
   - `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN*`
   - route send-rpc primary auth token,
4. under `executor bearer required`, fail-closes missing/mismatched send-rpc primary auth token when adapter send-rpc primary endpoint is configured,
5. under `executor bearer required`, fail-closes missing/mismatched send-rpc fallback auth token when adapter send-rpc fallback endpoint is configured.

### 2) Smoke pin expansion

`run_executor_preflight_case` adds 3 targeted negative pins:

1. missing adapter send-rpc auth token with configured adapter send-rpc endpoint,
2. mismatched adapter send-rpc auth token with configured adapter send-rpc endpoint,
3. mismatched adapter send-rpc fallback auth token with configured adapter send-rpc fallback endpoint.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
