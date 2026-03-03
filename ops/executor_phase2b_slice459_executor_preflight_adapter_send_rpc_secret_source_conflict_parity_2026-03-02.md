# Executor Phase 2B — Slice 459

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (preflight parity with runtime secret-source conflict semantics).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

Runtime adapter resolves route send-rpc secret sources (`resolve_secret_source`) for every route and fails closed on inline/file conflicts regardless send-rpc endpoint activation.  
Preflight only resolved these secrets inside send-rpc/bearer gates, so conflicts could be missed when topology was disabled.

## Changes

### 1) Unconditional send-rpc secret-source resolution in preflight

`tools/executor_preflight.sh` now resolves per-route:

1. `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_AUTH_TOKEN*`
2. `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_FALLBACK_AUTH_TOKEN*`

outside endpoint-gated bearer checks, then reuses resolved values in existing parity checks.

Result:

1. inline+file conflicts are fail-closed even without send-rpc endpoint activation,
2. missing/unreadable/empty secret-file errors are surfaced under the same conditions,
3. existing bearer-parity checks remain unchanged for active endpoints.

### 2) Smoke pin expansion

`run_executor_preflight_case` adds two targeted negatives (without enabling send-rpc endpoint):

1. `COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_AUTH_TOKEN` + `_FILE` conflict,
2. `COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_FALLBACK_AUTH_TOKEN` + `_FILE` conflict.

Both assert `preflight_verdict: FAIL` with deterministic conflict diagnostics from `read_secret_from_source`.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
