# Executor Phase 2B — Slice 456

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (adapter send-rpc route-topology parity with runtime guards).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

`executor_preflight` validated adapter submit/simulate route URLs but did not validate adapter send-rpc route URLs/fallback topology.  
Malformed send-rpc URL, fallback-without-primary, and fallback=primary collision could pass preflight while runtime adapter route config would fail.

## Changes

### 1) Adapter send-rpc topology parity in preflight

`tools/executor_preflight.sh` now:

1. resolves adapter send-rpc URLs via runtime-equivalent layering:
   - `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_URL` -> `COPYBOT_ADAPTER_SEND_RPC_URL`,
   - `COPYBOT_ADAPTER_ROUTE_<ROUTE>_SEND_RPC_FALLBACK_URL` -> `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_URL`,
2. validates adapter send-rpc primary/fallback URL schema with strict endpoint validator,
3. fail-closes when adapter send-rpc fallback is configured but primary send-rpc URL is missing,
4. fail-closes when adapter send-rpc fallback resolves to the same endpoint identity as adapter send-rpc primary.

### 2) Smoke pin expansion

`run_executor_preflight_case` adds 3 targeted negatives:

1. invalid adapter send-rpc URL scheme (`ftp://...`) -> `preflight_verdict: FAIL`,
2. adapter send-rpc fallback without primary -> `preflight_verdict: FAIL`,
3. adapter send-rpc fallback identity collision (`fallback == primary`) -> `preflight_verdict: FAIL`.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
