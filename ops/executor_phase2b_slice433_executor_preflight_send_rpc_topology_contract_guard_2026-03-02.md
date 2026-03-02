# Executor Phase 2B Slice 433 — executor preflight send-rpc topology contract guard

Date: 2026-03-02  
Owner: execution-dev  
Type: runtime/e2e hardening (ops contract path)

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. `executor_preflight.sh` now derives expected send-rpc topology from executor env:
   1. expected primary send-rpc routes (`expected_send_rpc_enabled_routes_csv`)
   2. expected send-rpc fallback routes (`expected_send_rpc_fallback_routes_csv`)
2. Health validation now compares expected vs `/healthz` topology fields:
   1. `send_rpc_enabled_routes` (fallback alias: `send_rpc_routes`)
   2. `send_rpc_fallback_routes`
3. Preflight now fail-closes on:
   1. missing expected send-rpc routes in health
   2. unexpected send-rpc routes in health
   3. fallback route present in health without corresponding primary send-rpc route
4. Preflight summary now emits expected/observed send-rpc route CSV fields for diagnostics.
5. Smoke harness updated:
   1. fake health now returns send-rpc topology fields,
   2. pass-path assertions pin expected/observed topology parity,
   3. negative case pins fail-close on health send-rpc topology mismatch.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. targeted harness:
   1. `run_executor_preflight_case` — PASS
   2. `run_executor_rollout_evidence_case` — PASS

## Residual

1. Full `bash tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted harness cases were executed instead.
