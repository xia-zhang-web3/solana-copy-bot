# Executor Phase 2B — Slices 450-452

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only ordering work).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Slice 450 — Auth topology runtime parity guards

`tools/executor_preflight.sh` now mirrors executor runtime auth-token topology constraints:

1. route-scoped upstream fallback auth token requires route submit/simulate fallback endpoint,
2. route-scoped send-rpc auth token requires route send-rpc primary endpoint,
3. route-scoped send-rpc fallback auth token requires route send-rpc fallback endpoint,
4. global upstream fallback auth default requires at least one fallback endpoint in effective allowlist topology,
5. global send-rpc auth default requires at least one send-rpc primary endpoint,
6. global send-rpc fallback auth default requires at least one send-rpc fallback endpoint.

All guards are fail-closed with deterministic env-key diagnostics.

## Slice 451 — Auth topology observability fields

Preflight summary now emits explicit auth topology diagnostics:

1. `executor_upstream_auth_configured`
2. `executor_upstream_fallback_auth_configured`
3. `executor_send_rpc_auth_configured`
4. `executor_send_rpc_fallback_auth_configured`
5. `executor_any_upstream_fallback_endpoint`
6. `executor_any_send_rpc_primary_endpoint`
7. `executor_any_send_rpc_fallback_endpoint`

## Slice 452 — Smoke guard expansion

`run_executor_preflight_case` now includes explicit negative pins for:

1. global upstream fallback auth token without fallback endpoints,
2. global send-rpc auth token without send-rpc endpoints,
3. global send-rpc fallback auth token without send-rpc fallback endpoints,
4. route-specific upstream fallback auth token without route fallback endpoint,
5. route-specific send-rpc auth token without route send-rpc endpoint,
6. route-specific send-rpc fallback auth token without route fallback endpoint.

All cases assert fail-closed `preflight_verdict: FAIL` plus deterministic contract diagnostics.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full cargo test were not re-run in this slice; targeted preflight path was executed.
