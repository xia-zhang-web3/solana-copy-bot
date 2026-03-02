# Executor Phase 2B Slice 437 — executor preflight health schema strictness + alias fallback coverage

Date: 2026-03-02  
Owner: execution-dev

## Scope

Runtime/e2e hardening for preflight health contract shape checks:

1. `tools/executor_preflight.sh`
   1. added `json_field_kind(...)` helper for `/healthz` field type introspection;
   2. fail-closed checks for route topology fields when present:
      1. `enabled_routes`,
      2. `routes`,
      3. `send_rpc_enabled_routes`,
      4. `send_rpc_fallback_routes`,
      5. `send_rpc_routes`;
   3. these fields must be `array` when present (`missing` still allowed for backward compatibility);
   4. summary now emits `*_field_kind` diagnostics.
2. `tools/ops_scripts_smoke_test.sh`
   1. fake health responder now supports raw JSON overrides for route topology fields:
      1. `FAKE_EXECUTOR_HEALTH_ENABLED_ROUTES_JSON`,
      2. `FAKE_EXECUTOR_HEALTH_ROUTES_JSON`,
      3. `FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_JSON`,
      4. `FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_JSON`,
      5. `FAKE_EXECUTOR_HEALTH_SEND_RPC_ROUTES_JSON`;
   2. `run_executor_preflight_case` now pins:
      1. alias fallback PASS when primary enabled lists are empty,
      2. FAIL on malformed `enabled_routes` type (`string`),
      3. FAIL on malformed `send_rpc_enabled_routes` type (`object`).

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Validation

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh`
2. `cargo check -p copybot-executor -q`
3. Targeted harness:
   1. source smoke harness (without final line),
   2. `create_legacy_db`,
   3. `run_executor_preflight_case`.

## Notes

1. Full `tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight case was executed.
