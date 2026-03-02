# Executor Phase 2B Slice 434 — executor preflight send-rpc alias consistency guard

Date: 2026-03-02  
Owner: execution-dev  
Type: runtime/e2e hardening (ops contract path)

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Change Summary

1. `executor_preflight.sh` now parses `send_rpc_routes` alias independently (`health_send_rpc_alias_routes_csv`) and still uses it as fallback for `send_rpc_enabled_routes` when needed.
2. Added explicit alias consistency checks when both fields are present:
   1. alias must not include routes absent in enabled set,
   2. alias must include all routes present in enabled set.
3. Added summary output field:
   1. `health_send_rpc_alias_routes_csv`
4. Smoke harness updated:
   1. fake health response now supports independent override `FAKE_EXECUTOR_HEALTH_SEND_RPC_ROUTES_CSV`,
   2. pass-path pins alias parity,
   3. negative case pins fail-close on alias drift.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. targeted harness:
   1. `run_executor_preflight_case` — PASS (includes new alias mismatch negative pin)

## Residual

1. Full `bash tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight case was executed.
