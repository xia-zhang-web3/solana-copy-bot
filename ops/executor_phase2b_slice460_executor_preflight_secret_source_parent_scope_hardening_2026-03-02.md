# Executor Phase 2B — Slice 460

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (fail-closed secret-source error propagation).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

`read_secret_from_source` appends diagnostics into shared `errors[]`, but many callers used command substitution (`value="$(read_secret_from_source ...)"`), executing the function in a subshell.  
As a result, conflict/file diagnostics could be dropped from parent `errors[]` while preflight continued as `PASS`.

## Changes

### 1) Parent-scope secret resolution across preflight

`tools/executor_preflight.sh` now uses output-variable form of `read_secret_from_source` at all critical call sites:

1. executor global defaults (`UPSTREAM_AUTH`, `UPSTREAM_FALLBACK_AUTH`, `SEND_RPC_AUTH`, `SEND_RPC_FALLBACK_AUTH`),
2. executor route-level auth chains (`ROUTE_*_FALLBACK_AUTH`, `ROUTE_*_SEND_RPC_AUTH`, `ROUTE_*_SEND_RPC_FALLBACK_AUTH`),
3. executor ingress/hmac secrets,
4. adapter global defaults (`UPSTREAM_AUTH`, `UPSTREAM_FALLBACK_AUTH`, `SEND_RPC_AUTH`, `SEND_RPC_FALLBACK_AUTH`),
5. adapter route auth chains (`ROUTE_*_AUTH`, `ROUTE_*_FALLBACK_AUTH`, and existing send-rpc route sources).

This keeps secret-source diagnostics in parent process and ensures fail-closed verdicting on conflicts/file errors.

### 2) Smoke pin expansion

`run_executor_preflight_case` adds two targeted negatives proving parent-scope capture for global adapter send-rpc secret conflicts:

1. `COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN` + `_FILE` conflict,
2. `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN` + `_FILE` conflict.

Both assert `preflight_verdict: FAIL` with deterministic conflict diagnostics.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
