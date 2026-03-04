# Executor Phase-2B Slice 484-486 — Signoff Chain Nested Strict Guard Observability

Date: 2026-03-04  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/lib/common.sh`
2. `tools/execution_windowed_signoff_report.sh`
3. `tools/execution_route_fee_signoff_report.sh`
4. `tools/execution_devnet_rehearsal.sh`
5. `tools/ops_scripts_smoke_test.sh`
6. `ROAD_TO_PRODUCTION.md`

## Why

`execution_go_nogo_report.sh` already emitted strict executor-upstream guard diagnostics (`executor_backend_mode_guard_*`, `executor_upstream_endpoint_guard_*`), but downstream signoff/rehearsal helpers did not fail-close these fields or expose them in their own summaries.

This left a propagation/observability gap in Stage C.5 evidence-chain outputs.

## Changes

1. `tools/lib/common.sh`
   1. added `normalize_strict_guard_verdict()` for strict guard token normalization (`PASS|WARN|UNKNOWN|SKIP`, else `UNKNOWN`).

2. `tools/execution_windowed_signoff_report.sh`
   1. per-window extraction for nested go/no-go strict guards:
      1. `executor_backend_mode_guard_verdict`, `executor_backend_mode_guard_reason_code`,
      2. `executor_upstream_endpoint_guard_verdict`, `executor_upstream_endpoint_guard_reason_code`.
   2. fail-closed validation:
      1. non-empty reason-code requirements,
      2. verdict token whitelist (`PASS|WARN|UNKNOWN|SKIP`),
      3. strict parity with `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`:
         1. required=`true` => verdict must not be `SKIP`,
         2. required=`false` => verdict must be `SKIP`.
   3. added per-window summary fields for both guard dimensions.

3. `tools/execution_route_fee_signoff_report.sh`
   1. same guard extraction + fail-closed validation/parity as windowed signoff,
   2. added per-window summary fields for both strict guard dimensions.

4. `tools/execution_devnet_rehearsal.sh`
   1. extracted nested go/no-go strict guard diagnostics:
      1. `go_nogo_executor_backend_mode_guard_verdict`, `go_nogo_executor_backend_mode_guard_reason_code`,
      2. `go_nogo_executor_upstream_endpoint_guard_verdict`, `go_nogo_executor_upstream_endpoint_guard_reason_code`.
   2. fail-closed validation for malformed/empty guard values and strict parity drift versus top-level `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`.
   3. published these fields in rehearsal summary output.

5. `tools/ops_scripts_smoke_test.sh`
   1. `windowed_signoff` targeted assertions now pin nested guard fields on GO path,
   2. `route_fee_signoff` targeted assertions now pin nested guard fields on HOLD/NO_GO paths,
   3. `devnet_rehearsal` targeted assertions now pin propagated nested guard fields on default/core-only paths.

6. `ROAD_TO_PRODUCTION.md`
   1. added entries `484`, `485`, `486`.

## Validation

1. `bash -n tools/lib/common.sh tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_devnet_rehearsal.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=windowed_signoff bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=route_fee_signoff bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=devnet_rehearsal bash tools/ops_scripts_smoke_test.sh` — PASS
5. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `2` (route-profile calibration evidence reliability).
2. `ROAD_TO_PRODUCTION.md` next-code-queue item `3` (fee decomposition sign-off evidence reliability).
3. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (Stage C.5 rehearsal evidence quality).
