# Executor Phase-2B Slice 509 — Ingestion Strict Guard Propagation Through Signoff/Final/Readiness

Date: 2026-03-04  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/execution_windowed_signoff_report.sh`
2. `tools/execution_route_fee_signoff_report.sh`
3. `tools/execution_route_fee_final_evidence_report.sh`
4. `tools/execution_runtime_readiness_report.sh`
5. `tools/ops_scripts_smoke_test.sh`
6. `ROAD_TO_PRODUCTION.md`

## Why

`execution_go_nogo_report.sh` and `execution_server_rollout_report.sh` already exposed strict ingestion gRPC guard diagnostics, but downstream signoff/final/readiness helpers did not validate or surface this guard chain end-to-end.

This left a propagation observability gap between go/no-go and later evidence packages.

## Changes

1. `tools/execution_windowed_signoff_report.sh`
   1. added strict bool parsing for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated it to nested go/no-go calls.
   3. extracted per-window nested fields:
      1. `window_${N}h_go_nogo_require_ingestion_grpc`,
      2. `window_${N}h_ingestion_grpc_guard_verdict`,
      3. `window_${N}h_ingestion_grpc_guard_reason_code`.
   4. fail-closed validation + parity:
      1. bool/token validation,
      2. verdict whitelist (`PASS|WARN|UNKNOWN|SKIP`),
      3. `true => verdict != SKIP`, `false => verdict == SKIP`.

2. `tools/execution_route_fee_signoff_report.sh`
   1. mirrored the same ingestion strict-guard parse/propagation/extraction/parity model per window.
   2. added matching window summary fields.

3. `tools/execution_route_fee_final_evidence_report.sh`
   1. added strict bool parse for `GO_NOGO_REQUIRE_INGESTION_GRPC` and passthrough to nested route-fee signoff.
   2. extended dynamic guard-window extraction with ingestion keys:
      1. `window_${signoff_guard_window_id}h_go_nogo_require_ingestion_grpc`,
      2. `window_${signoff_guard_window_id}h_ingestion_grpc_guard_verdict`,
      3. `window_${signoff_guard_window_id}h_ingestion_grpc_guard_reason_code`.
   3. fail-closed validation + parity checks.
   4. surfaced nested fields in final summary:
      1. `signoff_nested_go_nogo_require_ingestion_grpc`,
      2. `signoff_nested_ingestion_grpc_guard_verdict`,
      3. `signoff_nested_ingestion_grpc_guard_reason_code`.

4. `tools/execution_runtime_readiness_report.sh`
   1. added strict bool parse for top-level `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated setting into nested route-fee final helper call.
   3. extracted and validated nested route-fee ingestion fields:
      1. `signoff_nested_go_nogo_require_ingestion_grpc`,
      2. `signoff_nested_ingestion_grpc_guard_verdict`,
      3. `signoff_nested_ingestion_grpc_guard_reason_code`.
   4. fail-closed parity checks (`true => non-SKIP`, `false => SKIP`).
   5. exposed readiness observability fields:
      1. `route_fee_final_nested_go_nogo_require_ingestion_grpc`,
      2. `route_fee_final_nested_ingestion_grpc_guard_verdict`,
      3. `route_fee_final_nested_ingestion_grpc_guard_reason_code`.

5. `tools/ops_scripts_smoke_test.sh`
   1. `windowed_signoff` assertions now pin per-window ingestion strict fields.
   2. `route_fee_signoff` assertions now pin per-window + final nested ingestion strict fields (including dynamic window id path).
   3. `execution_runtime_readiness` assertions now pin route-fee nested ingestion strict fields across pass/strict/override/skip/profile/bundle branches.

6. `ROAD_TO_PRODUCTION.md`
   1. added item `509` for this propagation closure.

## Validation

1. `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES="windowed_signoff,route_fee_signoff,execution_runtime_readiness" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=route_fee_signoff bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `2` (route-profile calibration evidence quality).
2. `ROAD_TO_PRODUCTION.md` next-code-queue item `3` (fee decomposition sign-off evidence quality).
3. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (runtime-readiness evidence chain consistency).
