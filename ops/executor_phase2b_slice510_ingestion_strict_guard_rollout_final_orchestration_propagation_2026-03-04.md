# Executor Phase-2B Slice 510 — Ingestion Strict Guard Propagation Through Rollout/Final Orchestration

Date: 2026-03-04  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/execution_devnet_rehearsal.sh`
2. `tools/executor_rollout_evidence_report.sh`
3. `tools/executor_final_evidence_report.sh`
4. `tools/adapter_rollout_evidence_report.sh`
5. `tools/adapter_rollout_final_evidence_report.sh`
6. `tools/execution_server_rollout_report.sh`
7. `tools/ops_scripts_smoke_test.sh`
8. `ROAD_TO_PRODUCTION.md`

## Why

Slice `509` closed strict-ingestion propagation for signoff/final/readiness, but rollout/final orchestration helpers still lacked equivalent nested ingestion strict-guard parity checks and summary observability.

This left an end-to-end gap in the rollout chain (`server_rollout -> {executor_final, adapter_final} -> rollout -> rehearsal -> go_nogo`).

## Changes

1. `tools/execution_devnet_rehearsal.sh`
   1. added strict parse for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated it to nested `go_nogo`, `windowed_signoff`, and `route_fee_signoff` calls.
   3. extracted nested go/no-go ingestion fields:
      1. `go_nogo_require_ingestion_grpc`,
      2. `go_nogo_ingestion_grpc_guard_verdict`,
      3. `go_nogo_ingestion_grpc_guard_reason_code`.
   4. added fail-closed parity validation (`true => non-SKIP`, `false => SKIP`).
   5. exposed the ingestion strict fields in summary output.

2. `tools/executor_rollout_evidence_report.sh`
   1. added strict parse for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated it into nested `execution_devnet_rehearsal.sh`.
   3. extracted/validated nested rehearsal ingestion fields:
      1. `rehearsal_nested_go_nogo_require_ingestion_grpc`,
      2. `rehearsal_nested_ingestion_grpc_guard_verdict`,
      3. `rehearsal_nested_ingestion_grpc_guard_reason_code`.
   4. enforced fail-closed parity and emitted summary observability fields.

3. `tools/executor_final_evidence_report.sh`
   1. added strict parse for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated it to nested `executor_rollout_evidence_report.sh`.
   3. extracted/validated rollout nested ingestion fields:
      1. `rollout_nested_go_nogo_require_ingestion_grpc`,
      2. `rollout_nested_ingestion_grpc_guard_verdict`,
      3. `rollout_nested_ingestion_grpc_guard_reason_code`.
   4. enforced fail-closed parity and surfaced fields in final summary.

4. `tools/adapter_rollout_evidence_report.sh`
   1. added strict parse for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated to nested rehearsal + route-fee signoff helper calls.
   3. extracted/validated nested rehearsal ingestion strict fields.
   4. enforced fail-closed parity and surfaced nested ingestion fields in summary.

5. `tools/adapter_rollout_final_evidence_report.sh`
   1. added strict parse for `GO_NOGO_REQUIRE_INGESTION_GRPC`.
   2. propagated into nested `adapter_rollout_evidence_report.sh`.
   3. extracted/validated rollout nested ingestion strict fields.
   4. enforced fail-closed parity and surfaced nested ingestion fields in final summary.

6. `tools/execution_server_rollout_report.sh`
   1. added executor-final and adapter-final nested ingestion strict extraction/validation:
      1. top-level nested bool parity (`*_go_nogo_require_ingestion_grpc`),
      2. rollout nested bool parity (`*_rollout_nested_go_nogo_require_ingestion_grpc`),
      3. rollout nested guard verdict/reason validation (`*_rollout_nested_ingestion_grpc_guard_*`).
   2. enforced fail-closed parity against orchestrator strict setting.
   3. exposed new nested ingestion strict fields in server-rollout summary.

7. `tools/ops_scripts_smoke_test.sh`
   1. expanded `devnet_rehearsal`, `executor_rollout_evidence`, `adapter_rollout_evidence`, and `execution_server_rollout` assertions with new ingestion strict fields.
   2. added missing mock-override assertions for nested ingestion strict fields in executor-final and adapter-final branches.
   3. preserved existing strict semantics:
      1. strict enabled + yellowstone source -> PASS,
      2. strict disabled -> SKIP gate parity where expected.

8. `ROAD_TO_PRODUCTION.md`
   1. added item `510` documenting this rollout/final ingestion strict-chain closure.

## Validation

1. `bash -n tools/execution_devnet_rehearsal.sh tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES="devnet_rehearsal_fast,executor_rollout_evidence_fast,adapter_rollout_evidence_fast,execution_server_rollout_fast,execution_runtime_readiness_fast" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
4. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `1` (server rollout evidence quality and strict gate integrity).
2. `ROAD_TO_PRODUCTION.md` next-code-queue item `2` (route-profile calibration chain consistency).
3. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (runtime/devnet rehearsal evidence chain correctness).
