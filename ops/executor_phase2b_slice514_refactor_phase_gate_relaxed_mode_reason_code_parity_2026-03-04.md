# Executor Phase-2B Slice 514 — Refactor Phase-Gate Relaxed-Mode + Reason-Code Parity

Date: 2026-03-04  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/refactor_phase_gate.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Why

Slice `513` introduced strict verdict contracts and negative fail-closed pins, but relaxed-mode (`require=false`) behavior was not covered as first-class positive matrix paths, and strict guard reason-code contracts were not validated in phase-gate itself.

## Changes

1. `tools/refactor_phase_gate.sh`
   1. added strict reason-code validator for guard fields:
      1. `required=true => reason_code != gate_disabled`,
      2. `required=false => reason_code = gate_disabled`.
   2. applied reason-code checks to all strict guard chains:
      1. go-no-go (`executor_backend_mode_guard_reason_code`, `executor_upstream_endpoint_guard_reason_code`, `ingestion_grpc_guard_reason_code`),
      2. rehearsal (`go_nogo_executor_*_guard_reason_code`, `go_nogo_ingestion_grpc_guard_reason_code`),
      3. rollout (`rehearsal_nested_*_guard_reason_code`).

2. `tools/ops_scripts_smoke_test.sh`
   1. expanded `run_refactor_phase_gate_case` (full profile) with positive relaxed matrices:
      1. ingestion relaxed path:
         1. `REFACTOR_PHASE_GATE_REQUIRE_INGESTION_GRPC=false`,
         2. `REFACTOR_PHASE_GATE_INGESTION_SOURCE=helius_ws`,
         3. asserts `SKIP/gate_disabled` for go-no-go/rehearsal/rollout ingestion guard fields.
      2. executor-upstream relaxed path:
         1. `REFACTOR_PHASE_GATE_REQUIRE_EXECUTOR_UPSTREAM=false`,
         2. asserts `SKIP/gate_disabled` for go-no-go/rehearsal/rollout executor backend/topology guard fields.
   2. existing negative pins remain in place:
      1. invalid source under strict mode,
      2. invalid strict bool token.

3. `ROAD_TO_PRODUCTION.md`
   1. added item `514` for this closure.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh tools/refactor_baseline_prepare.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=auto OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
5. `OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
6. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (devnet/runtime rehearsal evidence-chain integrity).
2. strict/relaxed guard contract completeness for refactor phase-gate harness.
