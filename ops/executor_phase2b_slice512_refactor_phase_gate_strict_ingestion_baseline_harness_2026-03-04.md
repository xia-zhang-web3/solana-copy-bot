# Executor Phase-2B Slice 512 — Refactor Phase-Gate Strict Ingestion Baseline Harness

Date: 2026-03-04  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/refactor_baseline_prepare.sh`
2. `tools/refactor_phase_gate.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Why

`refactor_phase_gate` needed deterministic strict-gate baseline coverage for the ingestion guard chain. Existing fixture/setup did not always provide explicit upstream topology + ingestion source in baseline artifacts, and smoke lacked a dedicated phase-gate target asserting strict-ingestion propagation through normalized outputs.

## Changes

1. `tools/refactor_baseline_prepare.sh`
   1. baseline config now includes `[ingestion] source = "yellowstone_grpc"`.
   2. baseline `executor.env` now includes deterministic upstream topology for strict checks:
      1. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=paper`,
      2. `COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=http://127.0.0.1:18080/submit`,
      3. `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=http://127.0.0.1:18080/simulate`.

2. `tools/refactor_phase_gate.sh`
   1. go/no-go, rehearsal, and rollout calls now run with strict gates enabled:
      1. `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true`,
      2. `GO_NOGO_REQUIRE_INGESTION_GRPC=true`,
      3. `SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone_grpc`.
   2. summary now surfaces strict baseline context:
      1. `go_nogo_require_executor_upstream: true`,
      2. `go_nogo_require_ingestion_grpc: true`,
      3. `ingestion_source: yellowstone_grpc`.

3. `tools/ops_scripts_smoke_test.sh`
   1. added targeted case `refactor_phase_gate`.
   2. case verifies:
      1. phase summary fields and artifact paths,
      2. existence of raw/normalized checksum manifests,
      3. existence of normalized outputs,
      4. strict-ingestion parity fields in normalized captures:
         1. go/no-go: `ingestion_grpc_guard_verdict=PASS`,
         2. rehearsal: `go_nogo_ingestion_grpc_guard_verdict=PASS`,
         3. rollout: `rehearsal_nested_ingestion_grpc_guard_verdict=PASS`.

4. `ROAD_TO_PRODUCTION.md`
   1. added item `512` for this slice.

## Validation

1. `bash -n tools/refactor_baseline_prepare.sh tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
4. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (devnet/runtime rehearsal evidence-chain integrity).
2. `ROAD_TO_PRODUCTION.md` next-code-queue item `2` (route-profile/runtime signoff evidence continuity).
