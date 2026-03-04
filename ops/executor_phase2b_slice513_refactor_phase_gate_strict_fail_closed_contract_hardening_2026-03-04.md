# Executor Phase-2B Slice 513 — Refactor Phase-Gate Strict Fail-Closed Contract Hardening

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

Slice `512` added a deterministic strict-ingestion baseline harness, but phase-gate behavior was still mostly capture-oriented. It needed explicit fail-closed contract checks inside `refactor_phase_gate.sh` and stronger negative smoke coverage, while keeping default smoke runtime bounded.

## Changes

1. `tools/refactor_phase_gate.sh`
   1. added strict env controls:
      1. `REFACTOR_PHASE_GATE_REQUIRE_EXECUTOR_UPSTREAM` (`true|false`),
      2. `REFACTOR_PHASE_GATE_REQUIRE_INGESTION_GRPC` (`true|false`),
      3. `REFACTOR_PHASE_GATE_INGESTION_SOURCE` (non-empty).
   2. added fail-closed parsing for both strict bool controls.
   3. added stage-level strict validators over captured outputs:
      1. boolean parity checks (`go_nogo_require_*` fields),
      2. strict-guard verdict parity (`required=true => PASS`, `required=false => SKIP`),
      3. GO-verdict checks (`overall_go_nogo_verdict`, `devnet_rehearsal_verdict`, `adapter_rollout_verdict`).
   4. nested helper invocations are now wrapped with deterministic fail diagnostics:
      1. captured helper output is emitted on failure,
      2. explicit `phase-gate error: <helper> failed for stage=<...>` is printed,
      3. phase-gate exits `3`.
   5. summary fields now reflect normalized strict controls instead of hardcoded literals.

2. `tools/ops_scripts_smoke_test.sh`
   1. `run_refactor_phase_gate_case` now supports `case_profile={full|fast}`.
   2. `fast` profile retains baseline artifact/summary validation and returns early.
   3. `full` profile now includes negative fail-closed pins:
      1. invalid strict source (`REFACTOR_PHASE_GATE_INGESTION_SOURCE=helius_ws`) -> phase-gate failure + `overall_go_nogo_verdict: NO_GO`,
      2. invalid strict bool token (`REFACTOR_PHASE_GATE_REQUIRE_INGESTION_GRPC=sometimes`) -> strict parse failure.
   4. added targeted alias `refactor_phase_gate_fast`.
   5. added `refactor_phase_gate`/`refactor_phase_gate_fast` to heavy-case auto-profile detection.
   6. added `refactor_phase_gate` into `heavy_runtime_chain` expansion group.
   7. switched full-suite `main()` call from full to `fast` profile to reduce default runtime.

3. `ROAD_TO_PRODUCTION.md`
   1. added item `513` documenting this hardening slice.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh tools/refactor_baseline_prepare.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_PROFILE=auto OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS (`ops smoke targeted profile: fast`)
4. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (devnet/runtime rehearsal evidence-chain integrity).
2. Runtime smoke throughput objective (fast-by-default for heavy phase-gate path in full suite).
