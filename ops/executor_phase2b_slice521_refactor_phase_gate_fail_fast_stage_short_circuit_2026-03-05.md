# Executor Phase2b Slice521 — refactor phase-gate fail-fast stage short-circuit (2026-03-05)

## Scope

1. `tools/refactor_phase_gate.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Context

1. Slice is built on smoke acceleration baseline from commit `4cc86fc` (`OPS_SMOKE_PROFILE` default `auto`, heavy targeted => `fast`).
2. Problem: `refactor_phase_gate` still executed downstream stages before validating upstream stage output, making strict-negative paths unnecessarily slow/noisy.

## What changed

1. `tools/refactor_phase_gate.sh`
- Added stage fail helper `fail_phase_gate_stage`.
- Validation now runs immediately per stage:
  1. run+capture `go_nogo` -> validate -> fail-close immediately if invalid,
  2. run+capture `rehearsal` only after `go_nogo` validation PASS,
  3. run+capture `rollout` only after `rehearsal` validation PASS.
- Fail-close output now includes:
  - captured stage output,
  - explicit validation errors,
  - deterministic marker `phase-gate error: <script> failed for stage=<stage>`.

2. `tools/ops_scripts_smoke_test.sh`
- Updated `run_refactor_phase_gate_case` strict negative assertions to new fail-fast stage behavior:
  1. strict jito (`REFACTOR_PHASE_GATE_REQUIRE_JITO_RPC_POLICY=true`) -> stage=`go_nogo`,
  2. strict ingestion/source mismatch -> stage=`go_nogo`,
  3. strict non-bootstrap signer -> stage=`go_nogo` + direct go-no-go signer guard fields.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS (`auto` => `fast`, from `4cc86fc`)
5. `cargo check -p copybot-executor -q` — PASS
