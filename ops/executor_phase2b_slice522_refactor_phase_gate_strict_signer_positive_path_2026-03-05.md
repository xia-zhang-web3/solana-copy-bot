# Executor Phase2b Slice522 — refactor phase-gate strict signer positive-path coverage (2026-03-05)

## Scope

1. `tools/refactor_baseline_prepare.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Context

1. This slice extends strict signer coverage after:
   - `4cc86fc` (targeted smoke default `auto`, heavy => `fast`),
   - `21dafc4` / `521` (phase-gate fail-fast by stage).
2. Gap before this slice: refactor phase-gate had strict signer fail + invalid-token coverage, but no explicit strict signer PASS path across full nested chain.

## What changed

1. `tools/refactor_baseline_prepare.sh`
- Added optional fixture override:
  - `REFACTOR_BASELINE_EXECUTOR_SIGNER_PUBKEY`
- When set, helper appends `COPYBOT_EXECUTOR_SIGNER_PUBKEY=<value>` to generated `executor.env`.
- Default behavior unchanged (no signer key injected unless override is set).

2. `tools/ops_scripts_smoke_test.sh`
- Extended `run_refactor_phase_gate_case` full profile with strict signer positive matrix:
  - `REFACTOR_PHASE_GATE_REQUIRE_NON_BOOTSTRAP_SIGNER=true`
  - `REFACTOR_BASELINE_EXECUTOR_SIGNER_PUBKEY=Tokenkeg...` (non-bootstrap)
- Added parity assertions for PASS path across normalized outputs:
  - go-no-go: `non_bootstrap_signer_guard_verdict/reason_code`
  - rehearsal: `go_nogo_non_bootstrap_signer_guard_verdict/reason_code`
  - rollout: `rehearsal_nested_non_bootstrap_signer_guard_verdict/reason_code`
- Existing strict-fail and invalid-token pins remain in place.

## Validation

1. `bash -n tools/refactor_baseline_prepare.sh tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS (`auto` => `fast`, from `4cc86fc`)
5. `cargo check -p copybot-executor -q` — PASS
