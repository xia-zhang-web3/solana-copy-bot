# Executor Phase2b Slice520 — refactor phase-gate strict non-bootstrap signer parity (2026-03-05)

## Scope

1. `tools/refactor_phase_gate.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Context

1. This slice continues after smoke acceleration baseline commit `4cc86fc` (`OPS_SMOKE_PROFILE` default `auto`, heavy targeted auto-resolve to `fast`).
2. For parity hardening coverage in this slice, `refactor_phase_gate` full profile is still executed explicitly (`OPS_SMOKE_PROFILE=full`) so negative strict matrix remains pinned.

## What changed

1. `tools/refactor_phase_gate.sh`
- Added strict bool env control:
  - `REFACTOR_PHASE_GATE_REQUIRE_NON_BOOTSTRAP_SIGNER` (default `false`, fail-closed parse).
- Propagated signer strict gate to all nested helpers:
  - `execution_go_nogo_report.sh`
  - `execution_devnet_rehearsal.sh`
  - `adapter_rollout_evidence_report.sh`
- Added stage-level parity validation chain:
  - `go_nogo_require_non_bootstrap_signer` bool parity
  - `non_bootstrap_signer_guard_verdict` + reason-code parity on go/no-go
  - `go_nogo_non_bootstrap_signer_guard_*` parity on rehearsal
  - `rehearsal_nested_non_bootstrap_signer_guard_*` parity on rollout
- Added summary observability field:
  - `go_nogo_require_non_bootstrap_signer`

2. `tools/ops_scripts_smoke_test.sh`
- Extended `run_refactor_phase_gate_case` full-profile assertions:
  - baseline relaxed signer path (`required=false`) => `SKIP/gate_disabled` across normalized go-no-go/rehearsal/rollout outputs;
  - strict signer path (`required=true`) fail-closes against baseline fixture signer state;
  - invalid bool token reject for `REFACTOR_PHASE_GATE_REQUIRE_NON_BOOTSTRAP_SIGNER=sometimes`.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS (`auto` => `fast`, from `4cc86fc` baseline)
