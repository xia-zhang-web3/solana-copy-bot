# Slice 531 — refactor phase-gate strict submit-verify parity closure

## Scope
- `tools/refactor_phase_gate.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Problem
`refactor_phase_gate.sh` had no strict submit-verify control path, while other strict guards (executor-upstream, ingestion-grpc, fastlane, jito, non-bootstrap-signer) already had full coverage:
- no phase-gate env input for submit-verify strictness;
- no propagation to nested go-no-go/rehearsal/rollout calls;
- no stage-level parity checks on submit-verify guard fields;
- no summary observability for the strict submit-verify requirement.

This created a silent drift: enabling strict mode at phase-gate level was impossible, and nested scripts stayed on default relaxed submit-verify behavior.

## Implemented
1. Added fail-closed phase-gate input:
   - `REFACTOR_PHASE_GATE_REQUIRE_SUBMIT_VERIFY_STRICT` (default `false`)
   - strict bool parse with hard fail on invalid token.
2. Propagated strict submit-verify setting to all three nested calls:
   - `execution_go_nogo_report.sh`
   - `execution_devnet_rehearsal.sh`
   - `adapter_rollout_evidence_report.sh`
   via `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT`.
3. Added stage validations in `refactor_phase_gate.sh`:
   - go/no-go:
     - `go_nogo_require_submit_verify_strict`
     - `submit_verify_guard_verdict`
     - `submit_verify_guard_reason_code`
   - rehearsal:
     - `go_nogo_require_submit_verify_strict`
     - `go_nogo_submit_verify_guard_verdict`
     - `go_nogo_submit_verify_guard_reason_code`
   - rollout:
     - `go_nogo_require_submit_verify_strict`
     - `rehearsal_nested_go_nogo_require_submit_verify_strict`
     - `rehearsal_nested_submit_verify_guard_verdict`
     - `rehearsal_nested_submit_verify_guard_reason_code`
   with the existing strict contract:
   - required=true => verdict must be `PASS`
   - required=false => verdict must be `SKIP`
   - required=false => reason code must be `gate_disabled`.
4. Added summary field:
   - `go_nogo_require_submit_verify_strict`.
5. Extended `run_refactor_phase_gate_case` smoke coverage:
   - baseline relaxed path pins submit-verify `SKIP/gate_disabled` across go-no-go/rehearsal/rollout normalized outputs;
   - strict path (`REFACTOR_PHASE_GATE_REQUIRE_SUBMIT_VERIFY_STRICT=true`) pins fail-fast at go-no-go with `submit_verify_guard_verdict: WARN` and `submit_verify_guard_reason_code: submit_verify_strict_not_enabled`;
   - invalid bool token path pins fail-closed parse reject.

## Validation
1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS (`auto -> fast`)
5. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS

## Outcome
Submit-verify strictness is now first-class in the phase-gate strict harness with full fail-closed propagation/parity/observability, aligned with the other strict governance controls.
