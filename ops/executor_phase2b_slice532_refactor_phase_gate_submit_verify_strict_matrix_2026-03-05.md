# Slice 532 — refactor phase-gate strict submit-verify matrix expansion

## Scope
- `tools/refactor_baseline_prepare.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Problem
After slice 531, phase-gate had strict submit-verify propagation/parity, but full-path coverage was still biased to:
- relaxed baseline (`SKIP/gate_disabled`) and
- one strict negative case (`WARN/submit_verify_strict_not_enabled`).

There was no deterministic strict PASS chain in phase-gate harness and no phase-gate-level coverage for strict topology negatives already enforced by go/no-go (`fallback identity collision`, `invalid primary URL`).

## Implemented
1. `refactor_baseline_prepare.sh`
   - added optional submit-verify fixture overrides written into generated `executor.env`:
     - `REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_STRICT`
     - `REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_URL`
     - `REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL`
2. `ops_scripts_smoke_test.sh` (`run_refactor_phase_gate_case`, full profile)
   - added strict submit-verify PASS matrix:
     - `REFACTOR_PHASE_GATE_REQUIRE_SUBMIT_VERIFY_STRICT=true`
     - baseline overrides set strict=true + valid primary/fallback URLs
     - asserts PASS chain across normalized outputs:
       - go-no-go: `submit_verify_guard_verdict: PASS`, `submit_verify_guard_reason_code: submit_verify_strict_enabled`
       - rehearsal: `go_nogo_submit_verify_guard_verdict: PASS`, `go_nogo_submit_verify_guard_reason_code: submit_verify_strict_enabled`
       - rollout: `rehearsal_nested_submit_verify_guard_verdict: PASS`, `rehearsal_nested_submit_verify_guard_reason_code: submit_verify_strict_enabled`
   - added strict negative topology matrix: normalized fallback identity collision
     - primary: `https://verify.integration.test`
     - fallback: `https://VERIFY.integration.test:443/`
     - fail-fast at go-no-go with `WARN/submit_verify_fallback_same_as_primary`
   - added strict negative topology matrix: invalid primary URL
     - primary: `not-a-url`
     - fail-fast at go-no-go with `UNKNOWN/submit_verify_primary_invalid`

## Validation
1. `bash -n tools/refactor_baseline_prepare.sh tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS

## Outcome
Phase-gate strict submit-verify harness now covers full matrix (relaxed SKIP, strict PASS, strict WARN, strict UNKNOWN) with deterministic fixture controls and stage-level fail-fast validation at go-no-go boundary.
