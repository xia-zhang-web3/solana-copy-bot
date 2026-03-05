# Slice 530 — submit-verify strict parity closure in adapter/runtime/server chains

## Scope
- `tools/adapter_rollout_evidence_report.sh`
- `tools/adapter_rollout_final_evidence_report.sh`
- `tools/execution_runtime_readiness_report.sh`
- `tools/execution_server_rollout_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Problem
Strict submit-verify parity was fully wired in go/no-go + route-fee chains, but nested adapter/final/runtime/server observability and parity checks were incomplete:
1. adapter rollout/final helpers did not fully expose and validate nested submit-verify strict fields end-to-end;
2. runtime-readiness did not consistently pin both adapter-final and route-fee-final submit-verify nested parity fields;
3. server-rollout orchestration did not validate/surface adapter-final nested submit-verify strict fields with fail-closed parity.

## Implemented
1. `adapter_rollout_evidence_report.sh`
   - added strict parse/propagation for `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT`;
   - extracted nested rehearsal fields:
     - `go_nogo_require_submit_verify_strict`
     - `go_nogo_submit_verify_guard_verdict`
     - `go_nogo_submit_verify_guard_reason_code`;
   - enforced fail-closed validation + parity:
     - `required=true => verdict != SKIP`
     - `required=false => verdict == SKIP`;
   - exported nested summary fields as `rehearsal_nested_*`.
2. `adapter_rollout_final_evidence_report.sh`
   - propagated strict submit-verify setting to nested rollout helper;
   - validated nested `rehearsal_nested_*` submit-verify fields from rollout output;
   - exported as `rollout_nested_*` with the same parity contract.
3. `execution_runtime_readiness_report.sh`
   - propagated strict submit-verify setting to both nested branches (`adapter_rollout_final`, `execution_route_fee_final`);
   - added adapter branch extraction/validation for:
     - `rollout_nested_go_nogo_require_submit_verify_strict`
     - `rollout_nested_submit_verify_guard_verdict`
     - `rollout_nested_submit_verify_guard_reason_code`;
   - added route-fee branch extraction/validation for:
     - `signoff_nested_go_nogo_require_submit_verify_strict`
     - `signoff_nested_submit_verify_guard_verdict`
     - `signoff_nested_submit_verify_guard_reason_code`;
   - enforced fail-closed parity and exported `adapter_final_nested_*` + `route_fee_final_nested_*` summary fields.
4. `execution_server_rollout_report.sh`
   - added adapter-final nested submit-verify extraction/validation/parity:
     - `adapter_final_go_nogo_require_submit_verify_strict`
     - `adapter_final_rollout_nested_go_nogo_require_submit_verify_strict`
     - `adapter_final_rollout_nested_submit_verify_guard_verdict`
     - `adapter_final_rollout_nested_submit_verify_guard_reason_code`;
   - included fields in top-level server-rollout summary with fail-closed checks.
5. `ops_scripts_smoke_test.sh`
   - expanded assertions in:
     - `adapter_rollout_evidence`
     - `execution_runtime_readiness`
     - `execution_server_rollout`
   - covered pass/skip/profile/bundle/mock scenarios for newly exported submit-verify fields.

## Validation
1. `bash -n tools/adapter_rollout_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/execution_runtime_readiness_report.sh tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES="adapter_rollout_evidence,execution_server_rollout,execution_runtime_readiness" bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh` — PASS

## Outcome
Submit-verify strict parity is now fully observable and fail-closed across adapter rollout/final, runtime readiness, and server rollout orchestration chains, closing the remaining nested propagation gap.
