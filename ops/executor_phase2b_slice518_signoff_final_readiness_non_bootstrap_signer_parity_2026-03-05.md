# Executor Phase-2B Slice 518 — Non-Bootstrap Signer Guard Parity (Signoff/Final/Readiness)

Date: 2026-03-05  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/execution_windowed_signoff_report.sh`
2. `tools/execution_route_fee_signoff_report.sh`
3. `tools/execution_route_fee_final_evidence_report.sh`
4. `tools/execution_runtime_readiness_report.sh`
5. `tools/ops_scripts_smoke_test.sh`

## Why

Slice `517` added strict non-bootstrap signer guard in go/no-go and server rollout direct path, but signer strict observability/parity was not yet propagated through signoff/final/readiness chain. That left nested-chain drift risk in runtime evidence packages.

## Changes

1. `tools/execution_windowed_signoff_report.sh`
   1. added strict bool input `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER`.
   2. propagated control to nested go/no-go calls.
   3. extracted + fail-closed validated per-window nested fields:
      1. `window_*h_go_nogo_require_non_bootstrap_signer`,
      2. `window_*h_non_bootstrap_signer_guard_verdict`,
      3. `window_*h_non_bootstrap_signer_guard_reason_code`.
   4. enforced parity invariant: `required=true => verdict!=SKIP`, `required=false => verdict=SKIP`.

2. `tools/execution_route_fee_signoff_report.sh`
   1. added strict bool input `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER`.
   2. propagated control to nested go/no-go calls.
   3. added per-window extraction/validation/parity for signer strict fields with same fail-closed rules.

3. `tools/execution_route_fee_final_evidence_report.sh`
   1. added strict bool input `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER`.
   2. propagated control into nested `execution_route_fee_signoff_report.sh`.
   3. dynamic-window extraction extended with signer keys:
      1. `signoff_nested_go_nogo_require_non_bootstrap_signer`,
      2. `signoff_nested_non_bootstrap_signer_guard_verdict`,
      3. `signoff_nested_non_bootstrap_signer_guard_reason_code`.
   4. added fail-closed parity check for signer strict gate.

4. `tools/execution_runtime_readiness_report.sh`
   1. added strict bool input `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER`.
   2. propagated it into nested final helpers.
   3. extended route-fee nested extraction with signer strict fields:
      1. `route_fee_final_nested_go_nogo_require_non_bootstrap_signer`,
      2. `route_fee_final_nested_non_bootstrap_signer_guard_verdict`,
      3. `route_fee_final_nested_non_bootstrap_signer_guard_reason_code`.
   4. enforced fail-closed parity (`true => non-SKIP`, `false => SKIP`).

5. `tools/ops_scripts_smoke_test.sh`
   1. extended `windowed_signoff` assertions with signer strict fields (strict hold, GO, bundle).
   2. extended `route_fee_signoff` and route-fee-final assertions with signer strict fields (hold/no-go/go/windows 1,6/bundle).
   3. extended `execution_runtime_readiness` assertions with route-fee nested signer strict fields across pass/strict/override/skip/profile/bundle branches.

## Validation

1. `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh tools/execution_runtime_readiness_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=windowed_signoff bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=route_fee_signoff bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh` — PASS
5. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (runtime evidence-chain integrity before Stage D).
2. signer strict-gate propagation hardening for signoff -> final -> readiness chain.
