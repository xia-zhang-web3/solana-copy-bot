# Executor Phase-2B Slice 516 — Refactor Phase-Gate Jito Strict-Policy Parity

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

Slice `515` closed strict fastlane parity in `refactor_phase_gate`, but jito policy strictness was still outside the phase-gate harness parity chain. This left a gap where jito strict requirement could drift without phase-gate level fail-closed enforcement.

## Changes

1. `tools/refactor_phase_gate.sh`
   1. added strict bool env control:
      1. `REFACTOR_PHASE_GATE_REQUIRE_JITO_RPC_POLICY` (default `false`).
   2. added fail-closed strict bool parsing for this control.
   3. propagated control to all nested calls:
      1. `GO_NOGO_REQUIRE_JITO_RPC_POLICY` -> go-no-go,
      2. same -> rehearsal,
      3. same -> rollout.
   4. extended policy validator to support required-mode contracts:
      1. `PASS` (strict pass requirement),
      2. `NON_SKIP` (strict activation requirement).
   5. added stage-level parity checks for jito policy chain:
      1. bool parity on `go_nogo_require_jito_rpc_policy`,
      2. verdict parity on `jito_rpc_policy_verdict` (`true=>NON_SKIP`, `false=>SKIP`),
      3. reason-code parity on `jito_rpc_policy_reason_code` (`true=>not gate_disabled`, `false=>gate_disabled`).
   6. exposed `go_nogo_require_jito_rpc_policy` in phase summary.

2. `tools/ops_scripts_smoke_test.sh`
   1. `refactor_phase_gate` baseline assertions now pin `go_nogo_require_jito_rpc_policy=false`.
   2. added strict-jito negative matrix:
      1. `REFACTOR_PHASE_GATE_REQUIRE_JITO_RPC_POLICY=true`,
      2. baseline profile remains non-jito/rpc,
      3. assert fail-closed stage error at rehearsal boundary,
      4. assert nested jito policy signal in captured output (`jito_rpc_policy_verdict: WARN`, `jito_rpc_policy_reason_code: route_profile_not_pass`).
   3. added invalid-bool fail-closed pin:
      1. `REFACTOR_PHASE_GATE_REQUIRE_JITO_RPC_POLICY=sometimes` must fail strict parsing.

3. `ROAD_TO_PRODUCTION.md`
   1. added item `516` for this closure.

## Validation

1. `bash -n tools/refactor_phase_gate.sh tools/ops_scripts_smoke_test.sh tools/refactor_baseline_prepare.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=refactor_phase_gate_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_PROFILE=auto OPS_SMOKE_TARGET_CASES=refactor_phase_gate bash tools/ops_scripts_smoke_test.sh` — PASS
5. `OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
6. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (devnet/runtime rehearsal evidence-chain integrity).
2. strict-gate chain completeness for refactor phase-gate harness (executor-upstream + ingestion + fastlane + jito-policy).
