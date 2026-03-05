# Executor Phase-2B Slice 517 — Non-Bootstrap Signer Strict Gate + Rollout Parity

Date: 2026-03-05  
Owner: execution-dev

## Ordering Governance

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).

## Scope

1. `tools/execution_go_nogo_report.sh`
2. `tools/execution_server_rollout_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Why

`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` and ingestion strict-gates were already closed through nested evidence chains, but signer provenance still allowed bootstrap-signer drift to pass unless manually inspected. This slice adds a strict non-bootstrap signer gate and pins its propagation/observability through rollout orchestration.

## Changes

1. `tools/execution_go_nogo_report.sh`
   1. added strict bool control `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER` (default `false`) with fail-closed parsing.
   2. added signer classifier helper for bootstrap/placeholder pubkeys.
   3. added strict signer guard fields:
      1. `executor_signer_pubkey_observed`,
      2. `non_bootstrap_signer_guard_verdict`,
      3. `non_bootstrap_signer_guard_reason`,
      4. `non_bootstrap_signer_guard_reason_code`.
   4. strict behavior when required:
      1. missing env/signer -> `UNKNOWN`,
      2. bootstrap/placeholder signer -> `WARN`,
      3. non-bootstrap signer -> `PASS`.
   5. integrated into overall go/no-go verdict tree (`UNKNOWN/WARN` fail-close to `NO_GO`).

2. `tools/execution_server_rollout_report.sh`
   1. added strict bool parse/propagation for `GO_NOGO_REQUIRE_NON_BOOTSTRAP_SIGNER`.
   2. propagated env into nested `go_nogo`, `rehearsal`, `executor_final`, `adapter_final`.
   3. extracted and validated nested go/no-go signer fields fail-closed:
      1. strict bool parity: `go_nogo_require_non_bootstrap_signer`,
      2. verdict whitelist: `PASS|WARN|UNKNOWN|SKIP`,
      3. non-empty reason code,
      4. parity invariant (`required=true => non-SKIP`, `required=false => SKIP`).
   4. emitted signer strict observability fields in server-rollout summary.

3. `tools/ops_scripts_smoke_test.sh`
   1. `go_nogo_executor_backend_mode_guard`:
      1. added strict signer PASS pin (non-bootstrap signer),
      2. added strict signer NO_GO pin (bootstrap signer),
      3. added strict signer UNKNOWN pin (missing signer),
      4. added invalid bool token fail-closed pin.
   2. `execution_server_rollout`:
      1. added baseline assertions for signer strict gate (`false => SKIP/gate_disabled`),
      2. added strict signer negative pin (`true` + bootstrap signer => `NO_GO`),
      3. aligned assertion to current verdict precedence (`server_rollout_reason_code=adapter_final_not_go`).

4. `ROAD_TO_PRODUCTION.md`
   1. added item `517` for this closure.

## Validation

1. `bash -n tools/execution_go_nogo_report.sh tools/execution_server_rollout_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=execution_server_rollout_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh` — PASS
5. `cargo check -p copybot-executor -q` — PASS

## Mapping

1. `ROAD_TO_PRODUCTION.md` next-code-queue item `4` (runtime evidence-chain stability before Stage D).
2. strict safety closure for signer provenance in non-live/tiny-live go/no-go orchestration.
