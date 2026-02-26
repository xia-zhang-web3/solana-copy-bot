# Executor Phase 2B Slice 127 — Rollout Input Short-Circuit Hardening (2026-02-26)

## Scope

- close low-gap where rollout wrappers kept invoking downstream helpers after input validation had already failed
- reduce unnecessary runtime/ops load and noisy external calls under known-invalid input

## Changes

1. Updated `tools/executor_rollout_evidence_report.sh`:
   - `rotation` helper execution is now gated behind `input_errors==0`
   - `preflight` helper execution is now gated behind `input_errors==0`
   - `rehearsal` section now short-circuits to `NO_GO` + `input_error` reason when `input_errors>0`
2. Updated `tools/adapter_rollout_evidence_report.sh`:
   - `rotation` helper execution is now gated behind `input_errors==0`
   - `rehearsal` section now short-circuits to `NO_GO` + `input_error` reason when `input_errors>0`
   - top-level route-fee signoff section now short-circuits when `input_errors>0` (marks signoff as skipped/unknown with input-error reason code)
3. Strengthened smoke assertions in `tools/ops_scripts_smoke_test.sh` invalid-token branches:
   - executor rollout invalid-token case now asserts `rotation_readiness_verdict=UNKNOWN` and `preflight_verdict=UNKNOWN`
   - adapter rollout invalid-token case now asserts `rotation_readiness_verdict=UNKNOWN` and `route_fee_signoff_verdict=UNKNOWN`
4. Updated ROAD ledger item `287`.

## Files

- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh` — PASS
2. `EXECUTOR_ENV_PATH=<tmp>/executor.env ADAPTER_ENV_PATH=<tmp>/adapter.env CONFIG_PATH=<tmp>/config.toml GO_NOGO_REQUIRE_JITO_RPC_POLICY=maybe bash tools/executor_rollout_evidence_report.sh 24 60` — PASS (`exit 3`, explicit token error, helper sections remain unexecuted)
3. `ADAPTER_ENV_PATH=<tmp>/adapter.env CONFIG_PATH=<tmp>/config.toml REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED=perhaps bash tools/adapter_rollout_evidence_report.sh 24 60` — PASS (`exit 3`, explicit token error, helper sections remain unexecuted)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` was intentionally not executed in this slice to avoid long-running smoke loops during active iteration; targeted invalid-token branches were executed directly.
