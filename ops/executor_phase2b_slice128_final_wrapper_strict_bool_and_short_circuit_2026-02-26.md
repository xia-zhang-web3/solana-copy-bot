# Executor Phase 2B Slice 128 — Final Wrapper Strict Bool + Input Short-Circuit (2026-02-26)

## Scope

- harden top-level final evidence wrappers so invalid boolean gate input is rejected before nested rollout invocation
- avoid unnecessary nested rollout/helper execution when final-wrapper input is already invalid

## Changes

1. Updated `tools/executor_final_evidence_report.sh`:
   - strict boolean token parser added for all gate booleans (`true/false/1/0/yes/no/on/off`)
   - input validation now includes numeric window checks + required file-path checks + strict bool checks
   - when `input_errors>0`, wrapper short-circuits to fail-closed `NO_GO` (`rollout_reason_code=input_error`) without invoking `executor_rollout_evidence_report.sh`
2. Updated `tools/adapter_rollout_final_evidence_report.sh`:
   - same strict bool parser and input short-circuit policy
   - skips nested rollout invocation on invalid top-level input and returns fail-closed `NO_GO` with `input_error`
3. Added smoke coverage in `tools/ops_scripts_smoke_test.sh`:
   - invalid `GO_NOGO_REQUIRE_FASTLANE_DISABLED` branch for `executor_final_evidence_report.sh`
   - invalid `REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE` branch for `adapter_rollout_final_evidence_report.sh`
   - assertions include `rollout_reason_code=input_error` and `rollout_artifacts_written=false`
4. Updated ROAD ledger item `288`.

## Files

- `tools/executor_final_evidence_report.sh`
- `tools/adapter_rollout_final_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/executor_final_evidence_report.sh tools/adapter_rollout_final_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `EXECUTOR_ENV_PATH=<tmp>/executor.env ADAPTER_ENV_PATH=<tmp>/adapter.env CONFIG_PATH=<tmp>/config.toml GO_NOGO_REQUIRE_FASTLANE_DISABLED=sometimes bash tools/executor_final_evidence_report.sh 24 60` — PASS (`exit 3`, explicit input error)
3. `ADAPTER_ENV_PATH=<tmp>/adapter.env CONFIG_PATH=<tmp>/config.toml REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE=oops bash tools/adapter_rollout_final_evidence_report.sh 24 60` — PASS (`exit 3`, explicit input error)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` was intentionally not executed in this slice to avoid long-running smoke loops during active iteration; targeted invalid-token branches were executed directly.
