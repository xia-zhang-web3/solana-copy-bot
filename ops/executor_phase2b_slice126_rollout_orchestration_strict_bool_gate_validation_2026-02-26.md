# Executor Phase 2B Slice 126 — Rollout Orchestration Strict Bool Gate Validation (2026-02-26)

## Scope

- harden top-level rollout orchestration helpers against invalid boolean env tokens
- prevent silent fallback-to-false behavior in rollout wrappers before Stage C.5 / route-fee gating

## Changes

1. Added strict boolean parsing to `tools/executor_rollout_evidence_report.sh`:
   - accepts only: `true/false/1/0/yes/no/on/off`
   - invalid tokens are recorded as input errors and force fail-closed `executor_rollout_verdict=NO_GO`
2. Added strict boolean parsing to `tools/adapter_rollout_evidence_report.sh`:
   - same token contract and fail-closed behavior
3. Applied strict parsing to rollout gate settings in both scripts:
   - `RUN_TESTS`
   - `DEVNET_REHEARSAL_TEST_MODE`
   - `GO_NOGO_TEST_MODE`
   - `WINDOWED_SIGNOFF_REQUIRED`
   - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS`
   - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS`
   - `GO_NOGO_REQUIRE_JITO_RPC_POLICY`
   - `GO_NOGO_REQUIRE_FASTLANE_DISABLED`
   - `ROUTE_FEE_SIGNOFF_REQUIRED`
   - route-fee go/no-go mode toggles (`ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE`, `REHEARSAL_ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE` where applicable)
4. Added smoke assertions in `tools/ops_scripts_smoke_test.sh`:
   - invalid `GO_NOGO_REQUIRE_JITO_RPC_POLICY` for executor rollout helper
   - invalid `REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED` for adapter rollout helper
5. Updated ROAD ledger item `286`.

## Files

- `tools/executor_rollout_evidence_report.sh`
- `tools/adapter_rollout_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/executor_rollout_evidence_report.sh tools/adapter_rollout_evidence_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `EXECUTOR_ENV_PATH=/tmp/missing-executor.env ADAPTER_ENV_PATH=/tmp/missing-adapter.env CONFIG_PATH=/tmp/missing-config.toml GO_NOGO_REQUIRE_JITO_RPC_POLICY=maybe bash tools/executor_rollout_evidence_report.sh 24 60` — PASS (`exit 3`, explicit invalid-token input error)
3. `ADAPTER_ENV_PATH=/tmp/missing-adapter.env CONFIG_PATH=/tmp/missing-config.toml REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED=perhaps bash tools/adapter_rollout_evidence_report.sh 24 60` — PASS (`exit 3`, explicit invalid-token input error)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` was intentionally not executed in this slice to avoid long-running smoke loops during active iteration; targeted invalid-token coverage was executed directly.
