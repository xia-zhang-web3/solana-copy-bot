# Executor Phase-2B Slice 293 — Devnet Rehearsal Profile/Stage Toggles

Date: 2026-03-02  
Owner: execution-dev  
Status: implemented

## Scope

1. `tools/execution_devnet_rehearsal.sh`
   1. Added profile preset: `DEVNET_REHEARSAL_PROFILE={full|core_only}`.
   2. Added explicit stage toggles:
      1. `DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF`
      2. `DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF`
   3. Added fail-closed guards:
      1. invalid profile -> `exit 1`
      2. `WINDOWED_SIGNOFF_REQUIRED=true` requires `DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=true`
      3. `ROUTE_FEE_SIGNOFF_REQUIRED=true` requires `DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=true`
   4. Disabled stages now emit deterministic `SKIP` diagnostics (`reason_code=stage_disabled`) and keep artifact chain intact.
2. `tools/ops_scripts_smoke_test.sh`
   1. Extended `run_devnet_rehearsal_case` with:
      1. `core_only` positive case (`SKIP` stages, overall `GO` under test override)
      2. invalid profile fail-closed case (`exit 1`)
3. `ROAD_TO_PRODUCTION.md`
   1. Added item `422`.

## Verification

1. `bash -n tools/execution_devnet_rehearsal.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="devnet_rehearsal" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS
