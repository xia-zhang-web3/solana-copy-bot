# Executor Phase-2B Slice 294 — Devnet Rehearsal Required/Disabled Smoke Guards

Date: 2026-03-02  
Owner: execution-dev  
Status: implemented

## Scope

1. `tools/ops_scripts_smoke_test.sh`
   1. Added explicit negative guard case:
      - `WINDOWED_SIGNOFF_REQUIRED=true` with `DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=false`
   2. Added explicit negative guard case:
      - `ROUTE_FEE_SIGNOFF_REQUIRED=true` with `DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=false`
   3. Both guards assert:
      - `exit 3` (`NO_GO`)
      - `devnet_rehearsal_reason_code=config_error`
      - exact `config_error: ... requires ...=true` line
2. `ROAD_TO_PRODUCTION.md`
   1. Added item `423`.

## Verification

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="devnet_rehearsal" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS
