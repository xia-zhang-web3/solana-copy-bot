# Executor Phase 2B Slice 125 — Devnet Rehearsal Strict Bool Gate Validation (2026-02-26)

## Scope

- harden Stage C.5 orchestration helper input validation
- prevent silent disabling of rehearsal gates due to mistyped boolean env values

## Changes

1. Added strict boolean parser in `tools/execution_devnet_rehearsal.sh`:
   - accepts: `true/false/1/0/yes/no/on/off`
   - rejects all other tokens with explicit error and `exit 1`
2. Applied strict parsing to rehearsal gate toggles:
   - `RUN_TESTS`
   - `DEVNET_REHEARSAL_TEST_MODE`
   - `WINDOWED_SIGNOFF_REQUIRED`
   - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS`
   - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS`
   - `GO_NOGO_REQUIRE_JITO_RPC_POLICY`
   - `GO_NOGO_REQUIRE_FASTLANE_DISABLED`
   - `ROUTE_FEE_SIGNOFF_REQUIRED`
   - `ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE`
3. Added smoke coverage in `tools/ops_scripts_smoke_test.sh` for invalid token branches:
   - `WINDOWED_SIGNOFF_REQUIRED="maybe"`
   - `ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE="sometimes"`
4. Updated ROAD ledger item `285`.

## Files

- `tools/execution_devnet_rehearsal.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
