# Executor Phase 2B Slice 130 — Windowed Signoff + Go/No-Go Test Mode Strict Bool (2026-02-26)

## Scope

- harden remaining windowed-signoff gate booleans to strict/fail-closed parsing
- harden `GO_NOGO_TEST_MODE` parsing in go/no-go helper to strict/fail-closed behavior

## Changes

1. Updated `tools/execution_windowed_signoff_report.sh`:
   - added strict boolean parser (`true/false/1/0/yes/no/on/off`) with input-error accumulation
   - strict parsing now covers:
     - `GO_NOGO_REQUIRE_JITO_RPC_POLICY`
     - `GO_NOGO_REQUIRE_FASTLANE_DISABLED`
     - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS`
     - `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS`
     - forwarded `GO_NOGO_TEST_MODE`
   - invalid tokens now fail-close via existing `input_errors` path (`signoff_verdict=NO_GO`, `signoff_reason_code=input_error`)
   - normalized `go_nogo_test_mode` is now explicitly emitted in summary output
2. Updated `tools/execution_go_nogo_report.sh`:
   - `GO_NOGO_TEST_MODE` now parsed via strict parser at startup
   - invalid token now exits with explicit error (`exit 1`) before runtime checks
   - removed legacy fail-open normalization path for test mode in override branch
3. Updated `tools/ops_scripts_smoke_test.sh`:
   - added invalid-token branch for `GO_NOGO_TEST_MODE=sometimes` in go/no-go helper checks
   - added invalid-token branch for `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS=sometimes` in windowed-signoff checks
   - assertions validate fail-closed error messages and exit codes
4. Updated ROAD ledger item `290`.

## Files

- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_go_nogo_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/execution_windowed_signoff_report.sh tools/execution_go_nogo_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `GO_NOGO_TEST_MODE=sometimes CONFIG_PATH=configs/paper.toml SERVICE=copybot-smoke-service bash tools/execution_go_nogo_report.sh 24 60` — PASS (`exit 1`, explicit bool-token error)
3. `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS=sometimes CONFIG_PATH=configs/paper.toml SERVICE=copybot-smoke-service bash tools/execution_windowed_signoff_report.sh 24 60` — PASS (`exit 3`, `signoff_verdict=NO_GO`, explicit bool-token input error)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` not executed in this slice; targeted invalid-token branches were verified directly.
