# Executor Phase 2B Slice 137 — Ops Bool Normalization Fail-Closed (2026-02-26)

## Scope

- close remaining fail-open bool normalization gap in shared ops helper path (`tools/lib/common.sh`)
- align local bool normalizers used in standalone scripts
- add explicit smoke guard for common bool normalization contract

## Changes

1. Updated `tools/lib/common.sh`:
   - `normalize_bool_token` now accepts both true/false token sets:
     - true: `1|true|yes|on`
     - false: `""|0|false|no|off`
   - invalid non-empty tokens now fail closed with non-zero exit + explicit stderr message
2. Updated local duplicate in `tools/execution_fee_calibration_report.sh`:
   - mirrored fail-closed bool normalization semantics
   - added explicit startup guard:
     - invalid `execution.submit_adapter_require_policy_echo` now exits with error instead of silently normalizing to `false`
3. Updated local duplicate in `tools/execution_adapter_preflight.sh`:
   - mirrored fail-closed bool normalization semantics for consistency with shared helper policy
4. Updated `tools/ops_scripts_smoke_test.sh`:
   - added `run_common_bool_normalization_case`
   - covers:
     - normalize `" yes "` => `true`
     - normalize empty token => `false`
     - invalid token (`maybe`) => exit 1 + explicit error message
5. Updated ROAD ledger item `297`.

## Files

- `tools/lib/common.sh`
- `tools/execution_fee_calibration_report.sh`
- `tools/execution_adapter_preflight.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/lib/common.sh tools/execution_fee_calibration_report.sh tools/execution_adapter_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `ROOT_DIR="$PWD" bash -c 'set -euo pipefail; source "$ROOT_DIR/tools/lib/common.sh"; normalize_bool_token "maybe"'` — FAIL (expected), stderr contains invalid-token message
3. `CONFIG_PATH=<tmp-invalid-bool-config> bash tools/execution_fee_calibration_report.sh 24` — FAIL (expected), stderr contains:
   - `execution.submit_adapter_require_policy_echo must be a boolean token ... got: maybe`
4. `cargo check -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
