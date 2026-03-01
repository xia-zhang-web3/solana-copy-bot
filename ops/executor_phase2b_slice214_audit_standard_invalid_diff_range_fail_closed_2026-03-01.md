# Executor Phase 2B — Slice 214

Date: 2026-03-01

## Scope

Harden `audit_standard.sh` against malformed diff-range input by removing fail-open behavior and enforcing fail-fast before heavy baseline runs.

## Changes

1. `tools/audit_standard.sh`
   - Removed fail-open pattern:
     - `changed_files="$(collect_changed_files || true)"`
   - Added fail-closed validation:
     - `collect_changed_files` failure now emits explicit error and exits `1`.
   - Added fail-fast ordering:
     - changed-file collection and diff-range validation now execute before quick baseline (`audit_quick.sh`).
2. `tools/ops_scripts_smoke_test.sh`
   - Added `run_audit_standard_invalid_diff_range_guard_case`.
   - Guard asserts:
     - invalid `AUDIT_DIFF_RANGE` => `exit 1` + explicit error
     - quick baseline is not started when range is invalid.
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `374`.

## Verification

- `bash -n tools/audit_standard.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_DIFF_RANGE=not-a-valid-diff-range...HEAD bash tools/audit_standard.sh`
  - result: `rc=1`, explicit invalid-range error, no quick-baseline start line.
- `cargo check -p copybot-executor -q` — PASS
