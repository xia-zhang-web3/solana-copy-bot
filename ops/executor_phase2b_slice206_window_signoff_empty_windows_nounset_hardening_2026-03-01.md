# Executor Phase 2B — Slice 206

Date: 2026-03-01

## Scope

Harden signoff helper window parsing against `set -u` crashes when `WINDOWS_CSV` is empty.

## Changes

1. Updated window token loops to use nounset-safe array expansion:
   - `tools/execution_windowed_signoff_report.sh`
   - `tools/execution_route_fee_signoff_report.sh`
   - changed `for raw_token in "${raw_windows[@]}"` to `for raw_token in "${raw_windows[@]-}"`
2. Added targeted smoke guards in `tools/ops_scripts_smoke_test.sh`:
   - empty windows for windowed signoff helper (`""`, `60`)
   - empty windows for route/fee signoff helper (`""`, `60`)
   - both assert deterministic `NO_GO` and `input_error: no valid windows parsed from WINDOWS_CSV=`.
3. Updated `ROAD_TO_PRODUCTION.md` entry `366`.

## Verification

- `bash -n tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted repro:
  - `bash tools/execution_windowed_signoff_report.sh "," 60` → `rc=3`, contains `signoff_verdict: NO_GO`, contains `no valid windows parsed`.
  - `bash tools/execution_route_fee_signoff_report.sh "," 60` → `rc=3`, contains `signoff_verdict: NO_GO`, contains `no valid windows parsed`.
- `cargo check -p copybot-executor -q` — PASS
