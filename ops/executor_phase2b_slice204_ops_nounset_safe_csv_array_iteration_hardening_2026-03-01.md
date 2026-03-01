# Executor Phase 2B — Slice 204

Date: 2026-03-01

## Scope

Harden ops helper scripts against `set -u` crashes caused by empty Bash arrays in CSV/error iteration loops.

## Changes

1. Updated nounset-sensitive loops to use safe array expansion (`"${arr[@]-}"`) in:
   - `tools/execution_adapter_preflight.sh`
   - `tools/execution_fee_calibration_report.sh`
   - `tools/executor_preflight.sh`
2. Added targeted adapter preflight smoke guard in `tools/ops_scripts_smoke_test.sh`:
   - empty `submit_allowed_routes` under adapter mode
   - asserts deterministic `FAIL` with explicit config error (not shell crash).
3. Updated `ROAD_TO_PRODUCTION.md` entry `364`.

## Verification

- `bash -n tools/execution_adapter_preflight.sh tools/execution_fee_calibration_report.sh tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted repro (empty allowlist in adapter mode):
  - `SOLANA_COPY_BOT_EXECUTION_ENABLED=true SOLANA_COPY_BOT_EXECUTION_MODE=adapter_submit_confirm CONFIG_PATH=<tmp_cfg_with_submit_allowed_routes_[]> bash tools/execution_adapter_preflight.sh`
  - Result: `rc=1`, `preflight_verdict: FAIL`, includes `execution.submit_allowed_routes must not be empty in adapter_submit_confirm mode`, and no `unbound variable`.
- `cargo check -p copybot-executor -q` — PASS
