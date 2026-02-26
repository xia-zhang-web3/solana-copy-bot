# Executor Phase 2B Slice 133 — Executor Preflight Bool Fail-Closed Completion (2026-02-26)

## Scope

- remove subshell-based bool error loss in executor preflight
- ensure invalid `execution.enabled` tokens fail-close instead of being masked as `SKIP`

## Changes

1. Updated `tools/executor_preflight.sh`:
   - replaced `cfg_or_env_bool` (stdout-return pattern) with `cfg_or_env_bool_into` (`printf -v` into caller scope)
   - boolean parse errors now persist in `errors[]` (no subshell loss)
   - improved bool diagnostics to include invalid raw token values
   - added early fail-closed gate after `execution_enabled`/`execution_mode` resolution:
     - emits `preflight_verdict: FAIL`
     - emits `preflight_reason_code: config_error`
     - exits before SKIP branches if bool parsing already failed
2. Updated `tools/ops_scripts_smoke_test.sh`:
   - added invalid-token guard for `SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes`
   - assertions pin `preflight_verdict: FAIL`, `preflight_reason_code: config_error`, and explicit token error
3. Updated ROAD ledger item `293`.

## Files

- `tools/executor_preflight.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes ... bash tools/executor_preflight.sh` — PASS (`exit 1`, `preflight_verdict: FAIL`, `preflight_reason_code: config_error`, explicit bool-token error)
3. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` not executed in this slice; targeted invalid-token branch was validated directly.
