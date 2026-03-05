# Slice 525 Evidence — Executor Preflight Submit-Verify Topology Parity

Date (UTC): 2026-03-05
Owner: execution-dev
Scope: `tools/executor_preflight.sh` + `tools/ops_scripts_smoke_test.sh`

## Summary

This slice closes a preflight parity gap for executor submit-verify config (`COPYBOT_EXECUTOR_SUBMIT_VERIFY_*`):

1. strict bool parsing for `COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT` (fail-closed invalid token)
2. strict mode requires primary verify URL
3. fallback verify URL requires primary verify URL
4. fallback verify URL identity must differ from primary
5. in `COPYBOT_EXECUTOR_BACKEND_MODE=upstream`, placeholder hosts are rejected for verify primary/fallback URLs (`example.com` and `executor.mock.local` families)

Preflight summary now exposes:

1. `executor_submit_verify_strict`
2. `executor_submit_verify_configured`
3. `executor_submit_verify_fallback_configured`

## Files

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=executor_preflight_fast bash tools/ops_scripts_smoke_test.sh` — PASS
3. Targeted branch checks (custom harness over existing smoke helpers) — PASS:
   1. baseline summary emits `executor_submit_verify_*` fields as expected
   2. strict=true without primary verify URL -> FAIL with required-key message
   3. fallback verify URL without primary -> FAIL with required-key message
   4. placeholder verify URL in upstream mode -> FAIL
   5. verify fallback endpoint identity collision -> FAIL

## Notes

1. Full `OPS_SMOKE_TARGET_CASES=executor_preflight` remains available but is intentionally heavier; this slice used targeted branch execution for faster feedback while still exercising new negative paths directly.
