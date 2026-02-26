# Executor Phase 2B Slice 131 — Preflight + Secret Rotation Strict Bool (2026-02-26)

## Scope

- remove remaining fail-open bool parsing in adapter preflight env overrides
- harden adapter secret-rotation auth-mode bool parsing to fail-closed

## Changes

1. Updated `tools/execution_adapter_preflight.sh`:
   - `parse_env_bool_token` now returns non-zero on invalid token instead of empty-string success
   - `cfg_or_env_bool` now records explicit validation errors for invalid bool env overrides:
     - `${ENV_NAME} must be a boolean token (true/false/1/0/yes/no/on/off), got: ...`
   - invalid env bool token now fail-closes preflight via existing `errors[]` path (`preflight_verdict=FAIL`)
2. Updated `tools/adapter_secret_rotation_report.sh`:
   - added strict bool parser for `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED`
   - invalid token now appends explicit error and fail-closes with `rotation_readiness_verdict=FAIL`
3. Updated `tools/ops_scripts_smoke_test.sh`:
   - added invalid bool override case for adapter preflight:
     - `SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO=sometimes`
   - added invalid bool token case for adapter secret rotation:
     - `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=maybe`
   - assertions verify deterministic fail-closed error messages
4. Updated ROAD ledger item `291`.

## Files

- `tools/execution_adapter_preflight.sh`
- `tools/adapter_secret_rotation_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/execution_adapter_preflight.sh tools/adapter_secret_rotation_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `CONFIG_PATH=configs/paper.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true SOLANA_COPY_BOT_EXECUTION_MODE=adapter_submit_confirm SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO=sometimes bash tools/execution_adapter_preflight.sh` — PASS (`exit 1`, `preflight_verdict=FAIL`, explicit bool-token error)
3. `ADAPTER_ENV_PATH=<tmp>/adapter.env (COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=maybe) bash tools/adapter_secret_rotation_report.sh` — PASS (`exit 1`, `rotation_readiness_verdict=FAIL`, explicit bool-token error)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` was not executed in this slice; targeted invalid-token branches were validated directly.
