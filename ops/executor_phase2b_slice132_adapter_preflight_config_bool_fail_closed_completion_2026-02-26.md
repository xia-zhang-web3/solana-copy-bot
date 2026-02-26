# Executor Phase 2B Slice 132 — Adapter Preflight Config Bool Fail-Closed Completion (2026-02-26)

## Scope

- close remaining fail-open bool parsing path in adapter preflight config fallback
- ensure bool-parse errors cannot be masked by early SKIP branches

## Changes

1. Updated `tools/execution_adapter_preflight.sh`:
   - `cfg_or_env_bool_into` now strict-parses config fallback bool tokens as well (not only env overrides)
   - invalid config bool now appends explicit error:
     - `config [section].key must be a boolean token (true/false/1/0/yes/no/on/off), got: ...`
   - added early fail-closed block immediately after parsing `execution_enabled`/`execution_mode`:
     - if parse errors already exist, script exits `FAIL` before SKIP gates (`execution.enabled != true` / wrong mode)
   - this prevents silent classification drift where invalid bool token could previously degrade into `SKIP`
2. Updated `tools/ops_scripts_smoke_test.sh`:
   - added invalid-token guard for:
     - `SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes`
   - asserts deterministic `preflight_verdict: FAIL` and explicit bool-token error
3. Updated ROAD ledger item `292`.

## Files

- `tools/execution_adapter_preflight.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `bash -n tools/execution_adapter_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `CONFIG_PATH=<pass_cfg> SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes bash tools/execution_adapter_preflight.sh` — PASS (`exit 1`, `preflight_verdict=FAIL`, explicit bool-token error)
3. `CONFIG_PATH=<pass_cfg> SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO=sometimes bash tools/execution_adapter_preflight.sh` — PASS (`exit 1`, `preflight_verdict=FAIL`, explicit bool-token error)
4. `cargo check -p copybot-executor -q` — PASS

Note: full `tools/ops_scripts_smoke_test.sh` was not executed in this slice; targeted invalid-token branches were validated directly.
