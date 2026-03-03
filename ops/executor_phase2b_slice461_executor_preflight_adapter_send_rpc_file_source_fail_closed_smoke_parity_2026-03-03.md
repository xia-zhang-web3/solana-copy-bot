# Executor Phase 2B — Slice 461

Date: 2026-03-03  
Owner: execution-dev

## Scope

1. `tools/ops_scripts_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `coverage-only`.
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

After slice 460, global adapter send-rpc secret-source conflicts were pinned, but `_FILE` missing-path fail-closed paths for global adapter send-rpc auth were not explicitly pinned.

This slice adds deterministic smoke coverage for both global branches:

1. `COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE`
2. `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE`

Each now asserts `preflight_verdict: FAIL` and file-not-found diagnostics from `read_secret_from_source`.

## Changes

### 1) Smoke pin — global adapter send-rpc auth missing file

`run_executor_preflight_case` now executes preflight with:

1. `COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE="$TMP_DIR/missing-adapter-send-rpc-auth.secret"`

and asserts:

1. `preflight_verdict: FAIL`
2. `adapter send-rpc auth: COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE file not found:`

### 2) Smoke pin — global adapter send-rpc fallback auth missing file

`run_executor_preflight_case` now executes preflight with:

1. `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE="$TMP_DIR/missing-adapter-send-rpc-fallback-auth.secret"`

and asserts:

1. `preflight_verdict: FAIL`
2. `adapter send-rpc fallback auth: COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE file not found:`

## Targeted Verification

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
