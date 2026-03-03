# Executor Phase 2B — Slice 458

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/ops_scripts_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `coverage-only` (smoke parity expansion, no runtime logic changes).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Gap Closed

After slice 457, adapter send-rpc auth parity had targeted negative pins only for inline `*_AUTH_TOKEN` branches.  
`*_AUTH_TOKEN_FILE` branches used the same `read_secret_from_source` path, but did not have direct smoke pins.

## Changes

`run_executor_preflight_case` now adds `_FILE` parity negatives:

1. route send-rpc primary auth mismatch via:
   - `COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_AUTH_TOKEN_FILE`,
2. route send-rpc fallback auth mismatch via:
   - `COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE`.

Both assert fail-closed `preflight_verdict: FAIL` with the same deterministic mismatch diagnostics as inline token variants.

## Targeted Verification

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full long smoke and full `cargo test -p copybot-executor -q` were not re-run in this slice.
