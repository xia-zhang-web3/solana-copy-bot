# Slice 467 — executor embedded mock backend mode (`COPYBOT_EXECUTOR_BACKEND_MODE`)

Date: 2026-03-03  
Branch: `main` (local development)

## Scope

1. Runtime config hardening:
   1. Added strict `COPYBOT_EXECUTOR_BACKEND_MODE={upstream|mock}` parser (`upstream` default, fail-closed unknown token).
   2. Added executor config field `backend_mode` and startup log emission.
2. Executor route-adapter behavior:
   1. In `mock` mode, `/simulate` and `/submit` route adapters return contract-compatible in-process success payloads.
   2. Submit mock payload includes deterministic valid base58 signature (`tx_signature`), route + contract fields, and request identity passthrough.
3. Health/observability:
   1. `/healthz` payload now includes `backend_mode`.
4. Preflight parity:
   1. `tools/executor_preflight.sh` now parses `COPYBOT_EXECUTOR_BACKEND_MODE`.
   2. In `mock` mode, preflight synthesizes route submit/simulate URLs exactly like runtime fallback behavior.
   3. Added health backend-mode schema + parity checks (`string` type guard + mismatch fail-close).
5. Smoke coverage:
   1. `run_executor_preflight_case` now pins:
      1. invalid backend mode reject,
      2. mock-mode pass without explicit executor route submit/simulate URLs,
      3. backend-mode health mismatch reject.

## Changed files

1. `crates/executor/src/backend_mode.rs`
2. `crates/executor/src/main.rs`
3. `crates/executor/src/executor_config_env.rs`
4. `crates/executor/src/route_adapters.rs`
5. `crates/executor/src/healthz_payload.rs`
6. `crates/executor/src/healthz_endpoint.rs`
7. `tools/executor_preflight.sh`
8. `tools/ops_scripts_smoke_test.sh`
9. `ROAD_TO_PRODUCTION.md`

## Verification commands

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh`
2. `cargo test -p copybot-executor -q backend_mode`
3. `cargo test -p copybot-executor -q execute_route_action_uses_embedded_mock_backend_for_simulate`
4. `cargo test -p copybot-executor -q execute_route_action_uses_embedded_mock_backend_for_submit`
5. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh`
6. `cargo check -p copybot-executor -q`
