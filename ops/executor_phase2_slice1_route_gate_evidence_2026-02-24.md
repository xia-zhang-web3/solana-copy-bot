# Executor Phase 2 Slice 1 Evidence — Route Gate + Contract Guards

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Renamed internal config type to `ExecutorConfig` (naming cleanup, removes adapter drift in executor crate).
2. Added explicit fastlane feature gate in executor config path:
   1. `COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED` (default `false`)
   2. default allowlist now `paper,rpc,jito` (no fastlane by default)
   3. startup fail-closed if `fastlane` is in allowlist while flag is `false`
3. Added runtime defense-in-depth gate in request validation:
   1. reject `route=fastlane` when flag disabled (`fastlane_not_enabled`)
4. Strengthened contract smoke helper with additional guard tests:
   1. request/signal required field enforcement
   2. fastlane route-policy gate

## Files

1. `crates/executor/src/main.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ops/executor_contract_v1.md`
4. `ops/executor_backend_runbook.md`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (47)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS
4. `timeout 300 bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Fastlane path remains feature-gated and opt-in.
2. No behavior change for default deployment profiles (fastlane disabled).
