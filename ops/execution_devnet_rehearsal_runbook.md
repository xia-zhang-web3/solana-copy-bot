# Execution Devnet Rehearsal Runbook (Stage C.5)

This runbook executes the Stage C.5 rehearsal gate from `ROAD_TO_PRODUCTION.md` using a single orchestrator.

## 1) Preconditions

1. Config profile is prepared for adapter rehearsal:
   1. `execution.enabled = true`
   2. `execution.mode = "adapter_submit_confirm"`
   3. `execution.rpc_devnet_http_url` points to devnet RPC endpoint
2. Adapter preflight requirements are satisfied:
   1. signer pubkey
   2. adapter endpoint(s)
   3. route policy maps and strict echo policy (for production-like env profiles)
3. DB path points to rehearsal dataset (`sqlite.path`).

## 2) Rehearsal Command

```bash
cd /var/www/solana-copy-bot
OUTPUT_DIR="state/devnet_rehearsal_$(date -u +%Y%m%dT%H%M%SZ)" \
CONFIG_PATH="/etc/solana-copy-bot/live.server.toml" \
SERVICE="solana-copy-bot" \
./tools/execution_devnet_rehearsal.sh 24 60
```

Arguments:

1. `24` — execution evidence window in hours.
2. `60` — recent risk-events window in minutes.

Optional strictness knobs:
Defaults: `WINDOWED_SIGNOFF_REQUIRED=false`, `WINDOWED_SIGNOFF_WINDOWS_CSV=1,6,24`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS=false`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS=false`, `GO_NOGO_REQUIRE_JITO_RPC_POLICY=false`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED=false`, `ROUTE_FEE_SIGNOFF_REQUIRED=false`, `ROUTE_FEE_SIGNOFF_WINDOWS_CSV=1,6,24`.

1. `WINDOWED_SIGNOFF_REQUIRED=true`:
   1. rehearsal treats nested `execution_windowed_signoff_report.sh` verdict as required gate.
2. `WINDOWED_SIGNOFF_WINDOWS_CSV=1,6,24`:
   1. override window set used by nested windowed signoff helper.
3. `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS=true`:
   1. for windows where dynamic CU policy is enabled, require nested `dynamic_cu_hint_source_verdict=PASS`.
4. `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS=true`:
   1. for windows where dynamic tip policy is enabled, require nested `dynamic_tip_policy_verdict=PASS`.
5. `GO_NOGO_REQUIRE_JITO_RPC_POLICY=true`:
   1. require nested go/no-go to enforce strict route-profile target `primary=jito` and `fallback=rpc` in adapter mode (`jito_rpc_policy_verdict=PASS`).
6. `GO_NOGO_REQUIRE_FASTLANE_DISABLED=true`:
   1. require nested go/no-go to enforce strict `execution.submit_fastlane_enabled=false` in adapter mode (`fastlane_feature_flag_verdict=PASS`).
7. `ROUTE_FEE_SIGNOFF_REQUIRED=true`:
   1. rehearsal treats nested `execution_route_fee_signoff_report.sh` verdict as required gate.
8. `ROUTE_FEE_SIGNOFF_WINDOWS_CSV=1,6,24`:
   1. override window set used by nested route/fee signoff helper.

## 3) Exit Codes

1. `0` = `GO` (rehearsal gates passed).
2. `2` = `HOLD` (insufficient readiness evidence or tests intentionally skipped).
3. `3` = `NO_GO` (preflight/gates/tests/config failed).

## 4) What the Script Verifies

1. Devnet config contract:
   1. execution enabled
   2. mode is `adapter_submit_confirm`
   3. valid non-placeholder `execution.rpc_devnet_http_url`
2. Adapter contract preflight (`tools/execution_adapter_preflight.sh`).
3. Combined go/no-go evidence (`tools/execution_go_nogo_report.sh`).
4. Multi-window route/fee signoff evidence (`tools/execution_route_fee_signoff_report.sh`).
5. Targeted regression tests (default behavior with `RUN_TESTS=true`).

## 5) Artifacts

When `OUTPUT_DIR` is set, the script writes:

1. `execution_devnet_rehearsal_summary_*.txt`
2. `execution_devnet_rehearsal_preflight_*.txt`
3. `execution_devnet_rehearsal_go_nogo_*.txt`
4. `execution_devnet_rehearsal_windowed_signoff_*.txt`
5. `execution_devnet_rehearsal_route_fee_signoff_*.txt`
6. `execution_devnet_rehearsal_tests_*.txt`
7. nested go/no-go artifacts under `OUTPUT_DIR/go_nogo/`
8. nested windowed-signoff artifacts under `OUTPUT_DIR/windowed_signoff/`
9. nested route/fee-signoff artifacts under `OUTPUT_DIR/route_fee_signoff/`

## 6) Operator Actions by Verdict

1. `GO`:
   1. attach artifacts to Stage C.5 evidence pack
   2. proceed to Stage D planning
2. `HOLD`:
   1. collect larger adapter evidence window and rerun
   2. if tests were skipped, rerun with `RUN_TESTS=true`
3. `NO_GO`:
   1. stop progression to Stage D
   2. fix reported preflight/go-no-go/test failures
   3. rerun until `GO`

## 7) Smoke/Test Override (non-production only)

For smoke validation only:

```bash
RUN_TESTS=false DEVNET_REHEARSAL_TEST_MODE=true \
GO_NOGO_TEST_MODE=true GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
./tools/execution_devnet_rehearsal.sh 24 60
```

Never use `DEVNET_REHEARSAL_TEST_MODE=true` for real devnet sign-off.

## 8) Combined Adapter Rollout Evidence

When Stage C.5 evidence must be attached together with adapter secret-rotation readiness, run:

```bash
ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env \
CONFIG_PATH=/etc/solana-copy-bot/live.server.toml \
OUTPUT_DIR="state/adapter-rollout-$(date -u +%Y%m%dT%H%M%SZ)" \
./tools/adapter_rollout_evidence_report.sh 24 60
```

Expected top-level gate: `adapter_rollout_verdict: GO`.
