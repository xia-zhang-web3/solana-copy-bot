# Audit Package: Stage C.5 Devnet Rehearsal (2026-02-20)

Branch: `feat/yellowstone-grpc-migration`

Scope commit range:
1. `9e431e2` (primary scope)

Out of scope:
1. External adapter backend behavior.
2. Real devnet RPC availability/latency outside local code.

## Changelog (Audit-Oriented)

1. `9e431e2`: Stage C.5 rehearsal orchestration
   Files:
   1. `tools/execution_devnet_rehearsal.sh`
   2. `ops/execution_devnet_rehearsal_runbook.md`
   3. `tools/ops_scripts_smoke_test.sh`
   4. `ROAD_TO_PRODUCTION.md`
   5. `YELLOWSTONE_GRPC_MIGRATION_PLAN.md`
   Highlights:
   1. unified Stage C.5 verdict helper (`GO/HOLD/NO_GO`) with explicit exit codes (`0/2/3`)
   2. orchestration: config gate + adapter preflight + go/no-go summary + targeted regressions
   3. artifact export (`summary/preflight/go_nogo/tests`) + nested go/no-go artifacts
   4. smoke coverage for rehearsal helper via test-mode branch
   5. roadmap/migration docs synced for Stage C.5 in-progress workflow

## Mandatory Verification Points

1. Rehearsal config contract is fail-closed (`tools/execution_devnet_rehearsal.sh`)
   1. `execution.enabled=true`
   2. `execution.mode=adapter_submit_confirm`
   3. valid non-placeholder `execution.rpc_devnet_http_url`
2. Adapter preflight is hard gate
   1. `preflight_verdict != PASS` => `devnet_rehearsal_verdict=NO_GO`
3. Go/No-Go report is integrated and preserved
   1. `overall_go_nogo_verdict` from `tools/execution_go_nogo_report.sh` is surfaced in summary
   2. `NO_GO` remains `NO_GO`; `HOLD` remains `HOLD`
4. Test gate semantics are explicit
   1. default path runs targeted tests (`RUN_TESTS=true`)
   2. skipped tests produce `HOLD` (unless `DEVNET_REHEARSAL_TEST_MODE=true` for smoke only)
5. Artifact export is complete when `OUTPUT_DIR` is set
   1. rehearsal writes `summary/preflight/go_nogo/tests`
   2. nested go/no-go capture is always written under `OUTPUT_DIR/go_nogo/` (`execution_go_nogo_captured_*`)
   3. native go/no-go helper artifacts are best-effort and may be absent if `tools/execution_go_nogo_report.sh` fails before its own export block
6. Smoke coverage exists for rehearsal helper
   1. test-mode case validates verdict and artifact paths

## Targeted Test Commands

```bash
bash -n tools/execution_devnet_rehearsal.sh
bash -n tools/ops_scripts_smoke_test.sh

tools/ops_scripts_smoke_test.sh

# deterministic fail-closed check on default paper config (expected NO_GO, exit 3)
set +e
CONFIG_PATH=configs/paper.toml RUN_TESTS=true tools/execution_devnet_rehearsal.sh 24 60
test "$?" -eq 3
set -e

# smoke-like GO path with fixture config is covered by:
tools/ops_scripts_smoke_test.sh

cargo test --workspace -q
```

## Expected Outcome

1. Rehearsal helper emits deterministic Stage C.5 verdict with reason.
2. Gate precedence is fail-closed for config/preflight/go-no-go failures.
3. Artifact bundle is sufficient for Stage C.5 evidence review.
4. Smoke and workspace tests pass on current branch.
