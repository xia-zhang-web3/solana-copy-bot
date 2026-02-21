# Audit Package: Adapter Rollout Evidence Orchestrator (2026-02-20)

Branch: `feat/yellowstone-grpc-migration`

Scope commit range:
1. `310d971` (primary scope)
2. `fff3520` (hardening follow-up)

Out of scope:
1. Real server-side execution of rotation drill and systemd mounts.
2. External RPC/upstream availability outside local scripts.
3. Business decision on go-live thresholds (only tooling contract is audited here).

## Changelog (Audit-Oriented)

1. `310d971`: adapter rollout evidence orchestrator (first version)
   Files:
   1. `tools/adapter_rollout_evidence_report.sh`
   2. `tools/ops_scripts_smoke_test.sh`
   3. `ops/adapter_backend_runbook.md`
   4. `ops/execution_devnet_rehearsal_runbook.md`
   5. `ROAD_TO_PRODUCTION.md`
   Highlights:
   1. new combined helper composing secret-rotation readiness and Stage C.5 rehearsal
   2. unified top-level verdict `adapter_rollout_verdict=GO|HOLD|NO_GO`
   3. combined artifact capture for audit handoff
   4. smoke coverage for orchestrator basic branches

2. `fff3520`: hardening + coverage completion
   Files:
   1. `tools/adapter_rollout_evidence_report.sh`
   2. `tools/ops_scripts_smoke_test.sh`
   Highlights:
   1. removed early fail-fast exits; helper now always emits summary + standardized exit codes (`0/2/3`)
   2. section-based extraction of first warning/error from rotation helper
   3. smoke coverage extended for rehearsal-derived `HOLD`/`NO_GO` paths
   4. smoke coverage added for missing `ADAPTER_ENV_PATH` with fail-closed summary output

## Mandatory Verification Points

1. Unified output contract is deterministic and fail-closed (`tools/adapter_rollout_evidence_report.sh`)
   1. summary always printed
   2. `adapter_rollout_verdict` always present
   3. exit code always mapped to `GO=0`, `HOLD=2`, `NO_GO=3`
2. Input validation does not bypass verdict output
   1. invalid/missing inputs are captured as `input_error:*`
   2. top-level verdict remains `NO_GO` (not shell exit `1`)
3. Rotation/rehearsal composition is explicit
   1. rotation helper output is captured and normalized (`PASS|WARN|FAIL|UNKNOWN`)
   2. rehearsal helper output is captured and normalized (`GO|HOLD|NO_GO|UNKNOWN`)
   3. precedence:
      1. input errors => `NO_GO`
      2. rotation `FAIL|UNKNOWN` => `NO_GO`
      3. rehearsal `NO_GO|UNKNOWN` => `NO_GO`
      4. rotation `WARN` or rehearsal `HOLD` => `HOLD`
      5. only `rotation=PASS && rehearsal=GO` => `GO`
4. Rotation first-warning/first-error reason extraction is section-aware
   1. parser reads lines only inside `--- warnings ---` / `--- errors ---` blocks
5. Artifact contract is complete when `OUTPUT_DIR` is set
   1. summary artifact
   2. captured rotation raw output
   3. captured rehearsal raw output
6. Smoke coverage is sufficient for orchestrator paths (`tools/ops_scripts_smoke_test.sh`)
   1. `GO` path
   2. `HOLD` from rotation warning
   3. `NO_GO` from rotation failure
   4. `HOLD` from rehearsal (with rotation `PASS`)
   5. `NO_GO` from rehearsal (with rotation `PASS`)
   6. `NO_GO` on missing `ADAPTER_ENV_PATH` with summary+input_error

## Targeted Test Commands

```bash
bash -n tools/adapter_rollout_evidence_report.sh
bash -n tools/ops_scripts_smoke_test.sh

tools/ops_scripts_smoke_test.sh

cargo test --workspace -q

# fail-closed contract check: missing env still prints summary and returns NO_GO(3)
set +e
ADAPTER_ENV_PATH=/tmp/does-not-exist CONFIG_PATH=configs/paper.toml \
  bash tools/adapter_rollout_evidence_report.sh 24 60 > /tmp/adapter-rollout-missing-env.out 2>&1
test "$?" -eq 3
grep -F "=== Adapter Rollout Evidence Summary ===" /tmp/adapter-rollout-missing-env.out
grep -F "adapter_rollout_verdict: NO_GO" /tmp/adapter-rollout-missing-env.out
set -e
```

## Expected Outcome

1. Orchestrator can be used as a single audit artifact source for adapter rollout readiness.
2. All error paths are fail-closed without losing machine-readable verdict output.
3. Smoke + workspace tests pass on current branch.
4. Remaining work is operational execution on server (drill + archived evidence), not code gap.
