# Executor Phase 2B — Slice 301

Date: 2026-03-02  
Commit: pending

## Scope

Added targeted ops-smoke mode controls to audit wrappers so operators can run bounded `ops_scripts_smoke_test.sh` subsets through `audit_standard.sh` and `audit_full.sh` without full ops-smoke runtime.

## Changes

1. `tools/audit_standard.sh`
   1. Added strict parsing for:
      1. `AUDIT_OPS_SMOKE_MODE={full|targeted}`
      2. `AUDIT_OPS_SMOKE_TARGET_CASES` (required non-empty when mode=`targeted`)
   2. `run_ops_smoke()` now propagates `OPS_SMOKE_TARGET_CASES` only in `targeted` mode.
   3. Ops-smoke log line now prints mode.
2. `tools/audit_full.sh`
   1. Added the same strict mode/targets parsing and propagation behavior as `audit_standard.sh`.
   2. Ops-smoke log line now prints mode.
3. `tools/ops_scripts_smoke_test.sh`
   1. Added `run_audit_ops_smoke_mode_guard_case`:
      1. invalid mode reject
      2. targeted+empty target reject
      3. targeted propagation pass through `audit_full.sh`
      4. targeted propagation pass through `audit_standard.sh` with ops-scope marker
   2. Added targeted dispatcher alias: `audit_ops_smoke_mode_guard`.
   3. Added guard case to full smoke `main()`.
4. `ROAD_TO_PRODUCTION.md`
   1. Added item `428` describing targeted ops-smoke audit controls.

## Validation

1. `bash -n tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="audit_ops_smoke_mode_guard" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS

