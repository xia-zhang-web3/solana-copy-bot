# Executor Phase 2B — Slice 209

Date: 2026-03-01

## Scope

Hotfix compatibility regression introduced by removing `normalize_bool_token()` from shared ops helper.

## Problem

`tools/lib/common.sh` no longer exported `normalize_bool_token()`, while multiple ops scripts still called it.  
With `set -e`, those paths fail at runtime with `normalize_bool_token: command not found`.

## Changes

1. `tools/lib/common.sh`
   - Restored `normalize_bool_token()` as compatibility wrapper:
     - empty token => `false` (legacy behavior)
     - non-empty token => delegates to `parse_bool_token_strict`
     - invalid token => explicit error + return `1`
2. `ROAD_TO_PRODUCTION.md`
   - Added entry `369` documenting compatibility rollback.

## Verification

- `bash -n tools/lib/common.sh tools/ops_scripts_smoke_test.sh tools/audit_standard.sh` — PASS
- `source tools/lib/common.sh` targeted checks:
  - `normalize_bool_token ""` => `false`
  - `normalize_bool_token " yes "` => `true`
  - `normalize_bool_token maybe` => `rc=1` + expected error text
- `AUDIT_SKIP_OPS_SMOKE=maybe bash tools/audit_standard.sh` — `rc=1` (strict gate unchanged)
- `cargo check -p copybot-executor -q` — PASS
