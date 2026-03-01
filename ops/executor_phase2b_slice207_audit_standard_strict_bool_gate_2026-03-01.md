# Executor Phase 2B — Slice 207

Date: 2026-03-01

## Scope

Harden `tools/audit_standard.sh` boolean gate parsing for `AUDIT_SKIP_OPS_SMOKE` to fail-closed on invalid tokens.

## Changes

1. `tools/audit_standard.sh` now sources shared bool parsing helper:
   - `source "$SCRIPT_DIR/lib/common.sh"`
2. Replaced local fail-open `normalize_bool` behavior with strict parse:
   - `parse_bool_token_strict "$AUDIT_SKIP_OPS_SMOKE"`
   - invalid token now exits `1` with explicit error:
     - `AUDIT_SKIP_OPS_SMOKE must be boolean token ...`
3. Added ops smoke guard:
   - `run_audit_standard_strict_bool_guard_case` in `tools/ops_scripts_smoke_test.sh`
   - verifies `AUDIT_SKIP_OPS_SMOKE=maybe` returns exit code `1` and expected error text.
4. Updated `ROAD_TO_PRODUCTION.md` entry `367`.

## Verification

- `bash -n tools/audit_standard.sh tools/ops_scripts_smoke_test.sh` — PASS
- Targeted guard:
  - `AUDIT_SKIP_OPS_SMOKE=maybe bash tools/audit_standard.sh` — `rc=1`, explicit strict-bool error emitted.
- `cargo check -p copybot-executor -q` — PASS
