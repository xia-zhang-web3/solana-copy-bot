# Executor Phase 2B — Slice 208

Date: 2026-03-01

## Scope

Remove dead shared bool helper drift surface and pin canonical strict bool semantics in ops smoke.

## Changes

1. `tools/lib/common.sh`
   - Removed unused `normalize_bool_token()` helper.
   - Kept canonical `parse_bool_token_strict()` as single shared bool parser.
2. `tools/ops_scripts_smoke_test.sh`
   - Replaced `run_common_bool_normalization_case` with `run_common_strict_bool_parser_case`.
   - Guard now verifies:
     - `parse_bool_token_strict " yes "` => `true`
     - `parse_bool_token_strict "off"` => `false`
     - empty token rejects (`exit 1`)
     - invalid token (`maybe`) rejects (`exit 1`)
3. `ROAD_TO_PRODUCTION.md`
   - Added entry `368` for this hardening slice.

## Verification

- `bash -n tools/lib/common.sh tools/ops_scripts_smoke_test.sh tools/audit_standard.sh` — PASS
- Targeted strict parser guard:
  - `source tools/lib/common.sh`
  - `parse_bool_token_strict " yes "` => `true`
  - `parse_bool_token_strict off` => `false`
  - `parse_bool_token_strict ""` => reject (`rc=1`)
  - `parse_bool_token_strict maybe` => reject (`rc=1`)
- `AUDIT_SKIP_OPS_SMOKE=maybe bash tools/audit_standard.sh` — `rc=1`, explicit strict-bool error
- `cargo check -p copybot-executor -q` — PASS
