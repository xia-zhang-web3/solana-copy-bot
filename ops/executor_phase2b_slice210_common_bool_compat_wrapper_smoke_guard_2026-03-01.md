# Executor Phase 2B — Slice 210

Date: 2026-03-01

## Scope

Pin runtime compatibility contract for `normalize_bool_token()` in ops smoke to prevent future breakage in rollout/final helpers that still consume it.

## Changes

1. `tools/ops_scripts_smoke_test.sh`
   - Added `run_common_bool_compat_wrapper_case`.
   - Guard validates:
     - `normalize_bool_token " yes "` => `true`
     - `normalize_bool_token ""` => `false`
     - `normalize_bool_token "maybe"` => rejects with `exit 1` and expected error text.
   - Registered guard in `main()`.
2. `ROAD_TO_PRODUCTION.md`
   - Added entry `370`.

## Verification

- `bash -n tools/ops_scripts_smoke_test.sh tools/lib/common.sh` — PASS
- Targeted guard:
  - `cargo check -p copybot-executor -q` — PASS
  - `bash -lc 'source tools/lib/common.sh; normalize_bool_token ""'` => `false`
  - `bash -lc 'source tools/lib/common.sh; normalize_bool_token " yes "'` => `true`
  - `bash -lc 'source tools/lib/common.sh; normalize_bool_token maybe'` => `rc=1`, expected error
