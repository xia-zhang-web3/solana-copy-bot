# Executor Phase2b Slice519 — ops smoke default auto profile for heavy targeted cases (2026-03-06)

## Scope

1. `tools/ops_scripts_smoke_test.sh`
2. `ROAD_TO_PRODUCTION.md`

## What changed

1. `ops_scripts_smoke_test.sh`
- Default `OPS_SMOKE_PROFILE` changed from `full` to `auto`.
- `run_targeted_smoke_cases` fallback changed to `auto` so targeted runs without explicit profile auto-resolve by case weight.
- Heavy targeted default path is now pinned in `run_ops_smoke_targeted_dispatch_case`:
  - `OPS_SMOKE_TARGET_CASES=executor_preflight` => `ops smoke targeted profile: fast`.
- Non-heavy targeted default behavior stays pinned:
  - `OPS_SMOKE_TARGET_CASES=common_parsers` => `ops smoke targeted profile: full`.
- Explicit override contract is unchanged: `OPS_SMOKE_PROFILE=full|fast` remains authoritative.

## Validation

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS (`profile: full`)
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS (`profile: fast`, helper `(fast)`)
4. `OPS_SMOKE_TARGET_CASES=audit_ops_smoke_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
