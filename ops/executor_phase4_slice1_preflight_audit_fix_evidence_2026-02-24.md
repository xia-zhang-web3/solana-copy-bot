# Executor Phase 4 Slice 1 Audit Fix Evidence — Preflight Guard Hardening

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

Addressed auditor findings for `tools/executor_preflight.sh`:

1. Enforced HTTP 200 requirement for `/simulate` auth probes:
   1. without-auth probe must return HTTP 200,
   2. with-valid-bearer probe must return HTTP 200.
2. Enforced adapter route allowlist subset invariant:
   1. every adapter route must exist in executor route allowlist.
3. Enforced unauthenticated-mode consistency:
   1. when `COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true`, endpoint must not respond with `auth_missing`/`auth_invalid` for no-auth simulate probe.
4. Extended smoke coverage in `run_executor_preflight_case`:
   1. with-auth non-200 fail case,
   2. adapter allowlist subset fail case,
   3. unauth mismatch fail case.

## Files

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`

## Regression Pack

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted executor preflight harness (pass + 3 fail branches listed above) — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
