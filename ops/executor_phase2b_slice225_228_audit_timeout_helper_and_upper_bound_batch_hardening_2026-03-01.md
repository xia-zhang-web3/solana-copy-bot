# Executor Phase 2B — Slice 225-228

## Summary

This batch hardens audit runner timeout controls and de-duplicates timeout execution logic across quick/standard/full audit scripts.

1. Added shared timeout helpers in `tools/lib/common.sh`:
   - `parse_timeout_sec_strict(raw, min, max)`
   - `resolve_timeout_command()` (`timeout`/`gtimeout`)
   - `run_with_timeout_if_available(timeout, cmd...)`
2. Migrated timeout command execution in:
   - `tools/audit_quick.sh`
   - `tools/audit_standard.sh`
   - `tools/audit_full.sh`
3. Enforced fail-closed upper bound `1..=86400` for all audit timeout env gates:
   - `AUDIT_OPS_SMOKE_TIMEOUT_SEC`
   - `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC`
   - `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC`
   - `AUDIT_PACKAGE_TEST_TIMEOUT_SEC`
   - `AUDIT_WORKSPACE_TEST_TIMEOUT_SEC`
4. Expanded smoke coverage in `tools/ops_scripts_smoke_test.sh`:
   - shared parser guard for timeout helper (`valid/zero/over-limit`)
   - upper-bound fail-fast batch guards for quick/standard/full audit runners

## Files Changed

- `tools/lib/common.sh`
- `tools/audit_quick.sh`
- `tools/audit_standard.sh`
- `tools/audit_full.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Validation

1. `bash -n tools/lib/common.sh tools/audit_quick.sh tools/audit_standard.sh tools/audit_full.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_EXECUTOR_TEST_TIMEOUT_SEC=86401 bash tools/audit_quick.sh` — PASS (`exit 1`, fail-closed)
3. `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_PACKAGE_TEST_TIMEOUT_SEC=86401 bash tools/audit_standard.sh` — PASS (`exit 1`, fail-closed before baseline)
4. `AUDIT_SKIP_OPS_SMOKE=true AUDIT_SKIP_CONTRACT_SMOKE=true AUDIT_WORKSPACE_TEST_TIMEOUT_SEC=86401 bash tools/audit_full.sh` — PASS (`exit 1`, fail-closed before baseline)
5. `cargo check -p copybot-executor -q` — PASS

## Notes

- Runtime behavior remains backward compatible when `timeout` is unavailable: scripts still execute commands without timeout via shared fallback path.
- This slice is coverage/runtime-hardening for audit runners only; no executor crate runtime logic changed.
