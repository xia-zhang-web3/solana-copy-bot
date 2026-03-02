# Executor Phase-2B Slice 288 — Ops Smoke Targeted Dispatch Mode

Date: 2026-03-02  
Owner: execution-dev  
Scope: `tools/ops_scripts_smoke_test.sh` throughput hardening

## Change Summary

1. Added targeted smoke dispatch mode via `OPS_SMOKE_TARGET_CASES=<csv>`.
2. Added strict fail-closed handling for unknown target case names.
3. Added fail-closed handling for effectively empty target lists (e.g. `","`).
4. Preserved default behavior: when `OPS_SMOKE_TARGET_CASES` is unset, full smoke runs unchanged.
5. Added in-suite dispatcher guard test that:
   1. verifies only requested targeted cases execute,
   2. verifies unknown target case entry is rejected.

## Supported Target Case Names

1. `common_strict_bool_parser`
2. `common_bool_compat_wrapper`
3. `common_timeout_parser`
4. `audit_quick_bool_guard`
5. `audit_standard_bool_guard`
6. `evidence_bundle_pack`
7. `windowed_signoff`
8. `route_fee_signoff`
9. `devnet_rehearsal`
10. `executor_rollout_evidence`
11. `adapter_rollout_evidence`
12. `execution_server_rollout`
13. `execution_runtime_readiness`

(Each also accepts the corresponding `run_*` function name alias.)

## Validation (targeted)

1. `bash -n tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES="common_strict_bool_parser,common_timeout_parser" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES="unknown_case" bash tools/ops_scripts_smoke_test.sh` — expected fail (RC=1), error contains `unknown OPS_SMOKE_TARGET_CASES entry`
4. `cargo check -p copybot-executor -q` — PASS

## Result

Operators and development flow can now run deterministic, narrow smoke subsets for the exact changed area without paying full-suite runtime on every small ops slice, while keeping strict fail-closed parsing and preserving canonical full-suite default behavior.
