# Slice 533 — Followlist Activity Strict Guard + Runtime Snapshot Discovery Metrics

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e hardening (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `tools/runtime_snapshot.sh`
2. `tools/execution_go_nogo_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Changes

1. `runtime_snapshot.sh` now emits discovery-cycle summary fields from journal:
   1. `discovery_cycle_sample_available`
   2. `discovery_cycle_duration_ms_last`
   3. `eligible_wallets_last`
   4. `active_follow_wallets_last`
   5. `swaps_delta_fetched_last`
   6. `swaps_evicted_due_cap_last`
   7. `swaps_fetch_limit_reached_last`
2. Added strict go/no-go guard `GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY`:
   1. `SKIP/gate_disabled` when strict gate is off,
   2. `UNKNOWN` for missing/invalid/inconsistent metrics,
   3. `WARN/followlist_inactive` for zero activity,
   4. `PASS/followlist_active` for non-zero eligible+active wallets.
3. Integrated followlist strict gate into go/no-go precedence:
   1. strict `UNKNOWN` -> `NO_GO/followlist_activity_unknown`,
   2. strict `WARN` -> `NO_GO/followlist_activity_not_pass`,
   3. all-pass condition requires `followlist_activity_guard_verdict=PASS` when strict is enabled.
4. Extended smoke fixtures and checks:
   1. new fake journal mode `followlist_active`,
   2. strict followlist `PASS/WARN/UNKNOWN` pins,
   3. invalid bool pin for `GO_NOGO_REQUIRE_FOLLOWLIST_ACTIVITY`.

## Validation

1. `bash -n tools/runtime_snapshot.sh tools/execution_go_nogo_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_TARGET_CASES=common_parsers bash tools/ops_scripts_smoke_test.sh` — PASS
4. `OPS_SMOKE_TARGET_CASES=execution_server_rollout_fast bash tools/ops_scripts_smoke_test.sh` — PASS
5. `cargo check -p copybot-executor -q` — PASS

## Notes

1. Gate default is `false` to preserve backward compatibility for standalone runs.
2. Strict enable path is intended for production-like readiness checks where non-zero followlist activity is mandatory.
