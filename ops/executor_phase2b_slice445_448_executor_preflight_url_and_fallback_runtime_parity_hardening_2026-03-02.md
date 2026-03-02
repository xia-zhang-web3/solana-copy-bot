# Executor Phase 2B — Slices 445-448

Date: 2026-03-02  
Owner: execution-dev

## Scope

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Ordering Governance Gate

1. Read: `ops/executor_ordering_coverage_policy.md`.
2. Current ordering residual count: `N=0`.
3. Slice type: `runtime/e2e` (not coverage-only).
4. Mapping: `ROAD_TO_PRODUCTION.md` next-code-queue item `5`.

## Slice 445 — Route backend resolution parity

`tools/executor_preflight.sh` now resolves effective executor route backends the same way runtime config does:

1. `route_submit = first_non_empty(route_submit, global_submit_default)`
2. `route_simulate = first_non_empty(route_simulate, global_simulate_default)`

This closes the previous gap where fallback/identity checks were skipped for routes relying on global defaults.

## Slice 446 — Endpoint URL schema hardening

`endpoint_identity()` in preflight now fail-closes non-runtime-compatible URLs:

1. scheme must be `http|https`,
2. host must be present,
3. URL credentials are forbidden,
4. query/fragment are forbidden.

This strict validation is applied to:

1. executor route backend URLs,
2. executor route fallback URLs,
3. adapter upstream route URLs.

## Slice 447 — Fallback identity contract completion

Preflight now rejects fallback endpoint identity collisions per route:

1. submit fallback must differ from submit primary,
2. simulate fallback must differ from simulate primary,
3. send-rpc fallback must differ from send-rpc primary.

This aligns preflight with runtime fail-closed fallback topology guards.

## Slice 448 — Smoke expansion for new contracts

`run_executor_preflight_case` now pins new fail-closed negatives:

1. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` contains `fastlane` while `COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=false`,
2. invalid executor route URL scheme (`ftp://...`) for submit backend,
3. invalid adapter upstream simulate URL scheme (`ftp://...`),
4. submit fallback endpoint collides with primary,
5. send-rpc fallback endpoint collides with primary.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full `bash tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight case above was executed.
