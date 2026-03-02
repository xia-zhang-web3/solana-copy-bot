# Executor Phase 2B — Slices 442-444

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

## Slice 442 — Env allowlist strictness in preflight

`tools/executor_preflight.sh` now validates env route-allowlist inputs fail-closed before route checks:

1. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST`
2. `COPYBOT_ADAPTER_ROUTE_ALLOWLIST`

Contract for both:

1. at least one route token must remain after parsing,
2. empty CSV entries are rejected,
3. unknown routes are rejected outside canonical set (`paper,rpc,jito,fastlane`),
4. duplicate routes after normalization are rejected.

Diagnostics were expanded with both raw and normalized CSV fields in summary:

1. `executor_route_allowlist_raw`
2. `executor_route_allowlist_csv`
3. `adapter_route_allowlist_raw`
4. `adapter_route_allowlist_csv`

## Slice 443 — Signer pubkey contract parity in preflight

`tools/executor_preflight.sh` now enforces signer pubkey shape parity with executor runtime startup guards:

1. `COPYBOT_EXECUTOR_SIGNER_PUBKEY` must be valid base58,
2. decoded key length must be exactly 32 bytes,
3. malformed input now fails preflight with explicit `config_error` diagnostics.

## Slice 444 — Smoke guard expansion for new env contracts

`run_executor_preflight_case` now pins fail-closed negatives for the new checks:

1. executor allowlist duplicate token,
2. executor allowlist unknown token,
3. executor allowlist empty token,
4. adapter allowlist empty token,
5. malformed signer pubkey base58 shape.

Fixture update for preflight PASS path:

1. signer pubkey fixture switched to valid base58 32-byte key token (`11111111111111111111111111111111`) to match the new contract.

## Targeted Verification

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `cargo check -p copybot-executor -q` — PASS
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. Full `bash tools/ops_scripts_smoke_test.sh` was not re-run in this slice; targeted preflight path above was executed.
