# Executor Phase 2 Slice 13 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving startup route-allowlist parsing and fastlane feature-gate policy checks out of `main.rs` into a dedicated module.

## Implemented

1. `crates/executor/src/route_allowlist.rs`:
   1. Added known-route contract:
      1. `paper,rpc,jito,fastlane`
   2. Added parser:
      1. `parse_route_allowlist(csv)`
   3. Added policy gate:
      1. `validate_fastlane_route_policy(route_allowlist, submit_fastlane_enabled)`
   4. Added module tests:
      1. `route_allowlist_parse_accepts_known_routes`
      2. `route_allowlist_parse_rejects_unknown_route`
      3. `route_allowlist_fastlane_policy_rejects_when_disabled`
2. `crates/executor/src/main.rs`:
   1. Removed inline implementations of:
      1. `parse_route_allowlist`
      2. `validate_fastlane_route_policy`
   2. Rewired startup config path to call shared module functions.
   3. Removed local `KNOWN_ROUTES` constant from `main.rs`.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added direct module guard:
      1. `route_allowlist_parse_rejects_unknown_route`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q route_allowlist_` — PASS
2. `cargo test -p copybot-executor -q parse_route_allowlist_rejects_unknown_route` — PASS
3. `cargo test -p copybot-executor -q validate_fastlane_route_policy_enforces_feature_gate` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
