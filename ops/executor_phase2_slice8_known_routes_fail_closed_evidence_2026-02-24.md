# Executor Phase 2 Slice 8 Evidence — Known Routes Fail-Closed Guard

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added explicit known-route guard in `parse_route_allowlist`:
   1. supported routes are `paper|rpc|jito|fastlane`,
   2. unknown route token now fails startup with clear config error.
2. Added regression test:
   1. `parse_route_allowlist_rejects_unknown_route`.
3. Updated canonical contract to explicitly enumerate accepted `route` values for `/simulate` and `/submit`.
4. Added new guard test into contract smoke suite.

## Files

1. `crates/executor/src/main.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ops/executor_contract_v1.md`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. This closes configuration typo risk where unknown routes were previously accepted and only failed later in runtime behavior.
