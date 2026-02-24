# Executor Phase 2 Slice 2 Audit Fix Evidence — RPC Tip Coercion Ordering

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Audit Finding Addressed

1. `route=rpc` with `COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP=false` was rejected before coercion because tip checks used requested tip instead of effective route-normalized tip.

## Fix

1. Reordered submit validation in `handle_submit`:
   1. compute `route` and `(effective_tip_lamports, tip_policy_code)` first,
   2. run tip guards against `effective_tip_lamports` (not raw request tip).
2. Result:
   1. `rpc` now correctly coerces non-zero tip to `0` and proceeds,
   2. non-rpc routes (`jito`/`fastlane`) still reject non-zero tip when `allow_nonzero_tip=false`.

## Added Tests

1. `handle_submit_allows_rpc_tip_when_nonzero_tip_disabled`
2. `handle_submit_rejects_nonzero_tip_for_jito_when_disabled`
3. Contract smoke now includes `handle_submit_allows_rpc_tip_when_nonzero_tip_disabled`.

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS
