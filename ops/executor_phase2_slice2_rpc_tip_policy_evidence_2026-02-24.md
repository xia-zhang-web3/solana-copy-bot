# Executor Phase 2 Slice 2 Evidence — RPC Tip Policy Coercion + Trace

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Implemented route-specific submit tip normalization:
   1. `route=rpc` with non-zero `tip_lamports` is coerced to `0`.
   2. Coercion is explicit via response trace field `tip_policy`.
2. Enforced policy on forwarding path (not only response echo):
   1. submit payload forwarded upstream is rewritten with effective tip for `rpc`.
3. Added tests:
   1. unit: `normalize_submit_tip_for_route_forces_rpc_tip_to_zero`
   2. integration: `handle_submit_forces_rpc_tip_to_zero_and_emits_trace` (verifies upstream sees `tip_lamports=0`)
4. Contract smoke coverage updated to include the new integration test.
5. Canonical contract updated with optional `tip_policy` response trace fields.

## Files

1. `crates/executor/src/main.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ops/executor_contract_v1.md`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (50)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. This aligns implementation with contract section 6.1 route-specific tip semantics for `rpc`.
2. No change to `jito`/`fastlane` tip handling in this slice.
