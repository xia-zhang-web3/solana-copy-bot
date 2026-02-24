# Executor Phase 2 Slice 9 Evidence — Send RPC Topology Guard (Defense-in-Depth)

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added explicit defense-in-depth guard in send RPC path:
   1. reject terminally when fallback send RPC URL is configured without primary send RPC URL.
2. Added integration test for this topology mismatch:
   1. `send_signed_transaction_via_rpc_rejects_fallback_without_primary_url`.
3. Added this test to contract smoke guard set.

## Files

1. `crates/executor/src/send_rpc.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. Existing config parser already blocks this topology; this slice adds runtime hardening against future config-construction drift.
