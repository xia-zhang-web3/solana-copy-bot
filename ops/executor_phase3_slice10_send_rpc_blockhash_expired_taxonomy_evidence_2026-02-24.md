# Executor Phase 3 Slice 10 Evidence — Explicit Send-RPC Blockhash-Expired Taxonomy

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added explicit send-RPC payload classification for blockhash-expired conditions.
2. Blockhash-expired payloads now return terminal `executor_blockhash_expired` instead of generic terminal bucket.
3. Preserved separate handling for unknown payloads (`send_rpc_error_payload_terminal`).
4. Added dedicated tests:
   1. `send_signed_transaction_via_rpc_treats_blockhash_expired_payload_as_terminal`
   2. `send_signed_transaction_via_rpc_treats_unknown_error_payload_as_terminal`
5. Added blockhash-expired test to contract smoke guard pack.

## Files

1. `crates/executor/src/send_rpc.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (76)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS

