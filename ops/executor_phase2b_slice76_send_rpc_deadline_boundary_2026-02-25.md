# Executor Phase 2B Slice 76 — Send-RPC Deadline Boundary (2026-02-25)

## Scope

- harden `send_rpc` submit-path boundary with explicit deadline invariant
- prevent latency-budget bypass if send-rpc helper is reused directly

## Changes

1. Added `validate_send_rpc_deadline_context(...)` in `send_rpc`:
   - reject missing deadline:
     - code: `invalid_request_body`
     - detail: `submit send RPC missing deadline at send-rpc boundary`
2. Wired boundary guard at entry of `send_signed_transaction_via_rpc(...)` before backend/topology/request logic.
3. Added unit coverage in `send_rpc.rs`:
   - `send_rpc_deadline_context_rejects_missing_deadline`
   - `send_rpc_deadline_context_accepts_present_deadline`
4. Added integration pre-request coverage in `main.rs`:
   - `send_signed_transaction_via_rpc_rejects_missing_deadline_before_request`
   - `send_signed_transaction_via_rpc_rejects_missing_deadline_before_topology_check`
5. Updated existing send-rpc integration tests to pass explicit `SubmitDeadline`.
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q send_rpc_deadline_context_` — PASS
3. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_missing_deadline_before_request` — PASS
4. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_missing_deadline_before_topology_check` — PASS
5. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_treats_blockhash_expired_payload_as_terminal` — PASS
6. `cargo test -p copybot-executor -q send_signed_transaction_via_rpc_rejects_fallback_without_primary_url` — PASS
7. `bash tools/executor_contract_smoke_test.sh` — PASS
