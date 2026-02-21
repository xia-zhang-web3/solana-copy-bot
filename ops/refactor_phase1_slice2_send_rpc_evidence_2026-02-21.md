# Refactor Evidence: Phase 1 Slice 2 (`send_rpc`) (2026-02-21)

Branch:
- `refactor/phase-1-adapter`

Base commit before slice:
- `016f734` (Phase 1 slice 1)

## Scope
Move-only extraction of send-RPC submit path from `crates/adapter/src/main.rs` into `crates/adapter/src/send_rpc.rs`.

Extracted items:
1. `send_signed_transaction_via_rpc`
2. `SendRpcErrorPayloadDisposition`
3. `classify_send_rpc_error_payload`
4. `extract_expected_signature_from_signed_tx_bytes`
5. `parse_shortvec_len`

No contract/behavior changes intended.

## Files Changed
1. `crates/adapter/src/main.rs`
2. `crates/adapter/src/send_rpc.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-adapter -q`
3. `cargo test -p copybot-adapter -q send_signed_transaction_via_rpc_rejects_signature_mismatch`
4. `cargo test -p copybot-adapter -q send_signed_transaction_via_rpc_uses_fallback_auth_token_when_retrying`
5. `cargo test -p copybot-adapter -q verify_submit_signature_rejects_when_onchain_error_seen`
6. `cargo test --workspace -q`
7. `tools/ops_scripts_smoke_test.sh`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/adapter/src/main.rs`: raw `3003`, runtime (excl `#[cfg(test)]`) `1967`, cfg-test `1036`
- `crates/adapter/src/send_rpc.rs`: raw `318`, runtime `318`, cfg-test `0`

## Notes
- `main.rs` now remains below 2000 runtime LOC (excluding `#[cfg(test)]`).
- This slice only rewires function location and imports; test matrix remains unchanged and green.
