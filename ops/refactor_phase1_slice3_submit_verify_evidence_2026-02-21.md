# Refactor Evidence: Phase 1 Slice 3 (`submit_verify`) (2026-02-21)

Branch:
- `refactor/phase-1-adapter`

Base commit before slice:
- `bcf1586` (Phase 1 slice 2)

## Scope
Move-only extraction of submit-signature verification domain from `crates/adapter/src/main.rs` into `crates/adapter/src/submit_verify.rs`.

Extracted items:
1. `SubmitSignatureVerifyConfig`
2. `SubmitSignatureVerification`
3. `verify_submitted_signature_visibility`
4. `submit_signature_verification_to_json`
5. `parse_submit_signature_verify_config`
6. `build_submit_signature_verify_config`

No contract/behavior changes intended.

## Files Changed
1. `crates/adapter/src/main.rs`
2. `crates/adapter/src/submit_verify.rs`

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
- `crates/adapter/src/main.rs`: raw `2789`, runtime (excl `#[cfg(test)]`) `1753`, cfg-test `1036`
- `crates/adapter/src/submit_verify.rs`: raw `234`, runtime `234`, cfg-test `0`

## Notes
- Phase 1 micro-slice sequence (`http_utils`, `send_rpc`, `submit_verify`) is now completed.
- `main.rs` runtime LOC remains under 2000 after slice #3.
