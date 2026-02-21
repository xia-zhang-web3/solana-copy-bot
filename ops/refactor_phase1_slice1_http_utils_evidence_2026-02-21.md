# Refactor Evidence: Phase 1 Slice 1 (`http_utils`) (2026-02-21)

Branch:
- `refactor/phase-1-adapter`

Commit baseline:
- `b9ddef9` (Phase -1/0 evidence freeze)

## Scope
Move-only extraction of adapter HTTP utility helpers from `crates/adapter/src/main.rs` into `crates/adapter/src/http_utils.rs`.

Extracted functions:
1. `validate_endpoint_url`
2. `endpoint_identity`
3. `redacted_endpoint_label`
4. `classify_request_error`

No behavior changes intended.

## Files Changed
1. `crates/adapter/src/main.rs`
2. `crates/adapter/src/http_utils.rs`

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
- `crates/adapter/src/main.rs`: raw `3310`, runtime (excl `#[cfg(test)]`) `2274`, cfg-test `1036`
- `crates/adapter/src/http_utils.rs`: raw `77`, runtime `77`, cfg-test `0`

## Notes
- Extraction kept exact helper logic and error taxonomy intact.
- This slice is intentionally limited to `http_utils` only (per plan section 12).
