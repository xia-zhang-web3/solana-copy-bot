# Executor Phase 2B Slice 117 — Submit Verify Error Detail Truncation (2026-02-26)

## Scope

- extend runtime detail-truncation hardening into submit-signature verification path
- prevent oversized on-chain `err` payloads from bloating reject details

## Changes

1. `submit_verify` now truncates on-chain error payload detail before terminal reject:
   - path: `upstream_submit_failed_onchain`
   - helper: `truncate_detail_chars(..., MAX_HTTP_ERROR_BODY_DETAIL_CHARS)`
2. Added integration guard:
   - `verify_submit_signature_truncates_large_onchain_error_detail`
   - asserts `...[truncated]` marker is present
   - asserts tail marker does not leak into reject detail
3. Registered guard in contract smoke test list.
4. Updated ROAD ledger item `276`.

## Files

- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q verify_submit_signature_truncates_large_onchain_error_detail` — PASS
3. `cargo test -p copybot-executor -q verify_submit_signature_rejects_when_onchain_error_seen` — PASS
4. `cargo test -p copybot-executor -q submit_verify_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `cargo test -p copybot-executor -q` — PASS
