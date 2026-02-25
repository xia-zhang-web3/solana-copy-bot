# Executor Phase 2B Slice 78 — Submit-Verify Signature Boundary (2026-02-25)

## Scope

- harden `submit_verify` boundary with signature-shape validation
- fail closed before verify-config/request for malformed `tx_signature`

## Changes

1. Added `validate_submit_verify_signature_context(...)` in `submit_verify`:
   - validates `tx_signature` via `validate_signature_like`
   - reject malformed signature:
     - code: `invalid_request_body`
     - detail: `submit signature verify invalid tx_signature at submit-verify boundary: ...`
2. Wired signature guard at entry of `verify_submitted_signature_visibility(...)` after deadline guard and before verify-config/request logic.
3. Added unit coverage in `submit_verify.rs`:
   - `submit_verify_signature_context_rejects_invalid_signature`
   - `submit_verify_signature_context_accepts_valid_signature`
4. Added integration pre-request/config-priority coverage in `main.rs`:
   - `verify_submit_signature_rejects_invalid_signature_before_config_check`
   - `verify_submit_signature_rejects_invalid_signature_before_request`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_verify_signature_context_` — PASS
3. `cargo test -p copybot-executor -q verify_submit_signature_rejects_invalid_signature_before_config_check` — PASS
4. `cargo test -p copybot-executor -q verify_submit_signature_rejects_invalid_signature_before_request` — PASS
5. `cargo test -p copybot-executor -q verify_submit_signature_seen_when_rpc_reports_confirmation` — PASS
6. `cargo test -p copybot-executor -q verify_submit_signature_rejects_when_pending_and_strict` — PASS
7. `bash tools/executor_contract_smoke_test.sh` — PASS
