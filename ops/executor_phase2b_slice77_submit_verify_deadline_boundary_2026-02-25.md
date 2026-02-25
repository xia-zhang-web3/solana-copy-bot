# Executor Phase 2B Slice 77 — Submit-Verify Deadline Boundary (2026-02-25)

## Scope

- harden `submit_verify` submit-path boundary with explicit deadline invariant
- prevent submit verification helper reuse without latency-budget context

## Changes

1. Added `validate_submit_verify_deadline_context(...)` in `submit_verify`:
   - reject missing deadline:
     - code: `invalid_request_body`
     - detail: `submit signature verify missing deadline at submit-verify boundary`
2. Wired guard at entry of `verify_submitted_signature_visibility(...)` before verify-config and request logic.
3. Added unit coverage in `submit_verify.rs`:
   - `submit_verify_deadline_context_rejects_missing_deadline`
   - `submit_verify_deadline_context_accepts_present_deadline`
4. Added integration pre-request/config-priority coverage in `main.rs`:
   - `verify_submit_signature_rejects_missing_deadline_before_config_check`
   - `verify_submit_signature_rejects_missing_deadline_before_request`
5. Updated existing verification integration tests to pass explicit `SubmitDeadline`.
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/submit_verify.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_verify_deadline_context_` — PASS
3. `cargo test -p copybot-executor -q verify_submit_signature_rejects_missing_deadline_before_config_check` — PASS
4. `cargo test -p copybot-executor -q verify_submit_signature_rejects_missing_deadline_before_request` — PASS
5. `cargo test -p copybot-executor -q verify_submit_signature_seen_when_rpc_reports_confirmation` — PASS
6. `cargo test -p copybot-executor -q verify_submit_signature_rejects_when_pending_and_strict` — PASS
7. `bash tools/executor_contract_smoke_test.sh` — PASS
