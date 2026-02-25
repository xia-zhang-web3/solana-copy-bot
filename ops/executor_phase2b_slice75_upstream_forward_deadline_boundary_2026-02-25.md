# Executor Phase 2B Slice 75 — Upstream-Forward Deadline Boundary (2026-02-25)

## Scope

- harden deadline invariants at `forward_to_upstream` layer (defense-in-depth)
- prevent deadline-budget bypass if forward helper is reused outside route executor

## Changes

1. Added `validate_upstream_forward_deadline_context(...)` in `upstream_forward`:
   - reject `submit` without deadline:
     - code: `invalid_request_body`
     - detail: `submit upstream forward missing deadline ...`
   - reject `simulate` with deadline:
     - code: `invalid_request_body`
     - detail: `simulate upstream forward must not include submit deadline ...`
2. Wired validation at entry of `forward_to_upstream(...)` before any backend lookup/network request.
3. Added unit coverage in `upstream_forward.rs`:
   - `upstream_forward_deadline_context_rejects_submit_without_deadline`
   - `upstream_forward_deadline_context_rejects_simulate_with_deadline`
   - accept cases for submit-with-deadline and simulate-without-deadline
4. Added integration pre-request coverage in `main.rs`:
   - `forward_to_upstream_rejects_submit_without_deadline_before_request`
   - `forward_to_upstream_rejects_simulate_with_deadline_before_request`
5. Updated existing submit forward fallback tests to pass explicit `SubmitDeadline`:
   - retryable-status fallback
   - terminal-status no-fallback
   - fallback-auth-token path
6. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_forward_deadline_context_` — PASS
3. `cargo test -p copybot-executor -q forward_to_upstream_rejects_submit_without_deadline_before_request` — PASS
4. `cargo test -p copybot-executor -q forward_to_upstream_rejects_simulate_with_deadline_before_request` — PASS
5. `cargo test -p copybot-executor -q forward_to_upstream_uses_fallback_after_primary_retryable_status` — PASS
6. `cargo test -p copybot-executor -q forward_to_upstream_does_not_fallback_after_primary_terminal_status` — PASS
7. `cargo test -p copybot-executor -q forward_to_upstream_uses_fallback_auth_token_when_retrying` — PASS
8. `bash tools/executor_contract_smoke_test.sh` — PASS
