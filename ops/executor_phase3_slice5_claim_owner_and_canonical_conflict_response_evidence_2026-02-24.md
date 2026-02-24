# Executor Phase 3 Slice 5 Evidence — Claim Ownership Release + Canonical Conflict Return

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Fixed claim-release ownership semantics:
   1. `release_submit_claim` now requires both `client_order_id` and `request_id`.
   2. Claim release only succeeds for the owner request (prevents cross-request claim removal on overlap).
2. Hardened conflict behavior on idempotency insert race (`inserted=false`):
   1. submit path now loads and returns canonical stored response.
   2. If canonical response is unexpectedly missing, returns retryable `idempotency_store_unavailable` (fail-closed).
3. Added config guard for claim TTL:
   1. `COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC` must be `>= ceil(COPYBOT_EXECUTOR_REQUEST_TIMEOUT_MS / 1000)`.

## Files

1. `crates/executor/src/idempotency.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`

## Tests Added/Updated

1. `release_claim_requires_request_id_owner_match`
2. `handle_submit_returns_canonical_cached_response_when_store_conflicts`
3. Updated contract guard pack to include both tests.

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (72)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS

