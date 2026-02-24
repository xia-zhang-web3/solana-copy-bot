# Executor Phase 3 Slice 6 Evidence — Claim TTL Budget Hardening + Release Signal Visibility

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Strengthened minimum claim TTL invariant:
   1. Replaced weak lower bound `ceil(request_timeout_ms/1000)` with full submit-path budget function:
      - submit fallback hops,
      - send-rpc endpoint chain hops,
      - submit-signature-verify attempts × endpoint fanout,
      - verify interval waits,
      - safety padding.
   2. Startup fail-closed if `COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC` is below computed minimum.
2. Improved release diagnostics for owner-bound claims:
   1. `SubmitClaimGuard::drop` now warns on `Ok(false)` (owner-mismatch/no-row) instead of silently ignoring.
3. Added regression test for new TTL budget function:
   1. `min_claim_ttl_sec_for_submit_path_accounts_for_verify_and_fallback_hops`.

## Files

1. `crates/executor/src/main.rs`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (73)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS
4. `bash -n tools/executor_contract_smoke_test.sh crates/executor/src/main.rs crates/executor/src/idempotency.rs` — PASS

