# Executor Phase 3 Slice 4 Evidence — SQLite Claim Guard + Health Status Alignment

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Replaced process-local in-flight guard with durable SQLite claim guard:
   1. Added `executor_submit_idempotency_claims` table.
   2. Added atomic claim flow `load_cached_or_claim_submit(...)` returning `Cached | Claimed | InFlight`.
   3. Added explicit claim release in RAII guard via `release_submit_claim(...)`.
2. Added stale-claim cleanup via TTL (`COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC`, default 60s).
3. Kept first-success immutability for cached responses (`ON CONFLICT DO NOTHING`).
4. Aligned `/healthz` top-level `status` with idempotency probe state:
   1. `status=ok` when store probe passes,
   2. `status=degraded` when probe fails.

## Files

1. `crates/executor/src/idempotency.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`

## Tests Added/Updated

1. `claim_flow_returns_claimed_inflight_then_cached`
2. Guard pack now includes claim-flow regression.

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (70)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS

## Notes

1. This slice reduces residual multi-instance risk from process-local locking by moving claim arbitration into shared SQLite state.
2. Full distributed locking beyond shared-DB topology (e.g. cross-host Redis/KV lease) remains out-of-scope for this slice.
