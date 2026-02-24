# Executor Phase 3 Slice 1 Evidence — Durable Submit Idempotency Bootstrap

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Added persistent submit idempotency store:
   1. module: `crates/executor/src/idempotency.rs`
   2. sqlite schema bootstrap + WAL mode
   3. keyed by `client_order_id` with `request_id` index
   4. stores normalized submit JSON response payload
2. Wired store into app state:
   1. new env: `COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH`
   2. default path: `state/executor_idempotency.sqlite3`
   3. startup fail-closed if store cannot initialize
3. Submit path idempotency behavior:
   1. before upstream forward, load cached response by `client_order_id`,
   2. after successful submit normalization, persist response snapshot.
4. Added tests:
   1. `store_and_load_submit_response_round_trip` (module)
   2. `handle_submit_returns_cached_response_for_duplicate_client_order_id` (integration behavior)
5. Health contract alignment:
   1. `/healthz` now emits `idempotency_store_status: "ok"`.

## Files

1. `crates/executor/Cargo.toml`
2. `crates/executor/src/idempotency.rs`
3. `crates/executor/src/main.rs`
4. `tools/executor_contract_smoke_test.sh`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `bash -n tools/executor_contract_smoke_test.sh` — PASS

## Notes

1. This is the bootstrap step for durable idempotency model. Recovery policy for partial lifecycle states will be extended in next Phase 3 slices.
