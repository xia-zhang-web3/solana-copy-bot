# Executor Phase 3 Slice 3 Evidence — Idempotency Race Guard + Fail-Closed Persist

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Closed parallel-duplicate race window for submit idempotency:
   1. Added per-process in-flight guard keyed by `client_order_id`.
   2. Rejects concurrent duplicates with retryable `submit_in_flight` before upstream submit.
2. Closed fail-open persistence path:
   1. `store_submit_response` failure now maps to retryable `idempotency_store_unavailable` and aborts success response.
3. Hardened storage semantics:
   1. Idempotency insert changed from `ON CONFLICT DO UPDATE` to `ON CONFLICT DO NOTHING`.
   2. First successful response stays immutable under duplicate writes.
4. Improved health observability:
   1. `/healthz` now probes idempotency store dynamically and reports `ok` / `degraded`.

## Files

1. `crates/executor/src/main.rs`
2. `crates/executor/src/idempotency.rs`
3. `tools/executor_contract_smoke_test.sh`

## Tests Added/Updated

1. `handle_submit_rejects_parallel_duplicate_client_order_id_in_flight`
2. `store_does_not_overwrite_existing_response`
3. Contract smoke includes both tests in guard pack.

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS
4. `timeout 300 bash tools/ops_scripts_smoke_test.sh` — PASS
