# BUY Signal Recovery: Execution Report

Date: 2026-02-16  
Based on: `BUY_SIGNAL_UNIFIED_CONCLUSION.md`

## Status

Implemented all code-level items from the architecture path:
- Stage A: instrumentation
- Stage B: parallel ingestion pipeline
- Stage C: reorder + invariants
- Stage D: role-separated RPC endpoints
- Post-review hardening: causal ordering safeguards and lossless backpressure behavior

Stage E (threshold recalibration) is intentionally left as runtime strategy tuning after telemetry collection.

## Implemented Changes

1. Parallel ingestion pipeline (bounded)
- WS reader task with reconnect/backoff + dedupe.
- Bounded channel: WS -> fetch workers.
- Fetch worker pool (`getTransaction`) with configurable concurrency.
- Bounded channel: fetch workers -> consumer.

2. Reorder layer
- Added bounded reorder buffer in ingestion source.
- Release policy: ordered by `(slot, arrival_seq, signature)` with holdback (`reorder_hold_ms`) and hard cap (`reorder_max_buffer`).
- Preserves order characteristics needed by downstream shadow FIFO logic.

3. Observability
- Added periodic ingestion metrics log:
  - `ws_to_fetch_queue_depth`
  - `fetch_to_output_queue_depth`
  - `fetch_concurrency_inflight`
  - `fetch_latency_ms_p50/p95/p99`
  - `ingestion_lag_ms_p50/p95/p99`
  - `reorder_hold_ms_p95`
  - `reorder_buffer_size` and historical max
  - RPC `429` / `5xx` counters
- Added `ingestion_lag_ms` to app-level “observed swap stored” log line.

4. RPC role separation
- Added role-specific config/env for `discovery` and `shadow` quality requests.
- App now chooses role URL first, then falls back to ingestion URL.

5. Config surface updates
- Added ingestion pipeline knobs:
  - `fetch_concurrency`
  - `ws_queue_capacity`
  - `output_queue_capacity`
  - `reorder_hold_ms`
  - `reorder_max_buffer`
  - `telemetry_report_seconds`
- Added env overrides for all key knobs and role URLs.
- Updated `configs/dev.toml`, `configs/paper.toml`, `configs/prod.toml`.

6. Tests / invariants
- Added ingestion reorder invariant test (`reorder_releases_oldest_slot_signature`).
- Added ingestion invariant test that verifies `arrival_seq` tie-break inside same slot (`reorder_uses_arrival_sequence_within_same_slot`).
- Added parser ack-ignore test for `logsSubscribe` acknowledgements.
- Added app tests for role URL selection fallback/placeholder handling.
- Added app tests for per-key causal holdback behavior on SELL without open lot.

7. Post-review hardening fixes
- Fixed critical ingestion loss risk: signature is now marked `seen` only after successful enqueue.
- On `try_send` full, WS reader now falls back to awaited `send` (backpressure) instead of dropping the event.
- Added `ws_notifications_backpressured` metric and retained `ws_notifications_dropped` only for real channel-closed failures.
- Added ingestion pipeline self-heal restart when worker tasks are unhealthy / channel closes.
- Added env override for `SOLANA_COPY_BOT_INGESTION_TELEMETRY_REPORT_SECONDS`.
- Added per-`(wallet,token)` SELL causal holdback in app loop (`shadow.causal_holdback_*`) to reduce buy/sell inversion risk within same slot burst scenarios.

## Files Changed

- `crates/ingestion/src/source.rs`
- `crates/config/src/lib.rs`
- `crates/app/src/main.rs`
- `configs/dev.toml`
- `configs/paper.toml`
- `configs/prod.toml`
- `README.md`

## Verification

Command run:

```bash
cargo fmt --all
cargo test --workspace
```

Result:
- All workspace tests passed.
- No failing unit tests or doc tests.

## Runtime Rollout Checklist

1. Run with `configs/paper.toml` and collect 24h telemetry.
2. Monitor:
- `ingestion_lag_ms_p95`
- BUY drop reasons (`lag_exceeded` should stop dominating)
- queue depths and RPC 429/5xx counters
3. Only after lag stabilizes, adjust quality thresholds (Stage E).
