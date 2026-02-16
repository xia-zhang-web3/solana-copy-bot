# BUY Signals = 0: Unified Conclusion and Architecture Plan

Date: 2026-02-16  
Scope: `solana-copy-bot` shadow mode  
Source: internal code audit + external auditor report (merged into one decision)

## 1) Executive Conclusion

`BUY=0` is caused primarily by **ingestion latency backlog**, not by shadow worker pool limits.

The root issue is architectural:
- `logsSubscribe` events are consumed, but each event waits for a sequential `getTransaction` path.
- Arrival rate is higher than processing rate, so event age grows to minutes.
- BUY then fails `lag_exceeded`, and remaining BUYs are further filtered by buy-only quality gates.

So the correct fix is **pipeline redesign in ingestion (bounded parallel fetch + ordering control + role-separated RPC)**, not threshold tuning.

## 2) Facts That Are Consistent Across Both Audits

1. 8h metrics show asymmetry:
- `eligible_window_buy=101`, `signals_window_buy=0`
- `eligible_window_sell=92`, `signals_window_sell=40`
- `shadow_queue_saturated_window=0`

2. BUY drop reasons are dominated by:
- `lag_exceeded` (majority)
- then `too_new`, `low_holders`, `not_followed`

3. Observed latency is real and large:
- recorded sell latency in logs around `400k–800k ms` (6–13 min).

4. System stability is not the blocker:
- no queue_full/panic/JoinError incidents in the observed window.
- uptime is stable.

## 3) Unified Root-Cause Analysis (Prioritized)

## RC1 (Primary): Sequential ingestion fetch path creates backlog

Current flow effectively serializes fetch:
- WS notification -> `getTransaction` -> parse -> next notification.

When arrival exceeds service throughput, backlog accumulates and `swap.ts_utc` (blockTime) becomes stale by minutes at processing time.  
BUY path then fails lag gate.

Why this is primary:
- numerically explains large `lag_exceeded` bucket.
- consistent with high observed event lag and zero queue saturation in shadow workers.

## RC2 (Secondary): BUY-only quality gates eliminate survivors

After lag, BUY still passes through age/holders/liquidity/volume checks.  
SELL does not face this same gate path in the same way.

Effect:
- even if lag improves partially, part of BUY flow will still be dropped unless quality data and thresholds are calibrated.

## RC3 (Secondary): Shared HTTP RPC endpoint causes contention

Single `helius_http_url` serves:
- ingestion `getTransaction`,
- discovery token-quality refresh,
- shadow token-quality refresh.

This couples latency domains and worsens tail latency under bursts/rate limits.

## RC4 (Secondary): Follow temporal gating contributes to BUY drops

BUY has strict pre-gate behavior on follow status in app loop, and temporal retention ties to lag horizon.  
At high lag, this contributes additional BUY loss (`not_followed` bucket).

## RC5 (Observation): Memory growth (~1.8G) is a symptom, not root

Likely contributors:
- sustained backlog pressure,
- large transaction JSON handling,
- long-lived discovery in-memory window state.

Important, but not primary cause of BUY=0.

## 4) What Is NOT the Root Problem

Not supported by evidence as primary blockers:
- shadow worker pool size/capacity,
- panic/JoinError safety,
- queue overflow policy.

These were good hardening changes and should be kept.

## 5) Single Recommended Direction (No “quick hacks” as final fix)

Implement a **proper ingestion architecture** with bounded parallelism and ordering guarantees.

## 5.1 Target Architecture

1. WS reader task (fast path only)
- Reads `logsNotification`.
- Performs signature dedupe.
- Pushes notifications into bounded channel.

2. Fetch worker pool (bounded concurrency, e.g. 8–16)
- Concurrent `getTransaction` + parse.
- Writes parsed swaps into output channel.
- Handles retries with strict per-request budget.

3. Reorder layer (bounded)
- Preserves processing correctness for dependent flows.
- Recommended policy: order by `(slot, arrival_seq, signature)` with small holdback window.
- Guarantee: never process sell before its causally earlier buy for same key when both arrive within reorder window.

4. App/shadow path unchanged where possible
- Keep existing per-key serialization and FIFO lot close behavior.
- Keep existing safety/backpressure logic in shadow scheduler.
- Add per-`(wallet,token)` SELL causal holdback window before enqueue to reduce same-slot inversion edge-cases.

5. RPC role separation
- Separate HTTP endpoints/keys:
  - ingestion fetch path,
  - discovery/shadow quality path.
- Isolate failures and reduce cross-component tail-latency coupling.

## 5.2 Mandatory Observability (before and during rollout)

Add metrics:
- `ws_to_fetch_queue_depth`
- `fetch_concurrency_inflight`
- `fetch_latency_ms` (p50/p95/p99)
- `ingestion_lag_ms` (`processed_now - blockTime`)
- `buy_drop_rate_by_reason`
- `reorder_buffer_size` and `reorder_hold_ms`
- RPC `429/5xx` counters per role endpoint

Without these metrics, tuning will be guesswork.

## 6) Risk and Regression Matrix (Architecture Path)

1. Ordering regressions
- Risk: parallel fetch can reorder events.
- Control: bounded reorder buffer + deterministic sort key + invariant tests for buy-before-sell scenarios.

2. Memory pressure
- Risk: channels and concurrent JSON payloads increase memory.
- Control: strict bounded channels, concurrency caps, payload size monitoring, backpressure counters.

3. RPC rate limiting
- Risk: high concurrency hits provider limits.
- Control: token bucket/semaphore cap, adaptive backoff, split RPC keys by role.

4. SQLite write pressure
- Risk: higher ingest throughput increases write contention.
- Control: keep current retry behavior, monitor lock/busy rates, consider batched writes only if needed after metrics.

5. False-positive BUY due to stale events
- Risk: if lag still high, stale BUYs may pass when thresholds are loosened.
- Control: keep lag gate meaningful after backlog is solved; do not disable lag as permanent policy.

## 7) Rollout Plan (Correct Path)

1. Stage A: Instrumentation and baselining (short)
- Ship metrics first.
- Capture 24h baseline on current architecture.

2. Stage B: Parallel fetch pipeline (core change)
- Introduce WS->fetcher channels and bounded pool.
- Keep existing app/shadow contracts.

3. Stage C: Reorder + invariants
- Add reorder buffer and correctness tests for sequencing-sensitive cases.

4. Stage D: RPC separation
- Separate endpoints/keys for ingestion vs quality.
- Validate p95/p99 fetch latency and drop-rate improvements.

5. Stage E: Threshold recalibration
- Revisit quality thresholds only after latency path is fixed.
- Treat threshold tuning as strategy calibration, not infrastructure fix.

## 8) Acceptance Criteria for “Fixed BUY Path”

The issue is considered fixed only when all are true over sustained window:

1. `signals_window_buy > 0` consistently (not one-off spikes).  
2. BUY `lag_exceeded` drops become minority, not dominant reason.  
3. `ingestion_lag_ms` p95 stays in seconds-range, not minutes-range.  
4. No new ordering regressions in FIFO lot accounting.  
5. Memory remains bounded and non-linear growth is absent.

---

Final decision:
- **Do not treat lag/quality threshold tuning as final solution.**
- **Proceed with ingestion architecture refactor + ordering + RPC separation as the canonical fix path.**
