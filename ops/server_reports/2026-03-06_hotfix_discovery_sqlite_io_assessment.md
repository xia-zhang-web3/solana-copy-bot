# Hotfix Assessment: Discovery/SQLite I/O Degradation

Date: `2026-03-06`
Primary source report: [2026-03-06_evening_post_patch_runtime_report.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md)

## Bottom line

This does **not** look like an OOM, RAM leak, or CPU saturation problem.
It looks like an **I/O-bound degradation around the shared SQLite path**, with discovery as the primary pressure source and ingestion degrading secondarily behind it.

This is **not a previously closed issue**.
Some adjacent runtime hardening has already landed, but the core storage/discovery workload shape is still open.

This is **fixable in code and storage design**.
A stronger server, especially with materially better NVMe/IOPS, can buy time, but it is **not a complete fix**.

## What the runtime report confirms

From [2026-03-06_evening_post_patch_runtime_report.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md):

- Runtime stability is fine: no restarts, no OOM evidence. See [2026-03-06_evening_post_patch_runtime_report.md#L20](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md#L20).
- Discovery duration has materially degraded: `p95=151088 ms`, `max=592013 ms`, `last=190520 ms`. See [2026-03-06_evening_post_patch_runtime_report.md#L29](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md#L29).
- Ingestion lag/backpressure has materially degraded: queue depth pinned at max and backpressure counter rising. See [2026-03-06_evening_post_patch_runtime_report.md#L32](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md#L32).
- The live DB is already large: `live_copybot.db ~45G`, WAL `~4.7G`. See [2026-03-06_evening_post_patch_runtime_report.md#L59](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-06_evening_post_patch_runtime_report.md#L59).

The separate diagnosis mentioning very high `iowait`, high PSI I/O pressure, and NVMe saturation is not itself contained in this markdown report. So that part should be treated as a strong external operational observation that is consistent with the symptoms above, not as a self-contained proof embedded in this file.

## Why this looks like a code/storage problem

### 1. Discovery still does expensive full-window work every cycle

Discovery still:

- advances through `observed_swaps` after a cursor: [crates/discovery/src/lib.rs#L209](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L209)
- maintains a large in-memory window up to `max_window_swaps_in_memory`: [crates/discovery/src/lib.rs#L141](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L141)
- rebuilds wallet snapshots from the cached window each cycle: [crates/discovery/src/lib.rs#L371](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L371)
- recomputes rug metrics across buy observations and token history each cycle: [crates/discovery/src/lib.rs#L499](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L499)

This is exactly the kind of workload that gets progressively worse as the backing table and WAL pressure grow.

### 2. Discovery still writes a heavy SQLite transaction every cycle

After computing snapshots, discovery still writes:

- wallet upserts
- wallet metric inserts
- retention delete
- followlist activation/deactivation

through one SQLite write transaction in the current `origin/main` path:

- [crates/storage/src/discovery.rs#L71](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/discovery.rs#L71)

The retention delete is structurally suspicious:

- [crates/storage/src/discovery.rs#L142](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/discovery.rs#L142)

Even though retention windows are only `3`:

- [crates/storage/src/lib.rs#L18](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/lib.rs#L18)

the query still does `GROUP BY window_start ORDER BY window_start DESC LIMIT 3` against the whole `wallet_metrics` table every cycle. Without a dedicated index on `window_start`, this can still produce repeated scan/sort/temp-B-tree work.

So this should be treated as an immediate companion fix, not as a low-priority cleanup:

- add an index on `wallet_metrics(window_start)`
- then re-check `EXPLAIN QUERY PLAN` for the retention delete

### 3. Ingestion is degraded secondarily because it writes into the same DB

The app loop writes every observed swap into SQLite here:

- [crates/app/src/main.rs#L1716](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/app/src/main.rs#L1716)

Discovery runs concurrently in a separate spawned task that opens its own `SqliteStore` on the same `sqlite_path`:

- [crates/app/src/main.rs#L1905](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/app/src/main.rs#L1905)
- [crates/app/src/task_spawns.rs#L10](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/app/src/task_spawns.rs#L10)

So even without a classic lock storm, the system can still become disk-bound because:

- ingestion appends continuously into `observed_swaps`
- discovery scans/loads from `observed_swaps`
- discovery writes metrics/followlist through its own SQLite write transaction on the same DB file
- everything hits the same SQLite/WAL-backed file

### 4. The `observed_swaps` cursor query is the highest-confidence hotfix hypothesis pending live EXPLAIN

The cursor query path is here:

- [crates/storage/src/market_data.rs#L105](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/market_data.rs#L105)

The current predicate is:

```sql
WHERE ts > ?1
   OR (ts = ?1 AND slot > ?2)
   OR (ts = ?1 AND slot = ?2 AND signature > ?3)
ORDER BY ts ASC, slot ASC, signature ASC
LIMIT ?4
```

There is already a composite index for this access pattern:

- [0007_observed_swaps_time_scan_index.sql#L1](/Users/tigranambarcumyan/Documents/solana-copy-bot/migrations/0007_observed_swaps_time_scan_index.sql#L1)

The problem is that SQLite commonly does a poor job with this OR-pattern over a composite key. On a large `observed_swaps` table, this can degrade into an expensive scan path instead of a clean range seek.

The highest-confidence immediate SQL hotfix hypothesis is to rewrite this to tuple comparison:

```sql
WHERE (ts, slot, signature) > (?1, ?2, ?3)
ORDER BY ts ASC, slot ASC, signature ASC
LIMIT ?4
```

That is a strong candidate one-line fix that may turn a bad planner path into a proper index range scan on the existing `(ts, slot, signature)` B-tree. But it should be treated as a hypothesis until verified with real `EXPLAIN QUERY PLAN` on the live schema.

The recent-slice load path is here:

- [crates/storage/src/market_data.rs#L153](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/market_data.rs#L153)

This query is less suspicious than the cursor query, because it should be able to use the same index via reverse scan. But since `market_data.rs` is already being touched, the plan should be verified for both queries in the same hotfix batch.

On a DB already around `45G`, these `observed_swaps` paths remain the most plausible primary I/O drivers.

## Was this already fixed?

No.

What **was** already improved:

- contention/retry behavior around some storage writes
- app/runtime blocking behavior on certain SQLite paths
- several storage state-machine and retry hardenings

What **was not** fundamentally changed:

- discovery still recomputes large windows every cycle
- discovery and ingestion still share the same SQLite storage path
- `observed_swaps` still grows as the main append-heavy live table
- discovery is still writing a large metrics set every cycle even while `followlist=0`

So this is **not** a case of “we already fixed it and the server is just weak”.

The raw discovery logs already show `metrics_written` in the tens of thousands late in the degraded window, for example values like `37448` and `36148` in:

- [discovery_since_rollout.log](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/discovery_since_rollout.log)

## Is a stronger server enough?

No, not by itself.

A stronger server can help temporarily, especially if it gives:

- much faster NVMe
- better sustained write IOPS
- lower disk latency under mixed read/write pressure

That may be enough to buy time for `tiny live`.

But it is not a durable fix, because the workload shape is still wrong:

- the DB is already large
- the hot table keeps growing
- discovery work grows with live history and retained window behavior
- ingestion and discovery still contend for the same storage surface

In other words:

- **better hardware can delay the failure mode**
- **it does not remove the failure mode**

## What I would do first

### Immediate mitigation

1. Raise `discovery.refresh_seconds` temporarily so discovery stops running at the edge of its own wall-clock duration.
2. Reduce discovery workload for now:
   - smaller retained window
   - less expensive rug-analysis
   - lower per-cycle work until the storage path is fixed
3. Rewrite the `observed_swaps` cursor query to tuple comparison as the first hotfix hypothesis to verify:
   - [crates/storage/src/market_data.rs#L123](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/market_data.rs#L123)
4. Add an index on `wallet_metrics(window_start)`.
5. Re-check `EXPLAIN QUERY PLAN` for:
   - the cursor query on `observed_swaps`
   - the recent-slice query on `observed_swaps`
   - the retention delete on `wallet_metrics`
6. Replace the current retention delete with a simpler cutoff-driven delete if the indexed plan is still poor.

### Short-term code fix

1. Separate ingestion and discovery write/read pressure:
   - batching for observed swap writes, or
   - dedicated writer path, or
   - separate DB/storage surface for ingestion data
2. Rework discovery so it is less full-recompute-heavy per cycle.
3. Introduce real retention/partition strategy for `observed_swaps`.

## Practical conclusion

The current degradation is best understood as:

- **primary**: storage and disk I/O pressure from the shared SQLite live path
- **secondary**: ingestion lag and backpressure caused by discovery/storage pressure
- **not primary**: RAM, OOM, or CPU saturation

This is **fixable**.
It should be treated as an active engineering problem, not as proof that the current bot architecture is hopeless.

But I would not rely on “just buy a bigger server” as the only response.
That is a temporary mitigation, not a proper fix.

## Priority order

1. Add `wallet_metrics(window_start)` index.
2. Rewrite the `observed_swaps` cursor query to tuple comparison, but treat acceptance as `EXPLAIN`-verified rather than assumed.
3. Apply config mitigation on the live server now.
4. Then move to broader storage/discovery workload redesign.

## Hotfix batch scope for coder

Scope should stay narrow.
This is not the batch for incremental discovery redesign or DB split.

### Code changes

1. Add a migration that creates an index on `wallet_metrics(window_start)`.
2. In [crates/storage/src/market_data.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/market_data.rs), rewrite the cursor query used by `for_each_observed_swap_after_cursor(...)` from the current OR-chain to tuple comparison on `(ts, slot, signature)`.
3. Do not expand this batch into broader discovery algorithm changes.

### Verification

1. Capture `EXPLAIN QUERY PLAN` for:
   - `for_each_observed_swap_after_cursor(...)`
   - `load_recent_observed_swaps_since(...)`
   - the `wallet_metrics` retention delete
2. Confirm the cursor query no longer falls onto the bad scan path.
3. Confirm the recent-slice query still uses an acceptable indexed plan.
4. Run the relevant tests for storage/discovery paths after the SQL change.

### Acceptance condition

This batch is successful if:

1. `wallet_metrics` retention no longer relies on a bad unindexed `window_start` path,
2. the cursor query is planner-friendly on the live schema,
3. no behavioral regression is introduced in storage/discovery runtime logic.
