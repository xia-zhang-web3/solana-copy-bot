# PRED2-3 Aggregate Scoring Redesign

Date: 2026-03-09

Scope note:

1. Production ingestion scope is now `yellowstone_grpc` only.
2. `helius_ws` is explicitly unsupported as an ingestion source and is treated as legacy dead
   backup code, not part of the rollout safety contract for this redesign.

## Goal

Close the architectural conflict between:

1. short raw `observed_swaps` retention needed for disk safety, and
2. 30-day discovery/followlist scoring semantics.

The redesign makes persisted aggregate tables the new scoring source of truth, while raw
`observed_swaps` remains a short hot window for ingestion/runtime operations.

## Landed Components

### 1. Aggregate schema

Migration `0035_discovery_scoring_aggregates.sql` adds:

1. `wallet_scoring_days`
2. `wallet_scoring_tx_minutes`
3. `wallet_scoring_open_lots`
4. `wallet_scoring_carryover_lots`
5. `wallet_scoring_buy_facts`
6. `wallet_scoring_close_facts`
7. `discovery_scoring_state`

These tables are sufficient to reconstruct the current discovery score/eligibility formula without
requiring a 30-day raw swap archive in live SQLite.

### 2. Serialized write path

Aggregate writes run in the observed-swap serialized writer path, not in discovery:

1. `ObservedSwapWriter` writes raw swaps and wallet activity days first, then optionally materializes
   discovery scoring aggregates as a second phase.
2. Heavy aggregate preparation no longer runs inside the raw observed-swap write transaction.
3. Live ingest acknowledgements return after raw insert success; aggregate materialization remains on
   the same serialized worker thread, but no longer blocks `observed_swap_writer.write()` latency.
4. Live runtime aggregate writes are explicitly gated by `discovery.scoring_aggregates_write_enabled`.
5. runtime retention respects a temporary backfill source-protection watermark, so live raw sweeps
   do not delete the slice that a concurrent backfill replay is reading.
6. still-open lots are retained until consumption; retention only prunes historical facts, not
   live cost-basis inventory.
7. `wallet_scoring_carryover_lots` exists as a reserved escape hatch for future/manual compaction
   flows, but normal retention no longer depends on collapsing live open lots.
8. aggregate write coverage now advances on an exact cursor chain (`ts`, `slot`, `signature`), not
   on timestamp alone.
9. the first startup with `discovery.scoring_aggregates_write_enabled = true` replays every raw row
   after the last exact covered cursor before it accepts new live writes.
10. continuity gaps are also latched on an exact cursor chain, and repair only clears them when a
   successful replay actually observes the exact latched gap cursor.
11. startup replay now uses the same exact observation rule: if it reprocesses the latched gap row,
    it clears the blocker automatically; if that row is already absent from raw source, startup keeps
    raw ingestion alive but leaves readiness blocked for explicit backfill repair.
12. if the observed-swap writer thread dies, the main app loop now fails fast instead of silently
    continuing on stale ingestion.
13. WAL truncate checkpoint remains part of the retention reclaim flow.

### 3. Aggregate read path

Discovery can now rebuild wallet snapshots from aggregate tables instead of raw swap windows.

Important rollout safety:

Aggregate reads are gated by three conditions:

1. `discovery.scoring_aggregates_enabled = true`
2. `discovery_scoring_state.covered_since_ts <= scoring_window_start`
3. `discovery_scoring_state.covered_through_*` is present as a full exact cursor and its
   `covered_through_ts` component is near-head (within discovery refresh lag)
4. no latched `materialization_gap_since_ts` is present

If either condition is false, discovery stays on the existing hotfix/raw fallback path.

This staging is intentional. It allows:

1. code rollout,
2. historical backfill,
3. parity checks and trend snapshots,
4. only then live activation of aggregate scoring.

### 4. Admin backfill tool

New admin bin:

`cargo run -p copybot-storage --bin backfill_discovery_scoring -- <db_path> ...`

Properties:

1. cursor-based forward replay over `observed_swaps`
2. bounded batch commits
3. exact resume cursor support
4. explicit `--reset` guard for non-idempotent aggregate rebuilds
5. optional `--mark-covered` to write the historical coverage marker only when the operator intends
   to make the backfill eligible for later live use, and only when continuous lineage from the same
   `start_ts` is proven by persisted backfill progress
6. resumed backfill itself also requires matching persisted backfill progress; a first run from an
   arbitrary later cursor is rejected fail-closed and cannot seed fake lineage
7. config-derived scoring policy; parity-critical knobs come from `--config`, not ad-hoc CLI defaults
8. backfill refuses to run if either aggregate writes or aggregate reads are enabled in the target
   runtime config
9. mature rug facts are finalized as the replay cursor advances, so historical aggregate quality
   does not depend on a later discovery cycle
10. backfill now matches live writer policy by default: no implicit Helius quality fetches unless
   the operator explicitly passes `--helius-http-url`

## Current Rollout Mode

Tracked configs keep:

1. `discovery.scoring_aggregates_write_enabled = false`
2. `discovery.scoring_aggregates_enabled = false`

That means:

1. deploy is code-only and does not change discovery scoring semantics,
2. historical backfill can run without online aggregate double-count risk,
3. discovery will not switch to aggregate scoring until operators explicitly enable reads,
4. live aggregate writes remain an explicit later step after parity evidence,
5. even after coverage is marked, aggregate reads stay blocked until the live writer advances
   `covered_through_ts` back near head,
6. any live aggregate materialization failure latches a continuity gap and keeps aggregate reads
   disabled until replay/backfill repairs it,
7. the activation race between final tail catch-up and write enable is closed by startup replay from
   the exact covered cursor.
8. the first three aggregate-scored discovery cycles suppress both followlist promotions and
   followlist demotions, creating an explicit burn-in window before any live followlist churn.
9. if backfill is run against the live DB, runtime raw retention is temporarily pinned by TTL so
   the replay source does not disappear mid-run.
10. config activation is fail-closed: `discovery.scoring_aggregates_enabled = true` now requires
    `discovery.scoring_aggregates_write_enabled = true`.
11. stale failover override files that still request `SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws`
    now emit an explicit warning after tracing is initialized instead of being silently ignored.

## Backfill Procedure

Example full rebuild:

```bash
cargo run -p copybot-storage --bin backfill_discovery_scoring -- \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-02-08T00:00:00Z \
  --reset \
  --batch-size 5000 \
  --sleep-ms 50
```

Example exact resume:

```bash
cargo run -p copybot-storage --bin backfill_discovery_scoring -- \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-02-08T00:00:00Z \
  --resume-ts 2026-03-01T12:34:56Z \
  --resume-slot 123456789 \
  --resume-signature 5abc... \
  --batch-size 5000 \
  --sleep-ms 50
```

Mark coverage only after the completed rebuild you want discovery to trust:

```bash
cargo run -p copybot-storage --bin backfill_discovery_scoring -- \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-02-08T00:00:00Z \
  --resume-ts 2026-03-09T06:24:10Z \
  --resume-slot 123456789 \
  --resume-signature 5abc... \
  --mark-covered \
  --batch-size 5000
```

Operational warning:

The aggregate tables are not idempotent under overlapping replay. Do not restart from an
approximate timestamp. Resume only from the exact last committed cursor.

Safety rules:

1. keep `discovery.scoring_aggregates_write_enabled = false` during the historical rebuild
2. keep `discovery.scoring_aggregates_enabled = false` during the historical rebuild
3. if online raw ingestion continues during backfill, do a final exact tail catch-up before any
   activation step
4. `--mark-covered` on a resumed run is allowed only when persisted backfill progress proves that
   the replay is a continuous continuation from the same `start_ts`; arbitrary `resume_*` cannot
   certify historical coverage
5. aggregate reads must still wait for a near-head exact `covered_through_*` cursor
6. the backfill source-protection pin is temporary; long runs must finish before TTL expiry or
   refresh it by continuing normal batch progress
7. a successful full forward replay only clears the continuity blocker when it actually observes the
   exact latched gap cursor; if that row has already fallen out of raw source, the repair fails
   closed instead of clearing the blocker
8. enabling live aggregate writes no longer requires a quiesced stop; startup replay covers raw rows
   that arrived after the final backfill cursor but before the new writer came up

## Activation Procedure

1. Deploy code with both aggregate flags still `false`.
2. Run historical backfill for the required scoring horizon.
3. Do an exact tail catch-up to the intended activation point.
4. Compare aggregate-based scoring output against current live policy.
5. Optionally mark `covered_since_ts` plus the historical `covered_through_ts` produced by the
   replay.
6. Enable `discovery.scoring_aggregates_write_enabled = true`.
7. Collect trend snapshots to confirm live aggregate writes stay caught up, advance
   `covered_through_ts`, and do not distort ingestion latency.
8. Flip `discovery.scoring_aggregates_enabled = true` only after `covered_through_ts` is back near
   head under live writes.
9. Collect runtime/parity trend snapshots.
10. Only after parity is confirmed, remove the urgent bridges.

## Acceptance

The redesign is considered successful when:

1. raw `observed_swaps` retention can remain `1-7d`,
2. discovery/followlist can still promote and demote leaders normally after aggregate activation,
3. score semantics remain equivalent to the current policy,
4. live SQLite no longer needs month-scale raw event retention to support discovery scoring.
