Server: `52.28.0.218`
Date: `2026-03-26`
Type: `production-critical Stage 3 blocker`

## Summary

The primary Stage 3 path is operationally blocked.

Live swap ingestion is still running, but the bounded `recent_raw` snapshot path
that must accumulate the usable 5-day raw window is no longer advancing.

This is not a “wait another 3 days” situation.

## Live Facts

- `solana-copy-bot.service = active`
- `copybot-discovery-recent-raw-snapshot.timer = active/enabled`
- `copybot-discovery-runtime-export.timer = active`
- `/var/www/solana-copy-bot/state = 220G used / 247G avail / 48%`
- `/var/www/solana-copy-bot/state/discovery_restore/recent_raw = 183G`

## Bounded Snapshot Frontier

Current promoted bounded raw snapshot:

- `covered_since = 2026-03-24T12:07:11.775090344Z`
- `covered_through = 2026-03-26T07:33:59.609580569Z`
- `last_batch_completed_at = 2026-03-26T07:35:10.023741560Z`

That is only about `1d 19h 27m` of usable bounded raw coverage, not 5 days.

## Stage 3 Evidence State

`discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml --limit 5 --json`
currently reports:

- `verdict = insufficient_evidence`
- `reason = no_recent_wallet_freshness_captures_within_horizon`
- `latest_capture_age_seconds = 94817`

The latest persisted capture is still `2026-03-25T18:59:01Z`.

## Root Cause

The source raw DB is still growing, but the bounded snapshot builder cannot
finish a full SQLite online backup within its configured attempt-duration budget.

Observed repeated scheduled runs:

- `state = deferred`
- `latest_surface_status = healthy`
- `latest_surface_action = deferred_due_to_attempt_budget`
- `terminal_reason = attempt_duration_budget_exhausted`
- `snapshot_max_attempt_duration_ms = 120000`

Observed source DB growth during the stalled period:

- `source_db_bytes = 11100868608`
- `source_db_bytes = 11117019136`
- `source_db_bytes = 11136274432`
- `source_db_bytes = 11153293312`

Observed page-level snapshot progress during those same attempts:

- copied `2028544 / 2710194` pages, remaining `681650`
- copied `1978368 / 2714134` pages, remaining `735766`
- copied `2001920 / 2718817` pages, remaining `716897`
- copied `2014208 / 2722972` pages, remaining `708764`

The code path responsible:

- `crates/discovery/src/bin/discovery_recent_raw_snapshot.rs`
  - adaptive policy caps huge sources at:
    - `pages_per_step = 1024`
    - `max_attempt_duration = 120000ms`
- `crates/storage/src/lib.rs`
  - SQLite backup loop returns `Deferred` as soon as elapsed time crosses the
    configured max attempt duration

Because each scheduled attempt starts a fresh staged snapshot, the service never
accumulates partial copy progress across runs. Once the live source DB became
large enough that a full backup no longer fits inside 120 seconds, the bounded
`latest.sqlite` surface became permanently stale.

## Operational Meaning

- live swap ingestion still runs
- bounded usable raw coverage does not progress
- Stage 3 cannot recover by waiting alone
- shadow-trading readiness is blocked on this incident

## Immediate Operator Conclusion

Treat this as a production-critical incident on the primary readiness path.

The correct next step is to fix the bounded `recent_raw` snapshot policy/path so
that a fresh `latest.sqlite` can complete and promote again. Waiting for “day 4”
or “day 5” by itself will not resolve the blocker while the snapshot service
keeps returning `attempt_duration_budget_exhausted`.
