# 2026-03-15 Afternoon `a2db7ee` Observed-Swap Backpressure Rollout Report

## Scope

- Deployed `origin/main = a2db7eef4718`
- Runtime hotfix under test:
  - `a2db7ee Restore observed swap writer backpressure`
- Accumulated relevant preconditions already included on `main`:
  - `4df18eb Harden startup sqlite maintenance`
  - `cb35c42 Harden discovery and retention recovery`

## Pre-deploy baseline

Server state before rollout:

- previous live code: `9743a59`
- `solana-copy-bot.service` active
- `copybot-adapter.service` active
- `copybot-executor.service` active
- recovered strategy state already present in DB from earlier manual repair:
  - `followlist.active = 976`
- heads immediately before controlled restart:
  - `observed_swaps_max_ts = 2026-03-15T14:16:54.420562618+00:00`

## Rollout procedure

1. Fast-forwarded `/var/www/solana-copy-bot` to `a2db7ee`.
2. Rebuilt release binaries:
   - `copybot-app`
   - `copybot-adapter`
   - `copybot-executor`
3. Took a logical followlist backup before restart:
   - `/var/www/solana-copy-bot/state/followlist_backup_20260315T1420Z_pre_a2db7ee.sql`
4. Stopped:
   - `solana-copy-bot.service`
   - `copybot-adapter.service`
   - `copybot-executor.service`
5. Ran offline checkpoint before restart:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

Result:

- `0|0|0`
- active followlist check before startup:
  - `976`

6. Started services again:
   - `copybot-executor.service`
   - `copybot-adapter.service`
   - `solana-copy-bot.service`

## Immediate startup evidence

Fresh app process:

- `ActiveEnterTimestamp = 2026-03-15 14:19:27 UTC`
- `MainPID = 290475`
- `NRestarts = 0`

Startup logs:

- config loaded cleanly
- migrations clean:
  - `sqlite migrations applied: 0`
- startup WAL hygiene executed successfully:

```text
startup sqlite wal checkpoint completed
wal_checkpoint_mode=truncate
wal_checkpoint_busy=0
wal_log_frames=0
wal_checkpointed_frames=0
```

Immediate runtime health:

- `active_follow_wallets = 976`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_depth = 0`
- `yellowstone_output_queue_fill_ratio = 0.0`

## Pre-grace soak outcome

Observed from startup through the 30-minute startup grace:

1. The old pre-grace failure shape did not recur.
   - no `heartbeat write failed`
   - no `failed to insert observed swap batch`
   - no pending-writer backlog growth
   - no Yellowstone saturation
2. `observed_swaps` kept advancing continuously:
   - `2026-03-15T14:21:21.284826846+00:00`
   - `2026-03-15T14:28:12.186917023+00:00`
   - `2026-03-15T14:44:09.572979557+00:00`
3. SQLite contention remained flat at zero throughout the pre-grace soak:
   - `sqlite_busy_error_total = 0`
   - `sqlite_write_retry_total = 0`
4. Writer queue telemetry stayed healthy:
   - `observed_swap_writer_pending_requests = 0`
5. Yellowstone queue stayed bounded:
   - one short-lived spike reached `yellowstone_output_queue_depth = 714`
   - corresponding `fill_ratio = 0.3486328125`
   - it self-drained without saturation
   - `ws_notifications_replaced_oldest = 0`
   - later samples returned to `yellowstone_output_queue_depth = 0`

Interpretation:

- restoring commit-backed backpressure on the observed-swap hot path prevented the earlier firehose-to-writer-queue failure mode from recurring in the rollout soak.

## Discovery / followlist state during rollout

Runtime discovery behavior after restart:

- first protected cycle:
  - `scoring_source = persisted_wallet_metrics_truncated_warm_restore`
  - `active_follow_wallets = 976`
  - `eligible_wallets = 358`
  - `follow_promoted = 0`
  - `follow_demoted = 0`
  - `followlist_activations_suppressed = true`
  - `followlist_deactivations_suppressed = true`
- subsequent raw-window cycles:
  - `active_follow_wallets = 976`
  - `eligible_wallets` stabilized at `158`, later `339`
  - `follow_promoted = 0`
  - `follow_demoted = 0`
  - `metrics_written = 0`

Interpretation:

- the recovered active followlist remained intact during the rollout window;
- no repeat of the prior `active_follow_wallets -> 0` collapse was observed.

## Post-grace maintenance outcome

This was the critical branch to validate after the earlier incidents.

After startup grace expired, observed-swap retention ran in bounded mode:

At `2026-03-15 14:49:59 UTC`:

- `maintenance = observed_swap_retention`
- `deleted_observed_swap_rows = 500`
- `deleted_scoring_rows = 0`
- `observed_swap_delete_batches = 1`
- `completed_full_sweep = false`
- `stop_reason = "runtime_pressure"`
- `wal_checkpoint_mode = "skipped_bounded_run"`
- `duration_ms = 1214`

At `2026-03-15 14:51:27 UTC`:

- another bounded retry executed
- `deleted_observed_swap_rows = 0`
- `completed_full_sweep = false`
- `stop_reason = "runtime_pressure"`
- `wal_checkpoint_mode = "skipped_bounded_run"`

Critically, after these post-grace maintenance runs:

- `sqlite_busy_error_total` remained `0`
- `sqlite_write_retry_total` remained `0`
- `observed_swap_writer_pending_requests` remained `0`
- `yellowstone_output_queue_fill_ratio` remained `0.0`
- `observed_swaps` continued advancing:
  - `2026-03-15T14:49:27.496450714+00:00`
  - `2026-03-15T14:51:38.421799376+00:00`

Interpretation:

- the maintenance hardening from `4df18eb` behaved as intended under live load;
- the first post-grace retention runs no longer pushed the process into WAL/queue-pressure degradation.

## Final live state at end of soak

- deployed runtime: `a2db7ee`
- services:
  - `solana-copy-bot.service = active`
  - `copybot-adapter.service = active`
  - `copybot-executor.service = active`
- `NRestarts = 0`
- `followlist.active = 976`
- `observed_swaps_max_ts = 2026-03-15T14:59:30.505283474+00:00`

## Residual notes

1. Cached raw-window discovery summaries still logged `followlist_activations_suppressed = false` while `followlist_deactivations_suppressed = true` on some non-recompute ticks. In this rollout window that did not lead to any actual mutation:
   - `follow_promoted = 0`
   - `follow_demoted = 0`
   - `active_follow_wallets = 976`
   Treat this as a telemetry/cached-summary consistency note, not as evidence of a destructive followlist regression in this rollout.
2. WAL size during runtime telemetry stabilized around `85296392 bytes` rather than exploding into the previous multi-GB failure mode.

## Verdict

This rollout is accepted as successful.

What was confirmed in live:

1. `a2db7ee` restored enough observed-swap writer backpressure to prevent the old pre-grace data-plane collapse from recurring in this rollout window.
2. `4df18eb` maintenance hardening behaved correctly after startup grace:
   - bounded retention slices ran,
   - maintenance stopped early on runtime pressure,
   - no SQLite contention or Yellowstone saturation followed.
3. The recovered followlist state remained intact:
   - `active_follow_wallets = 976`
   - no repeat of the prior followlist-collapse incident.
4. Runtime remained healthy through both:
   - pre-grace ingestion load,
   - first post-grace maintenance runs.

Operational conclusion:

- the preserved Yellowstone ingestion follow-up line is now validated in live runtime behavior;
- the server should remain on `a2db7ee`.
